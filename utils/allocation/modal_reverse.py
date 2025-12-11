"""
Reverse Cancellation Modal - v3.0 (Improved UI/UX)
===================================================
Self-contained modal for reversing allocation cancellations.

IMPROVEMENTS in v3.0:
- After success: Dialog stays open, Reverse button disabled
- User must click Close to dismiss
- Clear progress display with st.status()
- Prevents accidental double-reversals
"""
import streamlit as st
import time
from datetime import datetime

from .allocation_service import AllocationService
from .formatters import format_number, format_date
from .allocation_email import AllocationEmailService
from ..auth import AuthManager


# Initialize services
allocation_service = AllocationService()
auth = AuthManager()
email_service = AllocationEmailService()


def get_actor_info() -> dict:
    """Get current user info for email notifications"""
    return {
        'email': st.session_state.user.get('email', ''),
        'name': st.session_state.user.get('full_name', st.session_state.user.get('username', 'Unknown'))
    }


def return_to_history_if_context():
    """Return to history modal if context exists"""
    if st.session_state.context.get('return_to_history'):
        st.session_state.modals['history'] = True
        st.session_state.selections['oc_for_history'] = st.session_state.context['return_to_history']['oc_detail_id']
        st.session_state.selections['oc_info'] = st.session_state.context['return_to_history']['oc_info']
        st.session_state.context['return_to_history'] = None


def reset_modal_state():
    """Reset all modal-specific state"""
    st.session_state.reverse_processing = False
    st.session_state.reverse_completed = False
    st.session_state.reverse_result = None
    st.session_state._reverse_data = None


@st.dialog("Reverse Cancellation", width="medium")
def show_reverse_modal():
    """Modal for reversing cancellation with improved UI/UX"""
    cancellation = st.session_state.selections.get('cancellation_for_reverse')
    
    if not cancellation:
        st.error("No cancellation selected")
        if st.button("Close"):
            st.session_state.modals['reverse'] = False
            st.rerun()
        return
    
    # Validate user session
    user_id = st.session_state.user.get('id')
    if not user_id:
        st.error("⚠️ Session error. Please login again.")
        time.sleep(2)
        auth.logout()
        st.switch_page("app.py")
        st.stop()
    
    # Initialize modal state
    if 'reverse_processing' not in st.session_state:
        st.session_state.reverse_processing = False
    if 'reverse_completed' not in st.session_state:
        st.session_state.reverse_completed = False
    if 'reverse_result' not in st.session_state:
        st.session_state.reverse_result = None
    
    # Get OC info
    oc_info = st.session_state.selections.get('oc_info', {})
    standard_uom = oc_info.get('standard_uom', '')
    
    # Header
    st.markdown(f"### Reverse Cancellation")
    st.markdown(f"**Allocation:** {cancellation.get('allocation_number', 'N/A')}")
    
    # Show cancellation details
    st.divider()
    
    cancelled_qty = cancellation.get('cancelled_qty', 0)
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Cancelled Qty", f"{format_number(cancelled_qty)} {standard_uom}")
    with col2:
        st.metric("Cancelled At", format_date(cancellation.get('cancelled_at')))
    
    # Show original cancellation reason
    st.markdown("**Original Cancellation Reason:**")
    original_reason = cancellation.get('cancellation_reason', 'N/A')
    st.info(f"📝 {original_reason}")
    
    st.divider()
    
    # ============================================================
    # INPUT SECTION - Disabled after completion
    # ============================================================
    is_completed = st.session_state.reverse_completed
    
    st.markdown("**Reversal Details:**")
    
    reason = st.text_area(
        "Reason for Reversal",
        placeholder="Please explain why this cancellation should be reversed...",
        max_chars=500,
        disabled=is_completed
    )
    
    # Validation
    is_valid = len(reason.strip()) >= 10 if reason else False
    
    if reason and not is_valid:
        st.warning("⚠️ Please provide a reason with at least 10 characters")
    
    # Show what will happen
    if not is_completed:
        st.info(f"ℹ️ This will restore {format_number(cancelled_qty)} {standard_uom} back to the allocation.")
    
    # ============================================================
    # ACTION BUTTONS
    # ============================================================
    col1, col2 = st.columns(2)
    
    with col1:
        # Disable Reverse button if:
        # - Validation failed
        # - Processing in progress
        # - Already completed
        reverse_disabled = (
            not is_valid or 
            st.session_state.reverse_processing or 
            st.session_state.reverse_completed
        )
        
        if st.button(
            "🔄 Confirm Reverse", 
            type="primary", 
            disabled=reverse_disabled, 
            use_container_width=True
        ):
            # Save data before processing
            st.session_state._reverse_data = {
                'reason': reason
            }
            st.session_state.reverse_processing = True
            st.rerun()
    
    with col2:
        if st.button(
            "Close", 
            use_container_width=True, 
            disabled=st.session_state.reverse_processing
        ):
            reset_modal_state()
            st.session_state.modals['reverse'] = False
            st.session_state.selections['cancellation_for_reverse'] = None
            return_to_history_if_context()
            st.cache_data.clear()
            st.rerun()
    
    # ============================================================
    # SHOW COMPLETED RESULT (if already done)
    # ============================================================
    if st.session_state.reverse_completed and st.session_state.reverse_result:
        result = st.session_state.reverse_result
        with st.status("✅ Reversal complete!", state="complete", expanded=False):
            st.write(result.get('message', 'Reversal successful'))
            if result.get('email_status'):
                st.write(result['email_status'])
    
    # ============================================================
    # PROCESS REVERSAL
    # ============================================================
    if st.session_state.reverse_processing and not st.session_state.reverse_completed:
        saved_data = st.session_state.get('_reverse_data', {})
        saved_reason = saved_data.get('reason', reason)
        
        with st.status("Processing reversal...", expanded=True) as status:
            result_data = {'success': False, 'message': '', 'email_status': ''}
            
            try:
                # Step 1: Execute reversal
                status.update(label="💾 Reversing cancellation...", state="running")
                
                result = allocation_service.reverse_cancellation(
                    cancellation_id=cancellation.get('cancellation_id'),
                    reversed_by=user_id,
                    reversal_reason=saved_reason
                )
                
                if result['success']:
                    # Build success message
                    result_data['message'] = f"✅ Restored {format_number(cancelled_qty)} {standard_uom} to allocation"
                    st.write(result_data['message'])
                    
                    # Step 2: Send email notification
                    status.update(label="📧 Sending email notification...", state="running")
                    
                    try:
                        actor_info = get_actor_info()
                        
                        email_success, email_msg = email_service.send_cancellation_reversed_email(
                            oc_info=oc_info,
                            actor_info=actor_info,
                            allocation_number=cancellation.get('allocation_number', ''),
                            restored_qty=cancelled_qty,
                            reversal_reason=saved_reason
                        )
                        
                        if email_success:
                            result_data['email_status'] = "✅ Email notification sent"
                        else:
                            result_data['email_status'] = f"⚠️ Email not sent: {email_msg}"
                        st.write(result_data['email_status'])
                        
                    except Exception as email_error:
                        result_data['email_status'] = f"⚠️ Email error: {str(email_error)}"
                        st.write(result_data['email_status'])
                    
                    # Step 3: Mark as complete (DO NOT auto-close)
                    status.update(label="✅ Reversal complete!", state="complete", expanded=False)
                    
                    result_data['success'] = True
                    st.session_state.reverse_result = result_data
                    st.session_state.reverse_processing = False
                    st.session_state.reverse_completed = True
                    
                    # Rerun to update button states
                    time.sleep(0.5)
                    st.rerun()
                    
                else:
                    error_msg = result.get('error', 'Unknown error occurred')
                    status.update(label="❌ Reversal failed", state="error")
                    st.error(f"❌ {error_msg}")
                    
                    st.session_state.reverse_processing = False
                    st.session_state._reverse_data = None
                    
                    if 'session' in error_msg.lower() or 'user' in error_msg.lower():
                        time.sleep(2)
                        auth.logout()
                        st.switch_page("app.py")
                        st.stop()
                        
            except Exception as e:
                status.update(label="❌ Error", state="error")
                st.error("❌ An unexpected error occurred. Please try again or contact support.")
                st.session_state.reverse_processing = False
                st.session_state._reverse_data = None