"""
Reverse Cancellation Modal - REFACTORED v2.0
==============================================
Self-contained modal for reversing cancellations with simplified email notifications.

CHANGES:
- Email service now receives oc_info and actor_info directly
- Removed ocd_id + user_id based queries for email
"""
import streamlit as st
import time

from .allocation_service import AllocationService
from .formatters import format_number, format_date
from .validators import AllocationValidator
from .uom_converter import UOMConverter
from .allocation_email import AllocationEmailService
from ..auth import AuthManager


# Initialize services
allocation_service = AllocationService()
validator = AllocationValidator()
uom_converter = UOMConverter()
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


@st.dialog("Reverse Cancellation", width="medium")
def show_reverse_cancellation_modal():
    """Modal for reversing a cancellation with user validation"""
    cancellation = st.session_state.selections['cancellation_for_reverse']
    
    if not cancellation:
        st.error("No cancellation selected")
        if st.button("Close"):
            st.session_state.modals['reverse'] = False
            st.session_state.selections['cancellation_for_reverse'] = None
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
    
    st.markdown("### Reverse Cancellation")
    
    # Get UOM info
    oc_info = st.session_state.selections.get('oc_info', {})
    standard_uom = oc_info.get('standard_uom', '')
    
    # Show cancellation info
    cancelled_std = cancellation['cancelled_qty']
    
    if uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
        cancelled_sell = uom_converter.convert_quantity(
            cancelled_std, 'standard', 'selling', 
            oc_info.get('uom_conversion', '1')
        )
        selling_uom = oc_info.get('selling_uom', '')
        st.info(
            f"Cancelled Quantity: {format_number(cancelled_std)} {standard_uom} "
            f"(= {format_number(cancelled_sell)} {selling_uom})"
        )
    else:
        st.info(f"Cancelled Quantity: {format_number(cancelled_std)} {standard_uom}")
    
    st.caption(f"Cancelled on: {format_date(cancellation['cancelled_date'])} by {cancellation['cancelled_by']}")
    st.caption(f"Original reason: {cancellation['reason']}")
    
    # Reversal reason
    reversal_reason = st.text_area(
        "Reversal Reason",
        help="Please explain why this cancellation is being reversed (minimum 10 characters)",
        placeholder="Explain why you are reversing this cancellation..."
    )
    
    # Validation
    valid, error = validator.validate_reverse_cancellation(
        cancellation,
        reversal_reason,
        st.session_state.user['role']
    )
    
    if not valid:
        st.error(f"❌ {error}")
    
    # Initialize processing state for this modal
    if 'reverse_processing' not in st.session_state:
        st.session_state.reverse_processing = False
    
    # Action buttons
    col1, col2 = st.columns(2)
    with col1:
        button_disabled = (not valid) or st.session_state.reverse_processing
        
        if st.button("Reverse Cancellation", type="primary", use_container_width=True, disabled=button_disabled):
            st.session_state.reverse_processing = True
            st.session_state._reverse_data = {'reversal_reason': reversal_reason}
            st.rerun()
    
    with col2:
        if st.button("Close", use_container_width=True, disabled=st.session_state.reverse_processing):
            st.session_state.modals['reverse'] = False
            st.session_state.reverse_processing = False
            return_to_history_if_context()
            st.rerun()
    
    # Process reversal when flag is set
    if st.session_state.reverse_processing:
        saved_reason = st.session_state.get('_reverse_data', {}).get('reversal_reason', reversal_reason)
        
        with st.status("Processing reversal...", expanded=True) as status:
            # Step 1: Execute reversal
            status.update(label="💾 Reversing cancellation...", state="running")
            
            result = allocation_service.reverse_cancellation(
                cancellation['cancellation_id'],
                saved_reason,
                user_id
            )
            
            if result['success']:
                cancelled_qty = cancellation.get('cancelled_qty', 0)
                st.write(f"✅ Restored {format_number(cancelled_qty)} {oc_info.get('standard_uom', '')} to allocation")
                
                # Step 2: Send email notification - REFACTORED
                status.update(label="📧 Sending email notification...", state="running")
                
                try:
                    actor_info = get_actor_info()
                    
                    email_success, email_msg = email_service.send_cancellation_reversed_email(
                        oc_info=oc_info,  # Pass oc_info directly
                        actor_info=actor_info,
                        allocation_number=cancellation.get('allocation_number', ''),
                        restored_qty=cancelled_qty,
                        reversal_reason=saved_reason
                    )
                    if email_success:
                        st.write("✅ Email notification sent")
                    else:
                        st.write(f"⚠️ Email not sent: {email_msg}")
                except Exception as email_error:
                    st.write(f"⚠️ Email error: {str(email_error)}")
                
                # Step 3: Complete
                status.update(label="✅ Reversal complete!", state="complete", expanded=False)
                time.sleep(1.5)
                
                # Cleanup
                st.session_state.reverse_processing = False
                st.session_state._reverse_data = None
                st.session_state.modals['reverse'] = False
                st.session_state.selections['cancellation_for_reverse'] = None
                
                return_to_history_if_context()
                st.cache_data.clear()
                st.rerun()
            else:
                error_msg = result.get('error', 'Unknown error')
                status.update(label="❌ Reversal failed", state="error")
                st.error(f"❌ {error_msg}")
                
                st.session_state.reverse_processing = False
                st.session_state._reverse_data = None
                
                if 'session' in error_msg.lower() or 'user' in error_msg.lower():
                    time.sleep(2)
                    auth.logout()
                    st.switch_page("app.py")
                    st.stop()