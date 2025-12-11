"""
Update ETD Modal - v3.0 (Improved UI/UX)
=========================================
Self-contained modal for updating allocated ETD.

IMPROVEMENTS in v3.0:
- After success: Dialog stays open, Update button disabled
- User must click Close to dismiss
- Clear progress display with st.status()
- Prevents accidental double-updates
"""
import streamlit as st
import pandas as pd
import time
from datetime import datetime

from .allocation_service import AllocationService
from .formatters import format_number, format_date
from .validators import AllocationValidator
from .allocation_email import AllocationEmailService
from ..auth import AuthManager


# Initialize services
allocation_service = AllocationService()
validator = AllocationValidator()
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
    st.session_state.etd_update_processing = False
    st.session_state.etd_update_completed = False
    st.session_state.etd_update_result = None


@st.dialog("Update Allocated ETD", width="medium")
def show_update_etd_modal():
    """Modal for updating allocated ETD with improved UI/UX"""
    allocation = st.session_state.selections['allocation_for_update']
    
    if not allocation:
        st.error("No allocation selected")
        if st.button("Close"):
            st.session_state.modals['update_etd'] = False
            st.session_state.selections['allocation_for_update'] = None
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
    if 'etd_update_processing' not in st.session_state:
        st.session_state.etd_update_processing = False
    if 'etd_update_completed' not in st.session_state:
        st.session_state.etd_update_completed = False
    if 'etd_update_result' not in st.session_state:
        st.session_state.etd_update_result = None
    
    st.markdown(f"### Update ETD for {allocation['allocation_number']}")
    
    st.info(f"Current Allocated ETD: {format_date(allocation['allocated_etd'])}")
    
    # Show pending quantity
    pending_qty = allocation.get('pending_allocated_qty', 0)
    oc_info = st.session_state.selections.get('oc_info', {})
    standard_uom = oc_info.get('standard_uom', '')
    
    st.caption(f"**Pending quantity affected:** {format_number(pending_qty)} {standard_uom}")
    
    # Show delivered quantity if any
    delivered_qty = allocation.get('delivered_qty', 0)
    if delivered_qty > 0:
        st.warning(f"ℹ️ {format_number(delivered_qty)} {standard_uom} already delivered. ETD update will only affect pending quantity.")
    
    # Validate
    valid, error = validator.validate_update_etd(
        allocation,
        allocation['allocated_etd'],
        st.session_state.user['role']
    )
    
    if not valid and error != "Invalid ETD format" and error != "New ETD is the same as current ETD":
        st.error(f"❌ {error}")
        if st.button("Close"):
            reset_modal_state()
            st.session_state.modals['update_etd'] = False
            st.rerun()
        return
    
    # New ETD input - disabled after completion
    current_etd = pd.to_datetime(allocation['allocated_etd']).date()
    new_etd = st.date_input(
        "New Allocated ETD",
        value=current_etd,
        disabled=st.session_state.etd_update_completed
    )
    
    # Show ETD change (only if not completed)
    if not st.session_state.etd_update_completed:
        if new_etd != current_etd:
            diff_days = (new_etd - current_etd).days
            if diff_days > 0:
                st.warning(f"⚠️ Delaying by {diff_days} days")
            else:
                st.success(f"✅ Advancing by {abs(diff_days)} days")
    
    # ============================================================
    # ACTION BUTTONS - Improved state management
    # ============================================================
    col1, col2 = st.columns(2)
    
    with col1:
        # Disable Update button if:
        # - Same date
        # - Processing in progress
        # - Already completed
        update_disabled = (
            new_etd == current_etd or 
            st.session_state.etd_update_processing or 
            st.session_state.etd_update_completed
        )
        
        if st.button(
            "Update ETD", 
            type="primary", 
            disabled=update_disabled, 
            use_container_width=True
        ):
            st.session_state.etd_update_processing = True
            st.rerun()
    
    with col2:
        # Close is always enabled (except during processing)
        if st.button(
            "Close", 
            use_container_width=True, 
            disabled=st.session_state.etd_update_processing
        ):
            reset_modal_state()
            st.session_state.modals['update_etd'] = False
            st.session_state.selections['allocation_for_update'] = None
            return_to_history_if_context()
            st.cache_data.clear()
            st.rerun()
    
    # ============================================================
    # SHOW COMPLETED RESULT (if already done)
    # ============================================================
    if st.session_state.etd_update_completed and st.session_state.etd_update_result:
        result = st.session_state.etd_update_result
        with st.status("✅ Update complete!", state="complete", expanded=False):
            st.write(result.get('message', 'ETD updated successfully'))
            if result.get('email_status'):
                st.write(result['email_status'])
    
    # ============================================================
    # PROCESS UPDATE
    # ============================================================
    if st.session_state.etd_update_processing and not st.session_state.etd_update_completed:
        with st.status("Processing...", expanded=True) as status:
            result_data = {'success': False, 'message': '', 'email_status': ''}
            
            # Step 1: Save to database
            status.update(label="💾 Saving ETD changes...", state="running")
            
            result = allocation_service.update_allocation_etd(
                allocation['allocation_detail_id'],
                new_etd,
                user_id
            )
            
            if result['success']:
                result_data['message'] = f"✅ ETD updated: {format_date(current_etd)} → {format_date(new_etd)}"
                st.write(result_data['message'])
                
                if result.get('update_count'):
                    update_msg = f"📝 This is update #{result['update_count']} for this allocation"
                    st.write(update_msg)
                    result_data['message'] += f"\n{update_msg}"
                
                # Step 2: Send email notification
                status.update(label="📧 Sending email notification...", state="running")
                
                try:
                    actor_info = get_actor_info()
                    
                    email_success, email_msg = email_service.send_allocation_etd_updated_email(
                        oc_info=oc_info,
                        actor_info=actor_info,
                        allocation_number=allocation.get('allocation_number', ''),
                        previous_etd=current_etd,
                        new_etd=new_etd,
                        pending_qty=pending_qty,
                        update_count=result.get('update_count', 1)
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
                status.update(label="✅ Update complete!", state="complete", expanded=False)
                
                result_data['success'] = True
                st.session_state.etd_update_result = result_data
                st.session_state.etd_update_processing = False
                st.session_state.etd_update_completed = True
                
                # Rerun to update button states
                time.sleep(0.5)
                st.rerun()
                
            else:
                error_msg = result.get('error', 'Unknown error')
                status.update(label="❌ Update failed", state="error")
                st.error(f"❌ {error_msg}")
                
                st.session_state.etd_update_processing = False
                
                if 'session' in error_msg.lower() or 'user' in error_msg.lower():
                    time.sleep(2)
                    auth.logout()
                    st.switch_page("app.py")
                    st.stop()