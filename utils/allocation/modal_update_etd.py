"""
Update ETD Modal - REFACTORED v2.0
===================================
Self-contained modal for updating allocated ETD with simplified email notifications.

CHANGES:
- Email service now receives oc_info and actor_info directly
- Removed ocd_id + user_id based queries for email
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


@st.dialog("Update Allocated ETD", width="medium")
def show_update_etd_modal():
    """Modal for updating allocated ETD with user validation"""
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
            st.session_state.modals['update_etd'] = False
            st.rerun()
        return
    
    # New ETD input
    current_etd = pd.to_datetime(allocation['allocated_etd']).date()
    new_etd = st.date_input(
        "New Allocated ETD",
        value=current_etd
    )
    
    # Show ETD change
    if new_etd != current_etd:
        diff_days = (new_etd - current_etd).days
        if diff_days > 0:
            st.warning(f"⚠️ Delaying by {diff_days} days")
        else:
            st.success(f"✅ Advancing by {abs(diff_days)} days")
    
    # Initialize processing state for this modal
    if 'etd_update_processing' not in st.session_state:
        st.session_state.etd_update_processing = False
    
    # Action buttons
    col1, col2 = st.columns(2)
    with col1:
        button_disabled = (new_etd == current_etd) or st.session_state.etd_update_processing
        
        if st.button("Update ETD", type="primary", disabled=button_disabled, use_container_width=True):
            st.session_state.etd_update_processing = True
            st.rerun()
    
    with col2:
        if st.button("Close", use_container_width=True, disabled=st.session_state.etd_update_processing):
            st.session_state.modals['update_etd'] = False
            st.session_state.etd_update_processing = False
            return_to_history_if_context()
            st.rerun()
    
    # Process update when flag is set
    if st.session_state.etd_update_processing:
        with st.status("Processing...", expanded=True) as status:
            # Step 1: Save to database
            status.update(label="💾 Saving ETD changes...", state="running")
            
            result = allocation_service.update_allocation_etd(
                allocation['allocation_detail_id'],
                new_etd,
                user_id
            )
            
            if result['success']:
                st.write(f"✅ ETD updated: {format_date(current_etd)} → {format_date(new_etd)}")
                if result.get('update_count'):
                    st.write(f"📝 This is update #{result['update_count']} for this allocation")
                
                # Step 2: Send email notification - REFACTORED
                status.update(label="📧 Sending email notification...", state="running")
                
                try:
                    actor_info = get_actor_info()
                    
                    email_success, email_msg = email_service.send_allocation_etd_updated_email(
                        oc_info=oc_info,  # Pass oc_info directly
                        actor_info=actor_info,
                        allocation_number=allocation.get('allocation_number', ''),
                        previous_etd=current_etd,
                        new_etd=new_etd,
                        pending_qty=pending_qty,
                        update_count=result.get('update_count', 1)
                    )
                    if email_success:
                        st.write("✅ Email notification sent")
                    else:
                        st.write(f"⚠️ Email not sent: {email_msg}")
                except Exception as email_error:
                    st.write(f"⚠️ Email error: {str(email_error)}")
                
                # Step 3: Complete
                status.update(label="✅ Update complete!", state="complete", expanded=False)
                time.sleep(1.5)
                
                # Cleanup and close
                st.session_state.etd_update_processing = False
                st.session_state.modals['update_etd'] = False
                st.session_state.selections['allocation_for_update'] = None
                
                return_to_history_if_context()
                
                st.cache_data.clear()
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