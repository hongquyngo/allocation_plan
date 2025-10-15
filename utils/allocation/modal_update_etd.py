"""
Update ETD Modal - Self-contained modal for updating allocated ETD
Extracted from main page for better organization
"""
import streamlit as st
import pandas as pd
import time
from datetime import datetime

from .allocation_service import AllocationService
from .formatters import format_number, format_date
from .validators import AllocationValidator
from ..auth import AuthManager


# Initialize services
allocation_service = AllocationService()
validator = AllocationValidator()
auth = AuthManager()


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
    
    # Action buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Update ETD", type="primary", disabled=(new_etd == current_etd)):
            result = allocation_service.update_allocation_etd(
                allocation['allocation_detail_id'],
                new_etd,
                user_id
            )
            
            if result['success']:
                st.success("✅ ETD updated successfully")
                if result.get('update_count'):
                    st.caption(f"This is update #{result['update_count']} for this allocation")
                time.sleep(1)
                
                return_to_history_if_context()
                
                st.cache_data.clear()
                st.rerun()
            else:
                error_msg = result.get('error', 'Unknown error')
                if 'session' in error_msg.lower() or 'user' in error_msg.lower():
                    st.error(f"⚠️ {error_msg}")
                    time.sleep(2)
                    auth.logout()
                    st.switch_page("app.py")
                    st.stop()
                else:
                    st.error(f"❌ {error_msg}")
    
    with col2:
        if st.button("Close"):
            st.session_state.modals['update_etd'] = False
            return_to_history_if_context()
            st.rerun()