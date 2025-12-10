"""
Reverse Cancellation Modal - Self-contained modal for reversing cancellations
Extracted from main page for better organization
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
    
    # Action buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Reverse Cancellation", type="primary", disabled=not valid):
            result = allocation_service.reverse_cancellation(
                cancellation['cancellation_id'],
                reversal_reason,
                user_id
            )
            
            if result['success']:
                st.success("✅ Cancellation reversed successfully")
                
                # Send email notification
                try:
                    # Get ocd_id from demand_reference_id field
                    ocd_id = cancellation.get('demand_reference_id') or cancellation.get('ocd_id')
                    email_success, email_msg = email_service.send_cancellation_reversed_email(
                        ocd_id=ocd_id,
                        allocation_number=cancellation.get('allocation_number', ''),
                        restored_qty=cancellation.get('cancelled_qty', 0),
                        reversal_reason=reversal_reason,
                        user_id=user_id
                    )
                    if email_success:
                        st.caption("📧 Email notification sent")
                    else:
                        st.caption(f"⚠️ Email not sent: {email_msg}")
                except Exception as email_error:
                    st.caption(f"⚠️ Email error: {str(email_error)}")
                
                time.sleep(1)
                
                # Close this modal before opening history
                st.session_state.modals['reverse'] = False
                st.session_state.selections['cancellation_for_reverse'] = None
                
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
            st.session_state.modals['reverse'] = False
            return_to_history_if_context()
            st.rerun()