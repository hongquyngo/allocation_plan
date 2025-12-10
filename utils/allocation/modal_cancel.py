"""
Cancel Allocation Modal - Self-contained modal for cancelling allocations
Extracted from main page for better organization
"""
import streamlit as st
import time

from .allocation_service import AllocationService
from .formatters import format_number, format_reason_category
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


@st.dialog("Cancel Allocation", width="medium")
def show_cancel_allocation_modal():
    """Modal for cancelling allocation with user validation"""
    
    # Get allocation from session state
    allocation = st.session_state.selections.get('allocation_for_cancel')
    
    # Early return if no allocation selected
    if not allocation:
        st.error("No allocation selected")
        if st.button("Close"):
            st.session_state.modals['cancel'] = False
            st.session_state.selections['allocation_for_cancel'] = None
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
    
    # Display header
    st.markdown(f"### Cancel Allocation {allocation.get('allocation_number', '')}")
    
    # Get UOM info with multiple fallback options
    oc_info = st.session_state.selections.get('oc_info')
    
    # Extract UOM information with fallbacks
    if oc_info and isinstance(oc_info, dict):
        standard_uom = oc_info.get('standard_uom', 'pcs')
        selling_uom = oc_info.get('selling_uom', 'pcs')
        conversion = oc_info.get('uom_conversion', '1')
    else:
        # Try to get from allocation data itself
        standard_uom = allocation.get('standard_uom', 'pcs')
        selling_uom = allocation.get('selling_uom', 'pcs')
        conversion = allocation.get('uom_conversion', '1')
        
        # If still not available, use defaults
        if not standard_uom:
            standard_uom = 'pcs'
        if not selling_uom:
            selling_uom = standard_uom
        if not conversion:
            conversion = '1'
    
    # Get pending quantity with validation
    pending_qty_std = float(allocation.get('pending_allocated_qty', 0))
    
    # Check if there's anything to cancel
    if pending_qty_std <= 0:
        st.error("❌ Cannot cancel - no pending quantity available")
        if st.button("Close"):
            st.session_state.modals['cancel'] = False
            st.session_state.selections['allocation_for_cancel'] = None
            return_to_history_if_context()
            st.rerun()
        return
    
    # Display pending quantity information
    if uom_converter.needs_conversion(conversion):
        pending_qty_sell = uom_converter.convert_quantity(
            pending_qty_std, 'standard', 'selling', conversion
        )
        st.info(
            f"Pending quantity (not yet delivered): "
            f"{format_number(pending_qty_std)} {standard_uom} "
            f"(= {format_number(pending_qty_sell)} {selling_uom})"
        )
    else:
        st.info(f"Pending quantity (not yet delivered): {format_number(pending_qty_std)} {standard_uom}")
    
    # Show delivered quantity warning if applicable
    delivered_qty = float(allocation.get('delivered_qty', 0))
    if delivered_qty > 0:
        if uom_converter.needs_conversion(conversion):
            delivered_qty_sell = uom_converter.convert_quantity(
                delivered_qty, 'standard', 'selling', conversion
            )
            st.warning(
                f"⚠️ {format_number(delivered_qty)} {standard_uom} "
                f"(= {format_number(delivered_qty_sell)} {selling_uom}) "
                f"already delivered and cannot be cancelled"
            )
        else:
            st.warning(
                f"⚠️ {format_number(delivered_qty)} {standard_uom} "
                f"already delivered and cannot be cancelled"
            )
    
    st.divider()
    
    # Cancel quantity input
    st.markdown(f"**Cancel quantity in {standard_uom} (standard UOM):**")
    
    cancel_qty = st.number_input(
        f"Quantity to Cancel",
        min_value=0.0,
        max_value=float(pending_qty_std),
        value=float(pending_qty_std),
        step=1.0,
        help=f"Maximum cancellable: {format_number(pending_qty_std)} {standard_uom}",
        label_visibility="collapsed"
    )
    
    # Show equivalent in selling UOM if different
    if cancel_qty > 0 and uom_converter.needs_conversion(conversion):
        cancel_qty_sell = uom_converter.convert_quantity(
            cancel_qty, 'standard', 'selling', conversion
        )
        st.caption(f"= {format_number(cancel_qty_sell)} {selling_uom}")
    
    # Reason category selection
    reason_category = st.selectbox(
        "Reason Category",
        options=['CUSTOMER_REQUEST', 'SUPPLY_ISSUE', 'QUALITY_ISSUE', 'BUSINESS_DECISION', 'OTHER'],
        format_func=lambda x: format_reason_category(x)
    )
    
    # Detailed reason input
    reason = st.text_area(
        "Detailed Reason", 
        help="Please provide a detailed reason (minimum 10 characters)",
        placeholder="Explain why this allocation is being cancelled..."
    )
    
    # Validate inputs
    errors = validator.validate_cancel_allocation(
        allocation,
        cancel_qty,
        reason,
        reason_category,
        st.session_state.user.get('role', 'viewer')
    )
    
    # Display validation errors if any
    if errors:
        for error in errors:
            st.error(f"❌ {error}")
    
    st.divider()
    
    # Action buttons
    col1, col2 = st.columns(2)
    
    with col1:
        # Determine if button should be disabled
        button_disabled = len(errors) > 0 or cancel_qty <= 0
        
        if st.button(
            "Cancel Allocation", 
            type="primary", 
            use_container_width=True,
            disabled=button_disabled
        ):
            # Store UOM info before any state changes
            saved_standard_uom = standard_uom
            saved_selling_uom = selling_uom
            saved_conversion = conversion
            saved_cancel_qty = cancel_qty
            
            try:
                # Execute cancellation with validated user_id
                result = allocation_service.cancel_allocation(
                    allocation_detail_id=allocation.get('allocation_detail_id'),
                    cancelled_qty=cancel_qty,
                    reason=reason,
                    reason_category=reason_category,
                    user_id=user_id
                )
                
                if result['success']:
                    # Prepare success message using saved values
                    if uom_converter.needs_conversion(saved_conversion):
                        cancel_qty_sell = uom_converter.convert_quantity(
                            saved_cancel_qty, 'standard', 'selling', saved_conversion
                        )
                        success_msg = (
                            f"✅ Successfully cancelled {format_number(saved_cancel_qty)} {saved_standard_uom} "
                            f"(= {format_number(cancel_qty_sell)} {saved_selling_uom})"
                        )
                    else:
                        success_msg = f"✅ Successfully cancelled {format_number(saved_cancel_qty)} {saved_standard_uom}"
                    
                    # Display success message
                    st.success(success_msg)
                    
                    # Display remaining quantity if any
                    remaining_qty = result.get('remaining_pending_qty', 0)
                    if remaining_qty > 0:
                        if uom_converter.needs_conversion(saved_conversion):
                            remaining_sell = uom_converter.convert_quantity(
                                remaining_qty, 'standard', 'selling', saved_conversion
                            )
                            st.info(
                                f"Remaining pending: {format_number(remaining_qty)} {saved_standard_uom} "
                                f"(= {format_number(remaining_sell)} {saved_selling_uom})"
                            )
                        else:
                            st.info(f"Remaining pending: {format_number(remaining_qty)} {saved_standard_uom}")
                    
                    # Send email notification
                    try:
                        # Get ocd_id from context (stored when opening from history)
                        ocd_id = None
                        if st.session_state.context.get('return_to_history'):
                            ocd_id = st.session_state.context['return_to_history'].get('oc_detail_id')
                        if not ocd_id:
                            ocd_id = st.session_state.selections.get('oc_for_history')
                        
                        if ocd_id:
                            email_success, email_msg = email_service.send_allocation_cancelled_email(
                                ocd_id=ocd_id,
                                allocation_number=allocation.get('allocation_number', ''),
                                cancelled_qty=saved_cancel_qty,
                                reason=reason,
                                reason_category=reason_category,
                                user_id=user_id
                            )
                            if email_success:
                                st.caption("📧 Email notification sent")
                            else:
                                st.caption(f"⚠️ Email not sent: {email_msg}")
                        else:
                            st.caption("⚠️ Email not sent: Missing OC reference")
                    except Exception as email_error:
                        st.caption(f"⚠️ Email error: {str(email_error)}")
                    
                    # Wait briefly for user to see the message
                    time.sleep(1.5)
                    
                    # Clear modal state
                    st.session_state.modals['cancel'] = False
                    st.session_state.selections['allocation_for_cancel'] = None
                    
                    # Return to history if context exists
                    return_to_history_if_context()
                    
                    # Clear cache and refresh
                    st.cache_data.clear()
                    st.rerun()
                    
                else:
                    # Display error message
                    error_msg = result.get('error', 'Unknown error occurred')
                    if 'session' in error_msg.lower() or 'user' in error_msg.lower():
                        st.error(f"⚠️ {error_msg}")
                        time.sleep(2)
                        auth.logout()
                        st.switch_page("app.py")
                        st.stop()
                    else:
                        st.error(f"❌ {error_msg}")
                    
            except Exception as e:
                # Handle unexpected errors
                st.error("❌ An unexpected error occurred. Please try again or contact support.")
    
    with col2:
        if st.button("Close", use_container_width=True):
            # Clear modal state
            st.session_state.modals['cancel'] = False
            st.session_state.selections['allocation_for_cancel'] = None
            
            # Return to history if context exists
            return_to_history_if_context()
            
            st.rerun()