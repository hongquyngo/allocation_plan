"""
Create Allocation Modal - WITH UPDATED COMMITTED TOOLTIP
Modal for creating new allocations with improved committed calculation explanation
"""
import streamlit as st
import time
from datetime import datetime

from .allocation_service import AllocationService
from .supply_data import SupplyData
from .formatters import format_number, format_date
from .validators import AllocationValidator
from .uom_converter import UOMConverter
from .allocation_email import AllocationEmailService
from ..auth import AuthManager


# Initialize services
allocation_service = AllocationService()
supply_data = SupplyData()
validator = AllocationValidator()
uom_converter = UOMConverter()
auth = AuthManager()
email_service = AllocationEmailService()


def show_dual_uom_metric(label: str, 
                         standard_qty: float, standard_uom: str,
                         selling_qty: float, selling_uom: str,
                         conversion_ratio: str):
    """Show metric with both standard and selling UOM"""
    if uom_converter.needs_conversion(conversion_ratio):
        st.metric(label, f"{format_number(standard_qty)} {standard_uom}")
        st.caption(f"= {format_number(selling_qty)} {selling_uom}")
    else:
        st.metric(label, f"{format_number(standard_qty)} {standard_uom}")


def format_supply_info_with_real_time_availability(supply, source_type, oc, current_total_selected):
    """Format supply information with real-time availability considering current selections"""
    if source_type == 'INVENTORY':
        info = f"Batch {supply['batch_number']} - Exp: {format_date(supply['expiry_date'])}"
    elif source_type == 'PENDING_CAN':
        info = f"{supply['arrival_note_number']} - Arr: {format_date(supply['arrival_date'])}"
    elif source_type == 'PENDING_PO':
        etd_str = format_date(supply['etd'])
        eta_str = format_date(supply.get('eta')) if supply.get('eta') else 'N/A'
        info = f"{supply['po_number']} - ETD: {etd_str} | ETA: {eta_str}"
    else:
        info = f"{supply['from_warehouse']} → {supply['to_warehouse']}"
    
    # Get quantities
    total_qty = supply.get('total_quantity', 0)
    committed_qty = supply.get('committed_quantity', 0)
    available_qty = supply.get('available_quantity', 0)
    standard_uom = supply.get('uom', 'pcs')
    
    # Format quantity string with real-time context
    if available_qty <= 0:
        qty_str = f"Total: {format_number(total_qty)} | ❌ Fully committed"
    elif committed_qty > 0:
        qty_str = f"Total: {format_number(total_qty)} | Committed: {format_number(committed_qty)} | ✅ Available: {format_number(available_qty)} {standard_uom}"
    else:
        qty_str = f"✅ Available: {format_number(available_qty)} {standard_uom}"
    
    # Add selling UOM if different
    if available_qty > 0 and uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
        qty_selling = uom_converter.convert_quantity(
            available_qty,
            'standard',
            'selling',
            oc.get('uom_conversion', '1')
        )
        selling_uom = oc.get('selling_uom', 'pcs')
        qty_str += f" (= {format_number(qty_selling)} {selling_uom})"
    
    return f"{info} - {qty_str}"


@st.dialog("Create Allocation", width="large")
def show_allocation_modal():
    """Allocation modal with user validation and improved committed tooltip"""
    oc = st.session_state.selections['oc_for_allocation']
    
    if not oc:
        st.error("No OC selected")
        if st.button("Close"):
            st.session_state.modals['allocation'] = False
            st.session_state.selections['oc_for_allocation'] = None
            st.rerun()
        return
    
    # Validate user session before allowing allocation
    user_id = st.session_state.user.get('id')
    if not user_id:
        st.error("⚠️ Session error. Please login again.")
        time.sleep(2)
        auth.logout()
        st.switch_page("app.py")
        st.stop()
    
    # Header
    st.markdown(f"### Allocate to {oc['oc_number']}")
    
    # Show warning if over-allocated
    if oc.get('is_over_allocated') == 'Yes':
        st.warning(f"⚠️ This OC is already over-allocated! {oc.get('allocation_warning', '')}")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Customer", oc['customer'])
    with col2:
        st.metric("Product", oc['product_name'][:30])
    with col3:
        show_dual_uom_metric(
            "Required",
            oc.get('pending_standard_delivery_quantity', 0),
            oc.get('standard_uom', 'pcs'),
            oc.get('pending_quantity', 0),
            oc.get('selling_uom', 'pcs'),
            oc.get('uom_conversion', '1')
        )
    
    st.divider()
    
    # Get supply summary first
    standard_uom = oc.get('standard_uom', 'pcs')
    supply_summary = supply_data.get_product_supply_summary(oc['product_id'])
    
    # ===== SUPPLY OVERVIEW WITH IMPROVED COMMITTED TOOLTIP =====
    st.markdown("**📊 Supply Overview:**")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Supply",
            f"{format_number(supply_summary['total_supply'])} {standard_uom}",
            help="Total quantity available from all sources"
        )
    
    with col2:
        # IMPROVED TOOLTIP WITH FORMULA
        committed_help = (
            "Already allocated but not yet delivered\n\n"
            "Formula:\n"
            "Committed = Σ MIN(pending_delivery, undelivered_allocated)\n\n"
            "This prevents over-blocking supply when delivery data is incomplete"
        )
        st.metric(
            "Committed",
            f"{format_number(supply_summary['total_committed'])} {standard_uom}",
            help=committed_help
        )
    
    with col3:
        availability_color = "🟢" if supply_summary['coverage_ratio'] > 50 else "🟡" if supply_summary['coverage_ratio'] > 20 else "🔴"
        st.metric(
            f"{availability_color} Available",
            f"{format_number(supply_summary['available'])} {standard_uom}",
            delta=f"{supply_summary['coverage_ratio']:.0f}% of total",
            help="Quantity available for new allocations"
        )
    
    with col4:
        st.metric(
            "Max SOFT Allocation",
            f"{format_number(supply_summary['available'])} {standard_uom}",
            help="Maximum quantity for SOFT allocation"
        )
    
    # Show warnings if needed
    if supply_summary['available'] <= 0:
        st.error("❌ No supply available for allocation. All supply has been committed to other orders.")
    elif supply_summary['coverage_ratio'] < 20:
        st.warning(f"⚠️ Low supply availability! Only {supply_summary['coverage_ratio']:.0f}% available.")
    
    st.divider()
    
    # Get supply details with availability
    supply_details = supply_data.get_supply_with_availability(oc['product_id'])
    
    if supply_details.empty and supply_summary['available'] <= 0:
        st.error("⛔ No supply available for this product")
        st.info("All existing supply has been committed. Please check with procurement team.")
        if st.button("Close", use_container_width=True):
            st.session_state.modals['allocation'] = False
            st.session_state.selections['oc_for_allocation'] = None
            st.rerun()
        return
    
    st.info("ℹ️ **Allocation Rule**: All allocations are made in standard UOM to ensure whole container quantities")
    
    # Supply selection
    st.markdown("**Supply Sources:**")
    
    selected_supplies = []
    total_selected_standard = 0
    
        # Check if supply_details has data before processing
    if supply_details.empty or 'source_type' not in supply_details.columns:
        st.warning("⚠️ No supply sources available for this product")
    else:

        # Group by source type
        for source_type in ['INVENTORY', 'PENDING_CAN', 'PENDING_PO', 'PENDING_WHT']:
            type_supplies = supply_details[supply_details['source_type'] == source_type]
            
            if not type_supplies.empty:
                source_label = {
                    'INVENTORY': '📦 Inventory',
                    'PENDING_CAN': '🚢 Pending CAN',
                    'PENDING_PO': '📋 Pending PO',
                    'PENDING_WHT': '🚚 WH Transfer'
                }.get(source_type, source_type)
                
                st.markdown(f"**{source_label}**")
                
                for idx, supply in type_supplies.iterrows():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        # Format supply info with real-time availability
                        info = format_supply_info_with_real_time_availability(
                            supply, source_type, oc, total_selected_standard
                        )
                        
                        # Check availability
                        is_available = supply.get('available_quantity', 0) > 0
                        
                        # Check if selecting this would exceed total available
                        would_exceed_supply = False
                        if is_available:
                            max_selectable = min(
                                supply.get('available_quantity', 0),
                                supply_summary['available'] - total_selected_standard
                            )
                            would_exceed_supply = max_selectable <= 0
                        
                        # Set appropriate help text
                        if not is_available:
                            help_text = "No quantity available - fully committed"
                        elif would_exceed_supply:
                            help_text = "⚠️ Cannot select - would exceed total available supply"
                        else:
                            help_text = None
                        
                        selected = st.checkbox(
                            info,
                            key=f"supply_{supply['source_id']}_{source_type}",
                            disabled=(not is_available or would_exceed_supply),
                            help=help_text
                        )
                    
                    with col2:
                        if selected and is_available and not would_exceed_supply:
                            # Calculate remaining requirement
                            pending_standard = oc.get('pending_standard_delivery_quantity', oc['pending_quantity'])
                            available_qty = supply.get('available_quantity', 0)
                            remaining_supply_cap = supply_summary['available'] - total_selected_standard
                            max_qty_standard = min(
                                available_qty, 
                                pending_standard - total_selected_standard,
                                remaining_supply_cap
                            )
                            
                            # Show equivalent selling quantity
                            if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
                                max_qty_selling = uom_converter.convert_quantity(
                                    max_qty_standard,
                                    'standard',
                                    'selling',
                                    oc.get('uom_conversion', '1')
                                )
                                help_text = f"Max: {format_number(max_qty_standard)} {standard_uom} (= {format_number(max_qty_selling)} {oc.get('selling_uom', 'pcs')})"
                            else:
                                help_text = f"Max: {format_number(max_qty_standard)} {standard_uom}"
                            
                            qty_standard = st.number_input(
                                f"Qty ({standard_uom})",
                                min_value=0.0,
                                max_value=float(max_qty_standard),
                                value=float(max_qty_standard),
                                step=1.0,
                                key=f"qty_{supply['source_id']}_{source_type}",
                                help=help_text
                            )
                            
                            if qty_standard > 0:
                                selected_supplies.append({
                                    'source_type': source_type,
                                    'source_id': supply['source_id'],
                                    'quantity': qty_standard,
                                    'supply_info': supply.to_dict()
                                })
                                total_selected_standard += qty_standard
    
    st.divider()
    
    # SOFT allocation option
    st.markdown("**OR**")
    use_soft = st.checkbox("🔄 SOFT Allocation (no specific source)")
    
    if use_soft:
        pending_standard = oc.get('pending_standard_delivery_quantity', oc['pending_quantity'])
        
        # Check available supply for SOFT allocation
        max_soft_qty = min(
            pending_standard,
            supply_summary.get('available', 0)
        )
        
        if max_soft_qty <= 0:
            st.error("❌ Cannot create SOFT allocation - no supply available")
        else:
            if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
                pending_selling = oc.get('pending_quantity', pending_standard)
                selling_uom = oc.get('selling_uom', 'pcs')
                max_soft_qty_selling = uom_converter.convert_quantity(
                    max_soft_qty,
                    'standard',
                    'selling',
                    oc.get('uom_conversion', '1')
                )
                help_text = f"Max: {format_number(max_soft_qty)} {standard_uom} (= {format_number(max_soft_qty_selling)} {selling_uom})"
            else:
                help_text = f"Max: {format_number(max_soft_qty)} {standard_uom}"
            
            # Show warning if supply is limited
            if max_soft_qty < pending_standard:
                st.warning(
                    f"⚠️ Available supply ({format_number(supply_summary['available'])} {standard_uom}) "
                    f"is less than pending quantity ({format_number(pending_standard)} {standard_uom})"
                )
            
            st.caption(f"Allocate quantity in {standard_uom} (standard UOM)")
            soft_qty_standard = st.number_input(
                "Quantity",
                min_value=0.0,
                max_value=float(max_soft_qty),
                value=0.0,
                step=1.0,
                help=help_text,
                disabled=max_soft_qty <= 0
            )
            
            if soft_qty_standard > 0:
                selected_supplies = [{
                    'source_type': None,
                    'source_id': None,
                    'quantity': soft_qty_standard,
                    'supply_info': {'type': 'SOFT', 'description': 'No specific source'}
                }]
                total_selected_standard = soft_qty_standard
    
    st.divider()
    
    # Summary section
    col1, col2 = st.columns(2)
    
    with col1:
        if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
            total_selected_selling = uom_converter.convert_quantity(
                total_selected_standard,
                'standard',
                'selling',
                oc.get('uom_conversion', '1')
            )
            selling_uom = oc.get('selling_uom', 'pcs')
            
            st.metric(
                "Total Selected", 
                f"{format_number(total_selected_standard)} {standard_uom}"
            )
            st.caption(f"= {format_number(total_selected_selling)} {selling_uom}")
        else:
            st.metric(
                "Total Selected", 
                f"{format_number(total_selected_standard)} {standard_uom}"
            )
    
    with col2:
        pending_standard = oc.get('pending_standard_delivery_quantity', oc['pending_quantity'])
        coverage = (total_selected_standard / pending_standard * 100) if pending_standard > 0 else 0
        st.metric("Coverage", f"{coverage:.1f}%")
    
    # Supply availability check
    if total_selected_standard > 0 and total_selected_standard > supply_summary.get('available', 0):
        st.error(
            f"❌ Total allocation ({format_number(total_selected_standard)} {standard_uom}) "
            f"exceeds available supply ({format_number(supply_summary['available'])} {standard_uom})"
        )
    
    # Over-allocation warning (OC level)
    effective_qty_standard = oc.get('standard_quantity', pending_standard)
    current_effective_allocated = oc.get('total_effective_allocated_qty_standard', 0)
    new_total_effective = current_effective_allocated + total_selected_standard
    
    if new_total_effective > effective_qty_standard:
        over_qty_standard = new_total_effective - effective_qty_standard
        over_pct = (over_qty_standard / effective_qty_standard * 100) if effective_qty_standard > 0 else 0
        max_allowed = effective_qty_standard  # 100% limit
        
        if new_total_effective > max_allowed:
            if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
                over_qty_selling = uom_converter.convert_quantity(
                    over_qty_standard,
                    'standard',
                    'selling',
                    oc.get('uom_conversion', '1')
                )
                st.error(
                    f"⚡ Would exceed OC limit by {format_number(over_qty_standard)} {standard_uom} "
                    f"(= {format_number(over_qty_selling)} {oc.get('selling_uom')}) - "
                    f"{over_pct:.1f}% over! Maximum allowed is 100% of OC quantity."
                )
            else:
                st.error(
                    f"⚡ Would exceed OC limit by {format_number(over_qty_standard)} {standard_uom} "
                    f"({over_pct:.1f}% over)! Maximum allowed is 100% of OC quantity."
                )
    
    # Additional fields
    allocated_etd = st.date_input("Allocated ETD", value=oc['etd'])

    # Validate allocated ETD against PO ETAs
    if not use_soft and selected_supplies:
        max_eta = None
        po_with_max_eta = None
        
        for supply_item in selected_supplies:
            if supply_item['source_type'] == 'PENDING_PO':
                supply_info = supply_item['supply_info']
                if supply_info.get('eta'):
                    try:
                        import pandas as pd
                        eta_date = pd.to_datetime(supply_info['eta']).date()
                        if max_eta is None or eta_date > max_eta:
                            max_eta = eta_date
                            po_with_max_eta = supply_info.get('po_number', 'PO')
                    except:
                        pass
        
        if max_eta and allocated_etd < max_eta:
            st.warning(
                f"⚠️ Allocated ETD ({format_date(allocated_etd)}) is earlier than the ETA of {po_with_max_eta} ({format_date(max_eta)}). "
                "The goods won't arrive until the ETA date. Consider adjusting the allocated ETD to be on or after the ETA."
            )

    notes = st.text_area("Notes (optional)")
    
    # Action buttons
    col1, col2 = st.columns(2)
    
    with col1:
        # Determine if save button should be enabled
        can_save = (
            total_selected_standard > 0 and 
            total_selected_standard <= supply_summary.get('available', float('inf'))
        )
        
        # Create helpful button text
        if not can_save and total_selected_standard == 0:
            button_help = "Please select at least one supply source or enter SOFT allocation quantity"
        elif not can_save and total_selected_standard > supply_summary.get('available', 0):
            button_help = f"Total selection exceeds available supply by {format_number(total_selected_standard - supply_summary['available'])} {standard_uom}"
        else:
            button_help = "Click to save allocation"
        
        if st.button(
            "💾 Save Allocation", 
            type="primary", 
            use_container_width=True, 
            disabled=not can_save,
            help=button_help
        ):
            # Validate user session again before saving
            user_id = st.session_state.user.get('id')
            if not user_id:
                st.error("⚠️ Session error. Please login again.")
                time.sleep(2)
                auth.logout()
                st.switch_page("app.py")
                st.stop()
            
            # Validate allocation
            errors = validator.validate_create_allocation(
                selected_supplies,
                oc,
                'SOFT' if use_soft else 'HARD',
                st.session_state.user['role']
            )
            
            if errors:
                for error in errors:
                    st.error(f"❌ {error}")
            else:
                # Save allocation with validated user_id
                result = allocation_service.create_allocation(
                    oc_detail_id=oc['ocd_id'],
                    allocations=selected_supplies,
                    mode='SOFT' if use_soft else 'HARD',
                    etd=allocated_etd,
                    notes=notes,
                    user_id=user_id
                )
                
                if result['success']:
                    success_msg = f"✅ Allocation Successful\nAllocated: {format_number(total_selected_standard)} {standard_uom}"
                    
                    if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
                        total_selected_selling = uom_converter.convert_quantity(
                            total_selected_standard,
                            'standard',
                            'selling',
                            oc.get('uom_conversion', '1')
                        )
                        selling_uom = oc.get('selling_uom', 'pcs')
                        success_msg += f" (= {format_number(total_selected_selling)} {selling_uom})"
                    
                    success_msg += f" to {oc['oc_number']}\nAllocation Number: {result['allocation_number']}"
                    success_msg += f"\nCreated by: {st.session_state.user.get('full_name', 'Unknown')}"
                    
                    st.success(success_msg)
                    st.balloons()
                    
                    # Send email notification
                    try:
                        email_success, email_msg = email_service.send_allocation_created_email(
                            ocd_id=oc['ocd_id'],
                            allocations=selected_supplies,
                            total_qty=total_selected_standard,
                            mode='SOFT' if use_soft else 'HARD',
                            etd=allocated_etd,
                            user_id=user_id,
                            allocation_number=result['allocation_number']
                        )
                        if email_success:
                            st.caption("📧 Email notification sent")
                        else:
                            st.caption(f"⚠️ Email not sent: {email_msg}")
                    except Exception as email_error:
                        st.caption(f"⚠️ Email error: {str(email_error)}")
                    
                    time.sleep(2)
                    st.session_state.modals['allocation'] = False
                    st.session_state.selections['oc_for_allocation'] = None
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
        if st.button("Cancel", use_container_width=True):
            st.session_state.modals['allocation'] = False
            st.session_state.selections['oc_for_allocation'] = None
            st.rerun()