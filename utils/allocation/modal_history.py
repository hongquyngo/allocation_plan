"""
Allocation History Modal - Self-contained modal for viewing allocation history
Extracted from main page for better organization
"""
import streamlit as st
import pandas as pd

from .allocation_data import AllocationData
from .formatters import format_number, format_date, format_allocation_mode, format_reason_category, format_percentage
from .validators import AllocationValidator
from .uom_converter import UOMConverter


# Initialize services
allocation_data = AllocationData()
validator = AllocationValidator()
uom_converter = UOMConverter()


def create_allocation_tooltip(alloc, oc_info) -> str:
    """Create unified tooltip for allocation details"""
    tooltip_lines = []
    
    def get_value(obj, key, default=0):
        try:
            if isinstance(obj, pd.Series):
                if key in obj.index:
                    value = obj[key]
                    if hasattr(value, 'item'):
                        return value.item()
                    elif isinstance(value, pd.Series):
                        return value.iloc[0] if len(value) > 0 else default
                    return value
                else:
                    return default
            else:
                return obj.get(key, default)
        except Exception:
            return default
    
    allocated_qty = float(get_value(alloc, 'allocated_qty', 0))
    cancelled_qty = float(get_value(alloc, 'cancelled_qty', 0))
    effective_qty = float(get_value(alloc, 'effective_qty', 0))
    delivered_qty = float(get_value(alloc, 'delivered_qty', 0))
    pending_qty = float(get_value(alloc, 'pending_qty', 0))
    standard_uom = str(get_value(oc_info, 'standard_uom', ''))
    
    tooltip_lines.append(f"📦 Allocation {get_value(alloc, 'allocation_number', '')}")
    tooltip_lines.append("")
    
    tooltip_lines.append(f"• Allocated Quantity: {format_number(allocated_qty)} {standard_uom}")
    if cancelled_qty > 0:
        tooltip_lines.append(f"• Cancelled: {format_number(cancelled_qty)} {standard_uom}")
    tooltip_lines.append(f"• Effective: {format_number(effective_qty)} {standard_uom}")
    if delivered_qty > 0:
        tooltip_lines.append(f"• Delivered: {format_number(delivered_qty)} {standard_uom}")
    tooltip_lines.append(f"• Pending: {format_number(pending_qty)} {standard_uom}")
    
    tooltip_lines.append("")
    tooltip_lines.append(f"• Created: {format_date(get_value(alloc, 'allocation_date'))}")
    tooltip_lines.append(f"• By: {str(get_value(alloc, 'created_by', ''))}")
    tooltip_lines.append(f"• Mode: {format_allocation_mode(get_value(alloc, 'allocation_mode', ''))}")
    
    if get_value(alloc, 'supply_source_type'):
        tooltip_lines.append(f"• Source: {str(get_value(alloc, 'supply_source_type', ''))}")
    
    return "\n".join(tooltip_lines)


def show_allocation_summary_metrics(oc_info):
    """Show summary metrics with correct coverage calculation"""
    metrics_cols = st.columns(3)
    
    with metrics_cols[0]:
        standard_qty = oc_info.get('pending_standard_delivery_quantity', 0)
        standard_uom = oc_info.get('standard_uom', '')
        selling_qty = oc_info['pending_quantity']
        selling_uom = oc_info.get('selling_uom', '')
        
        st.metric("Pending Qty", f"{format_number(standard_qty)} {standard_uom}")
        
        if uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
            st.caption(f"= {format_number(selling_qty)} {selling_uom}")
    
    with metrics_cols[1]:
        history_df = allocation_data.get_allocation_history_with_details(st.session_state.selections['oc_for_history'])
        
        if not history_df.empty:
            undelivered_allocated = history_df['pending_qty'].sum()
            
            if uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
                undelivered_selling = uom_converter.convert_quantity(
                    undelivered_allocated,
                    'standard',
                    'selling',
                    oc_info.get('uom_conversion', '1')
                )
                st.metric("Undelivered Allocated", f"{format_number(undelivered_allocated)} {standard_uom}")
                st.caption(f"= {format_number(undelivered_selling)} {selling_uom}")
            else:
                st.metric("Undelivered Allocated", f"{format_number(undelivered_allocated)} {standard_uom}")
        else:
            st.metric("Undelivered Allocated", f"0 {standard_uom}")
    
    with metrics_cols[2]:
        if not history_df.empty:
            pending_standard = oc_info.get('pending_standard_delivery_quantity', 0)
            undelivered_allocated = history_df['pending_qty'].sum()
            
            coverage = (undelivered_allocated / pending_standard * 100) if pending_standard > 0 else 0
            st.metric("Coverage", format_percentage(coverage))
            
            if coverage > 100:
                st.caption("⚡ Over-allocated - Review & cancel excess")
            elif coverage == 100:
                st.caption("✅ Fully covered")
            elif coverage >= 80:
                st.caption("🟡 Nearly covered - Allocate remaining")
            elif coverage > 0:
                st.caption("🔴 Partially covered - Need more allocation")
            else:
                st.caption("⚫ Not allocated - Urgent action needed")
        else:
            st.metric("Coverage", "0%")
            st.caption("⚫ Not allocated - Urgent action needed")


def show_allocation_header_with_tooltip(alloc, oc_info):
    """Show allocation header with tooltip"""
    status_color = {
        'ALLOCATED': '🟢',
        'DRAFT': '🟡',
        'CANCELLED': '🔴'
    }.get(alloc['status'], '⚪')
    
    tooltip = create_allocation_tooltip(alloc, oc_info)
    
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.markdown(
            f"{status_color} **{alloc['allocation_number']}**",
            help=tooltip
        )
    with col2:
        st.caption(f"Mode: {format_allocation_mode(alloc['allocation_mode'])}")
    with col3:
        st.caption(f"Status: {alloc['status']}")


def show_allocation_quantities_dual_uom(alloc, oc_info):
    """Show allocation quantities with delivery data"""
    detail_cols = st.columns([1, 1, 1, 1])
    
    standard_uom = oc_info.get('standard_uom', '')
    selling_uom = oc_info.get('selling_uom', '')
    conversion = oc_info.get('uom_conversion', '1')
    needs_conversion = uom_converter.needs_conversion(conversion)
    
    with detail_cols[0]:
        allocated_std = alloc.get('allocated_qty', 0) or 0
        if needs_conversion:
            allocated_sell = uom_converter.convert_quantity(
                allocated_std, 'standard', 'selling', conversion
            )
            st.metric("Allocated", f"{format_number(allocated_std)} {standard_uom}")
            st.caption(f"= {format_number(allocated_sell)} {selling_uom}")
        else:
            st.metric("Allocated", f"{format_number(allocated_std)} {standard_uom}")
    
    with detail_cols[1]:
        effective_std = alloc.get('effective_qty', 0) or 0
        if needs_conversion:
            effective_sell = uom_converter.convert_quantity(
                effective_std, 'standard', 'selling', conversion
            )
            st.metric("Effective", f"{format_number(effective_std)} {standard_uom}")
            st.caption(f"= {format_number(effective_sell)} {selling_uom}")
        else:
            st.metric("Effective", f"{format_number(effective_std)} {standard_uom}")
    
    with detail_cols[2]:
        delivered_std = alloc.get('delivered_qty', 0) or 0
        if needs_conversion:
            delivered_sell = uom_converter.convert_quantity(
                delivered_std, 'standard', 'selling', conversion
            )
            st.metric("Delivered", f"{format_number(delivered_std)} {standard_uom}")
            st.caption(f"= {format_number(delivered_sell)} {selling_uom}")
        else:
            st.metric("Delivered", f"{format_number(delivered_std)} {standard_uom}")
    
    with detail_cols[3]:
        cancelled_std = alloc.get('cancelled_qty', 0) or 0
        if needs_conversion:
            cancelled_sell = uom_converter.convert_quantity(
                cancelled_std, 'standard', 'selling', conversion
            )
            st.metric("Cancelled", f"{format_number(cancelled_std)} {standard_uom}")
            st.caption(f"= {format_number(cancelled_sell)} {selling_uom}")
        else:
            st.metric("Cancelled", f"{format_number(cancelled_std)} {standard_uom}")


def show_allocation_info(alloc):
    """Show allocation additional info"""
    info_cols = st.columns([1, 1, 1])
    
    with info_cols[0]:
        st.caption(f"📅 **Date:** {format_date(alloc['allocation_date'])}")
    
    with info_cols[1]:
        st.caption(f"📅 **Allocated ETD:** {format_date(alloc['allocated_etd'])}")
    
    with info_cols[2]:
        st.caption(f"👤 **Created by:** {alloc['created_by']}")
    
    st.caption(f"📦 **Source:** {alloc['supply_source_type'] or 'No specific source (SOFT)'}")
    
    if alloc.get('notes'):
        st.caption(f"📝 **Notes:** {alloc['notes']}")
    
    if alloc.get('cancellation_info'):
        st.warning(f"❌ {alloc['cancellation_info']}")


def get_allocation_actions_availability(allocation_detail):
    """Determine available actions based on allocation_delivery_links data"""
    allocated_qty = allocation_detail.get('allocated_qty', 0)
    cancelled_qty = allocation_detail.get('cancelled_qty', 0)
    delivered_qty = allocation_detail.get('delivered_qty', 0)
    
    pending_qty = allocation_detail.get('pending_qty', allocated_qty - cancelled_qty - delivered_qty)
    
    return {
        'can_update_etd': (
            pending_qty > 0 and
            allocation_detail.get('status') == 'ALLOCATED'
        ),
        'can_cancel': (
            pending_qty > 0 and
            allocation_detail.get('status') == 'ALLOCATED'
        ),
        'pending_qty': pending_qty,
        'max_cancellable_qty': pending_qty
    }


def show_allocation_actions(alloc, oc_info):
    """Show action buttons for allocation"""
    if alloc['status'] != 'ALLOCATED':
        return
    
    actions_availability = get_allocation_actions_availability(alloc)
    
    action_cols = st.columns([1, 1, 2])
    
    # Update ETD button
    with action_cols[0]:
        can_update_permission = validator.check_permission(st.session_state.user['role'], 'update')
        can_update = actions_availability['can_update_etd'] and can_update_permission
        
        if can_update:
            if st.button("📅 Update ETD", key=f"update_etd_{alloc['allocation_detail_id']}"):
                st.session_state.context['return_to_history'] = {
                    'oc_detail_id': st.session_state.selections['oc_for_history'],
                    'oc_info': st.session_state.selections['oc_info']
                }
                
                st.session_state.modals['history'] = False
                
                alloc_data = alloc.to_dict() if hasattr(alloc, 'to_dict') else dict(alloc)
                alloc_data['pending_allocated_qty'] = actions_availability['pending_qty']
                
                st.session_state.modals['update_etd'] = True
                st.session_state.selections['allocation_for_update'] = alloc_data
                st.rerun()
        else:
            if not can_update_permission:
                help_text = "No permission to update ETD"
            elif actions_availability['pending_qty'] <= 0:
                help_text = "Cannot update ETD - all quantity has been delivered"
            else:
                help_text = "Cannot update ETD"
                
            st.button(
                "📅 Update ETD", 
                key=f"update_etd_{alloc['allocation_detail_id']}_disabled", 
                disabled=True, 
                help=help_text
            )
    
    # Cancel button
    with action_cols[1]:
        can_cancel_permission = validator.check_permission(st.session_state.user['role'], 'cancel')
        can_cancel = actions_availability['can_cancel'] and can_cancel_permission
        
        if can_cancel:
            if st.button("❌ Cancel", key=f"cancel_{alloc['allocation_detail_id']}"):
                st.session_state.context['return_to_history'] = {
                    'oc_detail_id': st.session_state.selections['oc_for_history'],
                    'oc_info': st.session_state.selections['oc_info']
                }
                
                st.session_state.modals['history'] = False
                
                alloc_data = alloc.to_dict() if hasattr(alloc, 'to_dict') else dict(alloc)
                alloc_data['pending_allocated_qty'] = actions_availability['pending_qty']
                alloc_data['max_cancellable_qty'] = actions_availability['max_cancellable_qty']
                
                st.session_state.modals['cancel'] = True
                st.session_state.selections['allocation_for_cancel'] = alloc_data
                st.rerun()
        else:
            if not can_cancel_permission:
                help_text = "No permission to cancel allocation"
            elif actions_availability['pending_qty'] <= 0:
                help_text = "Cannot cancel - all quantity has been delivered"
            else:
                help_text = "Cannot cancel allocation"
                
            st.button(
                "❌ Cancel", 
                key=f"cancel_{alloc['allocation_detail_id']}_disabled", 
                disabled=True, 
                help=help_text
            )


def show_cancellation_history_dual_uom(alloc, oc_info):
    """Show cancellation history"""
    with st.expander("View Cancellation History"):
        cancellations = allocation_data.get_cancellation_history(alloc['allocation_detail_id'])
        for _, cancel in cancellations.iterrows():
            cancel_cols = st.columns([2, 1, 1, 1])
            
            with cancel_cols[0]:
                cancelled_std = cancel['cancelled_qty']
                standard_uom = oc_info.get('standard_uom', '')
                
                if uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
                    cancelled_sell = uom_converter.convert_quantity(
                        cancelled_std, 'standard', 'selling', 
                        oc_info.get('uom_conversion', '1')
                    )
                    selling_uom = oc_info.get('selling_uom', '')
                    st.text(f"Cancelled {format_number(cancelled_std)} {standard_uom}")
                    st.caption(f"= {format_number(cancelled_sell)} {selling_uom}")
                else:
                    st.text(f"Cancelled {format_number(cancelled_std)} {standard_uom}")
            
            with cancel_cols[1]:
                st.text(format_date(cancel['cancelled_date']))
            with cancel_cols[2]:
                st.text(format_reason_category(cancel['reason_category']))
            with cancel_cols[3]:
                if cancel['status'] == 'ACTIVE' and validator.check_permission(st.session_state.user['role'], 'reverse'):
                    if st.button("↩️ Reverse", key=f"reverse_{cancel['cancellation_id']}"):
                        st.session_state.context['return_to_history'] = {
                            'oc_detail_id': st.session_state.selections['oc_for_history'],
                            'oc_info': st.session_state.selections['oc_info']
                        }
                        
                        st.session_state.modals['history'] = False
                        st.session_state.modals['reverse'] = True
                        st.session_state.selections['cancellation_for_reverse'] = cancel.to_dict()
                        st.rerun()
            
            st.caption(f"Reason: {cancel['reason']}")
            if cancel['status'] == 'REVERSED':
                st.info(f"✅ Reversed on {format_date(cancel['reversed_date'])} by {cancel['reversed_by']}")


def render_compact_metric(label: str, value: str, help_text: str = None, color: str = None, delta: str = None):
    """Render a compact metric with smaller font size for professional look"""
    color_style = f"color: {color};" if color else ""
    
    help_icon = ""
    if help_text:
        # Simple tooltip using title attribute
        help_icon = f'<span title="{help_text}" style="cursor: help; color: #999; margin-left: 4px; font-size: 0.7rem;">ⓘ</span>'
    
    delta_html = ""
    if delta:
        delta_html = f'<div style="font-size: 0.7rem; color: #666; margin-top: 2px;">{delta}</div>'
    
    html = f"""
    <div style='margin-bottom: 8px;'>
        <div style='font-size: 0.75rem; color: #666; margin-bottom: 2px;'>{label}{help_icon}</div>
        <div style='font-size: 1rem; font-weight: 600; {color_style}'>{value}</div>
        {delta_html}
    </div>
    """
    return html


def show_delivery_details(alloc):
    """
    Show delivery details from allocation_delivery_links
    REFACTORED: Now displays both Original ETD and Latest ETD
    """
    delivery_count = alloc.get('delivery_count', 0)
    
    with st.expander(f"📦 View Delivery History ({delivery_count} deliveries)"):
        delivery_df = allocation_data.get_allocation_delivery_details(alloc['allocation_detail_id'])
        
        if delivery_df.empty:
            st.info("No delivery records found")
            return
        
        # Display each delivery
        for idx, delivery in delivery_df.iterrows():
            with st.container():
                # ===== HEADER ROW =====
                header_cols = st.columns([3, 2, 2, 2])
                
                with header_cols[0]:
                    st.markdown(f"**📄 {delivery['delivery_number']}**")
                    if delivery.get('customer_name'):
                        st.caption(f"Customer: {delivery['customer_name']}")
                
                with header_cols[1]:
                    st.markdown(
                        render_compact_metric(
                            "Delivered Qty",
                            f"{format_number(delivery['delivered_qty'])} pcs",
                            help_text="Quantity linked to this allocation"
                        ),
                        unsafe_allow_html=True
                    )
                
                with header_cols[2]:
                    status_emoji = {
                        'DELIVERED': '✅',
                        'ON_DELIVERY': '🚚',
                        'DISPATCHED': '📦',
                        'PENDING': '⏳',
                        'RECEIVED': '✅'
                    }.get(delivery['delivery_status'], '📋')
                    
                    status_color = {
                        'DELIVERED': '#10b981',
                        'ON_DELIVERY': '#3b82f6',
                        'DISPATCHED': '#8b5cf6',
                        'PENDING': '#f59e0b',
                        'RECEIVED': '#10b981'
                    }.get(delivery['delivery_status'], '#6b7280')
                    
                    st.markdown(
                        render_compact_metric(
                            "Status",
                            f"{status_emoji} {delivery['delivery_status']}",
                            color=status_color
                        ),
                        unsafe_allow_html=True
                    )
                
                with header_cols[3]:
                    if delivery.get('from_warehouse'):
                        st.markdown(
                            render_compact_metric(
                                "Warehouse",
                                delivery['from_warehouse']
                            ),
                            unsafe_allow_html=True
                        )
                
                # ===== DATES ROW =====
                st.markdown("**📅 Dates:**")
                date_cols = st.columns(4)
                
                with date_cols[0]:
                    # Original ETD
                    original_etd = delivery.get('original_etd')
                    if original_etd and not pd.isna(original_etd):
                        st.markdown(
                            render_compact_metric(
                                "Original ETD",
                                format_date(original_etd),
                                help_text="Initial Expected Time of Delivery (from stock_out_delivery.etd_date)"
                            ),
                            unsafe_allow_html=True
                        )
                    else:
                        st.caption("Original ETD: N/A")
                
                with date_cols[1]:
                    # Latest ETD (Adjusted)
                    latest_etd = delivery.get('latest_etd')
                    
                    if latest_etd and not pd.isna(latest_etd):
                        # Check if ETD was updated
                        etd_update_count = delivery.get('etd_update_count', 0)
                        
                        if etd_update_count and etd_update_count > 0:
                            st.markdown(
                                render_compact_metric(
                                    "Latest ETD",
                                    format_date(latest_etd),
                                    delta=f"Updated {etd_update_count}x",
                                    help_text="Adjusted Expected Time of Delivery (from stock_out_delivery.adjust_etd_date)"
                                ),
                                unsafe_allow_html=True
                            )
                        else:
                            st.markdown(
                                render_compact_metric(
                                    "Latest ETD",
                                    format_date(latest_etd),
                                    help_text="Adjusted Expected Time of Delivery (from stock_out_delivery.adjust_etd_date)"
                                ),
                                unsafe_allow_html=True
                            )
                    else:
                        # No adjustment, use original
                        if original_etd and not pd.isna(original_etd):
                            st.markdown(
                                render_compact_metric(
                                    "Latest ETD",
                                    format_date(original_etd),
                                    help_text="No adjustment made, using original ETD"
                                ),
                                unsafe_allow_html=True
                            )
                        else:
                            st.caption("Latest ETD: N/A")
                
                with date_cols[2]:
                    # Dispatch Date
                    dispatch_date = delivery.get('dispatch_date')
                    if dispatch_date and not pd.isna(dispatch_date):
                        st.markdown(
                            render_compact_metric(
                                "Dispatched",
                                format_date(dispatch_date),
                                help_text="Date when goods were dispatched"
                            ),
                            unsafe_allow_html=True
                        )
                    else:
                        st.caption("Dispatched: N/A")
                
                with date_cols[3]:
                    # Delivered Date
                    delivered_date = delivery.get('date_delivered')
                    if delivered_date and not pd.isna(delivered_date):
                        st.markdown(
                            render_compact_metric(
                                "Delivered",
                                format_date(delivered_date),
                                help_text="Date when goods were delivered"
                            ),
                            unsafe_allow_html=True
                        )
                    else:
                        st.caption("Delivered: N/A")
                
                # ===== ADDITIONAL INFO =====
                if delivery.get('total_delivery_qty') or delivery.get('total_delivery_qty_selling'):
                    info_cols = st.columns(2)
                    
                    with info_cols[0]:
                        if delivery.get('total_delivery_qty'):
                            st.caption(f"Total in DN: {format_number(delivery['total_delivery_qty'])} pcs (standard)")
                    
                    with info_cols[1]:
                        if delivery.get('total_delivery_qty_selling'):
                            st.caption(f"Total in DN: {format_number(delivery['total_delivery_qty_selling'])} pcs (selling)")
                
                # ===== ETD COMPARISON WARNING =====
                    original_date = pd.to_datetime(original_etd).date()
                    latest_date = pd.to_datetime(latest_etd).date()
                    
                    if original_date != latest_date:
                        diff_days = (latest_date - original_date).days
                        
                        if diff_days > 0:
                            st.warning(
                                f"⚠️ ETD was delayed by {diff_days} days "
                                f"(from {format_date(original_date)} to {format_date(latest_date)})"
                            )
                        else:
                            st.info(
                                f"ℹ️ ETD was advanced by {abs(diff_days)} days "
                                f"(from {format_date(original_date)} to {format_date(latest_date)})"
                            )
                
                # Separator between deliveries
                if idx < len(delivery_df) - 1:
                    st.markdown("---")


def show_allocation_history_item(alloc, oc_info):
    """Show single allocation history item"""
    with st.container():
        show_allocation_header_with_tooltip(alloc, oc_info)
        show_allocation_quantities_dual_uom(alloc, oc_info)
        show_allocation_info(alloc)
        show_allocation_actions(alloc, oc_info)
        
        if alloc.get('has_cancellations'):
            show_cancellation_history_dual_uom(alloc, oc_info)
        
        delivery_count = alloc.get('delivery_count')
        if delivery_count is not None and delivery_count > 0:
            show_delivery_details(alloc)
        
        st.divider()


@st.dialog("Allocation History", width="large")
def show_allocation_history_modal():
    """Show allocation history with delivery data"""
    if 'oc_for_history' not in st.session_state.selections or not st.session_state.selections['oc_for_history']:
        st.error("No OC selected")
        if st.button("Close"):
            st.session_state.modals['history'] = False
            st.rerun()
        return
    
    oc_detail_id = st.session_state.selections['oc_for_history']
    oc_info = st.session_state.selections.get('oc_info')
    
    if not oc_info:
        st.error("OC information not found")
        if st.button("Close"):
            st.session_state.modals['history'] = False
            st.session_state.selections['oc_for_history'] = None
            st.rerun()
        return
    
    # Header
    st.markdown(f"### Allocation History for {oc_info['oc_number']}")
    
    # Show over-allocation warning
    if oc_info.get('over_allocation_type') == 'Over-Committed':
        st.error("⚡ This OC is over-committed - total allocations exceed order quantity")
    elif oc_info.get('over_allocation_type') == 'Pending-Over-Allocated':
        st.warning("⚠️ This OC has pending over-allocation - undelivered allocations exceed pending quantity")
    
    col1, col2 = st.columns(2)
    with col1:
        st.caption(f"**Customer:** {oc_info['customer']}")
    with col2:
        st.caption(f"**Product:** {oc_info['product_name']}")
    
    # Summary metrics
    show_allocation_summary_metrics(oc_info)
    
    st.divider()
    
    # Get allocation history
    history_df = allocation_data.get_allocation_history_with_details(oc_detail_id)
    
    if history_df.empty:
        st.info("No allocation history found")
    else:
        for idx, alloc in history_df.iterrows():
            show_allocation_history_item(alloc, oc_info)
    
    # Note about UOM
    if uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
        st.info(f"ℹ️ Note: Allocation quantities are stored in {oc_info.get('standard_uom', 'standard UOM')}. " +
                f"Conversion: {oc_info.get('uom_conversion', 'N/A')}")
    
    # Close button
    if st.button("Close", use_container_width=True):
        st.session_state.modals['history'] = False
        st.session_state.selections['oc_for_history'] = None
        st.session_state.selections['oc_info'] = None
        st.session_state.context['return_to_history'] = None
        st.rerun()