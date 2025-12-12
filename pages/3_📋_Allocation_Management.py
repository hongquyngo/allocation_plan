"""
Allocation Management Dashboard
================================
Centralized dashboard for managing existing allocations.

Features:
- Overview statistics
- Search & filter allocations
- Drill-down to allocation details
- View delivery history & audit trail
- Update/Cancel/Reverse operations

Author: Prostech
Created: 2024-12
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional

# Page config
st.set_page_config(
    page_title="Allocation Management",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Authentication
from utils.auth import AuthManager

auth = AuthManager()
if not auth.check_session():
    st.warning("⚠️ Please login to access this page")
    st.stop()

# Get current user from session state
user = st.session_state.get('user', {})
if not user or not user.get('id'):
    st.error("Please login to access this page")
    st.stop()

# Import module
from utils.allocation_management import (
    AllocationManagementData,
    AllocationManagementService,
    AllocationManagementValidator,
    AllocationManagementEmail,
    AllocationSupplyData,
    AllocationManagementFormatters as fmt
)

# Initialize
@st.cache_resource
def get_data():
    return AllocationManagementData()

@st.cache_resource
def get_service():
    return AllocationManagementService()

@st.cache_resource
def get_supply():
    return AllocationSupplyData()

@st.cache_resource
def get_email():
    return AllocationManagementEmail()

@st.cache_resource
def get_validator():
    return AllocationManagementValidator()

data = get_data()
service = get_service()
supply = get_supply()
email_service = get_email()
validator = get_validator()


# ================================================================
# SESSION STATE
# ================================================================

def init_session_state():
    """Initialize session state variables"""
    defaults = {
        'mgmt_selected_ids': [],
        'mgmt_selected_allocation': None,
        'mgmt_show_update_qty': False,
        'mgmt_show_update_etd': False,
        'mgmt_show_cancel': False,
        'mgmt_show_reverse': False,
        'mgmt_show_bulk_etd': False,
        'mgmt_show_bulk_cancel': False,
        'mgmt_search_results': None,
        'mgmt_initial_load': False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()


# ================================================================
# HEADER & STATISTICS
# ================================================================

def render_header():
    """Render page header with dashboard statistics"""
    st.title("📋 Allocation Management")
    st.caption("Dashboard to manage and track existing allocations")
    
    # Get statistics
    stats = data.get_dashboard_statistics()
    
    # Row 1: Status metrics
    st.markdown("##### 📊 Allocation Status")
    cols = st.columns(5)
    
    with cols[0]:
        st.metric("Total", f"{stats['total']:,}")
    with cols[1]:
        st.metric("🔵 Pending", f"{stats['pending']:,}")
    with cols[2]:
        st.metric("🟡 Partial", f"{stats['partial']:,}")
    with cols[3]:
        st.metric("✅ Delivered", f"{stats['delivered']:,}")
    with cols[4]:
        delta = f"-{stats['overdue_count']}" if stats['overdue_count'] > 0 else None
        st.metric("⚠️ Overdue", f"{stats['overdue_count']:,}", delta=delta, delta_color="inverse")
    
    # Row 2: Supply source breakdown (collapsible)
    with st.expander("📦 By Supply Source", expanded=False):
        src_cols = st.columns(4)
        with src_cols[0]:
            st.metric("🏭 Inventory", f"{stats['from_inventory']:,}")
        with src_cols[1]:
            st.metric("📋 Pending CAN", f"{stats['from_can']:,}")
        with src_cols[2]:
            st.metric("📄 Pending PO", f"{stats['from_po']:,}")
        with src_cols[3]:
            st.metric("🚚 WH Transfer", f"{stats['from_wht']:,}")
    
    st.divider()


# ================================================================
# SEARCH & FILTERS
# ================================================================

def render_search_panel():
    """Render search and filter panel"""
    
    with st.expander("🔍 Search & Filters", expanded=True):
        options = data.get_filter_options()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            allocation_number = st.text_input("Allocation #", key="filter_alloc", placeholder="e.g. ALLOC-2025...")
            
            # Product selector
            product_options = [(None, "-- All Products --")] + options['products']
            product_id = st.selectbox(
                "Product",
                options=[p[0] for p in product_options],
                format_func=lambda x: dict(product_options).get(x, "-- All --"),
                key="filter_product"
            )
        
        with col2:
            # Customer selector
            customer_options = [(None, "-- All Customers --")] + options['customers']
            customer_code = st.selectbox(
                "Customer",
                options=[c[0] for c in customer_options],
                format_func=lambda x: dict(customer_options).get(x, "-- All --"),
                key="filter_customer"
            )
            
            # Supply source selector
            source_options = [(None, "-- All Sources --")] + [(s, s) for s in options['supply_sources']]
            supply_source = st.selectbox(
                "Supply Source",
                options=[s[0] for s in source_options],
                format_func=lambda x: {
                    None: "-- All Sources --",
                    'INVENTORY': '🏭 Inventory',
                    'PENDING_CAN': '📋 Pending CAN',
                    'PENDING_PO': '📄 Pending PO',
                    'PENDING_WHT': '🚚 WH Transfer'
                }.get(x, x),
                key="filter_source"
            )
        
        with col3:
            # Status selector
            status_options = [(None, "-- All Statuses --")] + [(s, s) for s in options['statuses']]
            effective_status = st.selectbox(
                "Status",
                options=[s[0] for s in status_options],
                format_func=lambda x: {
                    None: "-- All Statuses --",
                    'ALLOCATED': '🔵 Pending',
                    'PARTIAL_DELIVERED': '🟡 Partial',
                    'DELIVERED': '✅ Delivered',
                    'CANCELLED': '❌ Cancelled'
                }.get(x, x),
                key="filter_status"
            )
            
            # Creator selector
            creator_options = [(None, "-- All Creators --")] + options['creators']
            created_by = st.selectbox(
                "Created By",
                options=[c[0] for c in creator_options],
                format_func=lambda x: dict(creator_options).get(x, "-- All --"),
                key="filter_creator"
            )
        
        with col4:
            date_from = st.date_input(
                "From Date",
                value=date.today() - timedelta(days=90),
                key="filter_date_from"
            )
            date_to = st.date_input(
                "To Date",
                value=date.today(),
                key="filter_date_to"
            )
            
            show_overdue = st.checkbox("⚠️ Show Overdue Only", key="filter_overdue")
        
        col_btn1, col_btn2, _ = st.columns([1, 1, 4])
        with col_btn1:
            search_clicked = st.button("🔍 Search", type="primary", use_container_width=True)
        with col_btn2:
            if st.button("↻ Reset", use_container_width=True):
                for key in list(st.session_state.keys()):
                    if key.startswith('filter_'):
                        del st.session_state[key]
                st.session_state['mgmt_search_results'] = None
                st.session_state['mgmt_initial_load'] = False
                st.session_state['mgmt_selected_allocation'] = None
                st.rerun()
        
        # Auto-load on first visit OR when search clicked
        should_search = search_clicked or not st.session_state.get('mgmt_initial_load', False)
        
        if should_search:
            results = data.search_allocations(
                allocation_number=allocation_number if allocation_number else None,
                product_id=product_id,
                customer_code=customer_code,
                effective_status=effective_status,
                supply_source_type=supply_source,
                date_from=date_from,
                date_to=date_to,
                created_by=created_by,
                show_overdue_only=show_overdue
            )
            st.session_state['mgmt_search_results'] = results
            st.session_state['mgmt_initial_load'] = True
            
            if search_clicked:
                st.session_state['mgmt_selected_ids'] = []
                st.session_state['mgmt_selected_allocation'] = None


# ================================================================
# RESULTS TABLE
# ================================================================

def render_results_table():
    """Render search results table"""
    
    results = st.session_state.get('mgmt_search_results')
    
    if results is None:
        st.info("⏳ Loading data...")
        return
    
    if results.empty:
        st.warning("No allocations found matching filters")
        return
    
    # Header with count and bulk actions
    col_header, col_bulk1, col_bulk2 = st.columns([3, 1, 1])
    
    with col_header:
        st.subheader(f"📋 Results ({len(results):,} allocations)")
    
    selected_ids = st.session_state.get('mgmt_selected_ids', [])
    
    with col_bulk1:
        if st.button("📅 Bulk ETD", disabled=len(selected_ids) == 0, use_container_width=True):
            st.session_state['mgmt_show_bulk_etd'] = True
    
    with col_bulk2:
        if st.button("❌ Bulk Cancel", disabled=len(selected_ids) == 0, use_container_width=True):
            st.session_state['mgmt_show_bulk_cancel'] = True
    
    if len(selected_ids) > 0:
        st.caption(f"✓ Selected: {len(selected_ids)} items")
    
    # Prepare display dataframe
    display_df = results.copy()
    display_df.insert(0, 'select', False)
    
    # Format columns
    display_df['status_icon'] = display_df['effective_status'].apply(lambda x: {
        'ALLOCATED': '🔵',
        'PARTIAL_DELIVERED': '🟡',
        'DELIVERED': '✅',
        'CANCELLED': '❌'
    }.get(x, '⚪'))
    
    display_df['source_icon'] = display_df['supply_source_type'].apply(lambda x: {
        'INVENTORY': '🏭',
        'PENDING_CAN': '📋',
        'PENDING_PO': '📄',
        'PENDING_WHT': '🚚'
    }.get(x, ''))
    
    display_df['product_display'] = display_df.apply(
        lambda r: f"{r['pt_code']} | {str(r['product_name'])[:30]}..." if r['product_name'] and len(str(r['product_name'])) > 30 
        else f"{r['pt_code']} | {r['product_name']}", axis=1
    )
    
    display_df['customer_display'] = display_df.apply(
        lambda r: f"{r['customer_code']} - {str(r['customer_name'])[:20]}..." if r['customer_name'] and len(str(r['customer_name'])) > 20 
        else f"{r['customer_code']} - {r['customer_name']}", axis=1
    )
    
    display_df['etd_display'] = display_df['allocated_etd'].apply(
        lambda x: x.strftime('%d %b %Y') if pd.notna(x) else '-'
    )
    
    display_df['fulfillment_display'] = display_df['fulfillment_rate'].apply(
        lambda x: f"{x:.0f}%" if pd.notna(x) else '0%'
    )
    
    # Mark selected rows
    for idx in display_df.index:
        if display_df.loc[idx, 'id'] in selected_ids:
            display_df.loc[idx, 'select'] = True
    
    # Data editor
    edited_df = st.data_editor(
        display_df[[
            'select', 'id', 'allocation_number', 'product_display', 'customer_display',
            'source_icon', 'allocated_qty', 'delivered_qty', 'remaining_qty',
            'fulfillment_display', 'status_icon', 'etd_display'
        ]],
        column_config={
            'select': st.column_config.CheckboxColumn('✓', width='small'),
            'id': st.column_config.NumberColumn('ID', width='small'),
            'allocation_number': st.column_config.TextColumn('Allocation #', width='medium'),
            'product_display': st.column_config.TextColumn('Product', width='large'),
            'customer_display': st.column_config.TextColumn('Customer', width='medium'),
            'source_icon': st.column_config.TextColumn('Source', width='small'),
            'allocated_qty': st.column_config.NumberColumn('Allocated', format="%.2f", width='small'),
            'delivered_qty': st.column_config.NumberColumn('Delivered', format="%.2f", width='small'),
            'remaining_qty': st.column_config.NumberColumn('Remaining', format="%.2f", width='small'),
            'fulfillment_display': st.column_config.TextColumn('%', width='small'),
            'status_icon': st.column_config.TextColumn('Status', width='small'),
            'etd_display': st.column_config.TextColumn('ETD', width='small'),
        },
        hide_index=True,
        use_container_width=True,
        height=400,
        key="results_table"
    )
    
    # Update selected IDs
    new_selected = edited_df[edited_df['select'] == True]['id'].tolist()
    if new_selected != selected_ids:
        st.session_state['mgmt_selected_ids'] = new_selected
    
    # Row click to view details
    st.caption("💡 Click an ID below to view allocation details")
    
    # Show clickable IDs for first 20 results
    id_cols = st.columns(10)
    for i, (idx, row) in enumerate(results.head(20).iterrows()):
        col_idx = i % 10
        with id_cols[col_idx]:
            alloc_id = row['id']
            status_icon = {'ALLOCATED': '🔵', 'PARTIAL_DELIVERED': '🟡', 'DELIVERED': '✅', 'CANCELLED': '❌'}.get(row['effective_status'], '')
            if st.button(f"{status_icon}{alloc_id}", key=f"view_{alloc_id}", use_container_width=True):
                st.session_state['mgmt_selected_allocation'] = alloc_id
                st.rerun()


# ================================================================
# DETAIL PANEL
# ================================================================

def render_detail_panel():
    """Render detail panel for selected allocation"""
    
    selected_id = st.session_state.get('mgmt_selected_allocation')
    if not selected_id:
        return
    
    st.divider()
    
    allocation = data.get_allocation_detail(selected_id)
    if not allocation:
        st.error("Allocation not found")
        return
    
    # Header with close button and actions
    col_title, col_close = st.columns([6, 1])
    
    with col_title:
        status_icon = {'ALLOCATED': '🔵', 'PARTIAL_DELIVERED': '🟡', 'DELIVERED': '✅', 'CANCELLED': '❌'}.get(allocation.get('effective_status'), '')
        st.subheader(f"{status_icon} {allocation.get('allocation_number', 'N/A')}")
    
    with col_close:
        if st.button("✕ Close", use_container_width=True):
            st.session_state['mgmt_selected_allocation'] = None
            st.rerun()
    
    # Main info cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📦 Product Info**")
        st.write(f"**Code:** {allocation.get('pt_code', 'N/A')}")
        st.write(f"**Name:** {allocation.get('product_name', 'N/A')}")
        st.write(f"**Brand:** {allocation.get('brand_name', 'N/A')}")
        source_icon = {'INVENTORY': '🏭', 'PENDING_CAN': '📋', 'PENDING_PO': '📄', 'PENDING_WHT': '🚚'}.get(allocation.get('supply_source_type'), '')
        st.write(f"**Source:** {source_icon} {allocation.get('supply_source_type', 'N/A')}")
    
    with col2:
        st.markdown("**👤 Customer & OC**")
        st.write(f"**Customer:** {allocation.get('customer_name', 'N/A')}")
        st.write(f"**Code:** {allocation.get('customer_code', 'N/A')}")
        st.write(f"**OC #:** {allocation.get('demand_number', 'N/A')}")
        st.write(f"**Created By:** {allocation.get('creator_name', 'N/A')}")
    
    with col3:
        st.markdown("**📊 Quantities**")
        allocated = float(allocation.get('allocated_qty', 0))
        delivered = float(allocation.get('delivered_qty', 0))
        remaining = float(allocation.get('remaining_qty', 0))
        cancelled = float(allocation.get('cancelled_qty', 0))
        
        st.write(f"**Allocated:** {allocated:,.2f}")
        st.write(f"**Delivered:** {delivered:,.2f}")
        st.write(f"**Remaining:** {remaining:,.2f}")
        if cancelled > 0:
            st.write(f"**Cancelled:** {cancelled:,.2f}")
        
        fulfillment = allocation.get('fulfillment_rate', 0) or 0
        st.progress(min(fulfillment / 100, 1.0), text=f"Progress: {fulfillment:.0f}%")
    
    # ETD & Dates
    st.markdown("---")
    date_cols = st.columns(4)
    with date_cols[0]:
        etd = allocation.get('allocated_etd')
        etd_str = etd.strftime('%d %b %Y') if etd else 'N/A'
        days_past = allocation.get('days_past_etd', 0) or 0
        if days_past > 0:
            st.error(f"📅 ETD: {etd_str} (Overdue {days_past} days)")
        else:
            st.info(f"📅 ETD: {etd_str}")
    
    with date_cols[1]:
        alloc_date = allocation.get('allocation_date')
        alloc_str = alloc_date.strftime('%d %b %Y') if alloc_date else 'N/A'
        st.info(f"📆 Created: {alloc_str}")
    
    with date_cols[2]:
        update_count = allocation.get('etd_update_count', 0) or 0
        st.info(f"🔄 ETD Updates: {update_count}")
    
    with date_cols[3]:
        cancel_count = allocation.get('cancel_count', 0) or 0
        if cancel_count > 0:
            st.warning(f"❌ Cancellations: {cancel_count}")
    
    # Action buttons
    st.markdown("---")
    st.markdown("**⚡ Actions**")
    
    remaining_qty = float(allocation.get('remaining_qty', 0))
    delivered_qty = float(allocation.get('delivered_qty', 0))
    effective_status = allocation.get('effective_status', '')
    
    col_a1, col_a2, col_a3, col_a4, _ = st.columns([1, 1, 1, 1, 2])
    
    with col_a1:
        disabled = effective_status in ('DELIVERED', 'CANCELLED')
        if st.button("📝 Update Qty", use_container_width=True, disabled=disabled):
            st.session_state['mgmt_show_update_qty'] = True
    
    with col_a2:
        disabled = effective_status in ('DELIVERED', 'CANCELLED')
        if st.button("📅 Update ETD", use_container_width=True, disabled=disabled):
            st.session_state['mgmt_show_update_etd'] = True
    
    with col_a3:
        disabled = remaining_qty <= 0 or effective_status == 'CANCELLED'
        if st.button("❌ Cancel", use_container_width=True, disabled=disabled):
            st.session_state['mgmt_show_cancel'] = True
    
    with col_a4:
        disabled = delivered_qty <= 0
        if st.button("↩️ Reverse", use_container_width=True, disabled=disabled):
            st.session_state['mgmt_show_reverse'] = True
    
    # Detail tabs
    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["🚚 Delivery History", "📜 Change History", "ℹ️ JSON Details"])
    
    with tab1:
        render_deliveries_tab(selected_id)
    
    with tab2:
        render_history_tab(selected_id)
    
    with tab3:
        st.json(allocation)


def render_deliveries_tab(allocation_detail_id: int):
    """Render deliveries history tab"""
    deliveries = data.get_delivery_links(allocation_detail_id)
    
    if deliveries.empty:
        st.info("📭 No deliveries yet")
        return
    
    st.dataframe(
        deliveries[['delivery_link_id', 'dn_number', 'delivered_qty', 'delivery_date', 'delivery_status']],
        column_config={
            'delivery_link_id': 'ID',
            'dn_number': 'DN Number',
            'delivered_qty': st.column_config.NumberColumn('Quantity', format="%.2f"),
            'delivery_date': 'Date',
            'delivery_status': 'Status'
        },
        hide_index=True,
        use_container_width=True
    )


def render_history_tab(allocation_detail_id: int):
    """Render audit history tab"""
    
    # Cancellation history
    cancellations = data.get_cancellation_history(allocation_detail_id)
    if not cancellations.empty:
        st.markdown("**❌ Cancellation History**")
        st.dataframe(
            cancellations[['cancelled_qty', 'reason_category', 'reason', 'cancelled_by_username', 'cancelled_date', 'status']],
            column_config={
                'cancelled_qty': st.column_config.NumberColumn('Quantity', format="%.2f"),
                'reason_category': 'Category',
                'reason': 'Reason',
                'cancelled_by_username': 'Cancelled By',
                'cancelled_date': 'Date',
                'status': 'Status'
            },
            hide_index=True,
            use_container_width=True
        )
    
    # Audit log
    audit = data.get_audit_history(allocation_detail_id)
    if not audit.empty:
        st.markdown("**📝 Audit Log**")
        st.dataframe(
            audit[['action_type', 'change_reason', 'performed_by_username', 'performed_at']],
            column_config={
                'action_type': 'Action',
                'change_reason': 'Reason',
                'performed_by_username': 'Performed By',
                'performed_at': 'Date/Time'
            },
            hide_index=True,
            use_container_width=True
        )
    
    if cancellations.empty and audit.empty:
        st.info("📭 No change history")


# ================================================================
# ACTION MODALS (giữ nguyên logic, chỉ cập nhật field names)
# ================================================================

def render_update_qty_modal():
    """Render update quantity modal"""
    if not st.session_state.get('mgmt_show_update_qty'):
        return
    
    selected_id = st.session_state.get('mgmt_selected_allocation')
    allocation = data.get_allocation_detail(selected_id)
    
    with st.container():
        st.markdown("---")
        st.subheader("📝 Update Quantity")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Allocation:** {allocation.get('allocation_number')}")
            st.write(f"**Product:** {allocation.get('product_name')}")
        with col2:
            st.write(f"**Current Allocated:** {allocation.get('allocated_qty'):,.2f}")
            st.write(f"**Delivered:** {allocation.get('delivered_qty'):,.2f}")
        
        # Get limits
        delivered = float(allocation.get('delivered_qty', 0))
        requested = float(allocation.get('requested_qty', 0))
        current_allocated = float(allocation.get('allocated_qty', 0))
        
        st.caption(f"Min: {delivered:,.2f} (delivered) | Max: {requested:,.2f} (requested)")
        
        new_qty = st.number_input(
            "New Quantity",
            min_value=delivered,
            max_value=requested,
            value=current_allocated,
            step=1.0,
            key="update_qty_input"
        )
        
        reason = st.text_area("Reason for change (required)", key="update_qty_reason")
        send_email = st.checkbox("Send email notification", value=True, key="update_qty_email")
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("✓ Update", type="primary", use_container_width=True):
                if not reason.strip():
                    st.error("Please enter a reason")
                else:
                    user_id = user.get('id')
                    result = service.update_quantity(selected_id, new_qty, reason, user_id)
                    
                    if result.success:
                        st.success(result.message)
                        if send_email:
                            email_service.send_quantity_updated_email(
                                selected_id, result.data['old_qty'], result.data['new_qty'], reason, user_id
                            )
                        st.session_state['mgmt_show_update_qty'] = False
                        st.session_state['mgmt_search_results'] = None
                        st.rerun()
                    else:
                        st.error(result.message)
        
        with col_btn2:
            if st.button("Cancel", use_container_width=True):
                st.session_state['mgmt_show_update_qty'] = False
                st.rerun()


def render_update_etd_modal():
    """Render update ETD modal"""
    if not st.session_state.get('mgmt_show_update_etd'):
        return
    
    selected_id = st.session_state.get('mgmt_selected_allocation')
    allocation = data.get_allocation_detail(selected_id)
    
    with st.container():
        st.markdown("---")
        st.subheader("📅 Update ETD")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Allocation:** {allocation.get('allocation_number')}")
            st.write(f"**Product:** {allocation.get('product_name')}")
        with col2:
            current_etd = allocation.get('allocated_etd')
            st.write(f"**Current ETD:** {current_etd.strftime('%d %b %Y') if current_etd else 'N/A'}")
            st.write(f"**Update Count:** {allocation.get('etd_update_count', 0)}")
        
        if current_etd:
            if isinstance(current_etd, str):
                current_etd = datetime.strptime(current_etd, '%Y-%m-%d').date()
            elif isinstance(current_etd, datetime):
                current_etd = current_etd.date()
        else:
            current_etd = date.today()
        
        new_etd = st.date_input("New ETD", value=current_etd, min_value=date.today(), key="update_etd_input")
        reason = st.text_area("Reason for change (required)", key="update_etd_reason")
        send_email = st.checkbox("Send email notification", value=True, key="update_etd_email")
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("✓ Update", type="primary", use_container_width=True, key="etd_update_btn"):
                if not reason.strip():
                    st.error("Please enter a reason")
                else:
                    user_id = user.get('id')
                    result = service.update_etd(selected_id, new_etd, reason, user_id)
                    
                    if result.success:
                        st.success(result.message)
                        if send_email:
                            email_service.send_etd_updated_email(
                                selected_id, result.data['old_etd'], result.data['new_etd'], reason, user_id
                            )
                        st.session_state['mgmt_show_update_etd'] = False
                        st.session_state['mgmt_search_results'] = None
                        st.rerun()
                    else:
                        st.error(result.message)
        
        with col_btn2:
            if st.button("Cancel", use_container_width=True, key="etd_cancel_btn"):
                st.session_state['mgmt_show_update_etd'] = False
                st.rerun()


def render_cancel_modal():
    """Render cancel allocation modal"""
    if not st.session_state.get('mgmt_show_cancel'):
        return
    
    selected_id = st.session_state.get('mgmt_selected_allocation')
    allocation = data.get_allocation_detail(selected_id)
    
    with st.container():
        st.markdown("---")
        st.subheader("❌ Cancel Allocation")
        st.warning("⚠️ This action will release the allocated supply")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Allocation:** {allocation.get('allocation_number')}")
            st.write(f"**Product:** {allocation.get('product_name')}")
        with col2:
            st.write(f"**Allocated:** {allocation.get('allocated_qty'):,.2f}")
            st.write(f"**Remaining:** {allocation.get('remaining_qty'):,.2f}")
        
        max_cancel = float(allocation.get('remaining_qty', 0))
        st.info(f"Maximum cancellable: {max_cancel:,.2f}")
        
        cancel_type = st.radio("Cancel Type", ["Full Cancel", "Partial Cancel"], key="cancel_type")
        
        if cancel_type == "Full Cancel":
            cancel_qty = max_cancel
            st.write(f"Will cancel: **{cancel_qty:,.2f}** units")
        else:
            cancel_qty = st.number_input("Cancel Quantity", min_value=0.01, max_value=max_cancel, value=min(max_cancel, 1.0), step=1.0, key="cancel_qty_input")
        
        reason_category = st.selectbox(
            "Reason Category",
            ["CUSTOMER_REQUEST", "SUPPLY_ISSUE", "QUALITY_ISSUE", "BUSINESS_DECISION", "OTHER"],
            format_func=lambda x: {'CUSTOMER_REQUEST': 'Customer Request', 'SUPPLY_ISSUE': 'Supply Issue', 'QUALITY_ISSUE': 'Quality Issue', 'BUSINESS_DECISION': 'Business Decision', 'OTHER': 'Other'}.get(x, x),
            key="cancel_category"
        )
        
        reason = st.text_area("Detailed Reason (required)", key="cancel_reason")
        send_email = st.checkbox("Send email notification", value=True, key="cancel_email")
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("❌ Confirm Cancel", type="primary", use_container_width=True):
                if not reason.strip():
                    st.error("Please enter a reason")
                else:
                    user_id = user.get('id')
                    result = service.cancel_allocation(selected_id, cancel_qty, reason, reason_category, user_id)
                    
                    if result.success:
                        st.success(result.message)
                        if send_email:
                            email_service.send_cancelled_email(selected_id, cancel_qty, reason, reason_category, user_id)
                        st.session_state['mgmt_show_cancel'] = False
                        st.session_state['mgmt_search_results'] = None
                        st.rerun()
                    else:
                        st.error(result.message)
        
        with col_btn2:
            if st.button("Back", use_container_width=True, key="cancel_back"):
                st.session_state['mgmt_show_cancel'] = False
                st.rerun()


def render_reverse_modal():
    """Render reverse delivery modal"""
    if not st.session_state.get('mgmt_show_reverse'):
        return
    
    selected_id = st.session_state.get('mgmt_selected_allocation')
    allocation = data.get_allocation_detail(selected_id)
    
    with st.container():
        st.markdown("---")
        st.subheader("↩️ Reverse Delivery")
        st.warning("⚠️ Advanced operation to reverse a completed delivery")
        
        deliveries = data.get_delivery_links(selected_id)
        
        if deliveries.empty:
            st.error("No deliveries to reverse")
            if st.button("Back"):
                st.session_state['mgmt_show_reverse'] = False
                st.rerun()
            return
        
        st.markdown("**Select delivery to reverse**")
        
        delivery_options = []
        for _, row in deliveries.iterrows():
            label = f"DN: {row.get('dn_number', 'N/A')} | Qty: {row['delivered_qty']:,.2f}"
            delivery_options.append((row['delivery_link_id'], label))
        
        selected_delivery = st.selectbox(
            "Delivery",
            options=[d[0] for d in delivery_options],
            format_func=lambda x: dict(delivery_options).get(x, str(x)),
            key="reverse_delivery_select"
        )
        
        delivery_row = deliveries[deliveries['delivery_link_id'] == selected_delivery].iloc[0]
        max_reverse = float(delivery_row['delivered_qty'])
        
        st.info(f"Reversible quantity: {max_reverse:,.2f}")
        
        reverse_qty = st.number_input("Reverse Quantity", min_value=0.01, max_value=max_reverse, value=max_reverse, step=1.0, key="reverse_qty_input")
        reason = st.text_area("Reason for reversal (required)", key="reverse_reason")
        send_email = st.checkbox("Send email notification", value=True, key="reverse_email")
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("↩️ Confirm Reverse", type="primary", use_container_width=True):
                if not reason.strip():
                    st.error("Please enter a reason")
                else:
                    user_id = user.get('id')
                    result = service.reverse_delivery(selected_id, selected_delivery, reverse_qty, reason, user_id)
                    
                    if result.success:
                        st.success(result.message)
                        if send_email:
                            email_service.send_reversed_email(selected_id, reverse_qty, reason, user_id)
                        st.session_state['mgmt_show_reverse'] = False
                        st.session_state['mgmt_search_results'] = None
                        st.rerun()
                    else:
                        st.error(result.message)
        
        with col_btn2:
            if st.button("Back", use_container_width=True, key="reverse_back"):
                st.session_state['mgmt_show_reverse'] = False
                st.rerun()


def render_bulk_etd_modal():
    """Render bulk ETD update modal"""
    if not st.session_state.get('mgmt_show_bulk_etd'):
        return
    
    selected_ids = st.session_state.get('mgmt_selected_ids', [])
    
    with st.container():
        st.markdown("---")
        st.subheader(f"📅 Bulk Update ETD ({len(selected_ids)} allocations)")
        
        allocations_df = data.get_allocations_by_ids(selected_ids)
        st.dataframe(
            allocations_df[['id', 'allocation_number', 'product_name', 'allocated_etd']],
            hide_index=True, use_container_width=True, height=200
        )
        
        new_etd = st.date_input("New ETD for all", value=date.today() + timedelta(days=7), min_value=date.today(), key="bulk_etd_input")
        reason = st.text_area("Reason (required)", key="bulk_etd_reason")
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("✓ Update All", type="primary", use_container_width=True):
                if not reason.strip():
                    st.error("Please enter a reason")
                else:
                    user_id = user.get('id')
                    result = service.bulk_update_etd(selected_ids, new_etd, reason, user_id)
                    st.success(result.message) if result.success else st.warning(result.message)
                    st.session_state['mgmt_show_bulk_etd'] = False
                    st.session_state['mgmt_selected_ids'] = []
                    st.session_state['mgmt_search_results'] = None
                    st.rerun()
        
        with col_btn2:
            if st.button("Cancel", use_container_width=True, key="bulk_etd_cancel"):
                st.session_state['mgmt_show_bulk_etd'] = False
                st.rerun()


def render_bulk_cancel_modal():
    """Render bulk cancel modal"""
    if not st.session_state.get('mgmt_show_bulk_cancel'):
        return
    
    selected_ids = st.session_state.get('mgmt_selected_ids', [])
    
    with st.container():
        st.markdown("---")
        st.subheader(f"❌ Bulk Cancel ({len(selected_ids)} allocations)")
        st.error("⚠️ This will cancel all remaining quantity from selected allocations")
        
        allocations_df = data.get_allocations_by_ids(selected_ids)
        st.dataframe(
            allocations_df[['id', 'allocation_number', 'product_name', 'allocated_qty', 'remaining_qty']],
            hide_index=True, use_container_width=True, height=200
        )
        
        total_cancel = allocations_df['remaining_qty'].sum()
        st.info(f"Total to cancel: {total_cancel:,.2f}")
        
        reason_category = st.selectbox("Category", ["CUSTOMER_REQUEST", "SUPPLY_ISSUE", "QUALITY_ISSUE", "BUSINESS_DECISION", "OTHER"], key="bulk_cancel_cat")
        reason = st.text_area("Reason (required)", key="bulk_cancel_reason")
        confirm = st.checkbox("I confirm I want to cancel all selected allocations", key="bulk_cancel_confirm")
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("❌ Cancel All", type="primary", use_container_width=True, disabled=not confirm):
                if not reason.strip():
                    st.error("Please enter a reason")
                else:
                    user_id = user.get('id')
                    result = service.bulk_cancel(selected_ids, reason, reason_category, user_id)
                    st.success(result.message) if result.success else st.warning(result.message)
                    st.session_state['mgmt_show_bulk_cancel'] = False
                    st.session_state['mgmt_selected_ids'] = []
                    st.session_state['mgmt_search_results'] = None
                    st.rerun()
        
        with col_btn2:
            if st.button("Back", use_container_width=True, key="bulk_cancel_back"):
                st.session_state['mgmt_show_bulk_cancel'] = False
                st.rerun()


# ================================================================
# MAIN
# ================================================================

def main():
    """Main page function"""
    render_header()
    render_search_panel()
    render_results_table()
    render_detail_panel()
    
    # Modals
    render_update_qty_modal()
    render_update_etd_modal()
    render_cancel_modal()
    render_reverse_modal()
    render_bulk_etd_modal()
    render_bulk_cancel_modal()


if __name__ == "__main__":
    main()
