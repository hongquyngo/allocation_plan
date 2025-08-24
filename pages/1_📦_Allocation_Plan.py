"""
Allocation Planning System - Product Centric View
Redesigned for better UX with product-first approach
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
from sqlalchemy import text

# Import utilities
from utils.auth import AuthManager
from utils.config import config
from utils.allocation.data_service import AllocationDataService
from utils.allocation.allocation_service import AllocationService
from utils.allocation.formatters import (
    format_number, format_date, format_status, 
    format_percentage, format_currency
)
from utils.allocation.validators import AllocationValidator

# Setup logging
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Allocation Planning",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize services
auth = AuthManager()
data_service = AllocationDataService()
allocation_service = AllocationService()
validator = AllocationValidator()

# Check authentication
if not auth.check_session():
    st.warning("⚠️ Please login to access this page")
    st.switch_page("app.py")
    st.stop()

# Initialize session state
if 'selected_product' not in st.session_state:
    st.session_state.selected_product = None
if 'show_allocation_modal' not in st.session_state:
    st.session_state.show_allocation_modal = False
if 'selected_oc_for_allocation' not in st.session_state:
    st.session_state.selected_oc_for_allocation = None
if 'filters' not in st.session_state:
    st.session_state.filters = {}
if 'expanded_products' not in st.session_state:
    st.session_state.expanded_products = set()
if 'page_number' not in st.session_state:
    st.session_state.page_number = 1
if 'show_advanced_filters' not in st.session_state:
    st.session_state.show_advanced_filters = False
if 'user_id' not in st.session_state:
    # Assuming user_id is stored in session by auth module
    st.session_state.user_id = st.session_state.get('authenticated_user_id', 1)  # Default to 1 if not found

# Constants
ITEMS_PER_PAGE = 50

# Header
col1, col2 = st.columns([6, 1])
with col1:
    st.title("📦 Allocation Planning System")
    st.caption("Product-centric view for efficient allocation management")
with col2:
    if st.button("🚪 Logout", use_container_width=True):
        auth.logout()
        st.switch_page("app.py")

# User info
st.caption(f"👤 {auth.get_user_display_name()} | 🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}")

def get_supply_status_indicator(total_demand, total_supply):
    """Get visual status indicator for supply vs demand"""
    if total_supply >= total_demand:
        return "🟢", "Sufficient"
    elif total_supply >= total_demand * 0.5:
        return "🟡", "Partial"
    elif total_supply > 0:
        return "🔴", "Low"
    else:
        return "⚫", "No Supply"

def show_metrics_row():
    """Display key metrics in a row"""
    try:
        metrics = data_service.get_dashboard_metrics_product_view()
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Total Products",
                format_number(metrics.get('total_products', 0)),
                help="Products with pending demand"
            )
        
        with col2:
            st.metric(
                "Total Demand",
                format_number(metrics.get('total_demand_qty', 0)),
                help="Total quantity required"
            )
        
        with col3:
            st.metric(
                "Total Supply",
                format_number(metrics.get('total_supply_qty', 0)),
                help="Total available from all sources"
            )
        
        with col4:
            st.metric(
                "🔴 Critical Items",
                format_number(metrics.get('critical_products', 0)),
                help="Products with <20% supply coverage"
            )
        
        with col5:
            st.metric(
                "⚠️ Urgent ETD",
                format_number(metrics.get('urgent_etd_count', 0)),
                help="OCs with ETD in next 7 days"
            )
    except Exception as e:
        logger.error(f"Error loading metrics: {e}")
        st.error(f"Error loading metrics: {str(e)}")
        # Show empty metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        for col in [col1, col2, col3, col4, col5]:
            with col:
                st.metric("Loading...", "---")

def show_search_and_filters():
    """Display search bar and filter controls"""
    # Initialize state
    if 'show_advanced_filters' not in st.session_state:
        st.session_state.show_advanced_filters = False
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_query = st.text_input(
            "🔍 Search products, PT code, customer...",
            placeholder="Type to search...",
            label_visibility="collapsed",
            key="search_input"
        )
        if search_query:
            st.session_state.filters['search'] = search_query
        else:
            st.session_state.filters.pop('search', None)
    
    with col2:
        if st.button("⚙️ Advanced Filters", use_container_width=True):
            st.session_state.show_advanced_filters = not st.session_state.show_advanced_filters
    
    # Advanced filters panel
    if st.session_state.show_advanced_filters:
        with st.expander("Advanced Filters", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Customer filter
                customers = data_service.get_customer_list()
                if not customers.empty:
                    selected_customer = st.selectbox(
                        "Customer",
                        options=['All'] + customers['customer_name'].tolist(),
                        key="filter_customer"
                    )
                    if selected_customer != 'All':
                        customer_code = customers[customers['customer_name'] == selected_customer]['customer_code'].iloc[0]
                        st.session_state.filters['customer'] = customer_code
                    else:
                        st.session_state.filters.pop('customer', None)
                
                # Brand filter
                brands = data_service.get_brand_list()
                if not brands.empty:
                    selected_brand = st.selectbox(
                        "Brand",
                        options=['All'] + brands['brand_name'].tolist(),
                        key="filter_brand"
                    )
                    if selected_brand != 'All':
                        brand_id = brands[brands['brand_name'] == selected_brand]['brand_id'].iloc[0]
                        st.session_state.filters['brand'] = brand_id
                    else:
                        st.session_state.filters.pop('brand', None)
            
            with col2:
                # ETD range filter
                etd_option = st.selectbox(
                    "ETD Range",
                    ["All dates", "Next 7 days", "Next 14 days", "Next 30 days", "Custom range"],
                    key="filter_etd_range"
                )
                
                if etd_option == "Next 7 days":
                    st.session_state.filters['etd_days'] = 7
                elif etd_option == "Next 14 days":
                    st.session_state.filters['etd_days'] = 14
                elif etd_option == "Next 30 days":
                    st.session_state.filters['etd_days'] = 30
                elif etd_option == "Custom range":
                    date_from = st.date_input("From date", key="filter_date_from")
                    date_to = st.date_input("To date", key="filter_date_to")
                    st.session_state.filters['date_from'] = date_from
                    st.session_state.filters['date_to'] = date_to
                else:
                    st.session_state.filters.pop('etd_days', None)
                    st.session_state.filters.pop('date_from', None)
                    st.session_state.filters.pop('date_to', None)
            
            with col3:
                # Supply coverage filter
                coverage_option = st.selectbox(
                    "Supply Coverage",
                    ["All", "Critical (<20%)", "Low (<50%)", "Partial (50-99%)", "Full (≥100%)"],
                    key="filter_coverage"
                )
                
                if coverage_option != "All":
                    st.session_state.filters['coverage'] = coverage_option
                else:
                    st.session_state.filters.pop('coverage', None)
            
            # Apply filters button
            if st.button("Apply Filters", type="primary", use_container_width=True):
                st.session_state.page_number = 1
                st.rerun()
    
    # Quick filters - với visual feedback
    st.markdown("**Quick Filters:**")
    filter_cols = st.columns(5)
    
    # Helper function to show active state
    def is_filter_active(filter_type):
        if filter_type == 'all':
            return len(st.session_state.filters) == 0
        elif filter_type == 'low_supply':
            return st.session_state.filters.get('supply_status') == 'low'
        elif filter_type == 'urgent':
            return st.session_state.filters.get('etd_urgency') == 'urgent'
        elif filter_type == 'not_allocated':
            return st.session_state.filters.get('allocation_status') == 'none'
        elif filter_type == 'has_inventory':
            return st.session_state.filters.get('has_inventory') == True
        return False
    
    with filter_cols[0]:
        button_type = "primary" if is_filter_active('all') else "secondary"
        if st.button("All", use_container_width=True, type=button_type):
            st.session_state.filters = {}
            st.session_state.page_number = 1
            st.rerun()
    
    with filter_cols[1]:
        button_type = "primary" if is_filter_active('low_supply') else "secondary"
        if st.button("⚠️ Low Supply", use_container_width=True, type=button_type):
            st.session_state.filters = {'supply_status': 'low'}
            st.session_state.page_number = 1
            st.rerun()
    
    with filter_cols[2]:
        button_type = "primary" if is_filter_active('urgent') else "secondary"
        if st.button("🔴 Urgent ETD", use_container_width=True, type=button_type):
            st.session_state.filters = {'etd_urgency': 'urgent'}
            st.session_state.page_number = 1
            st.rerun()
    
    with filter_cols[3]:
        button_type = "primary" if is_filter_active('not_allocated') else "secondary"
        if st.button("❌ Not Allocated", use_container_width=True, type=button_type):
            st.session_state.filters = {'allocation_status': 'none'}
            st.session_state.page_number = 1
            st.rerun()
    
    with filter_cols[4]:
        button_type = "primary" if is_filter_active('has_inventory') else "secondary"
        if st.button("📦 Has Inventory", use_container_width=True, type=button_type):
            st.session_state.filters = {'has_inventory': True}
            st.session_state.page_number = 1
            st.rerun()
    
    # Show active filters với clear button cho từng filter
    if st.session_state.filters:
        st.markdown("**Active Filters:**")
        filter_container = st.container()
        with filter_container:
            cols = st.columns([1, 1, 1, 1, 1])
            col_idx = 0
            
            for key, value in st.session_state.filters.items():
                if key != 'search' and col_idx < 5:
                    with cols[col_idx]:
                        filter_label = {
                            'supply_status': '⚠️ Low Supply',
                            'etd_urgency': '🔴 Urgent ETD',
                            'allocation_status': '❌ Not Allocated',
                            'has_inventory': '📦 Has Inventory',
                            'customer': f'Customer: {value}',
                            'brand': f'Brand: {value}',
                            'etd_days': f'ETD: Next {value} days',
                            'coverage': f'Coverage: {value}'
                        }.get(key, f"{key}: {value}")
                        
                        if st.button(f"{filter_label} ✕", key=f"clear_{key}"):
                            st.session_state.filters.pop(key)
                            st.session_state.page_number = 1
                            st.rerun()
                    col_idx += 1
            
            # Clear all button
            if st.button("Clear All Filters", type="secondary"):
                st.session_state.filters = {}
                st.session_state.page_number = 1
                st.rerun()

def show_product_list():
    """Display product list with demand/supply summary"""
    # Debug info (remove in production)
    with st.expander("Debug Info", expanded=False):
        st.write("Current Filters:", st.session_state.filters)
        st.write("Page Number:", st.session_state.page_number)
        
        # Test database connection
        try:
            test_query = """
                SELECT COUNT(*) as product_count FROM products WHERE delete_flag = 0
            """
            with data_service.engine.connect() as conn:
                result = conn.execute(text(test_query)).fetchone()
                st.write(f"Total products in database: {result[0] if result else 'Error'}")
                
            # Test OC view
            test_oc_query = """
                SELECT COUNT(*) as oc_count 
                FROM outbound_oc_pending_delivery_view 
                WHERE pending_standard_delivery_quantity > 0
            """
            with data_service.engine.connect() as conn:
                result = conn.execute(text(test_oc_query)).fetchone()
                st.write(f"Total pending OCs: {result[0] if result else 'Error'}")
        except Exception as e:
            st.error(f"Database test error: {str(e)}")
    
    # Get product data
    try:
        products_df = data_service.get_products_with_demand_supply(
            filters=st.session_state.filters,
            page=st.session_state.page_number,
            page_size=ITEMS_PER_PAGE
        )
        
        # Debug: Show query result info
        if products_df is not None:
            st.caption(f"Found {len(products_df)} products")
    except Exception as e:
        st.error(f"Error loading products: {str(e)}")
        logger.error(f"Error in show_product_list: {e}")
        products_df = pd.DataFrame()
    
    if products_df.empty:
        # Show more helpful message
        st.info("No products found with current filters")
        
        # Suggest actions
        if st.session_state.filters:
            st.write("Try:")
            st.write("- Clearing some filters")
            st.write("- Using different search terms")
            st.write("- Checking if there are pending orders in the selected date range")
            
            if st.button("Clear All Filters and Retry"):
                st.session_state.filters = {}
                st.session_state.page_number = 1
                st.rerun()
        else:
            st.write("No products with pending demand found.")
            st.write("This could mean:")
            st.write("- All orders are already fully allocated")
            st.write("- No pending orders in the system")
            st.write("- Data sync issue")
        return
    
    # Create header
    header_cols = st.columns([3, 1.5, 1.5, 0.5, 0.5])
    with header_cols[0]:
        st.markdown("**PRODUCT INFO**")
    with header_cols[1]:
        st.markdown("**TOTAL DEMAND**")
    with header_cols[2]:
        st.markdown("**TOTAL SUPPLY**")
    with header_cols[3]:
        st.markdown("**STATUS**")
    with header_cols[4]:
        st.markdown("**ACTION**")
    
    st.divider()
    
    # Display each product
    for idx, row in products_df.iterrows():
        product_id = row['product_id']
        is_expanded = product_id in st.session_state.expanded_products
        
        # Main product row
        cols = st.columns([3, 1.5, 1.5, 0.5, 0.5])
        
        with cols[0]:
            # Product info with expand/collapse button
            if st.button(
                f"{'▼' if is_expanded else '▶'} {row['product_name']}", 
                key=f"expand_{product_id}",
                use_container_width=True,
                type="secondary"
            ):
                if is_expanded:
                    st.session_state.expanded_products.remove(product_id)
                else:
                    st.session_state.expanded_products.add(product_id)
                st.rerun()
            
            st.caption(f"{row['pt_code']} | {row['package_size']} {row['standard_uom']}/box | {row['standard_uom']}")
        
        with cols[1]:
            # Demand info
            st.markdown(f"**{format_number(row['total_demand'])} {row['standard_uom']}**")
            st.caption(f"{row['oc_count']} OCs pending")
        
        with cols[2]:
            # Supply info
            st.markdown(f"**{format_number(row['total_supply'])} {row['standard_uom']}**")
            supply_breakdown = []
            if row['inventory_qty'] > 0:
                supply_breakdown.append(f"Inv: {format_number(row['inventory_qty'])}")
            if row['can_qty'] > 0:
                supply_breakdown.append(f"CAN: {format_number(row['can_qty'])}")
            if row['po_qty'] > 0:
                supply_breakdown.append(f"PO: {format_number(row['po_qty'])}")
            if row.get('wht_qty', 0) > 0:
                supply_breakdown.append(f"WHT: {format_number(row['wht_qty'])}")
            st.caption(" | ".join(supply_breakdown) if supply_breakdown else "No supply")
        
        with cols[3]:
            # Status indicator
            indicator, status = get_supply_status_indicator(row['total_demand'], row['total_supply'])
            st.markdown(f"{indicator}")
        
        with cols[4]:
            # Action button
            if st.button("View", key=f"view_{product_id}", use_container_width=True):
                st.session_state.selected_product = row.to_dict()
                st.session_state.expanded_products.add(product_id)
                st.rerun()
        
        # Expanded details
        if is_expanded:
            show_product_details(row)
        
        st.divider()
    
    # Pagination
    show_pagination(products_df)

def show_product_details(product_row):
    """Show expanded product details with OCs and supply sources"""
    with st.container():
        # Tabs for Demand and Supply
        tab1, tab2 = st.tabs(["📋 Demand (Order Confirmations)", "📦 Supply (Available Sources)"])
        
        with tab1:
            show_product_demand_details(product_row['product_id'])
        
        with tab2:
            show_product_supply_details(product_row['product_id'])

def show_product_demand_details(product_id):
    """Show OCs for a product"""
    ocs_df = data_service.get_ocs_by_product(product_id)
    
    if ocs_df.empty:
        st.info("No pending OCs for this product")
        return
    
    # Create OC table
    for idx, oc in ocs_df.iterrows():
        cols = st.columns([2, 2, 1, 1, 1, 1.5])
        
        with cols[0]:
            st.text(f"📄 {oc['oc_number']}")
        
        with cols[1]:
            st.text(f"🏢 {oc['customer']}")
        
        with cols[2]:
            etd_days = (pd.to_datetime(oc['etd']).date() - datetime.now().date()).days
            etd_color = "🔴" if etd_days <= 7 else "🟡" if etd_days <= 14 else ""
            st.text(f"{etd_color} {format_date(oc['etd'])}")
        
        with cols[3]:
            st.text(f"{format_number(oc['pending_quantity'])}")
        
        with cols[4]:
            if oc['allocated_quantity'] > 0:
                coverage = (oc['allocated_quantity'] / oc['pending_quantity'] * 100)
                st.text(f"{format_percentage(coverage)}")
            else:
                st.text("0%")
        
        with cols[5]:
            if st.button("Allocate", key=f"alloc_oc_{oc['ocd_id']}", use_container_width=True, type="primary"):
                st.session_state.selected_oc_for_allocation = oc.to_dict()
                st.session_state.show_allocation_modal = True
                st.rerun()

def show_product_supply_details(product_id):
    """Show supply sources for a product"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("**📦 Inventory**")
        inventory_df = data_service.get_inventory_summary(product_id)
        if not inventory_df.empty:
            for _, inv in inventory_df.iterrows():
                st.metric(
                    f"Batch {inv['batch_number']}",
                    format_number(inv['available_quantity']),
                    delta=f"Exp: {format_date(inv['expiry_date'])}"
                )
        else:
            st.caption("No inventory")
    
    with col2:
        st.markdown("**🚢 Pending CAN**")
        can_df = data_service.get_can_summary(product_id)
        if not can_df.empty:
            for _, can in can_df.iterrows():
                st.metric(
                    can['arrival_note_number'],
                    format_number(can['pending_quantity']),
                    delta=f"Arr: {format_date(can['arrival_date'])}"
                )
        else:
            st.caption("No pending CAN")
    
    with col3:
        st.markdown("**📋 Pending PO**")
        po_df = data_service.get_po_summary(product_id)
        if not po_df.empty:
            for _, po in po_df.iterrows():
                st.metric(
                    po['po_number'],
                    format_number(po['pending_quantity']),
                    delta=f"ETD: {format_date(po['etd'])}"
                )
        else:
            st.caption("No pending PO")
    
    with col4:
        st.markdown("**🚚 WH Transfer**")
        wht_df = data_service.get_wht_summary(product_id)
        if not wht_df.empty:
            for _, wht in wht_df.iterrows():
                st.metric(
                    f"{wht['from_warehouse']} → {wht['to_warehouse']}",
                    format_number(wht['transfer_quantity']),
                    delta=wht['status']
                )
        else:
            st.caption("No transfers")

def show_pagination(df):
    """Show pagination controls"""
    if len(df) == ITEMS_PER_PAGE:
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            if st.session_state.page_number > 1:
                if st.button("← Previous", use_container_width=True):
                    st.session_state.page_number -= 1
                    st.rerun()
        
        with col2:
            st.markdown(f"<center>Page {st.session_state.page_number}</center>", unsafe_allow_html=True)
        
        with col3:
            if st.button("Next →", use_container_width=True):
                st.session_state.page_number += 1
                st.rerun()

@st.dialog("Create Allocation", width="large")
def show_allocation_modal():
    """Simple allocation modal with checkbox selection"""
    oc = st.session_state.selected_oc_for_allocation
    
    if not oc:
        st.error("No OC selected")
        return
    
    # Header info
    st.markdown(f"### Allocate to {oc['oc_number']}")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Customer", oc['customer'])
    with col2:
        st.metric("Product", oc['product_name'][:30])
    with col3:
        st.metric("Required", f"{format_number(oc['pending_quantity'])} {oc.get('standard_uom', 'pcs')}")
    
    st.divider()
    
    # Get available supply
    supply_details = data_service.get_all_supply_for_product(oc['product_id'])
    
    if supply_details.empty:
        st.error("❌ No available supply for this product")
        if st.button("Close"):
            st.session_state.show_allocation_modal = False
            st.rerun()
        return
    
    # Supply selection
    st.markdown("**Available Supply:**")
    
    selected_supplies = []
    total_selected = 0
    
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
                    # Format supply info based on type
                    if source_type == 'INVENTORY':
                        info = f"Batch {supply['batch_number']} - Exp: {format_date(supply['expiry_date'])}"
                    elif source_type == 'PENDING_CAN':
                        info = f"{supply['arrival_note_number']} - Arr: {format_date(supply['arrival_date'])}"
                    elif source_type == 'PENDING_PO':
                        info = f"{supply['po_number']} - ETD: {format_date(supply['etd'])}"
                    else:
                        info = f"{supply['from_warehouse']} → {supply['to_warehouse']}"
                    
                    selected = st.checkbox(
                        f"{info} ({format_number(supply['available_quantity'])} {supply.get('uom', 'pcs')})",
                        key=f"supply_{supply['source_id']}_{source_type}"
                    )
                
                with col2:
                    if selected:
                        max_qty = min(supply['available_quantity'], oc['pending_quantity'] - total_selected)
                        qty = st.number_input(
                            "Qty",
                            min_value=0.0,
                            max_value=float(max_qty),
                            value=float(max_qty),
                            step=1.0,
                            key=f"qty_{supply['source_id']}_{source_type}",
                            label_visibility="collapsed"
                        )
                        
                        if qty > 0:
                            selected_supplies.append({
                                'source_type': source_type,
                                'source_id': supply['source_id'],
                                'quantity': qty,
                                'supply_info': supply.to_dict()
                            })
                            total_selected += qty
    
    st.divider()
    
    # SOFT allocation option
    st.markdown("**OR**")
    use_soft = st.checkbox("🔄 SOFT Allocation (no specific source)")
    
    if use_soft:
        soft_qty = st.number_input(
            "Quantity",
            min_value=0.0,
            max_value=float(oc['pending_quantity']),
            value=0.0,
            step=1.0
        )
        
        if soft_qty > 0:
            selected_supplies = [{
                'source_type': None,
                'source_id': None,
                'quantity': soft_qty,
                'supply_info': {'type': 'SOFT', 'description': 'No specific source'}
            }]
            total_selected = soft_qty
    
    st.divider()
    
    # Summary
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Selected", format_number(total_selected))
    with col2:
        coverage = (total_selected / oc['pending_quantity'] * 100) if oc['pending_quantity'] > 0 else 0
        st.metric("Coverage", format_percentage(coverage))
    
    # Additional fields
    allocated_etd = st.date_input("Allocated ETD", value=oc['etd'])
    notes = st.text_area("Notes (optional)")
    
    # Action buttons
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("💾 Save Allocation", type="primary", use_container_width=True, disabled=total_selected == 0):
            # Validate allocation
            errors = []
            
            # Basic validation
            if total_selected > oc['pending_quantity']:
                errors.append(f"Cannot allocate more than required ({format_number(oc['pending_quantity'])})")
            
            if not use_soft:
                # Check supply availability
                for supply in selected_supplies:
                    availability = data_service.check_supply_availability(
                        supply['source_type'],
                        supply['source_id'],
                        oc['product_id']
                    )
                    if not availability['available']:
                        errors.append(f"Supply {supply['source_type']} is no longer available")
                    elif supply['quantity'] > availability['available_qty']:
                        errors.append(f"Insufficient {supply['source_type']} (available: {availability['available_qty']})")
            
            if errors:
                for error in errors:
                    st.error(f"❌ {error}")
            else:
                # Save allocation
                result = allocation_service.create_allocation(
                    oc_detail_id=oc['ocd_id'],
                    allocations=selected_supplies,
                    mode='SOFT' if use_soft else 'HARD',
                    etd=allocated_etd,
                    notes=notes,
                    user_id=st.session_state.user_id
                )
                
                if result['success']:
                    st.success(f"✅ Allocation Successful\nAllocated: {format_number(total_selected)} {oc.get('standard_uom', 'pcs')} to {oc['oc_number']}\nAllocation Number: {result['allocation_number']}")
                    st.balloons()
                    
                    # Close modal after short delay
                    import time
                    time.sleep(2)
                    st.session_state.show_allocation_modal = False
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"❌ {result['error']}")
    
    with col2:
        if st.button("Cancel", use_container_width=True):
            st.session_state.show_allocation_modal = False
            st.rerun()

# Main UI
show_metrics_row()
st.divider()
show_search_and_filters()
st.divider()
show_product_list()

# Show allocation modal if needed
if st.session_state.show_allocation_modal:
    show_allocation_modal()