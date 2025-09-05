"""
Allocation Planning System - Cleaned Version
Product-centric view with dual UOM display and proper delivery tracking
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import time

# Import utilities
from utils.auth import AuthManager
from utils.config import config
from utils.allocation.data_service import AllocationDataService
from utils.allocation.allocation_service import AllocationService
from utils.allocation.formatters import (
    format_number, format_date, 
    format_percentage, format_allocation_mode,
    format_reason_category
)
from utils.allocation.validators import AllocationValidator
from utils.allocation.uom_converter import UOMConverter

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
uom_converter = UOMConverter()

# Check authentication
if not auth.check_session():
    st.warning("⚠️ Please login to access this page")
    st.switch_page("app.py")
    st.stop()

# ==================== SESSION STATE MANAGEMENT ====================
DEFAULT_SESSION_STATE = {
    'modals': {
        'allocation': False,
        'cancel': False,
        'update_etd': False,
        'reverse': False,
        'history': False
    },
    'selections': {
        'product': None,
        'oc_for_allocation': None,
        'oc_for_history': None,
        'oc_info': None,
        'allocation_for_cancel': None,
        'allocation_for_update': None,
        'cancellation_for_reverse': None
    },
    'filters': {},
    'ui': {
        'page_number': 1,
        'expanded_products': set()
    },
    'user': {
        'id': None,
        'role': 'viewer'
    },
    'context': {
        'return_to_history': None
    }
}

def init_session_state():
    """Initialize session state with default values"""
    if 'state_initialized' not in st.session_state:
        for key, value in DEFAULT_SESSION_STATE.items():
            if key not in st.session_state:
                st.session_state[key] = value.copy() if isinstance(value, (dict, set)) else value
        st.session_state.state_initialized = True
    
    # Ensure modal states are properly initialized
    if 'modals' not in st.session_state:
        st.session_state.modals = DEFAULT_SESSION_STATE['modals'].copy()
    
    # Set user info
    if st.session_state.user['id'] is None:
        st.session_state.user['id'] = st.session_state.get('authenticated_user_id', 1)
    if st.session_state.user['role'] == 'viewer':
        st.session_state.user['role'] = st.session_state.get('user_role', 'viewer')

# Initialize session state
init_session_state()

# Constants
ITEMS_PER_PAGE = config.get_app_setting('ITEMS_PER_PAGE', 50)

# ==================== HEADER ====================
def show_header():
    """Display page header"""
    col1, col2 = st.columns([6, 1])
    with col1:
        st.title("📦 Allocation Planning System")
        st.caption("Product-centric view with complete allocation management")
    with col2:
        if st.button("🚪 Logout", use_container_width=True):
            reset_all_modals()
            auth.logout()
            st.switch_page("app.py")

    st.caption(f"👤 {auth.get_user_display_name()} ({st.session_state.user['role']}) | 🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# ==================== METRICS ====================
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
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            st.metric(
                "Total Products",
                format_number(metrics.get('total_products', 0)),
                help="Number of unique products with pending customer orders"
            )
        
        with col2:
            st.metric(
                "Total Demand",
                format_number(metrics.get('total_demand_qty', 0)),
                help="Total quantity required across all pending orders (standard UOM)"
            )
        
        with col3:
            st.metric(
                "Total Supply",
                format_number(metrics.get('total_supply_qty', 0)),
                help="Total available quantity from all sources"
            )
        
        with col4:
            st.metric(
                "🔴 Critical Items",
                format_number(metrics.get('critical_products', 0)),
                help="Products where available supply is less than 20% of demand"
            )
        
        with col5:
            st.metric(
                "⚠️ Urgent ETD",
                format_number(metrics.get('urgent_etd_count', 0)),
                help="Products with at least one order due within the next 7 days"
            )
        
        with col6:
            st.metric(
                "⚡ Over-Allocated",
                format_number(metrics.get('over_allocated_count', 0)),
                help="Number of orders that are over-allocated",
                delta="Needs attention" if metrics.get('over_allocated_count', 0) > 0 else None
            )
    except Exception as e:
        logger.error(f"Error loading metrics: {e}")
        st.error(f"Error loading metrics: {str(e)}")

# ==================== SEARCH AND FILTERS ====================

def show_search_bar():
    """Display search bar with autocomplete"""
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_container = st.container()
        with search_container:
            search_col, clear_col = st.columns([10, 1])
            
            # Check if we need to clear the search
            if 'clear_search_flag' in st.session_state and st.session_state.clear_search_flag:
                search_value = ""
                del st.session_state.clear_search_flag
            else:
                search_value = st.session_state.get('search_input', '')
            
            with search_col:
                search_query = st.text_input(
                    "🔍 Search",
                    placeholder="Search by product name, PT code, brand, customer, OC number, or package size...",
                    key="search_input",
                    value=search_value,
                    help="Type at least 2 characters to see suggestions",
                    label_visibility="collapsed"
                )
            
            with clear_col:
                if search_query:
                    if st.button("✖", key="clear_search", help="Clear search"):
                        # Set a flag to clear the search on rerun
                        st.session_state.clear_search_flag = True
                        st.session_state.filters.pop('search', None)
                        st.rerun()
            
            # Show suggestions
            if search_query and len(search_query) >= 2:
                show_search_suggestions(search_query)
            
            if search_query:
                st.session_state.filters['search'] = search_query
            else:
                st.session_state.filters.pop('search', None)

def show_search_suggestions(search_query):
    """Display search suggestions"""
    suggestions = data_service.get_search_suggestions(search_query)
    
    has_suggestions = any(suggestions.values())
    
    if has_suggestions:
        with st.container():
            # Display suggestions by category
            if suggestions['products']:
                st.markdown("**📦 Products**")
                for idx, product in enumerate(suggestions['products'][:5]):
                    if st.button(
                        product, 
                        key=f"prod_suggest_{idx}",
                        use_container_width=True
                    ):
                        product_name = product.split(" | ")[0]
                        st.session_state.search_input = product_name
                        reset_all_modals()
                        st.rerun()
            
            if suggestions['brands']:
                st.markdown("**🏷️ Brands**")
                for idx, brand in enumerate(suggestions['brands'][:5]):
                    if st.button(
                        brand,
                        key=f"brand_suggest_{idx}",
                        use_container_width=True
                    ):
                        st.session_state.search_input = brand
                        reset_all_modals()
                        st.rerun()
            
            if suggestions['customers']:
                st.markdown("**🏢 Customers**")
                for idx, customer in enumerate(suggestions['customers'][:5]):
                    if st.button(
                        customer,
                        key=f"cust_suggest_{idx}",
                        use_container_width=True
                    ):
                        st.session_state.search_input = customer
                        reset_all_modals()
                        st.rerun()
            
            if suggestions['oc_numbers']:
                st.markdown("**📄 OC Numbers**")
                for idx, oc in enumerate(suggestions['oc_numbers'][:5]):
                    if st.button(
                        oc,
                        key=f"oc_suggest_{idx}",
                        use_container_width=True
                    ):
                        st.session_state.search_input = oc
                        reset_all_modals()
                        st.rerun()

# ==================== QUICK FILTERS ====================
def show_quick_filters():
    """Display quick filter buttons"""
    st.markdown("**Quick Filters:**")
    filter_cols = st.columns(6)
    
    quick_filter_buttons = [
        ("All", "all", None),
        ("⚠️ Low Supply", "low_supply", {'supply_status': 'low'}),
        ("🔴 Urgent ETD", "urgent", {'etd_urgency': 'urgent'}),
        ("⏳ Not Allocated", "not_allocated", {'allocation_status': 'none'}),
        ("📦 Has Inventory", "has_inventory", {'has_inventory': True}),
        ("⚡ Over Allocated", "over_allocated", {'over_allocated': True})
    ]
    
    for idx, (label, filter_type, filter_value) in enumerate(quick_filter_buttons):
        with filter_cols[idx]:
            is_active = is_filter_active(filter_type)
            button_type = "primary" if is_active else "secondary"
            
            if st.button(label, use_container_width=True, type=button_type):
                if filter_type == "all":
                    search_filter = st.session_state.filters.get('search')
                    st.session_state.filters = {}
                    if search_filter:
                        st.session_state.filters['search'] = search_filter
                else:
                    st.session_state.filters.update(filter_value)
                
                st.session_state.ui['page_number'] = 1
                reset_all_modals()
                st.rerun()

def is_filter_active(filter_type):
    """Check if a filter is active"""
    if filter_type == 'all':
        return len([k for k in st.session_state.filters.keys() if k != 'search']) == 0
    elif filter_type == 'low_supply':
        return st.session_state.filters.get('supply_status') == 'low'
    elif filter_type == 'urgent':
        return st.session_state.filters.get('etd_urgency') == 'urgent'
    elif filter_type == 'not_allocated':
        return st.session_state.filters.get('allocation_status') == 'none'
    elif filter_type == 'has_inventory':
        return st.session_state.filters.get('has_inventory') == True
    elif filter_type == 'over_allocated':
        return st.session_state.filters.get('over_allocated') == True
    return False

def show_active_filters():
    """Show active filters with clear buttons"""
    active_filters = [(k, v) for k, v in st.session_state.filters.items() if k != 'search']
    if not active_filters:
        return
    
    st.markdown("**Active Filters:**")
    filter_container = st.container()
    with filter_container:
        display_filters = []
        
        for key, value in active_filters:
            filter_label = get_filter_label(key, value)
            if filter_label:
                display_filters.append((filter_label, key))
        
        cols = st.columns(5)
        for idx, (label, key) in enumerate(display_filters[:5]):
            with cols[idx % 5]:
                if st.button(f"{label} ✕", key=f"clear_{key}"):
                    st.session_state.filters.pop(key, None)
                    st.session_state.ui['page_number'] = 1
                    st.rerun()

def get_filter_label(key, value):
    """Get display label for filter"""
    filter_labels = {
        'supply_status': '⚠️ Low Supply',
        'etd_urgency': '🔴 Urgent ETD',
        'allocation_status': '⏳ Not Allocated',
        'has_inventory': '📦 Has Inventory',
        'over_allocated': '⚡ Over Allocated'
    }
    return filter_labels.get(key, f"{key}: {value}")

# ==================== PRODUCT LIST ====================
def show_product_list():
    """Display product list with demand/supply summary"""
    try:
        products_df = data_service.get_products_with_demand_supply(
            filters=st.session_state.filters,
            page=st.session_state.ui['page_number'],
            page_size=ITEMS_PER_PAGE
        )
        
        if products_df is not None and not products_df.empty:
            st.caption(f"Found {len(products_df)} products (Page {st.session_state.ui['page_number']})")
    except Exception as e:
        st.error(f"Error loading products: {str(e)}")
        logger.error(f"Error in show_product_list: {e}")
        products_df = pd.DataFrame()
    
    if products_df.empty:
        show_empty_state()
        return
    
    show_product_header()
    
    # Display each product
    for idx, row in products_df.iterrows():
        show_product_row(row)
    
    show_pagination(products_df)

def show_empty_state():
    """Show empty state when no products found"""
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.info("No products found with current filters")
        
        if st.session_state.filters:
            st.write("**Try:**")
            st.write("• Clearing some filters")
            st.write("• Using different search terms")
            
            if st.button("🔄 Clear All Filters and Retry", use_container_width=True):
                st.session_state.filters = {}
                st.session_state.ui['page_number'] = 1
                reset_all_modals()
                st.rerun()
        else:
            st.write("**No products with pending demand found**")

def show_product_header():
    """Show product list header"""
    header_cols = st.columns([3.5, 1.5, 1.5, 0.5])
    with header_cols[0]:
        st.markdown("**PRODUCT INFO**")
    with header_cols[1]:
        st.markdown("**TOTAL DEMAND**")
    with header_cols[2]:
        st.markdown("**TOTAL SUPPLY**")
    with header_cols[3]:
        st.markdown("**STATUS**")
    
    st.divider()

def show_product_row(row):
    """Display a single product row"""
    product_id = row['product_id']
    is_expanded = product_id in st.session_state.ui['expanded_products']
    
    # Main product row
    cols = st.columns([3.5, 1.5, 1.5, 0.5])
    
    with cols[0]:
        show_product_info(row, product_id, is_expanded)
    
    with cols[1]:
        show_demand_info(row)
    
    with cols[2]:
        show_supply_info(row)
    
    with cols[3]:
        show_status_indicator(row)
    
    # Expanded details
    if is_expanded:
        # Safety check
        if st.session_state.modals.get('history') and not st.session_state.selections.get('oc_for_history'):
            st.session_state.modals['history'] = False
        
        show_product_details(row)
    
    st.divider()

def show_product_info(row, product_id, is_expanded):
    """Show product information"""
    if st.button(
        f"{'▼' if is_expanded else '▶'} {row['product_name']}", 
        key=f"expand_{product_id}",
        use_container_width=True,
        type="secondary"
    ):
        if is_expanded:
            st.session_state.ui['expanded_products'].remove(product_id)
            reset_all_modals()
        else:
            st.session_state.ui['expanded_products'].add(product_id)
            reset_all_modals()
        st.rerun()

    # Show additional info
    info_parts = [row['pt_code']]
    if pd.notna(row.get('brand_name')):
        info_parts.append(row['brand_name'])
    if pd.notna(row.get('package_size')) and row['package_size']:
        info_parts.append(row['package_size'])
    info_parts.append(row['standard_uom'])
    
    st.caption(" | ".join(info_parts))
    
    # Show customer and OC info
    if pd.notna(row.get('customers')) and row['customers']:
        customer_list = row['customers'].split(', ')
        if len(customer_list) > 2:
            st.caption(f"🏢 {', '.join(customer_list[:2])}... (+{len(customer_list)-2} more)")
        else:
            st.caption(f"🏢 {row['customers']}")
    
    if pd.notna(row.get('oc_numbers')) and row['oc_numbers']:
        oc_list = row['oc_numbers'].split(', ')
        if len(oc_list) > 3:
            st.caption(f"📄 OCs: {', '.join(oc_list[:3])}... (+{len(oc_list)-3} more)")
        else:
            st.caption(f"📄 OCs: {row['oc_numbers']}")
    
    # Show over-allocation warning
    if row.get('has_over_allocation'):
        st.warning(f"⚡ {row.get('over_allocated_count', 0)} OCs are over-allocated")

def show_demand_info(row):
    """Show demand information"""
    st.markdown(f"**{format_number(row['total_demand'])} {row['standard_uom']}**")
    st.caption(f"{row['oc_count']} OCs pending")

def show_supply_info(row):
    """Show supply information"""
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

def show_status_indicator(row):
    """Show status indicator"""
    indicator, status = get_supply_status_indicator(row['total_demand'], row['total_supply'])
    st.markdown(f"{indicator}", help=f"Supply Status: {status}")

def show_product_details(product_row):
    """Show expanded product details with OCs and supply sources"""
    with st.container():
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
    
    # Add headers
    header_cols = st.columns([2, 2, 1, 1.5, 1.5, 1])
    with header_cols[0]:
        st.markdown("**OC Number**")
    with header_cols[1]:
        st.markdown("**Customer**")
    with header_cols[2]:
        st.markdown("**ETD**")
    with header_cols[3]:
        st.markdown("**Pending Qty**")
    with header_cols[4]:
        st.markdown("**Total Allocated**")
    with header_cols[5]:
        st.markdown("**Action**")
    
    # Create OC rows
    for idx, oc in ocs_df.iterrows():
        show_oc_row_dual_uom(oc)

def show_oc_row_dual_uom(oc):
    """Display a single OC row with dual UOM handling"""
    # Check for over-allocation
    over_allocation_type = oc.get('over_allocation_type', 'Normal')
    
    # Show warning với context rõ ràng hơn
    if over_allocation_type == 'Over-Committed':
        over_qty = oc.get('over_committed_qty_standard', 0)
        effective_qty = oc.get('standard_quantity', 0)  # Effective quantity after OC cancellation
        # Lấy phân bổ hiệu lực
        total_allocated = oc.get('total_allocated_qty_standard', 0)
        cancelled_allocated = oc.get('total_allocation_cancelled_qty_standard', 0)
        effective_allocated = total_allocated - cancelled_allocated
        
        if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
            over_qty_selling = uom_converter.convert_quantity(
                over_qty, 'standard', 'selling', oc.get('uom_conversion', '1')
            )
            effective_qty_selling = uom_converter.convert_quantity(
                effective_qty, 'standard', 'selling', oc.get('uom_conversion', '1')
            )
            effective_allocated_selling = uom_converter.convert_quantity(
                effective_allocated, 'standard', 'selling', oc.get('uom_conversion', '1')
            )
            st.error(
                f"⚡ Over-committed by {format_number(over_qty_selling)} {oc.get('selling_uom')} - "
                f"Effective allocation ({format_number(effective_allocated_selling)} {oc.get('selling_uom')}) "
                f"exceeds OC effective quantity ({format_number(effective_qty_selling)} {oc.get('selling_uom')})"
            )
        else:
            st.error(
                f"⚡ Over-committed by {format_number(over_qty)} {oc.get('standard_uom')} - "
                f"Effective allocation ({format_number(effective_allocated)} {oc.get('standard_uom')}) "
                f"exceeds OC effective quantity ({format_number(effective_qty)} {oc.get('standard_uom')})"
            )
    
    elif over_allocation_type == 'Pending-Over-Allocated':
        over_qty = oc.get('pending_over_allocated_qty_standard', 0)
        if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
            over_qty_selling = uom_converter.convert_quantity(
                over_qty, 'standard', 'selling', oc.get('uom_conversion', '1')
            )
            st.warning(f"⚠️ Pending over-allocated by {format_number(over_qty_selling)} {oc.get('selling_uom')} - Undelivered allocation exceeds pending delivery")
        else:
            st.warning(f"⚠️ Pending over-allocated by {format_number(over_qty)} {oc.get('standard_uom')} - Undelivered allocation exceeds pending delivery")
    
    # Row display
    cols = st.columns([2, 2, 1, 1.5, 1.5, 1])
    
    with cols[0]:
        st.text(f"📄 {oc['oc_number']}")
    
    with cols[1]:
        st.text(f"🏢 {oc['customer']}")
    
    with cols[2]:
        show_etd_with_urgency(oc['etd'])
    
    with cols[3]:
        show_pending_quantity_dual_uom(oc)
    
    with cols[4]:
        show_allocated_quantity_dual_uom(oc, over_allocation_type != 'Normal')
    
    with cols[5]:
        # Updated logic based on validation với effective allocation
        pending_qty_standard = oc.get('pending_standard_delivery_quantity', 0)
        total_effective_allocated = oc.get('total_effective_allocated_qty_standard', 0)
        undelivered_allocated_qty = oc.get('undelivered_allocated_qty_standard', 0)
        
        # Check both over-allocation scenarios from the view
        is_over_committed = oc.get('is_over_committed', 'No') == 'Yes'
        is_pending_over_allocated = oc.get('is_pending_over_allocated', 'No') == 'Yes'
        
        # Determine if can allocate more
        can_allocate_more = not (is_over_committed or is_pending_over_allocated)
        
        # Generate appropriate help text
        if is_over_committed:
            help_text = f"Cannot allocate more - Effective allocation ({format_number(total_effective_allocated)} {oc.get('standard_uom')}) exceeds order quantity"
        elif is_pending_over_allocated:
            help_text = f"Cannot allocate more - Undelivered allocation ({format_number(undelivered_allocated_qty)} {oc.get('standard_uom')}) exceeds pending delivery quantity ({format_number(pending_qty_standard)} {oc.get('standard_uom')})"
        else:
            # Can still allocate
            remaining_allowed = pending_qty_standard - undelivered_allocated_qty
            if remaining_allowed > 0:
                help_text = f"Can allocate up to {format_number(remaining_allowed)} {oc.get('standard_uom')} more"
            else:
                help_text = "Fully allocated"
                can_allocate_more = False
        
        button_type = "primary" if can_allocate_more else "secondary"
        
        if st.button(
            "Allocate", 
            key=f"alloc_oc_{oc['ocd_id']}", 
            use_container_width=True, 
            type=button_type,
            disabled=not can_allocate_more,
            help=help_text
        ):
            reset_all_modals()
            st.session_state.selections['oc_for_allocation'] = oc.to_dict()
            st.session_state.modals['allocation'] = True
            st.rerun()

def show_etd_with_urgency(etd):
    """Show ETD with urgency indicator"""
    etd_days = (pd.to_datetime(etd).date() - datetime.now().date()).days
    etd_color = ""
    if etd_days <= 0:
        etd_color = "⚫"  # Overdue
    elif etd_days <= 7:
        etd_color = "🔴"  # Urgent
    elif etd_days <= 14:
        etd_color = "🟡"  # Soon
    
    st.text(f"{etd_color} {format_date(etd)}")

def show_pending_quantity_dual_uom(oc):
    """Show pending quantity with dual UOM display"""
    standard_qty = format_number(oc.get('pending_standard_delivery_quantity', 0))
    standard_uom = oc.get('standard_uom', '')
    selling_qty = format_number(oc['pending_quantity'])
    selling_uom = oc.get('selling_uom', '')
    
    if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
        st.markdown(f"**{standard_qty} {standard_uom}**")
        st.caption(f"= {selling_qty} {selling_uom}")
    else:
        st.markdown(f"**{standard_qty} {standard_uom}**")

def show_allocated_quantity_dual_uom(oc, is_over_allocated):
    """Show allocated quantity with dual UOM display"""
    allocated_qty_standard = oc.get('total_allocated_qty_standard', 0)
    cancelled_qty_standard = oc.get('total_allocation_cancelled_qty', 0)
    
    # Calculate effective allocated
    effective_allocated_standard = allocated_qty_standard - cancelled_qty_standard
    
    standard_uom = oc.get('standard_uom', '')
    selling_uom = oc.get('selling_uom', '')
    allocation_count = oc.get('allocation_count', 0)
    
    if effective_allocated_standard > 0:
        # Build button label
        if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
            effective_allocated_selling = uom_converter.convert_quantity(
                effective_allocated_standard,
                'standard',
                'selling',
                oc.get('uom_conversion', '1')
            )
            button_label = f"{format_number(effective_allocated_standard)} {standard_uom}"
            if allocation_count > 1:
                button_label += f" ({allocation_count})"
            help_text = f"= {format_number(effective_allocated_selling)} {selling_uom}. Click to view allocation history"
        else:
            button_label = f"{format_number(effective_allocated_standard)} {standard_uom}"
            if allocation_count > 1:
                button_label += f" ({allocation_count})"
            help_text = f"Click to view {allocation_count} allocation(s)"
        
        if cancelled_qty_standard > 0:
            help_text += f". Cancelled: {format_number(cancelled_qty_standard)} {standard_uom}"
        
        if st.button(
            button_label, 
            key=f"view_alloc_{oc['ocd_id']}", 
            help=help_text,
            use_container_width=True,
            type="secondary"
        ):
            reset_all_modals()
            st.session_state.modals['history'] = True
            st.session_state.selections['oc_for_history'] = oc['ocd_id']
            st.session_state.selections['oc_info'] = {
                'oc_number': oc['oc_number'],
                'customer': oc['customer'],
                'product_name': oc['product_name'],
                'selling_uom': oc.get('selling_uom', ''),
                'standard_uom': oc.get('standard_uom', ''),
                'pending_quantity': oc['pending_quantity'],
                'pending_standard_delivery_quantity': oc.get('pending_standard_delivery_quantity', 0),
                'allocation_warning': oc.get('allocation_warning', ''),
                'uom_conversion': oc.get('uom_conversion', '1'),
                'over_allocation_type': oc.get('over_allocation_type', 'Normal'),
                'total_allocated_qty_standard': allocated_qty_standard,
                'total_allocation_cancelled_qty': cancelled_qty_standard
            }
            st.rerun()
    else:
        st.text(f"0 {standard_uom}")

def show_product_supply_details(product_id):
    """Show supply sources for a product"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        show_supply_summary(product_id, 'inventory', data_service.get_inventory_summary)
    
    with col2:
        show_supply_summary(product_id, 'can', data_service.get_can_summary)
    
    with col3:
        show_supply_summary(product_id, 'po', data_service.get_po_summary)
    
    with col4:
        show_supply_summary(product_id, 'wht', data_service.get_wht_summary)

def show_supply_summary(product_id, supply_type, data_fetcher):
    """Generic supply summary display"""
    titles = {
        'inventory': '📦 Inventory',
        'can': '🚢 Pending CAN',
        'po': '📋 Pending PO',
        'wht': '🚚 WH Transfer'
    }
    
    st.markdown(f"**{titles.get(supply_type, supply_type)}**")
    
    df = data_fetcher(product_id)
    if not df.empty:
        for _, item in df.iterrows():
            if supply_type == 'inventory':
                label = f"Batch {item['batch_number']}"
                if item.get('warehouse_name'):
                    label += f" | {item['warehouse_name']}"
                st.metric(
                    label,
                    f"{format_number(item['available_quantity'])} {item.get('standard_uom', '')}",
                    delta=f"Exp: {format_date(item['expiry_date'])}"
                )
                if item.get('location'):
                    st.caption(f"📍 Location: {item['location']}")
                    
            elif supply_type == 'can':
                qty_str = f"{format_number(item['pending_quantity'])} {item.get('standard_uom', '')}"
                st.metric(
                    item['arrival_note_number'],
                    qty_str,
                    delta=f"Arr: {format_date(item['arrival_date'])}"
                )
                
            elif supply_type == 'po':
                qty_str = f"{format_number(item['pending_quantity'])} {item.get('standard_uom', '')}"
                st.metric(
                    item['po_number'],
                    qty_str,
                    delta=f"ETD: {format_date(item['etd'])}"
                )
                
            elif supply_type == 'wht':
                st.metric(
                    f"{item['from_warehouse']} → {item['to_warehouse']}",
                    f"{format_number(item['transfer_quantity'])} {item.get('standard_uom', '')}",
                    delta=item['status']
                )
    else:
        captions = {
            'inventory': "No inventory",
            'can': "No pending CAN",
            'po': "No pending PO",
            'wht': "No transfers"
        }
        st.caption(captions.get(supply_type, "No data"))

def show_pagination(df):
    """Show pagination controls"""
    if len(df) == ITEMS_PER_PAGE:
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            if st.session_state.ui['page_number'] > 1:
                if st.button("← Previous", use_container_width=True):
                    st.session_state.ui['page_number'] -= 1
                    reset_all_modals()
                    st.rerun()
        
        with col2:
            st.markdown(f"<center>Page {st.session_state.ui['page_number']}</center>", unsafe_allow_html=True)
        
        with col3:
            if st.button("Next →", use_container_width=True):
                st.session_state.ui['page_number'] += 1
                reset_all_modals()
                st.rerun()

def reset_all_modals():
    """Reset all modal states and selections"""
    st.session_state.modals = {
        'allocation': False,
        'cancel': False,
        'update_etd': False,
        'reverse': False,
        'history': False
    }
    st.session_state.selections['oc_for_allocation'] = None
    st.session_state.selections['oc_for_history'] = None
    st.session_state.selections['oc_info'] = None
    st.session_state.selections['allocation_for_cancel'] = None
    st.session_state.selections['allocation_for_update'] = None
    st.session_state.selections['cancellation_for_reverse'] = None
    st.session_state.context['return_to_history'] = None

# ==================== ALLOCATION MODAL ====================
@st.dialog("Create Allocation", width="large")
def show_allocation_modal():
    """Allocation modal with dual UOM display"""
    oc = st.session_state.selections['oc_for_allocation']
    
    if not oc:
        st.error("No OC selected")
        if st.button("Close"):
            st.session_state.modals['allocation'] = False
            st.session_state.selections['oc_for_allocation'] = None
            st.rerun()
        return
    
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
    
    # Get available supply
    supply_details = data_service.get_all_supply_for_product(oc['product_id'])
    
    if supply_details.empty:
        st.error("⏳ No available supply for this product")
        if st.button("Close"):
            st.session_state.modals['allocation'] = False
            st.session_state.selections['oc_for_allocation'] = None
            st.rerun()
        return
    
    st.info("ℹ️ **Allocation Rule**: All allocations are made in standard UOM to ensure whole container quantities")
    
    # Supply selection
    st.markdown("**Available Supply:**")
    
    selected_supplies = []
    total_selected_standard = 0
    
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
                    info = format_supply_info_dual_uom(supply, source_type, oc)
                    
                    selected = st.checkbox(
                        info,
                        key=f"supply_{supply['source_id']}_{source_type}"
                    )
                
                with col2:
                    if selected:
                        # Calculate remaining requirement
                        pending_standard = oc.get('pending_standard_delivery_quantity', oc['pending_quantity'])
                        max_qty_standard = min(supply['available_quantity'], pending_standard - total_selected_standard)
                        
                        standard_uom = oc.get('standard_uom', 'pcs')
                        
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
        standard_uom = oc.get('standard_uom', 'pcs')
        
        if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
            pending_selling = oc.get('pending_quantity', pending_standard)
            selling_uom = oc.get('selling_uom', 'pcs')
            help_text = f"Max: {format_number(pending_standard)} {standard_uom} (= {format_number(pending_selling)} {selling_uom})"
        else:
            help_text = f"Max: {format_number(pending_standard)} {standard_uom}"
        
        st.caption(f"Allocate quantity in {standard_uom} (standard UOM)")
        soft_qty_standard = st.number_input(
            "Quantity",
            min_value=0.0,
            max_value=float(pending_standard),
            value=0.0,
            step=1.0,
            help=help_text
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
    
    # Summary
    show_allocation_summary_dual_uom(oc, total_selected_standard, selected_supplies, use_soft)

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

def format_supply_info_dual_uom(supply, source_type, oc):
    """Format supply information with dual UOM display"""
    if source_type == 'INVENTORY':
        info = f"Batch {supply['batch_number']} - Exp: {format_date(supply['expiry_date'])}"
    elif source_type == 'PENDING_CAN':
        info = f"{supply['arrival_note_number']} - Arr: {format_date(supply['arrival_date'])}"
    elif source_type == 'PENDING_PO':
        info = f"{supply['po_number']} - ETD: {format_date(supply['etd'])}"
    else:
        info = f"{supply['from_warehouse']} → {supply['to_warehouse']}"
    
    # Format quantity
    qty_standard = supply['available_quantity']
    standard_uom = supply.get('uom', 'pcs')
    
    qty_str = f"{format_number(qty_standard)} {standard_uom}"
    
    # Add selling UOM if different
    if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
        qty_selling = uom_converter.convert_quantity(
            qty_standard,
            'standard',
            'selling',
            oc.get('uom_conversion', '1')
        )
        selling_uom = oc.get('selling_uom', 'pcs')
        qty_str += f" (= {format_number(qty_selling)} {selling_uom})"
    
    return f"{info} - Available: {qty_str}"

def show_allocation_summary_dual_uom(oc, total_selected_standard, selected_supplies, use_soft):
    """Show allocation summary with dual UOM display"""
    col1, col2 = st.columns(2)
    
    with col1:
        standard_uom = oc.get('standard_uom', 'pcs')
        
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
        st.metric("Coverage", format_percentage(coverage))
    
    # Over-allocation warning
    if total_selected_standard > pending_standard:
        over_qty_standard = total_selected_standard - pending_standard
        over_pct = (over_qty_standard / pending_standard * 100)
        max_allowed = pending_standard * 1.1
        
        if total_selected_standard > max_allowed:
            if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
                over_qty_selling = uom_converter.convert_quantity(
                    over_qty_standard,
                    'standard',
                    'selling',
                    oc.get('uom_conversion', '1')
                )
                st.error(
                    f"⚡ Over-allocating by {format_number(over_qty_standard)} {oc.get('standard_uom')} "
                    f"(= {format_number(over_qty_selling)} {oc.get('selling_uom')}) - "
                    f"{format_percentage(over_pct)}! Maximum allowed is 110%."
                )
            else:
                st.error(
                    f"⚡ Over-allocating by {format_number(over_qty_standard)} {oc.get('standard_uom')} "
                    f"({format_percentage(over_pct)})! Maximum allowed is 110%."
                )
    
    # Additional fields
    allocated_etd = st.date_input("Allocated ETD", value=oc['etd'])
    notes = st.text_area("Notes (optional)")
    
    # Action buttons
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("💾 Save Allocation", type="primary", use_container_width=True, disabled=total_selected_standard == 0):
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
                # Save allocation
                result = allocation_service.create_allocation(
                    oc_detail_id=oc['ocd_id'],
                    allocations=selected_supplies,
                    mode='SOFT' if use_soft else 'HARD',
                    etd=allocated_etd,
                    notes=notes,
                    user_id=st.session_state.user['id']
                )
                
                if result['success']:
                    standard_uom = oc.get('standard_uom', 'pcs')
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
                    
                    st.success(success_msg)
                    st.balloons()
                    
                    time.sleep(2)
                    st.session_state.modals['allocation'] = False
                    st.session_state.selections['oc_for_allocation'] = None
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"❌ {result['error']}")
    
    with col2:
        if st.button("Cancel", use_container_width=True):
            st.session_state.modals['allocation'] = False
            st.session_state.selections['oc_for_allocation'] = None
            st.rerun()

# ==================== ALLOCATION HISTORY MODAL ====================
@st.dialog("Allocation History", width="large")
def show_allocation_history_modal():
    """Show allocation history with delivery data from allocation_delivery_links"""
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
    history_df = data_service.get_allocation_history_with_details(oc_detail_id)
    
    if history_df.empty:
        st.info("No allocation history found")
    else:
        for idx, alloc in history_df.iterrows():
            show_allocation_history_item_dual_uom(alloc, oc_info)
    
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

def show_allocation_summary_metrics(oc_info):
    """Show summary metrics with data from allocation_delivery_links"""
    metrics_cols = st.columns(3)
    
    with metrics_cols[0]:
        # Pending quantity
        standard_qty = oc_info.get('pending_standard_delivery_quantity', 0)
        standard_uom = oc_info.get('standard_uom', '')
        selling_qty = oc_info['pending_quantity']
        selling_uom = oc_info.get('selling_uom', '')
        
        st.metric("Pending Qty", f"{format_number(standard_qty)} {standard_uom}")
        
        if uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
            st.caption(f"= {format_number(selling_qty)} {selling_uom}")
    
    with metrics_cols[1]:
        # Get actual data from history
        history_df = data_service.get_allocation_history_with_details(st.session_state.selections['oc_for_history'])
        
        if not history_df.empty:
            # Calculate total effective (allocated - cancelled)
            total_effective_standard = history_df['effective_qty'].sum()
            
            # Hiển thị cả tổng allocated và effective
            total_allocated_standard = history_df['allocated_qty'].sum()
            total_cancelled_standard = history_df['cancelled_qty'].sum()
            
            if uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
                total_effective_selling = uom_converter.convert_quantity(
                    total_effective_standard,
                    'standard',
                    'selling',
                    oc_info.get('uom_conversion', '1')
                )
                st.metric("Effective Allocated", f"{format_number(total_effective_standard)} {standard_uom}")
                st.caption(f"= {format_number(total_effective_selling)} {selling_uom}")
            else:
                st.metric("Effective Allocated", f"{format_number(total_effective_standard)} {standard_uom}")
            
            # Show total allocated và cancelled if any
            if total_cancelled_standard > 0:
                st.caption(f"(Total: {format_number(total_allocated_standard)}, Cancelled: {format_number(total_cancelled_standard)} {standard_uom})")
        else:
            st.metric("Effective Allocated", f"0 {standard_uom}")
    
    with metrics_cols[2]:
        if not history_df.empty:
            # Coverage dựa trên effective allocation
            pending_standard = oc_info.get('pending_standard_delivery_quantity', 0)
            effective_standard = oc_info.get('standard_quantity', 0)  # OC effective quantity
            total_effective_standard = history_df['effective_qty'].sum()
            
            # Tính coverage dựa trên OC effective quantity
            coverage = (total_effective_standard / effective_standard * 100) if effective_standard > 0 else 0
            st.metric("Coverage", format_percentage(coverage))
            
            if coverage > 100:
                st.caption("⚡ Over-committed")
            elif coverage > 95:
                st.caption("✅ Fully covered")
        else:
            st.metric("Coverage", "0%")

def show_allocation_history_item_dual_uom(alloc, oc_info):
    """Show single allocation history item with delivery data"""
    with st.container():
        # Allocation header
        show_allocation_header(alloc)
        
        # Allocation quantities
        show_allocation_quantities_dual_uom(alloc, oc_info)
        
        # Additional info
        show_allocation_info(alloc)
        
        # Action buttons
        show_allocation_actions(alloc, oc_info)
        
        # Show cancellation history
        if alloc.get('has_cancellations'):
            show_cancellation_history_dual_uom(alloc, oc_info)
        
        # Show delivery details if any
        # Fix: Handle None values properly
        delivery_count = alloc.get('delivery_count')
        if delivery_count is not None and delivery_count > 0:
            show_delivery_details(alloc)
        
        st.divider()

def show_allocation_header(alloc):
    """Show allocation header"""
    status_color = {
        'ALLOCATED': '🟢',
        'DRAFT': '🟡',
        'CANCELLED': '🔴'
    }.get(alloc['status'], '⚪')
    
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.markdown(f"{status_color} **{alloc['allocation_number']}**")
    with col2:
        st.caption(f"Mode: {format_allocation_mode(alloc['allocation_mode'])}")
    with col3:
        st.caption(f"Status: {alloc['status']}")

def show_allocation_quantities_dual_uom(alloc, oc_info):
    """Show allocation quantities with delivery data from allocation_delivery_links"""
    detail_cols = st.columns([1, 1, 1, 1])
    
    standard_uom = oc_info.get('standard_uom', '')
    selling_uom = oc_info.get('selling_uom', '')
    conversion = oc_info.get('uom_conversion', '1')
    needs_conversion = uom_converter.needs_conversion(conversion)
    
    with detail_cols[0]:
        allocated_std = alloc.get('allocated_qty', 0) or 0  # Handle None
        if needs_conversion:
            allocated_sell = uom_converter.convert_quantity(
                allocated_std, 'standard', 'selling', conversion
            )
            st.metric("Allocated", f"{format_number(allocated_std)} {standard_uom}")
            st.caption(f"= {format_number(allocated_sell)} {selling_uom}")
        else:
            st.metric("Allocated", f"{format_number(allocated_std)} {standard_uom}")
    
    with detail_cols[1]:
        effective_std = alloc.get('effective_qty', 0) or 0  # Handle None
        if needs_conversion:
            effective_sell = uom_converter.convert_quantity(
                effective_std, 'standard', 'selling', conversion
            )
            st.metric("Effective", f"{format_number(effective_std)} {standard_uom}")
            st.caption(f"= {format_number(effective_sell)} {selling_uom}")
        else:
            st.metric("Effective", f"{format_number(effective_std)} {standard_uom}")
    
    with detail_cols[2]:
        # Delivered qty now comes from allocation_delivery_links
        delivered_std = alloc.get('delivered_qty', 0) or 0  # Handle None
        if needs_conversion:
            delivered_sell = uom_converter.convert_quantity(
                delivered_std, 'standard', 'selling', conversion
            )
            st.metric("Delivered", f"{format_number(delivered_std)} {standard_uom}")
            st.caption(f"= {format_number(delivered_sell)} {selling_uom}")
        else:
            st.metric("Delivered", f"{format_number(delivered_std)} {standard_uom}")
    
    with detail_cols[3]:
        cancelled_std = alloc.get('cancelled_qty', 0) or 0  # Handle None
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
    
    # Supply source and notes
    st.caption(f"📦 **Source:** {alloc['supply_source_type'] or 'No specific source (SOFT)'}")
    
    if alloc.get('notes'):
        st.caption(f"📝 **Notes:** {alloc['notes']}")
    
    if alloc.get('cancellation_info'):
        st.warning(f"❌ {alloc['cancellation_info']}")

def show_allocation_actions(alloc, oc_info):
    """Show action buttons for allocation"""
    if alloc['status'] != 'ALLOCATED':
        return
    
    # Get availability
    actions_availability = get_allocation_actions_availability(alloc)
    
    action_cols = st.columns([1, 1, 2])
    
    # Update ETD button
    with action_cols[0]:
        show_update_etd_button(alloc, actions_availability)
    
    # Cancel button
    with action_cols[1]:
        show_cancel_button(alloc, actions_availability)

def get_allocation_actions_availability(allocation_detail: Dict) -> Dict[str, bool]:
    """Determine available actions based on allocation_delivery_links data"""
    allocated_qty = allocation_detail.get('allocated_qty', 0)
    cancelled_qty = allocation_detail.get('cancelled_qty', 0)
    delivered_qty = allocation_detail.get('delivered_qty', 0)  # From allocation_delivery_links
    
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

def show_update_etd_button(alloc, actions_availability):
    """Show update ETD button"""
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

def show_cancel_button(alloc, actions_availability):
    """Show cancel button"""
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
        cancellations = data_service.get_cancellation_history(alloc['allocation_detail_id'])
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

def show_delivery_details(alloc):
    """Show delivery details from allocation_delivery_links"""
    with st.expander(f"📦 View Delivery History ({alloc['delivery_count']} deliveries)"):
        delivery_df = data_service.get_allocation_delivery_details(alloc['allocation_detail_id'])
        
        if not delivery_df.empty:
            for _, delivery in delivery_df.iterrows():
                del_cols = st.columns([2, 1, 1, 1])
                
                with del_cols[0]:
                    st.text(f"📄 {delivery['delivery_number']}")
                    st.caption(f"Date: {format_date(delivery['delivery_date'])}")
                
                with del_cols[1]:
                    st.text(f"{format_number(delivery['delivered_qty'])} pcs")
                    
                with del_cols[2]:
                    st.text(delivery['delivery_status'])
                    
                with del_cols[3]:
                    st.text(delivery['from_warehouse'])

# ==================== UPDATE ETD MODAL ====================
@st.dialog("Update Allocated ETD", width="medium")
def show_update_etd_modal():
    """Modal for updating allocated ETD"""
    allocation = st.session_state.selections['allocation_for_update']
    
    if not allocation:
        st.error("No allocation selected")
        if st.button("Close"):
            st.session_state.modals['update_etd'] = False
            st.session_state.selections['allocation_for_update'] = None
            st.rerun()
        return
    
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
                st.session_state.user['id']
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
                st.error(f"❌ {result['error']}")
    
    with col2:
        if st.button("Close"):
            st.session_state.modals['update_etd'] = False
            return_to_history_if_context()
            st.rerun()

# ==================== CANCEL ALLOCATION MODAL ====================
@st.dialog("Cancel Allocation", width="medium")
def show_cancel_allocation_modal():
    """Modal for cancelling allocation"""
    allocation = st.session_state.selections['allocation_for_cancel']
    
    if not allocation:
        st.error("No allocation selected")
        if st.button("Close"):
            st.session_state.modals['cancel'] = False
            st.session_state.selections['allocation_for_cancel'] = None
            st.rerun()
        return
    
    st.markdown(f"### Cancel Allocation {allocation['allocation_number']}")
    
    # Get UOM info
    oc_info = st.session_state.selections.get('oc_info', {})
    standard_uom = oc_info.get('standard_uom', '')
    selling_uom = oc_info.get('selling_uom', '')
    conversion = oc_info.get('uom_conversion', '1')
    
    # Show pending quantity
    pending_qty_std = allocation.get('pending_allocated_qty', 0)
    
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
    
    # Show delivered quantity if any
    delivered_qty = allocation.get('delivered_qty', 0)
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
            st.warning(f"⚠️ {format_number(delivered_qty)} {standard_uom} already delivered and cannot be cancelled")
    
    if pending_qty_std <= 0:
        st.error("❌ Cannot cancel - all quantity has been delivered")
        if st.button("Close"):
            st.session_state.modals['cancel'] = False
            st.rerun()
        return
    
    # Cancel quantity input
    st.markdown(f"**Cancel quantity in {standard_uom} (standard UOM):**")
    cancel_qty = st.number_input(
        f"Quantity to Cancel",
        min_value=0.0,
        max_value=float(pending_qty_std),
        value=float(pending_qty_std),
        step=1.0,
        help=f"Maximum cancellable: {format_number(pending_qty_std)} {standard_uom}"
    )
    
    # Show equivalent in selling UOM
    if cancel_qty > 0 and uom_converter.needs_conversion(conversion):
        cancel_qty_sell = uom_converter.convert_quantity(
            cancel_qty, 'standard', 'selling', conversion
        )
        st.caption(f"= {format_number(cancel_qty_sell)} {selling_uom}")
    
    # Reason category
    reason_category = st.selectbox(
        "Reason Category",
        options=['CUSTOMER_REQUEST', 'SUPPLY_ISSUE', 'QUALITY_ISSUE', 'BUSINESS_DECISION', 'OTHER'],
        format_func=lambda x: format_reason_category(x)
    )
    
    # Detailed reason
    reason = st.text_area(
        "Detailed Reason", 
        help="Please provide a detailed reason (minimum 10 characters)",
        placeholder="Explain why this allocation is being cancelled..."
    )
    
    # Validation
    errors = validator.validate_cancel_allocation(
        allocation,
        cancel_qty,
        reason,
        reason_category,
        st.session_state.user['role']
    )
    
    if errors:
        for error in errors:
            st.error(f"❌ {error}")
    
    # Action buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel Allocation", type="primary", disabled=len(errors) > 0):
            result = allocation_service.cancel_allocation(
                allocation['allocation_detail_id'],
                cancel_qty,
                reason,
                reason_category,
                st.session_state.user['id']
            )
            
            if result['success']:
                if uom_converter.needs_conversion(conversion):
                    cancel_qty_sell = uom_converter.convert_quantity(
                        cancel_qty, 'standard', 'selling', conversion
                    )
                    st.success(
                        f"✅ Successfully cancelled {format_number(cancel_qty)} {standard_uom} "
                        f"(= {format_number(cancel_qty_sell)} {selling_uom})"
                    )
                else:
                    st.success(f"✅ Successfully cancelled {format_number(cancel_qty)} {standard_uom}")
                
                if result.get('remaining_pending_qty', 0) > 0:
                    remaining_std = result['remaining_pending_qty']
                    if uom_converter.needs_conversion(conversion):
                        remaining_sell = uom_converter.convert_quantity(
                            remaining_std, 'standard', 'selling', conversion
                        )
                        st.info(
                            f"Remaining pending: {format_number(remaining_std)} {standard_uom} "
                            f"(= {format_number(remaining_sell)} {selling_uom})"
                        )
                    else:
                        st.info(f"Remaining pending: {format_number(remaining_std)} {standard_uom}")
                
                time.sleep(1)
                return_to_history_if_context()
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"❌ {result['error']}")
    
    with col2:
        if st.button("Close"):
            st.session_state.modals['cancel'] = False
            return_to_history_if_context()
            st.rerun()

# ==================== REVERSE CANCELLATION MODAL ====================
@st.dialog("Reverse Cancellation", width="medium")
def show_reverse_cancellation_modal():
    """Modal for reversing a cancellation"""
    cancellation = st.session_state.selections['cancellation_for_reverse']
    
    if not cancellation:
        st.error("No cancellation selected")
        if st.button("Close"):
            st.session_state.modals['reverse'] = False
            st.session_state.selections['cancellation_for_reverse'] = None
            st.rerun()
        return
    
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
                st.session_state.user['id']
            )
            
            if result['success']:
                st.success("✅ Cancellation reversed successfully")
                time.sleep(1)
                
                return_to_history_if_context()
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"❌ {result['error']}")
    
    with col2:
        if st.button("Close"):
            st.session_state.modals['reverse'] = False
            return_to_history_if_context()
            st.rerun()

def return_to_history_if_context():
    """Return to history modal if context exists"""
    if st.session_state.context.get('return_to_history'):
        st.session_state.modals['history'] = True
        st.session_state.selections['oc_for_history'] = st.session_state.context['return_to_history']['oc_detail_id']
        st.session_state.selections['oc_info'] = st.session_state.context['return_to_history']['oc_info']
        st.session_state.context['return_to_history'] = None

# ==================== MAIN EXECUTION ====================
def main():
    """Main function to run the allocation planning page"""
    # Safety checks
    if st.session_state.modals['history'] and not st.session_state.selections.get('oc_for_history'):
        st.session_state.modals['history'] = False
    if st.session_state.modals['allocation'] and not st.session_state.selections.get('oc_for_allocation'):
        st.session_state.modals['allocation'] = False
    if st.session_state.modals['cancel'] and not st.session_state.selections.get('allocation_for_cancel'):
        st.session_state.modals['cancel'] = False
    if st.session_state.modals['update_etd'] and not st.session_state.selections.get('allocation_for_update'):
        st.session_state.modals['update_etd'] = False
    if st.session_state.modals['reverse'] and not st.session_state.selections.get('cancellation_for_reverse'):
        st.session_state.modals['reverse'] = False
    
    show_header()
    show_metrics_row()
    st.divider()
    
    # Search and filters
    show_search_bar()
    show_quick_filters()
    show_active_filters()
    
    st.divider()
    
    # Product list
    show_product_list()
    
    # Show modals based on state
    if st.session_state.modals['allocation'] and st.session_state.selections.get('oc_for_allocation'):
        show_allocation_modal()
    
    if st.session_state.modals['history'] and st.session_state.selections.get('oc_for_history'):
        show_allocation_history_modal()
    
    if st.session_state.modals['cancel'] and st.session_state.selections.get('allocation_for_cancel'):
        show_cancel_allocation_modal()
    
    if st.session_state.modals['update_etd'] and st.session_state.selections.get('allocation_for_update'):
        show_update_etd_modal()
    
    if st.session_state.modals['reverse'] and st.session_state.selections.get('cancellation_for_reverse'):
        show_reverse_cancellation_modal()

# Run the main function
if __name__ == "__main__":
    main()