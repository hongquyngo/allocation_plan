"""
Allocation Planning System - Complete Version with Dual UOM Display
Product-centric view with dual UOM display and standard UOM allocation
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
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
# Improved session state with grouped related states
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
        'cancellation_for_reverse': None,
        'customers': [],
        'brands': [],
        'oc_numbers': [],
        'products': []
    },
    'filters': {},
    'ui': {
        'page_number': 1,
        'show_advanced_filters': False,
        'expanded_products': set()
    },
    'user': {
        'id': None,
        'role': 'viewer'
    },
    'context': {
        'return_to_history': None  # For returning to history after update
    }
}

def init_session_state():
    """Initialize session state with default values"""
    if 'state_initialized' not in st.session_state:
        for key, value in DEFAULT_SESSION_STATE.items():
            if key not in st.session_state:
                st.session_state[key] = value.copy() if isinstance(value, (dict, set)) else value
        st.session_state.state_initialized = True
    
    # Ensure modal states are properly initialized on each page load
    if 'modals' not in st.session_state:
        st.session_state.modals = {
            'allocation': False,
            'cancel': False,
            'update_etd': False,
            'reverse': False,
            'history': False
        }
    
    # Set user info if not already set
    if st.session_state.user['id'] is None:
        st.session_state.user['id'] = st.session_state.get('authenticated_user_id', 1)
    if st.session_state.user['role'] == 'viewer':
        st.session_state.user['role'] = st.session_state.get('user_role', 'viewer')

# Initialize session state
init_session_state()

# Debug modal states (remove in production)
if config.get_app_setting('DEBUG_MODE', False):
    with st.sidebar:
        st.write("Modal States:", st.session_state.modals)

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
            # Reset all modals before logout
            reset_all_modals()
            auth.logout()
            st.switch_page("app.py")

    # User info
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
            
            with search_col:
                search_query = st.text_input(
                    "🔍 Search",
                    placeholder="Search by product name, PT code, brand, customer, OC number, or package size...",
                    key="search_input",
                    help="Type at least 2 characters to see suggestions",
                    label_visibility="collapsed"
                )
            
            with clear_col:
                if search_query:
                    if st.button("✖", key="clear_search", help="Clear search"):
                        st.session_state.search_input = ""
                        st.session_state.filters.pop('search', None)
                        st.rerun()
            
            # Show suggestions
            if search_query and len(search_query) >= 2:
                show_search_suggestions(search_query)
            
            if search_query:
                st.session_state.filters['search'] = search_query
            else:
                st.session_state.filters.pop('search', None)
    
    with col2:
        if st.button("⚙️ Advanced Filters", use_container_width=True):
            st.session_state.ui['show_advanced_filters'] = not st.session_state.ui['show_advanced_filters']
            # Reset modals when toggling advanced filters
            reset_all_modals()
            st.rerun()

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
                        # Reset modals when selecting search suggestion
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

def show_advanced_filters():
    """Display advanced filter panel"""
    if not st.session_state.ui['show_advanced_filters']:
        return
    
    with st.expander("Advanced Filters", expanded=True):
        # Get filter data
        customers_df = data_service.get_customer_list_with_stats()
        brands_df = data_service.get_brand_list_with_stats()
        oc_numbers_df = data_service.get_oc_number_list()
        products_df = data_service.get_product_list_for_filter()
        
        # Customer and Brand filters
        col1, col2 = st.columns(2)
        
        with col1:
            show_customer_filter(customers_df)
        
        with col2:
            show_brand_filter(brands_df)
        
        # OC Number and Product filters
        col3, col4 = st.columns(2)
        
        with col3:
            show_oc_number_filter(oc_numbers_df)
        
        with col4:
            show_product_filter(products_df)
        
        # Date and status filters
        show_date_and_status_filters()
        
        # Apply and Clear buttons
        show_filter_actions()

def show_customer_filter(customers_df):
    """Show customer filter"""
    st.markdown("**Customer**")
    
    customer_mode = st.radio(
        "Customer filter mode",
        ["Include", "Exclude"],
        key="customer_mode",
        horizontal=True,
        label_visibility="collapsed"
    )
    
    if not customers_df.empty:
        customer_options = []
        customer_map = {}
        for _, row in customers_df.iterrows():
            option = f"{row['customer_name']} ({row['order_count']} orders, {row['product_count']} products)"
            customer_options.append(option)
            customer_map[option] = row['customer_code']
        
        selected_customer_options = st.multiselect(
            "Select customers",
            options=customer_options,
            default=st.session_state.selections['customers'],
            key="multiselect_customers",
            placeholder="Choose customers...",
            label_visibility="collapsed"
        )
        
        st.session_state.selections['customers'] = selected_customer_options
        return customer_map, selected_customer_options, customer_mode
    
    return {}, [], customer_mode

def show_brand_filter(brands_df):
    """Show brand filter"""
    st.markdown("**Brand**")
    
    brand_mode = st.radio(
        "Brand filter mode",
        ["Include", "Exclude"],
        key="brand_mode",
        horizontal=True,
        label_visibility="collapsed"
    )
    
    if not brands_df.empty:
        brand_options = []
        brand_map = {}
        for _, row in brands_df.iterrows():
            option = f"{row['brand_name']} ({row['product_count']} products)"
            brand_options.append(option)
            brand_map[option] = row['brand_id']
        
        selected_brand_options = st.multiselect(
            "Select brands",
            options=brand_options,
            default=st.session_state.selections['brands'],
            key="multiselect_brands",
            placeholder="Choose brands...",
            label_visibility="collapsed"
        )
        
        st.session_state.selections['brands'] = selected_brand_options
        return brand_map, selected_brand_options, brand_mode
    
    return {}, [], brand_mode

def show_oc_number_filter(oc_numbers_df):
    """Show OC number filter"""
    st.markdown("**OC Number**")
    
    oc_mode = st.radio(
        "OC filter mode",
        ["Include", "Exclude"],
        key="oc_mode",
        horizontal=True,
        label_visibility="collapsed"
    )
    
    if not oc_numbers_df.empty:
        oc_options = []
        oc_map = {}
        for _, row in oc_numbers_df.iterrows():
            option = f"{row['oc_number']} - {row['customer']} ({row['product_count']} items)"
            oc_options.append(option)
            oc_map[option] = row['oc_number']
        
        selected_oc_options = st.multiselect(
            "Select OC numbers",
            options=oc_options,
            default=st.session_state.selections['oc_numbers'],
            key="multiselect_oc_numbers",
            placeholder="Choose OC numbers...",
            label_visibility="collapsed"
        )
        
        st.session_state.selections['oc_numbers'] = selected_oc_options
        return oc_map, selected_oc_options, oc_mode
    
    return {}, [], oc_mode

def show_product_filter(products_df):
    """Show product filter"""
    st.markdown("**PT Code - Product Name**")
    
    product_mode = st.radio(
        "Product filter mode",
        ["Include", "Exclude"],
        key="product_mode",
        horizontal=True,
        label_visibility="collapsed"
    )
    
    if not products_df.empty:
        product_options = []
        product_map = {}
        for _, row in products_df.iterrows():
            option = row['display_name']
            if pd.notna(row.get('brand_name')):
                option += f" [{row['brand_name']}]"
            product_options.append(option)
            product_map[option] = row['product_id']
        
        selected_product_options = st.multiselect(
            "Select products",
            options=product_options,
            default=st.session_state.selections['products'],
            key="multiselect_products",
            placeholder="Choose products...",
            label_visibility="collapsed"
        )
        
        st.session_state.selections['products'] = selected_product_options
        return product_map, selected_product_options, product_mode
    
    return {}, [], product_mode

def show_date_and_status_filters():
    """Show date and status filters"""
    col5, col6, col7 = st.columns(3)
    
    with col5:
        st.markdown("**ETD Range**")
        etd_option = st.selectbox(
            "ETD Range",
            ["All dates", "Next 7 days", "Next 14 days", "Next 30 days", "Custom range"],
            key="filter_etd_range",
            label_visibility="collapsed"
        )
        
        if etd_option == "Custom range":
            date_col1, date_col2 = st.columns(2)
            with date_col1:
                date_from = st.date_input("From date", key="filter_date_from")
            with date_col2:
                date_to = st.date_input("To date", key="filter_date_to")
    
    with col6:
        st.markdown("**Supply Coverage**")
        coverage_option = st.selectbox(
            "Supply Coverage",
            ["All", "Critical (<20%)", "Low (<50%)", "Partial (50-99%)", "Full (≥100%)"],
            key="filter_coverage",
            label_visibility="collapsed"
        )
    
    with col7:
        st.markdown("**Allocation Status**")
        allocation_status = st.selectbox(
            "Allocation Status",
            ["All", "Not Allocated", "Partially Allocated", "Fully Allocated", "Over Allocated"],
            key="filter_allocation_status",
            label_visibility="collapsed"
        )

def show_filter_actions():
    """Show filter action buttons"""
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("Apply Filters", type="primary", use_container_width=True):
            apply_advanced_filters()
    
    with col2:
        if st.button("Clear All", type="secondary", use_container_width=True):
            clear_all_filters()

def apply_advanced_filters():
    """Apply advanced filters"""
    # Build filter dictionary
    new_filters = {}
    
    # Get filter components (would need to be passed or stored)
    # This is a simplified version - in real implementation, 
    # these values would be collected from the filter functions
    
    # Keep search filter if exists
    if 'search' in st.session_state.filters:
        new_filters['search'] = st.session_state.filters['search']
    
    st.session_state.filters = new_filters
    st.session_state.ui['page_number'] = 1
    
    # Reset modals when applying filters
    reset_all_modals()
    
    st.rerun()

def clear_all_filters():
    """Clear all filters"""
    # Clear all multiselect states
    st.session_state.selections['customers'] = []
    st.session_state.selections['brands'] = []
    st.session_state.selections['oc_numbers'] = []
    st.session_state.selections['products'] = []
    
    # Clear filters except search
    search_filter = st.session_state.filters.get('search')
    st.session_state.filters = {}
    if search_filter:
        st.session_state.filters['search'] = search_filter
    
    st.session_state.ui['page_number'] = 1
    
    # Reset modals when clearing filters
    reset_all_modals()
    
    st.rerun()

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
                
                # Reset modals when changing quick filters
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
        'over_allocated': '⚡ Over Allocated',
        'etd_days': f'ETD: Next {value} days',
        'coverage': f'Coverage: {value}',
        'allocation_status_detail': f'Status: {value}'
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
            st.write("• Changing include/exclude mode")
            st.write("• Using different search terms")
            
            if st.button("🔄 Clear All Filters and Retry", use_container_width=True):
                st.session_state.filters = {}
                st.session_state.ui['page_number'] = 1
                # Reset modals when clearing all filters
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
        # Safety check: ensure no stale modal states when showing details
        if st.session_state.modals.get('history') and not st.session_state.selections.get('oc_for_history'):
            st.session_state.modals['history'] = False
        
        show_product_details(row)
    
    st.divider()

def show_product_info(row, product_id, is_expanded):
    """Show product information"""
    # Product info with expand/collapse button
    if st.button(
        f"{'▼' if is_expanded else '▶'} {row['product_name']}", 
        key=f"expand_{product_id}",
        use_container_width=True,
        type="secondary"
    ):
        if is_expanded:
            st.session_state.ui['expanded_products'].remove(product_id)
            # Clear modals when collapsing product
            reset_all_modals()
        else:
            st.session_state.ui['expanded_products'].add(product_id)
            # Clear any open modals when expanding a new product
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
        # Tabs for Demand and Supply
        tab1, tab2 = st.tabs(["📋 Demand (Order Confirmations)", "📦 Supply (Available Sources)"])
        
        with tab1:
            show_product_demand_details(product_row['product_id'])
        
        with tab2:
            show_product_supply_details(product_row['product_id'])

def show_product_demand_details(product_id):
    """Show OCs for a product with over-allocation warnings"""
    ocs_df = data_service.get_ocs_by_product(product_id)
    
    if ocs_df.empty:
        st.info("No pending OCs for this product")
        return
    
    # Add headers for the OC table
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
    
    # Create OC table rows
    for idx, oc in ocs_df.iterrows():
        show_oc_row_dual_uom(oc)

def show_oc_row_dual_uom(oc):
    """Display a single OC row with proper dual UOM handling"""
    # Check for over-allocation
    is_over_allocated = oc.get('is_over_allocated') == 'Yes'
    
    # Show warning if over-allocated
    if is_over_allocated:
        st.error(f"⚡ {oc.get('allocation_warning', 'Over-allocated')}")
    
    cols = st.columns([2, 2, 1, 1.5, 1.5, 1])
    
    with cols[0]:
        st.text(f"📄 {oc['oc_number']}")
    
    with cols[1]:
        st.text(f"🏢 {oc['customer']}")
    
    with cols[2]:
        show_etd_with_urgency(oc['etd'])
    
    with cols[3]:
        # Show pending quantity with dual UOM
        show_pending_quantity_dual_uom(oc)
    
    with cols[4]:
        # Show allocated quantity with dual UOM
        show_allocated_quantity_dual_uom(oc, is_over_allocated)
    
    with cols[5]:
        if st.button("Allocate", key=f"alloc_oc_{oc['ocd_id']}", use_container_width=True, type="primary"):
            # Clear any other open modals
            reset_all_modals()
            
            # Open allocation modal
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
    # Standard UOM
    standard_qty = format_number(oc.get('pending_standard_delivery_quantity', 0))
    standard_uom = oc.get('standard_uom', '')
    
    # Selling UOM
    selling_qty = format_number(oc['pending_quantity'])
    selling_uom = oc.get('selling_uom', '')
    
    # Display based on UOM difference
    if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
        # Show standard as primary (for allocation)
        st.markdown(f"**{standard_qty} {standard_uom}**")
        st.caption(f"= {selling_qty} {selling_uom}")
    else:
        # Same UOM, show once
        st.markdown(f"**{standard_qty} {standard_uom}**")

def show_allocated_quantity_dual_uom(oc, is_over_allocated):
    """Show allocated quantity with dual UOM display"""
    # Standard UOM (stored in DB)
    allocated_qty_standard = oc.get('allocated_quantity_standard', oc.get('allocated_quantity', 0))
    standard_uom = oc.get('standard_uom', '')
    
    # Selling UOM (for display)
    allocated_qty_selling = oc.get('allocated_quantity', 0)
    selling_uom = oc.get('selling_uom', '')
    
    allocation_count = oc.get('allocation_count', 0)
    
    if allocated_qty_standard > 0:
        # Build button label with dual UOM
        if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
            button_label = f"{format_number(allocated_qty_standard)} {standard_uom}"
            if allocation_count > 1:
                button_label += f" ({allocation_count})"
            help_text = f"= {format_number(allocated_qty_selling)} {selling_uom}. Click to view allocation history"
        else:
            button_label = f"{format_number(allocated_qty_standard)} {standard_uom}"
            if allocation_count > 1:
                button_label += f" ({allocation_count})"
            help_text = f"Click to view {allocation_count} allocation(s)"
        
        # Color coding for allocation status
        button_type = "secondary"
        
        if st.button(
            button_label, 
            key=f"view_alloc_{oc['ocd_id']}", 
            help=help_text,
            use_container_width=True,
            type=button_type
        ):
            # Open history modal with full OC info
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
                'is_over_allocated': is_over_allocated,
                'allocation_warning': oc.get('allocation_warning', ''),
                'uom_conversion': oc.get('uom_conversion', '1'),
                'can_update_etd': oc.get('can_update_etd', 'No'),
                'can_cancel': oc.get('can_cancel', 'No'),
                'max_cancellable_qty': oc.get('max_cancellable_qty', 0)
            }
            st.rerun()
    else:
        st.text(f"0 {standard_uom}")

def show_product_supply_details(product_id):
    """Show supply sources for a product"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        show_inventory_summary(product_id)
    
    with col2:
        show_can_summary(product_id)
    
    with col3:
        show_po_summary(product_id)
    
    with col4:
        show_wht_summary(product_id)

def show_inventory_summary(product_id):
    """Show inventory summary"""
    st.markdown("**📦 Inventory**")
    inventory_df = data_service.get_inventory_summary(product_id)
    if not inventory_df.empty:
        for _, inv in inventory_df.iterrows():
            # Include warehouse in the label
            label = f"Batch {inv['batch_number']}"
            if inv.get('warehouse_name'):
                label += f" | {inv['warehouse_name']}"
            
            st.metric(
                label,
                f"{format_number(inv['available_quantity'])} {inv.get('standard_uom', '')}",
                delta=f"Exp: {format_date(inv['expiry_date'])}"
            )
            
            # Optionally show location as caption if available
            if inv.get('location'):
                st.caption(f"📍 Location: {inv['location']}")
    else:
        st.caption("No inventory")

def show_can_summary(product_id):
    """Show CAN summary"""
    st.markdown("**🚢 Pending CAN**")
    can_df = data_service.get_can_summary(product_id)
    if not can_df.empty:
        for _, can in can_df.iterrows():
            qty_str = f"{format_number(can['pending_quantity'])} {can.get('standard_uom', '')}"
            
            if pd.notna(can.get('buying_quantity')) and pd.notna(can.get('buying_uom')):
                if uom_converter.needs_conversion(can.get('uom_conversion', '1')):
                    qty_str = f"{format_number(can['buying_quantity'])} {can['buying_uom']}"
                    standard_str = f"{format_number(can['pending_quantity'])} {can['standard_uom']}"
            
            st.metric(
                can['arrival_note_number'],
                qty_str,
                delta=f"Arr: {format_date(can['arrival_date'])}"
            )
            
            if pd.notna(can.get('buying_uom')) and uom_converter.needs_conversion(can.get('uom_conversion', '1')):
                st.caption(f"= {standard_str}")
    else:
        st.caption("No pending CAN")

def show_po_summary(product_id):
    """Show PO summary"""
    st.markdown("**📋 Pending PO**")
    po_df = data_service.get_po_summary(product_id)
    if not po_df.empty:
        for _, po in po_df.iterrows():
            qty_str = f"{format_number(po['pending_quantity'])} {po.get('standard_uom', '')}"
            
            if pd.notna(po.get('buying_quantity')) and pd.notna(po.get('buying_uom')):
                if uom_converter.needs_conversion(po.get('uom_conversion', '1')):
                    qty_str = f"{format_number(po['buying_quantity'])} {po['buying_uom']}"
                    standard_str = f"{format_number(po['pending_quantity'])} {po['standard_uom']}"
            
            st.metric(
                po['po_number'],
                qty_str,
                delta=f"ETD: {format_date(po['etd'])}"
            )
            
            if pd.notna(po.get('buying_uom')) and uom_converter.needs_conversion(po.get('uom_conversion', '1')):
                st.caption(f"= {standard_str}")
    else:
        st.caption("No pending PO")

def show_wht_summary(product_id):
    """Show warehouse transfer summary"""
    st.markdown("**🚚 WH Transfer**")
    wht_df = data_service.get_wht_summary(product_id)
    if not wht_df.empty:
        for _, wht in wht_df.iterrows():
            st.metric(
                f"{wht['from_warehouse']} → {wht['to_warehouse']}",
                f"{format_number(wht['transfer_quantity'])} {wht.get('standard_uom', '')}",
                delta=wht['status']
            )
    else:
        st.caption("No transfers")

def show_pagination(df):
    """Show pagination controls"""
    if len(df) == ITEMS_PER_PAGE:
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            if st.session_state.ui['page_number'] > 1:
                if st.button("← Previous", use_container_width=True):
                    st.session_state.ui['page_number'] -= 1
                    # Reset modals when changing page
                    reset_all_modals()
                    st.rerun()
        
        with col2:
            st.markdown(f"<center>Page {st.session_state.ui['page_number']}</center>", unsafe_allow_html=True)
        
        with col3:
            if st.button("Next →", use_container_width=True):
                st.session_state.ui['page_number'] += 1
                # Reset modals when changing page
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

# ==================== ALLOCATION MODAL WITH DUAL UOM ====================
@st.dialog("Create Allocation", width="large")
def show_allocation_modal():
    """Allocation modal with dual UOM display and standard UOM allocation"""
    oc = st.session_state.selections['oc_for_allocation']
    
    if not oc:
        st.error("No OC selected")
        if st.button("Close"):
            st.session_state.modals['allocation'] = False
            st.session_state.selections['oc_for_allocation'] = None
            st.rerun()
        return
    
    # Header info
    st.markdown(f"### Allocate to {oc['oc_number']}")
    
    # Show warning if OC is already over-allocated
    if oc.get('is_over_allocated') == 'Yes':
        st.warning(f"⚠️ This OC is already over-allocated! {oc.get('allocation_warning', '')}")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Customer", oc['customer'])
    with col2:
        st.metric("Product", oc['product_name'][:30])
    with col3:
        # Show requirement in BOTH UOMs
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
    
    # Important notice
    st.info("ℹ️ **Allocation Rule**: All allocations are made in standard UOM to ensure whole container quantities")
    
    # Supply selection with dual UOM display
    st.markdown("**Available Supply:**")
    
    selected_supplies = []
    total_selected_standard = 0  # Track in standard UOM
    
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
                    # Format supply info with dual UOM
                    info = format_supply_info_dual_uom(supply, source_type, oc)
                    
                    selected = st.checkbox(
                        info,
                        key=f"supply_{supply['source_id']}_{source_type}"
                    )
                
                with col2:
                    if selected:
                        # Calculate remaining requirement in standard UOM
                        pending_standard = oc.get('pending_standard_delivery_quantity', oc['pending_quantity'])
                        max_qty_standard = min(supply['available_quantity'], pending_standard - total_selected_standard)
                        
                        # Input in standard UOM only
                        standard_uom = oc.get('standard_uom', 'pcs')
                        
                        # Show equivalent selling quantity for reference
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
                                'quantity': qty_standard,  # Always in standard UOM
                                'supply_info': supply.to_dict()
                            })
                            total_selected_standard += qty_standard
    
    st.divider()
    
    # SOFT allocation option
    st.markdown("**OR**")
    use_soft = st.checkbox("📄 SOFT Allocation (no specific source)")
    
    if use_soft:
        pending_standard = oc.get('pending_standard_delivery_quantity', oc['pending_quantity'])
        standard_uom = oc.get('standard_uom', 'pcs')
        
        # Show equivalent in selling UOM
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
                'quantity': soft_qty_standard,  # In standard UOM
                'supply_info': {'type': 'SOFT', 'description': 'No specific source'}
            }]
            total_selected_standard = soft_qty_standard
    
    st.divider()
    
    # Summary with dual UOM display and validation
    show_allocation_summary_dual_uom(oc, total_selected_standard, selected_supplies, use_soft)

def show_dual_uom_metric(label: str, 
                         standard_qty: float, standard_uom: str,
                         selling_qty: float, selling_uom: str,
                         conversion_ratio: str):
    """Show metric with both standard and selling UOM"""
    if uom_converter.needs_conversion(conversion_ratio):
        # Show primary in standard UOM
        st.metric(label, f"{format_number(standard_qty)} {standard_uom}")
        # Show selling UOM as caption
        st.caption(f"= {format_number(selling_qty)} {selling_uom}")
    else:
        # Same UOM, show once
        st.metric(label, f"{format_number(standard_qty)} {standard_uom}")

def format_supply_info_dual_uom(supply, source_type, oc):
    """Format supply information with dual UOM display"""
    # Get base info
    if source_type == 'INVENTORY':
        info = f"Batch {supply['batch_number']} - Exp: {format_date(supply['expiry_date'])}"
    elif source_type == 'PENDING_CAN':
        info = f"{supply['arrival_note_number']} - Arr: {format_date(supply['arrival_date'])}"
    elif source_type == 'PENDING_PO':
        info = f"{supply['po_number']} - ETD: {format_date(supply['etd'])}"
    else:
        info = f"{supply['from_warehouse']} → {supply['to_warehouse']}"
    
    # Format quantity with dual UOM
    qty_standard = supply['available_quantity']
    standard_uom = supply.get('uom', 'pcs')
    
    # Always show standard UOM first
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
        # Show total selected with dual UOM
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
    
    # Show over-allocation warning with dual UOM
    if total_selected_standard > pending_standard:
        over_qty_standard = total_selected_standard - pending_standard
        over_pct = (over_qty_standard / pending_standard * 100)
        
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
                    # Show success with dual UOM
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
                    
                    # Close modal and clear selection after short delay
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

# ==================== ALLOCATION HISTORY MODAL WITH DUAL UOM ====================
@st.dialog("Allocation History", width="large")
def show_allocation_history_modal():
    """Show allocation history for selected OC with management actions"""
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
    
    # Show over-allocation warning if applicable
    if oc_info.get('is_over_allocated'):
        st.error(f"⚡ {oc_info.get('allocation_warning', 'This OC is over-allocated')}")
    
    col1, col2 = st.columns(2)
    with col1:
        st.caption(f"**Customer:** {oc_info['customer']}")
    with col2:
        st.caption(f"**Product:** {oc_info['product_name']}")
    
    # Summary metrics with both UOMs
    show_allocation_summary_metrics(oc_info)
    
    st.divider()
    
    # Get allocation history with cancellation details
    history_df = data_service.get_allocation_history_with_details(oc_detail_id)
    
    if history_df.empty:
        st.info("No allocation history found")
    else:
        # Display each allocation
        for idx, alloc in history_df.iterrows():
            show_allocation_history_item_dual_uom(alloc, oc_info)
    
    # Note about UOM conversion
    if uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
        st.info(f"ℹ️ Note: Allocation quantities are stored in {oc_info.get('standard_uom', 'standard UOM')}. " +
                f"Conversion: {oc_info.get('uom_conversion', 'N/A')}")
    
    # Show action availability
    show_action_availability(oc_info)
    
    # Close button
    if st.button("Close", use_container_width=True):
        st.session_state.modals['history'] = False
        st.session_state.selections['oc_for_history'] = None
        st.session_state.selections['oc_info'] = None
        st.session_state.context['return_to_history'] = None
        st.rerun()

def show_allocation_summary_metrics(oc_info):
    """Show summary metrics for allocation with dual UOM"""
    metrics_cols = st.columns(3)
    
    with metrics_cols[0]:
        # Show pending with dual UOM
        standard_qty = oc_info.get('pending_standard_delivery_quantity', 0)
        standard_uom = oc_info.get('standard_uom', '')
        selling_qty = oc_info['pending_quantity']
        selling_uom = oc_info.get('selling_uom', '')
        
        st.metric("Pending Qty", f"{format_number(standard_qty)} {standard_uom}")
        
        if uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
            st.caption(f"= {format_number(selling_qty)} {selling_uom}")
    
    with metrics_cols[1]:
        # Get total allocated from history
        history_df = data_service.get_allocation_history_with_details(st.session_state.selections['oc_for_history'])
        if not history_df.empty:
            total_effective_standard = history_df['effective_qty'].sum()
            
            if uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
                total_effective_selling = uom_converter.convert_quantity(
                    total_effective_standard,
                    'standard',
                    'selling',
                    oc_info.get('uom_conversion', '1')
                )
                st.metric("Total Allocated", f"{format_number(total_effective_standard)} {standard_uom}")
                st.caption(f"= {format_number(total_effective_selling)} {selling_uom}")
            else:
                st.metric("Total Allocated", f"{format_number(total_effective_standard)} {standard_uom}")
        else:
            st.metric("Total Allocated", f"0 {standard_uom}")
    
    with metrics_cols[2]:
        if not history_df.empty:
            pending_standard = oc_info.get('pending_standard_delivery_quantity', oc_info['pending_quantity'])
            coverage = (total_effective_standard / pending_standard * 100) if pending_standard > 0 else 0
            st.metric("Coverage", format_percentage(coverage))
        else:
            st.metric("Coverage", "0%")

def show_allocation_history_item_dual_uom(alloc, oc_info):
    """Show single allocation history item with dual UOM"""
    with st.container():
        # Allocation header with status color
        show_allocation_header(alloc)
        
        # Allocation quantities with dual UOM
        show_allocation_quantities_dual_uom(alloc, oc_info)
        
        # Additional info
        show_allocation_info(alloc)
        
        # Action buttons
        show_allocation_actions(alloc, oc_info)
        
        # Show cancellation history if exists
        if alloc.get('has_cancellations'):
            show_cancellation_history_dual_uom(alloc, oc_info)
        
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
    """Show allocation quantities with dual UOM display"""
    detail_cols = st.columns([1, 1, 1, 1])
    
    standard_uom = oc_info.get('standard_uom', '')
    selling_uom = oc_info.get('selling_uom', '')
    conversion = oc_info.get('uom_conversion', '1')
    needs_conversion = uom_converter.needs_conversion(conversion)
    
    with detail_cols[0]:
        allocated_std = alloc['allocated_qty']
        if needs_conversion:
            allocated_sell = uom_converter.convert_quantity(
                allocated_std, 'standard', 'selling', conversion
            )
            st.metric("Allocated", f"{format_number(allocated_std)} {standard_uom}")
            st.caption(f"= {format_number(allocated_sell)} {selling_uom}")
        else:
            st.metric("Allocated", f"{format_number(allocated_std)} {standard_uom}")
    
    with detail_cols[1]:
        effective_std = alloc['effective_qty']
        if needs_conversion:
            effective_sell = uom_converter.convert_quantity(
                effective_std, 'standard', 'selling', conversion
            )
            st.metric("Effective", f"{format_number(effective_std)} {standard_uom}")
            st.caption(f"= {format_number(effective_sell)} {selling_uom}")
        else:
            st.metric("Effective", f"{format_number(effective_std)} {standard_uom}")
    
    with detail_cols[2]:
        delivered_std = alloc['delivered_qty']
        if needs_conversion:
            delivered_sell = uom_converter.convert_quantity(
                delivered_std, 'standard', 'selling', conversion
            )
            st.metric("Delivered", f"{format_number(delivered_std)} {standard_uom}")
            st.caption(f"= {format_number(delivered_sell)} {selling_uom}")
        else:
            st.metric("Delivered", f"{format_number(delivered_std)} {standard_uom}")
    
    with detail_cols[3]:
        cancelled_std = alloc['cancelled_qty']
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
    
    # Show cancellation info if any
    if alloc.get('cancellation_info'):
        st.warning(f"❌ {alloc['cancellation_info']}")

def show_allocation_actions(alloc, oc_info):
    """Show action buttons for allocation"""
    if alloc['status'] != 'ALLOCATED':
        return
    
    action_cols = st.columns([1, 1, 2])
    
    # Calculate pending quantity for this allocation
    pending_qty = alloc['allocated_qty'] - alloc.get('cancelled_qty', 0) - alloc.get('delivered_qty', 0)
    
    # Update ETD button
    with action_cols[0]:
        show_update_etd_button(alloc, pending_qty)
    
    # Cancel button
    with action_cols[1]:
        show_cancel_button(alloc, pending_qty)

def show_update_etd_button(alloc, pending_qty):
    """Show update ETD button with validation"""
    # Check if can update ETD based on pending quantity and mode
    can_update = (
        pending_qty > 0 and
        alloc['allocation_mode'] == 'SOFT' and
        validator.check_permission(st.session_state.user['role'], 'update')
    )
    
    if can_update:
        if st.button("📅 Update ETD", key=f"update_etd_{alloc['allocation_detail_id']}"):
            # Store context to return to history
            st.session_state.context['return_to_history'] = {
                'oc_detail_id': st.session_state.selections['oc_for_history'],
                'oc_info': st.session_state.selections['oc_info']
            }
            
            # Close history modal
            st.session_state.modals['history'] = False
            
            # Open update ETD modal
            alloc_data = alloc.to_dict()
            alloc_data['pending_allocated_qty'] = pending_qty
            st.session_state.modals['update_etd'] = True
            st.session_state.selections['allocation_for_update'] = alloc_data
            st.rerun()
    else:
        # Show disabled button with tooltip
        if alloc['allocation_mode'] == 'HARD':
            help_text = "Cannot update ETD for HARD allocation"
        elif pending_qty <= 0:
            help_text = "Cannot update ETD - all quantity has been delivered"
        else:
            help_text = "No permission to update"
        st.button("📅 Update ETD", key=f"update_etd_{alloc['allocation_detail_id']}_disabled", 
                 disabled=True, help=help_text)

def show_cancel_button(alloc, pending_qty):
    """Show cancel button with validation"""
    # Check if can cancel based on pending quantity
    can_cancel = (
        pending_qty > 0 and
        validator.check_permission(st.session_state.user['role'], 'cancel')
    )
    
    if can_cancel:
        if st.button("❌ Cancel", key=f"cancel_{alloc['allocation_detail_id']}"):
            # Store context to return to history
            st.session_state.context['return_to_history'] = {
                'oc_detail_id': st.session_state.selections['oc_for_history'],
                'oc_info': st.session_state.selections['oc_info']
            }
            
            # Close history modal
            st.session_state.modals['history'] = False
            
            # Open cancel modal
            alloc_data = alloc.to_dict()
            alloc_data['pending_allocated_qty'] = pending_qty
            st.session_state.modals['cancel'] = True
            st.session_state.selections['allocation_for_cancel'] = alloc_data
            st.rerun()
    else:
        # Show disabled button with tooltip
        if pending_qty <= 0:
            help_text = "Cannot cancel - all quantity has been delivered"
        else:
            help_text = "No permission to cancel"
        st.button("❌ Cancel", key=f"cancel_{alloc['allocation_detail_id']}_disabled", 
                 disabled=True, help=help_text)

def show_cancellation_history_dual_uom(alloc, oc_info):
    """Show cancellation history with dual UOM"""
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
                        # Store context and open reverse modal
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

def show_action_availability(oc_info):
    """Show available actions based on permissions"""
    if oc_info.get('can_update_etd') == 'Yes' or oc_info.get('can_cancel') == 'Yes':
        st.caption("**Available Actions:**")
        if oc_info.get('can_update_etd') == 'Yes':
            st.caption("• ETD can be updated for SOFT allocations with pending quantity")
        if oc_info.get('can_cancel') == 'Yes':
            max_qty = oc_info.get('max_cancellable_qty', 0)
            standard_uom = oc_info.get('standard_uom', '')
            st.caption(f"• Can cancel up to {format_number(max_qty)} {standard_uom} (pending quantity)")

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
    
    # Show current ETD
    st.info(f"Current Allocated ETD: {format_date(allocation['allocated_etd'])}")
    
    # Show pending quantity that will be affected
    pending_qty = allocation.get('pending_allocated_qty', 0)
    oc_info = st.session_state.selections.get('oc_info', {})
    standard_uom = oc_info.get('standard_uom', '')
    
    st.caption(f"**Pending quantity affected:** {format_number(pending_qty)} {standard_uom}")
    
    # Show delivered quantity if any
    delivered_qty = allocation.get('delivered_qty', 0)
    if delivered_qty > 0:
        st.warning(f"ℹ️ {format_number(delivered_qty)} {standard_uom} already delivered. ETD update will only affect pending quantity.")
    
    # Validate if can update
    valid, error = validator.validate_update_etd(
        allocation,
        allocation['allocated_etd'],  # Dummy for initial check
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
                
                # Return to history if context exists
                return_to_history_if_context()
                
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"❌ {result['error']}")
    
    with col2:
        if st.button("Close"):
            st.session_state.modals['update_etd'] = False
            
            # Return to history if context exists
            return_to_history_if_context()
            
            st.rerun()

# ==================== CANCEL ALLOCATION MODAL WITH DUAL UOM ====================
@st.dialog("Cancel Allocation", width="medium")
def show_cancel_allocation_modal():
    """Modal for cancelling allocation with dual UOM display"""
    allocation = st.session_state.selections['allocation_for_cancel']
    
    if not allocation:
        st.error("No allocation selected")
        if st.button("Close"):
            st.session_state.modals['cancel'] = False
            st.session_state.selections['allocation_for_cancel'] = None
            st.rerun()
        return
    
    st.markdown(f"### Cancel Allocation {allocation['allocation_number']}")
    
    # Get UOM info from session state
    oc_info = st.session_state.selections.get('oc_info', {})
    standard_uom = oc_info.get('standard_uom', '')
    selling_uom = oc_info.get('selling_uom', '')
    conversion = oc_info.get('uom_conversion', '1')
    
    # Show pending quantity with dual UOM
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
    
    # Validate if can cancel
    if allocation['allocation_mode'] == 'HARD' and st.session_state.user['role'] not in ['GM', 'MD', 'admin', 'sales_manager']:
        st.error("❌ Cannot cancel HARD allocation. Please contact manager for approval.")
        if st.button("Close"):
            st.session_state.modals['cancel'] = False
            st.rerun()
        return
    
    if pending_qty_std <= 0:
        st.error("❌ Cannot cancel - all quantity has been delivered")
        if st.button("Close"):
            st.session_state.modals['cancel'] = False
            st.rerun()
        return
    
    # Cancel quantity input (in standard UOM only)
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
                # Success message with dual UOM
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
                
                # Return to history if context exists
                return_to_history_if_context()
                
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"❌ {result['error']}")
    
    with col2:
        if st.button("Close"):
            st.session_state.modals['cancel'] = False
            
            # Return to history if context exists
            return_to_history_if_context()
            
            st.rerun()

# ==================== REVERSE CANCELLATION MODAL ====================
@st.dialog("Reverse Cancellation", width="medium")
def show_reverse_cancellation_modal():
    """Modal for reversing a cancellation with dual UOM display"""
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
    
    # Show cancellation info with dual UOM if applicable
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
                
                # Return to history if context exists
                return_to_history_if_context()
                
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"❌ {result['error']}")
    
    with col2:
        if st.button("Close"):
            st.session_state.modals['reverse'] = False
            
            # Return to history if context exists
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
    # Safety check: Reset modals if no corresponding selection
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
    
    # Search and filters section
    show_search_bar()
    show_advanced_filters()
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