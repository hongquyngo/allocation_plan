"""
Allocation Planning System - Refactored Main Page
Product-centric view with proper user session management
UPDATED: Added tooltips and detailed over-allocation messages
"""
import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import time
from sqlalchemy import text

# Import utilities
from utils.auth import AuthManager
from utils.config import config

# Import data repositories
from utils.allocation.product_data import ProductData
from utils.allocation.supply_data import SupplyData
from utils.allocation.allocation_data import AllocationData

# Import modals
from utils.allocation.modal_allocation import show_allocation_modal
from utils.allocation.modal_history import show_allocation_history_modal
from utils.allocation.modal_cancel import show_cancel_allocation_modal
from utils.allocation.modal_update_etd import show_update_etd_modal
from utils.allocation.modal_reverse import show_reverse_cancellation_modal

# Import core utilities
from utils.allocation.allocation_service import AllocationService
from utils.allocation.formatters import (
    format_number, format_date, 
    format_percentage, format_allocation_mode
)
from utils.allocation.validators import AllocationValidator
from utils.allocation.uom_converter import UOMConverter

# Import tooltip helpers
from utils.allocation.tooltip_helpers import (
    create_oc_tooltip,
    get_oc_allocation_status,
    get_allocation_status_color
)

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
product_data = ProductData()
supply_data = SupplyData()
allocation_data = AllocationData()
allocation_service = AllocationService()
validator = AllocationValidator()
uom_converter = UOMConverter()

# Check authentication first
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
    'context': {
        'return_to_history': None
    }
}

def init_session_state():
    """Initialize session state with proper user validation"""
    if 'state_initialized' not in st.session_state:
        for key, value in DEFAULT_SESSION_STATE.items():
            if key not in st.session_state:
                st.session_state[key] = value.copy() if isinstance(value, (dict, set)) else value
        st.session_state.state_initialized = True
    
    # Ensure modal states are properly initialized
    if 'modals' not in st.session_state:
        st.session_state.modals = DEFAULT_SESSION_STATE['modals'].copy()
    
    # Handle user session with validation
    if 'user' not in st.session_state:
        st.session_state.user = {}
    
    # Get user ID with proper validation
    user_id = auth.get_current_user_id()
    
    if user_id is None:
        logger.error("No valid user session found, redirecting to login")
        st.error("⚠️ Your session has expired. Please login again.")
        time.sleep(2)
        auth.logout()
        st.switch_page("app.py")
        st.stop()
    
    # Validate user still exists and is active
    if not auth.validate_user_exists(user_id):
        logger.error(f"User {user_id} no longer exists or is inactive")
        st.error("⚠️ Your account is no longer active. Please contact an administrator.")
        time.sleep(2)
        auth.logout()
        st.switch_page("app.py")
        st.stop()
    
    # Update user information in session state
    if 'user' not in st.session_state or st.session_state.user.get('id') != user_id:
        st.session_state.user = {
            'id': user_id,
            'role': st.session_state.get('user_role', 'viewer'),
            'username': st.session_state.get('username', 'Unknown'),
            'full_name': st.session_state.get('user_fullname', st.session_state.get('username', 'User'))
        }
        logger.info(f"Session initialized for user {st.session_state.user['username']} (ID: {user_id})")

# Initialize session state with validation
init_session_state()

# Constants
ITEMS_PER_PAGE = config.get_app_setting('ITEMS_PER_PAGE', 50)

# ==================== HEADER ====================
def show_header():
    """Display page header with current user info"""
    col1, col2 = st.columns([6, 1])
    with col1:
        st.title("📦 Allocation Planning System")
        st.caption("Product-centric view with complete allocation management")
    with col2:
        if st.button("🚪 Logout", use_container_width=True):
            reset_all_modals()
            auth.logout()
            st.switch_page("app.py")

    # Show current user info
    user_display = auth.get_user_display_name()
    user_role = st.session_state.user.get('role', 'viewer')
    user_id = st.session_state.user.get('id', 'Unknown')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    st.caption(f"👤 {user_display} (ID: {user_id}, Role: {user_role}) | 🕐 {current_time}")

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
        metrics = allocation_data.get_dashboard_metrics_product_view()
        
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
    suggestions = product_data.get_search_suggestions(search_query)
    
    has_suggestions = any(suggestions.values())
    
    if has_suggestions:
        with st.container():
            if suggestions['products']:
                st.markdown("**📦 Products**")
                for idx, product in enumerate(suggestions['products'][:5]):
                    if st.button(product, key=f"prod_suggest_{idx}", use_container_width=True):
                        product_name = product.split(" | ")[0]
                        st.session_state.search_input = product_name
                        reset_all_modals()
                        st.rerun()
            
            if suggestions['brands']:
                st.markdown("**🏷️ Brands**")
                for idx, brand in enumerate(suggestions['brands'][:5]):
                    if st.button(brand, key=f"brand_suggest_{idx}", use_container_width=True):
                        st.session_state.search_input = brand
                        reset_all_modals()
                        st.rerun()
            
            if suggestions['customers']:
                st.markdown("**🏢 Customers**")
                for idx, customer in enumerate(suggestions['customers'][:5]):
                    if st.button(customer, key=f"cust_suggest_{idx}", use_container_width=True):
                        st.session_state.search_input = customer
                        reset_all_modals()
                        st.rerun()
            
            if suggestions['oc_numbers']:
                st.markdown("**📄 OC Numbers**")
                for idx, oc in enumerate(suggestions['oc_numbers'][:5]):
                    if st.button(oc, key=f"oc_suggest_{idx}", use_container_width=True):
                        st.session_state.search_input = oc
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
        
        filter_labels = {
            'supply_status': '⚠️ Low Supply',
            'etd_urgency': '🔴 Urgent ETD',
            'allocation_status': '⏳ Not Allocated',
            'has_inventory': '📦 Has Inventory',
            'over_allocated': '⚡ Over Allocated'
        }
        
        for key, value in active_filters:
            filter_label = filter_labels.get(key, f"{key}: {value}")
            if filter_label:
                display_filters.append((filter_label, key))
        
        cols = st.columns(5)
        for idx, (label, key) in enumerate(display_filters[:5]):
            with cols[idx % 5]:
                if st.button(f"{label} ✕", key=f"clear_{key}"):
                    st.session_state.filters.pop(key, None)
                    st.session_state.ui['page_number'] = 1
                    st.rerun()

# ==================== PRODUCT LIST ====================
def show_product_list():
    """Display product list with demand/supply summary"""
    try:
        products_df = product_data.get_products_with_demand_supply(
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
    ocs_df = product_data.get_ocs_by_product(product_id)
    
    if ocs_df.empty:
        st.info("No pending OCs for this product")
        return
    
    # Headers
    header_cols = st.columns([2, 2, 1, 1.5, 1.5, 1])
    with header_cols[0]:
        st.markdown("**OC Number**")
    with header_cols[1]:
        st.markdown("**Customer**")
    with header_cols[2]:
        st.markdown("**ETD**")
    with header_cols[3]:
        st.markdown("**Pending Delivery**")
    with header_cols[4]:
        st.markdown("**Undelivered Alloc**")
    with header_cols[5]:
        st.markdown("**Action**")
    
    # Create OC rows
    for idx, oc in ocs_df.iterrows():
        show_oc_row(oc)

def show_oc_row(oc):
    """Display a single OC row with DETAILED over-allocation warnings and TOOLTIP"""
    # ===== DETAILED OVER-ALLOCATION WARNINGS =====
    over_allocation_type = oc.get('over_allocation_type', 'Normal')
    
    if over_allocation_type == 'Over-Committed':
        over_qty = oc.get('over_committed_qty_standard', 0)
        effective_qty = oc.get('standard_quantity', 0)
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
                f"❌ Over-committed by {format_number(over_qty_selling)} {oc.get('selling_uom')} - "
                f"Effective allocation ({format_number(effective_allocated_selling)} {oc.get('selling_uom')}) "
                f"exceeds OC effective quantity ({format_number(effective_qty_selling)} {oc.get('selling_uom')})"
            )
        else:
            st.error(
                f"❌ Over-committed by {format_number(over_qty)} {oc.get('standard_uom')} - "
                f"Effective allocation ({format_number(effective_allocated)} {oc.get('standard_uom')}) "
                f"exceeds OC effective quantity ({format_number(effective_qty)} {oc.get('standard_uom')})"
            )
    
    elif over_allocation_type == 'Pending-Over-Allocated':
        over_qty = oc.get('pending_over_allocated_qty_standard', 0)
        if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
            over_qty_selling = uom_converter.convert_quantity(
                over_qty, 'standard', 'selling', oc.get('uom_conversion', '1')
            )
            st.warning(
                f"⚠️ Pending over-allocated by {format_number(over_qty_selling)} {oc.get('selling_uom')} - "
                f"Undelivered allocation exceeds pending delivery"
            )
        else:
            st.warning(
                f"⚠️ Pending over-allocated by {format_number(over_qty)} {oc.get('standard_uom')} - "
                f"Undelivered allocation exceeds pending delivery"
            )
    
    # ===== OC ROW DISPLAY =====
    cols = st.columns([2, 2, 1, 1.5, 1.5, 1])
    
    with cols[0]:
        st.text(f"📄 {oc['oc_number']}")
    
    with cols[1]:
        st.text(f"🏢 {oc['customer']}")
    
    with cols[2]:
        show_etd_with_urgency(oc['etd'])
    
    with cols[3]:  # Pending Delivery with TOOLTIP
        pending_std = float(oc.get('pending_standard_delivery_quantity', 0))
        standard_uom = oc.get('standard_uom', '')
        
        # Create comprehensive tooltip
        tooltip = create_oc_tooltip(oc)
        
        st.markdown(
            f"**{format_number(pending_std)} {standard_uom}**",
            help=tooltip  # ← TOOLTIP HERE
        )
        
        if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
            pending_selling = float(oc.get('pending_quantity', pending_std))
            st.caption(f"= {format_number(pending_selling)} {oc.get('selling_uom')}")
    
    with cols[4]:  # Undelivered Allocated with COLOR
        show_undelivered_allocated(oc)
    
    with cols[5]:
        show_allocation_action_button(oc)

def show_undelivered_allocated(oc):
    """Show undelivered allocated quantity with color coding"""
    undelivered_std = float(oc.get('undelivered_allocated_qty_standard', 0))
    pending_std = float(oc.get('pending_standard_delivery_quantity', 0))
    standard_uom = oc.get('standard_uom', '')
    
    # Get color indicator using helper function
    color = get_allocation_status_color(pending_std, undelivered_std)
    
    # Create clickable metric if there are allocations
    if oc.get('allocation_count', 0) > 0:
        if st.button(
            f"{color} {format_number(undelivered_std)} {standard_uom}",
            key=f"view_alloc_{oc['ocd_id']}",
            help="Click to view allocation history",
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
                'uom_conversion': oc.get('uom_conversion', '1'),
                'over_allocation_type': oc.get('over_allocation_type', 'Normal')
            }
            st.rerun()
        
        if oc.get('allocation_count', 0) > 1:
            st.caption(f"({oc['allocation_count']} allocations)")
    else:
        st.markdown(f"{color} {format_number(undelivered_std)} {standard_uom}")
    
    if uom_converter.needs_conversion(oc.get('uom_conversion', '1')):
        undelivered_selling = uom_converter.convert_quantity(
            undelivered_std, 'standard', 'selling', oc.get('uom_conversion', '1')
        )
        st.caption(f"= {format_number(undelivered_selling)} {oc.get('selling_uom')}")

def show_allocation_action_button(oc):
    """Show allocation action button"""
    pending_qty_standard = oc.get('pending_standard_delivery_quantity', 0)
    undelivered_allocated_qty = oc.get('undelivered_allocated_qty_standard', 0)
    
    is_over_committed = oc.get('is_over_committed', 'No') == 'Yes'
    is_pending_over_allocated = oc.get('is_pending_over_allocated', 'No') == 'Yes'
    
    product_id = oc.get('product_id')
    supply_summary = supply_data.get_product_supply_summary(product_id)
    has_available_supply = supply_summary.get('available', 0) > 0
    
    can_allocate_more = (
        not is_over_committed and 
        not is_pending_over_allocated and 
        has_available_supply and
        pending_qty_standard > undelivered_allocated_qty
    )
    
    if is_over_committed:
        help_text = "Cannot allocate more - Over-committed"
        button_type = "secondary"
    elif is_pending_over_allocated:
        help_text = "Cannot allocate more - Pending over-allocated"
        button_type = "secondary"
    elif not has_available_supply:
        help_text = "No supply available for allocation"
        button_type = "secondary"
    else:
        remaining_allowed = pending_qty_standard - undelivered_allocated_qty
        if remaining_allowed > 0:
            help_text = f"Can allocate up to {format_number(remaining_allowed)} {oc.get('standard_uom')} more"
            button_type = "primary"
        else:
            help_text = "Fully allocated"
            can_allocate_more = False
            button_type = "secondary"
    
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
    if etd is None or pd.isna(etd):
        st.text("⚫ No ETD")
        return
        
    try:
        etd_date = pd.to_datetime(etd).date()
        etd_days = (etd_date - datetime.now().date()).days
        
        etd_color = ""
        if etd_days <= 0:
            etd_color = "⚫"
        elif etd_days <= 7:
            etd_color = "🔴"
        elif etd_days <= 14:
            etd_color = "🟡"
        
        st.text(f"{etd_color} {format_date(etd)}")
    except Exception as e:
        logger.error(f"Error formatting ETD {etd}: {e}")
        st.text(f"📅 {etd}")

def show_product_supply_details(product_id):
    """Show supply sources for a product"""
    # Get product standard UOM
    try:
        query = text("SELECT uom FROM products WHERE id = :product_id AND delete_flag = 0")
        with product_data.engine.connect() as conn:
            result = conn.execute(query, {'product_id': product_id}).fetchone()
            standard_uom = result[0] if result else 'pcs'
    except Exception as e:
        logger.error(f"Error getting product UOM: {e}")
        standard_uom = 'pcs'
    
    # Get supply summary
    supply_summary = supply_data.get_product_supply_summary(product_id)
    
    # Show overview
    st.markdown("### 📊 Supply Overview")
    overview_cols = st.columns(4)
    
    with overview_cols[0]:
        st.metric("Total Supply", f"{format_number(supply_summary['total_supply'])} {standard_uom}")
    
    with overview_cols[1]:
        st.metric("Committed", f"{format_number(supply_summary['total_committed'])} {standard_uom}")
    
    with overview_cols[2]:
        availability_color = "🟢" if supply_summary['coverage_ratio'] > 50 else "🟡" if supply_summary['coverage_ratio'] > 20 else "🔴"
        st.metric(
            f"{availability_color} Available",
            f"{format_number(supply_summary['available'])} {standard_uom}",
            delta=f"{supply_summary['coverage_ratio']:.0f}% of total"
        )
    
    with overview_cols[3]:
        st.metric("Max SOFT Allocation", f"{format_number(supply_summary['available'])} {standard_uom}")
    
    if supply_summary['available'] <= 0:
        st.error("❌ No supply available for allocation")
    elif supply_summary['coverage_ratio'] < 20:
        st.warning(f"⚠️ Low supply availability! Only {supply_summary['coverage_ratio']:.0f}% available")
    
    st.divider()
    
    # Show details by type
    st.markdown("### 📦 Supply Details by Source")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        show_supply_type_summary(product_id, 'inventory', standard_uom)
    
    with col2:
        show_supply_type_summary(product_id, 'can', standard_uom)
    
    with col3:
        show_supply_type_summary(product_id, 'po', standard_uom)
    
    with col4:
        show_supply_type_summary(product_id, 'wht', standard_uom)

def show_supply_type_summary(product_id, supply_type, standard_uom):
    """Show individual supply type summary with WAREHOUSE & LOCATION"""
    titles = {
        'inventory': '📦 Inventory',
        'can': '🚢 Pending CAN',
        'po': '📋 Pending PO',
        'wht': '🚚 WH Transfer'
    }
    
    st.markdown(f"**{titles.get(supply_type, supply_type)}**")
    
    if supply_type == 'inventory':
        df = supply_data.get_inventory_summary(product_id)
        if not df.empty:
            for _, item in df.iterrows():
                # ===== BUILD LABEL WITH WAREHOUSE =====
                label = f"Batch {item['batch_number']}"
                if item.get('warehouse_name'):
                    label += f" | {item['warehouse_name']}"  # ← WAREHOUSE NAME
                
                st.metric(
                    label,
                    f"{format_number(item['available_quantity'])} {standard_uom}",
                    delta=f"Exp: {format_date(item['expiry_date'])}"
                )
                
                # ===== SHOW LOCATION =====
                if item.get('location'):
                    st.caption(f"📍 Location: {item['location']}")  # ← LOCATION
        else:
            st.caption("No inventory")
    
    elif supply_type == 'can':
        df = supply_data.get_can_summary(product_id)
        if not df.empty:
            for _, item in df.iterrows():
                st.metric(
                    item['arrival_note_number'],
                    f"{format_number(item['pending_quantity'])} {standard_uom}",
                    delta=f"Arr: {format_date(item['arrival_date'])}"
                )
        else:
            st.caption("No pending CAN")
    
    elif supply_type == 'po':
        df = supply_data.get_po_summary(product_id)
        if not df.empty:
            for _, item in df.iterrows():
                etd_str = format_date(item['etd'])
                eta_str = format_date(item.get('eta')) if item.get('eta') else 'N/A'
                st.metric(
                    item['po_number'],
                    f"{format_number(item['pending_quantity'])} {standard_uom}",
                    delta=f"ETD: {etd_str} | ETA: {eta_str}"
                )
        else:
            st.caption("No pending PO")
    
    elif supply_type == 'wht':
        df = supply_data.get_wht_summary(product_id)
        if not df.empty:
            for _, item in df.iterrows():
                st.metric(
                    f"{item['from_warehouse']} → {item['to_warehouse']}",
                    f"{format_number(item['transfer_quantity'])} {standard_uom}",
                    delta=item['status']
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

# ==================== MAIN EXECUTION ====================
def main():
    """Main function - orchestrates the page"""
    
    # Validate user session
    if not auth.check_session():
        st.warning("⚠️ Your session has expired. Please login again.")
        time.sleep(2)
        st.switch_page("app.py")
        st.stop()
    
    # Ensure user data is properly loaded
    if not st.session_state.get('user', {}).get('id'):
        init_session_state()
    
    # Safety checks for modals
    if st.session_state.modals.get('history') and not st.session_state.selections.get('oc_for_history'):
        st.session_state.modals['history'] = False
    if st.session_state.modals.get('allocation') and not st.session_state.selections.get('oc_for_allocation'):
        st.session_state.modals['allocation'] = False
    if st.session_state.modals.get('cancel') and not st.session_state.selections.get('allocation_for_cancel'):
        st.session_state.modals['cancel'] = False
    if st.session_state.modals.get('update_etd') and not st.session_state.selections.get('allocation_for_update'):
        st.session_state.modals['update_etd'] = False
    if st.session_state.modals.get('reverse') and not st.session_state.selections.get('cancellation_for_reverse'):
        st.session_state.modals['reverse'] = False
    
    # Display main page
    show_header()
    show_metrics_row()
    st.divider()
    
    show_search_bar()
    show_quick_filters()
    show_active_filters()
    
    st.divider()
    
    show_product_list()
    
    # Handle modals
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