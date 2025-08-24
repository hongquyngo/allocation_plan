"""
Allocation Planning System - Multiselect Filter Version
Enhanced UX with multiselect filters and exclude/include options
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
    st.session_state.user_id = st.session_state.get('authenticated_user_id', 1)
if 'show_allocation_history' not in st.session_state:
    st.session_state.show_allocation_history = False
if 'selected_oc_for_history' not in st.session_state:
    st.session_state.selected_oc_for_history = None
if 'selected_oc_info' not in st.session_state:
    st.session_state.selected_oc_info = None

# Initialize multiselect states
if 'selected_customers' not in st.session_state:
    st.session_state.selected_customers = []
if 'selected_brands' not in st.session_state:
    st.session_state.selected_brands = []
if 'selected_oc_numbers' not in st.session_state:
    st.session_state.selected_oc_numbers = []
if 'selected_products' not in st.session_state:
    st.session_state.selected_products = []

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
                help="Total available quantity from all sources: Inventory + CAN + PO + Transfer"
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
    except Exception as e:
        logger.error(f"Error loading metrics: {e}")
        st.error(f"Error loading metrics: {str(e)}")

def show_search_and_filters():
    """Display enhanced search bar with autocomplete and multiselect filter controls"""
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Search input with clear button
        search_container = st.container()
        with search_container:
            # Create columns for search input and clear button
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
            
            # Show suggestions if user is typing
            if search_query and len(search_query) >= 2:
                suggestions = data_service.get_search_suggestions(search_query)
                
                # Check if there are any suggestions
                has_suggestions = any(suggestions.values())
                
                if has_suggestions:
                    # Display suggestions in a container
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
                                    st.rerun()
            
            # Update search filter
            if search_query:
                st.session_state.filters['search'] = search_query
            else:
                st.session_state.filters.pop('search', None)
    
    with col2:
        if st.button("⚙️ Advanced Filters", use_container_width=True):
            st.session_state.show_advanced_filters = not st.session_state.show_advanced_filters
    
    # Advanced filters panel with multiselect
    if st.session_state.show_advanced_filters:
        with st.expander("Advanced Filters", expanded=True):
            # Get filter data
            customers_df = data_service.get_customer_list_with_stats()
            brands_df = data_service.get_brand_list_with_stats()
            oc_numbers_df = data_service.get_oc_number_list()
            products_df = data_service.get_product_list_for_filter()
            
            # First row: Customer and Brand
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Customer**")
                
                # Include/Exclude toggle
                customer_mode = st.radio(
                    "Customer filter mode",
                    ["Include", "Exclude"],
                    key="customer_mode",
                    horizontal=True,
                    label_visibility="collapsed"
                )
                
                # Customer multiselect
                if not customers_df.empty:
                    # Format options with stats
                    customer_options = []
                    customer_map = {}
                    for _, row in customers_df.iterrows():
                        option = f"{row['customer_name']} ({row['order_count']} orders, {row['product_count']} products)"
                        customer_options.append(option)
                        customer_map[option] = row['customer_code']
                    
                    selected_customer_options = st.multiselect(
                        "Select customers",
                        options=customer_options,
                        default=st.session_state.selected_customers,
                        key="multiselect_customers",
                        placeholder="Choose customers...",
                        label_visibility="collapsed"
                    )
                    
                    # Map back to customer codes
                    selected_customer_codes = [customer_map[opt] for opt in selected_customer_options]
                    st.session_state.selected_customers = selected_customer_options
            
            with col2:
                st.markdown("**Brand**")
                
                # Include/Exclude toggle
                brand_mode = st.radio(
                    "Brand filter mode",
                    ["Include", "Exclude"],
                    key="brand_mode",
                    horizontal=True,
                    label_visibility="collapsed"
                )
                
                # Brand multiselect
                if not brands_df.empty:
                    # Format options with stats
                    brand_options = []
                    brand_map = {}
                    for _, row in brands_df.iterrows():
                        option = f"{row['brand_name']} ({row['product_count']} products)"
                        brand_options.append(option)
                        brand_map[option] = row['brand_id']
                    
                    selected_brand_options = st.multiselect(
                        "Select brands",
                        options=brand_options,
                        default=st.session_state.selected_brands,
                        key="multiselect_brands",
                        placeholder="Choose brands...",
                        label_visibility="collapsed"
                    )
                    
                    # Map back to brand IDs
                    selected_brand_ids = [brand_map[opt] for opt in selected_brand_options]
                    st.session_state.selected_brands = selected_brand_options
            
            # Second row: OC Number and Product
            col3, col4 = st.columns(2)
            
            with col3:
                st.markdown("**OC Number**")
                
                # Include/Exclude toggle
                oc_mode = st.radio(
                    "OC filter mode",
                    ["Include", "Exclude"],
                    key="oc_mode",
                    horizontal=True,
                    label_visibility="collapsed"
                )
                
                # OC Number multiselect
                if not oc_numbers_df.empty:
                    # Format options with customer info
                    oc_options = []
                    oc_map = {}
                    for _, row in oc_numbers_df.iterrows():
                        option = f"{row['oc_number']} - {row['customer']} ({row['product_count']} items)"
                        oc_options.append(option)
                        oc_map[option] = row['oc_number']
                    
                    selected_oc_options = st.multiselect(
                        "Select OC numbers",
                        options=oc_options,
                        default=st.session_state.selected_oc_numbers,
                        key="multiselect_oc_numbers",
                        placeholder="Choose OC numbers...",
                        label_visibility="collapsed"
                    )
                    
                    # Map back to OC numbers
                    selected_oc_numbers = [oc_map[opt] for opt in selected_oc_options]
                    st.session_state.selected_oc_numbers = selected_oc_options
            
            with col4:
                st.markdown("**PT Code - Product Name**")
                
                # Include/Exclude toggle
                product_mode = st.radio(
                    "Product filter mode",
                    ["Include", "Exclude"],
                    key="product_mode",
                    horizontal=True,
                    label_visibility="collapsed"
                )
                
                # Product multiselect
                if not products_df.empty:
                    # Format options
                    product_options = []
                    product_map = {}
                    for _, row in products_df.iterrows():
                        option = row['display_name']
                        if pd.notna(row['brand_name']):
                            option += f" [{row['brand_name']}]"
                        product_options.append(option)
                        product_map[option] = row['product_id']
                    
                    selected_product_options = st.multiselect(
                        "Select products",
                        options=product_options,
                        default=st.session_state.selected_products,
                        key="multiselect_products",
                        placeholder="Choose products...",
                        label_visibility="collapsed"
                    )
                    
                    # Map back to product IDs
                    selected_product_ids = [product_map[opt] for opt in selected_product_options]
                    st.session_state.selected_products = selected_product_options
            
            # Third row: ETD Range and Supply Coverage (single select)
            col5, col6 = st.columns(2)
            
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
            
            # Apply and Clear buttons
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 1, 2])
            
            with col1:
                if st.button("Apply Filters", type="primary", use_container_width=True):
                    # Build filter dictionary
                    new_filters = {}
                    
                    # Customer filter
                    if 'selected_customer_codes' in locals() and selected_customer_codes:
                        new_filters['customers'] = selected_customer_codes
                        new_filters['exclude_customers'] = customer_mode == "Exclude"
                    
                    # Brand filter
                    if 'selected_brand_ids' in locals() and selected_brand_ids:
                        new_filters['brands'] = selected_brand_ids
                        new_filters['exclude_brands'] = brand_mode == "Exclude"
                    
                    # OC Number filter
                    if 'selected_oc_numbers' in locals() and selected_oc_numbers:
                        new_filters['oc_numbers'] = selected_oc_numbers
                        new_filters['exclude_oc_numbers'] = oc_mode == "Exclude"
                    
                    # Product filter
                    if 'selected_product_ids' in locals() and selected_product_ids:
                        new_filters['products'] = selected_product_ids
                        new_filters['exclude_products'] = product_mode == "Exclude"
                    
                    # ETD filter
                    if etd_option == "Next 7 days":
                        new_filters['etd_days'] = 7
                    elif etd_option == "Next 14 days":
                        new_filters['etd_days'] = 14
                    elif etd_option == "Next 30 days":
                        new_filters['etd_days'] = 30
                    elif etd_option == "Custom range":
                        new_filters['date_from'] = date_from
                        new_filters['date_to'] = date_to
                    
                    # Coverage filter
                    if coverage_option != "All":
                        new_filters['coverage'] = coverage_option
                    
                    # Keep search filter if exists
                    if 'search' in st.session_state.filters:
                        new_filters['search'] = st.session_state.filters['search']
                    
                    st.session_state.filters = new_filters
                    st.session_state.page_number = 1
                    st.rerun()
            
            with col2:
                if st.button("Clear All", type="secondary", use_container_width=True):
                    # Clear all multiselect states
                    st.session_state.selected_customers = []
                    st.session_state.selected_brands = []
                    st.session_state.selected_oc_numbers = []
                    st.session_state.selected_products = []
                    
                    # Clear filters except search
                    search_filter = st.session_state.filters.get('search')
                    st.session_state.filters = {}
                    if search_filter:
                        st.session_state.filters['search'] = search_filter
                    
                    st.session_state.page_number = 1
                    st.rerun()
    
    # Quick filters with visual feedback
    st.markdown("**Quick Filters:**")
    filter_cols = st.columns(5)
    
    # Helper function to show active state
    def is_filter_active(filter_type):
        if filter_type == 'all':
            # Check if only search filter exists
            return len([k for k in st.session_state.filters.keys() if k != 'search']) == 0
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
            # Keep search filter
            search_filter = st.session_state.filters.get('search')
            st.session_state.filters = {}
            if search_filter:
                st.session_state.filters['search'] = search_filter
            st.session_state.page_number = 1
            st.rerun()
    
    with filter_cols[1]:
        button_type = "primary" if is_filter_active('low_supply') else "secondary"
        if st.button("⚠️ Low Supply", use_container_width=True, type=button_type):
            st.session_state.filters['supply_status'] = 'low'
            st.session_state.page_number = 1
            st.rerun()
    
    with filter_cols[2]:
        button_type = "primary" if is_filter_active('urgent') else "secondary"
        if st.button("🔴 Urgent ETD", use_container_width=True, type=button_type):
            st.session_state.filters['etd_urgency'] = 'urgent'
            st.session_state.page_number = 1
            st.rerun()
    
    with filter_cols[3]:
        button_type = "primary" if is_filter_active('not_allocated') else "secondary"
        if st.button("❌ Not Allocated", use_container_width=True, type=button_type):
            st.session_state.filters['allocation_status'] = 'none'
            st.session_state.page_number = 1
            st.rerun()
    
    with filter_cols[4]:
        button_type = "primary" if is_filter_active('has_inventory') else "secondary"
        if st.button("📦 Has Inventory", use_container_width=True, type=button_type):
            st.session_state.filters['has_inventory'] = True
            st.session_state.page_number = 1
            st.rerun()
    
    # Show active filters with clear button for each
    active_filters = [(k, v) for k, v in st.session_state.filters.items() if k != 'search']
    if active_filters:
        st.markdown("**Active Filters:**")
        filter_container = st.container()
        with filter_container:
            # Display multiselect filters with exclude/include info
            display_filters = []
            
            # Format filter display
            for key, value in active_filters:
                if key == 'customers':
                    mode = "Exclude" if st.session_state.filters.get('exclude_customers') else "Include"
                    display_filters.append((f"Customers ({mode}): {len(value)} selected", key))
                elif key == 'brands':
                    mode = "Exclude" if st.session_state.filters.get('exclude_brands') else "Include"
                    display_filters.append((f"Brands ({mode}): {len(value)} selected", key))
                elif key == 'oc_numbers':
                    mode = "Exclude" if st.session_state.filters.get('exclude_oc_numbers') else "Include"
                    display_filters.append((f"OC Numbers ({mode}): {len(value)} selected", key))
                elif key == 'products':
                    mode = "Exclude" if st.session_state.filters.get('exclude_products') else "Include"
                    display_filters.append((f"Products ({mode}): {len(value)} selected", key))
                elif key not in ['exclude_customers', 'exclude_brands', 'exclude_oc_numbers', 'exclude_products']:
                    # Other filters
                    filter_label = {
                        'supply_status': '⚠️ Low Supply',
                        'etd_urgency': '🔴 Urgent ETD',
                        'allocation_status': '❌ Not Allocated',
                        'has_inventory': '📦 Has Inventory',
                        'etd_days': f'ETD: Next {value} days',
                        'coverage': f'Coverage: {value}'
                    }.get(key, f"{key}: {value}")
                    display_filters.append((filter_label, key))
            
            # Display in columns
            cols = st.columns(5)
            for idx, (label, key) in enumerate(display_filters[:5]):
                with cols[idx % 5]:
                    if st.button(f"{label} ✕", key=f"clear_{key}"):
                        # Remove filter and related exclude flag
                        st.session_state.filters.pop(key, None)
                        if key == 'customers':
                            st.session_state.filters.pop('exclude_customers', None)
                        elif key == 'brands':
                            st.session_state.filters.pop('exclude_brands', None)
                        elif key == 'oc_numbers':
                            st.session_state.filters.pop('exclude_oc_numbers', None)
                        elif key == 'products':
                            st.session_state.filters.pop('exclude_products', None)
                        st.session_state.page_number = 1
                        st.rerun()

def show_product_list():
    """Display product list with demand/supply summary"""
    # Get product data
    try:
        products_df = data_service.get_products_with_demand_supply(
            filters=st.session_state.filters,
            page=st.session_state.page_number,
            page_size=ITEMS_PER_PAGE
        )
        
        if products_df is not None and not products_df.empty:
            st.caption(f"Found {len(products_df)} products (Page {st.session_state.page_number})")
    except Exception as e:
        st.error(f"Error loading products: {str(e)}")
        logger.error(f"Error in show_product_list: {e}")
        products_df = pd.DataFrame()
    
    if products_df.empty:
        # Show helpful message
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
                    st.session_state.page_number = 1
                    st.rerun()
            else:
                st.write("**No products with pending demand found**")
        return
    
    # Create header
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
    
    # Display each product
    for idx, row in products_df.iterrows():
        product_id = row['product_id']
        is_expanded = product_id in st.session_state.expanded_products
        
        # Main product row
        cols = st.columns([3.5, 1.5, 1.5, 0.5])
        
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

            # Show additional info with brand
            info_parts = [row['pt_code']]
            if pd.notna(row.get('brand_name')):
                info_parts.append(row['brand_name'])
            if pd.notna(row.get('package_size')) and row['package_size']:
                info_parts.append(row['package_size'])
            info_parts.append(row['standard_uom'])
            
            st.caption(" | ".join(info_parts))
            
            # Show customer and OC info if available
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
            # Status indicator with tooltip
            indicator, status = get_supply_status_indicator(row['total_demand'], row['total_supply'])
            st.markdown(f"{indicator}", help=f"Supply Status: {status}")
        
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
    
    # Add headers for the OC table
    header_cols = st.columns([2, 2, 1, 1, 1.5, 1.5])
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
        cols = st.columns([2, 2, 1, 1, 1.5, 1.5])
        
        with cols[0]:
            st.text(f"📄 {oc['oc_number']}")
        
        with cols[1]:
            st.text(f"🏢 {oc['customer']}")
        
        with cols[2]:
            etd_days = (pd.to_datetime(oc['etd']).date() - datetime.now().date()).days
            etd_color = ""
            if etd_days <= 0:
                etd_color = "⚫"  # Overdue
            elif etd_days <= 7:
                etd_color = "🔴"  # Urgent
            elif etd_days <= 14:
                etd_color = "🟡"  # Soon
            
            st.text(f"{etd_color} {format_date(oc['etd'])}")
        
        with cols[3]:
            # Show pending quantity with UOM
            st.text(f"{format_number(oc['pending_quantity'])} {oc.get('selling_uom', '')}")
        
        with cols[4]:
            # Show total allocated quantity with clickable button
            allocated_qty = oc.get('allocated_quantity', 0)
            allocation_count = oc.get('allocation_count', 0)
            
            if allocated_qty > 0:
                button_label = f"{format_number(allocated_qty)} {oc.get('selling_uom', '')}"
                if allocation_count > 1:
                    button_label += f" ({allocation_count})"
                
                if st.button(
                    button_label, 
                    key=f"view_alloc_{oc['ocd_id']}", 
                    help=f"Click to view {allocation_count} allocation(s)",
                    use_container_width=True
                ):
                    st.session_state.show_allocation_history = True
                    st.session_state.selected_oc_for_history = oc['ocd_id']
                    st.session_state.selected_oc_info = {
                        'oc_number': oc['oc_number'],
                        'customer': oc['customer'],
                        'product_name': oc['product_name'],
                        'selling_uom': oc.get('selling_uom', '')
                    }
                    st.rerun()
            else:
                st.text(f"0 {oc.get('selling_uom', '')}")
        
        with cols[5]:
            if st.button("Allocate", key=f"alloc_oc_{oc['ocd_id']}", use_container_width=True, type="primary"):
                st.session_state.selected_oc_for_allocation = oc.to_dict()
                st.session_state.show_allocation_modal = True
                st.rerun()

@st.dialog("Allocation History", width="large")
def show_allocation_history_modal():
    """Show allocation history for selected OC"""
    if 'selected_oc_for_history' not in st.session_state:
        st.error("No OC selected")
        return
    
    oc_detail_id = st.session_state.selected_oc_for_history
    oc_info = st.session_state.selected_oc_info
    
    # Header
    st.markdown(f"### Allocation History for {oc_info['oc_number']}")
    
    col1, col2 = st.columns(2)
    with col1:
        st.caption(f"**Customer:** {oc_info['customer']}")
    with col2:
        st.caption(f"**Product:** {oc_info['product_name']}")
    
    st.divider()
    
    # Get allocation history
    history_df = data_service.get_allocation_history(oc_detail_id)
    
    if history_df.empty:
        st.info("No allocation history found")
    else:
        # Display each allocation
        for idx, alloc in history_df.iterrows():
            with st.container():
                # Allocation header with status color
                status_color = {
                    'ALLOCATED': '🟢',
                    'DRAFT': '🟡',
                    'CANCELLED': '🔴'
                }.get(alloc['status'], '⚪')
                
                col1, col2, col3 = st.columns([2, 1, 1])
                with col1:
                    st.markdown(f"{status_color} **{alloc['allocation_number']}**")
                with col2:
                    st.caption(f"Mode: {alloc['allocation_mode']}")
                with col3:
                    st.caption(f"Status: {alloc['status']}")
                
                # Allocation details
                detail_cols = st.columns([1, 1, 1, 1])
                
                uom = oc_info.get('selling_uom', '')
                
                with detail_cols[0]:
                    st.metric("Allocated Qty", f"{format_number(alloc['allocated_qty'])} {uom}")
                
                with detail_cols[1]:
                    st.metric("Effective Qty", f"{format_number(alloc['effective_qty'])} {uom}")
                
                with detail_cols[2]:
                    st.metric("Delivered Qty", f"{format_number(alloc['delivered_qty'])} {uom}")
                
                with detail_cols[3]:
                    st.metric("Cancelled Qty", f"{format_number(alloc['cancelled_qty'])} {uom}")
                
                # Additional info
                info_cols = st.columns([1, 1, 1])
                
                with info_cols[0]:
                    st.caption(f"📅 **Date:** {format_date(alloc['allocation_date'])}")
                
                with info_cols[1]:
                    st.caption(f"📅 **Allocated ETD:** {format_date(alloc['allocated_etd'])}")
                
                with info_cols[2]:
                    st.caption(f"👤 **Created by:** {alloc['created_by']}")
                
                # Supply source and notes
                st.caption(f"📦 **Source:** {alloc['supply_source_type']}")
                
                if alloc.get('notes'):
                    st.caption(f"📝 **Notes:** {alloc['notes']}")
                
                if alloc.get('cancellation_info'):
                    st.warning(f"⚠️ {alloc['cancellation_info']}")
                
                st.divider()
    
    # Close button
    if st.button("Close", use_container_width=True):
        st.session_state.show_allocation_history = False
        st.session_state.selected_oc_for_history = None
        st.session_state.selected_oc_info = None
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

# Show allocation history modal if needed
if st.session_state.show_allocation_history:
    show_allocation_history_modal()