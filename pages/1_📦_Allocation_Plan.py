"""
Allocation Planning System - Main Page
Handles OC review and allocation management
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import logging

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
if 'selected_oc' not in st.session_state:
    st.session_state.selected_oc = None
if 'show_allocation_modal' not in st.session_state:
    st.session_state.show_allocation_modal = False
if 'refresh_data' not in st.session_state:
    st.session_state.refresh_data = True
if 'filters' not in st.session_state:
    st.session_state.filters = {}

# Header
col1, col2 = st.columns([6, 1])
with col1:
    st.title("📦 Allocation Planning System")
with col2:
    if st.button("🚪 Logout", use_container_width=True):
        auth.logout()
        st.switch_page("app.py")

# User info
st.caption(f"👤 {auth.get_user_display_name()} | 🕒 {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# Functions definitions (moved before usage)
def show_metrics():
    """Display key metrics"""
    try:
        metrics = data_service.get_dashboard_metrics()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Pending OCs",
                format_number(metrics.get('total_pending_ocs', 0)),
                help="Number of OCs with pending delivery"
            )
        
        with col2:
            st.metric(
                "Total Pending Value",
                format_currency(metrics.get('total_pending_value_usd', 0)),
                help="Total USD value of pending deliveries"
            )
        
        with col3:
            st.metric(
                "Unallocated Items",
                format_number(metrics.get('unallocated_count', 0)),
                delta=f"-{metrics.get('unallocated_change', 0)}" if metrics.get('unallocated_change', 0) > 0 else None,
                help="OCs without any allocation"
            )
        
        with col4:
            st.metric(
                "⚠️ Over-allocated",
                format_number(metrics.get('over_allocated_count', 0)),
                delta_color="inverse",
                help="OCs with allocation exceeding order quantity"
            )
        
        st.markdown("---")
        
    except Exception as e:
        logger.error(f"Error loading metrics: {e}")
        st.error("Failed to load metrics")

def show_filters():
    """Display filter controls"""
    with st.expander("🔍 Filters", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            # Date range filter
            date_option = st.selectbox(
                "ETD Range",
                ["Next 7 days", "Next 14 days", "Next 30 days", "Custom range", "All dates"],
                index=1
            )
            
            if date_option == "Next 7 days":
                st.session_state.filters['date_from'] = datetime.now().date()
                st.session_state.filters['date_to'] = datetime.now().date() + timedelta(days=7)
            elif date_option == "Next 14 days":
                st.session_state.filters['date_from'] = datetime.now().date()
                st.session_state.filters['date_to'] = datetime.now().date() + timedelta(days=14)
            elif date_option == "Next 30 days":
                st.session_state.filters['date_from'] = datetime.now().date()
                st.session_state.filters['date_to'] = datetime.now().date() + timedelta(days=30)
            elif date_option == "Custom range":
                st.session_state.filters['date_from'] = st.date_input("From date", datetime.now().date())
                st.session_state.filters['date_to'] = st.date_input("To date", datetime.now().date() + timedelta(days=30))
            else:
                st.session_state.filters.pop('date_from', None)
                st.session_state.filters.pop('date_to', None)
        
        with col2:
            # Allocation status filter
            status_options = ["All", "Not Allocated", "Partially Allocated", "Fully Allocated", "Over Allocated"]
            status_filter = st.selectbox("Allocation Status", status_options)
            
            if status_filter != "All":
                st.session_state.filters['allocation_status'] = status_filter
            else:
                st.session_state.filters.pop('allocation_status', None)
        
        # Customer filter
        customers = data_service.get_customer_list()
        selected_customers = st.multiselect(
            "Customers",
            options=customers['customer_code'].tolist() if not customers.empty else [],
            format_func=lambda x: f"{x} - {customers[customers['customer_code']==x]['customer_name'].iloc[0]}" if not customers.empty else x
        )
        
        if selected_customers:
            st.session_state.filters['customers'] = selected_customers
        else:
            st.session_state.filters.pop('customers', None)
        
        # Apply filters button
        if st.button("Apply Filters", type="primary", use_container_width=True):
            st.session_state.refresh_data = True
            st.rerun()

def show_oc_list(df):
    """Display OC list with allocation actions"""
    # Add selection column
    df_display = df.copy()
    
    # Display each OC as a card
    for idx, row in df_display.iterrows():
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 1])
            
            with col1:
                st.markdown(f"**{row['oc_number']}**")
                st.caption(f"{row['customer']} | {row['product_name']}")
            
            with col2:
                st.metric(
                    "Pending",
                    format_number(row['pending_quantity']),
                    delta=None,
                    label_visibility="visible"
                )
            
            with col3:
                allocated_pct = (row['allocated_quantity'] / row['pending_quantity'] * 100) if row['pending_quantity'] > 0 else 0
                st.metric(
                    "Allocated",
                    format_number(row['allocated_quantity']),
                    delta=f"{allocated_pct:.0f}%"
                )
            
            with col4:
                status_emoji = format_status(row['allocation_status'])
                if row['is_over_allocated'] == 'Yes':
                    st.error(status_emoji)
                else:
                    st.info(status_emoji)
            
            with col5:
                if st.button("Allocate", key=f"btn_alloc_{idx}", use_container_width=True):
                    st.session_state.selected_oc = row.to_dict()
                    st.session_state.show_allocation_modal = True
                    st.rerun()
            
            st.divider()

def show_supply_summary():
    """Display supply sources summary"""
    st.subheader("📊 Supply Sources")
    
    # Get selected product if any
    selected_product_id = None
    if st.session_state.selected_oc:
        selected_product_id = st.session_state.selected_oc.get('product_id')
    
    # Supply source tabs
    tab1, tab2, tab3, tab4 = st.tabs(["Inventory", "Pending CAN", "Pending PO", "WH Transfer"])
    
    with tab1:
        try:
            inventory = data_service.get_inventory_summary(selected_product_id)
            if not inventory.empty:
                for _, item in inventory.iterrows():
                    st.metric(
                        item['product_name'][:30] + "..." if len(item['product_name']) > 30 else item['product_name'],
                        format_number(item['total_quantity']),
                        delta=f"{item['locations']} locations"
                    )
            else:
                st.info("No inventory available")
        except Exception as e:
            logger.error(f"Error loading inventory: {e}")
            st.error("Failed to load inventory")
    
    with tab2:
        try:
            can_data = data_service.get_can_summary(selected_product_id)
            if not can_data.empty:
                for _, item in can_data.iterrows():
                    st.metric(
                        item['product_name'][:30] + "..." if len(item['product_name']) > 30 else item['product_name'],
                        format_number(item['pending_quantity']),
                        delta=f"Arrives {format_date(item['arrival_date'])}"
                    )
            else:
                st.info("No pending CAN")
        except Exception as e:
            logger.error(f"Error loading CAN data: {e}")
            st.error("Failed to load CAN data")
    
    with tab3:
        try:
            po_data = data_service.get_po_summary(selected_product_id)
            if not po_data.empty:
                for _, item in po_data.iterrows():
                    st.metric(
                        item['product_name'][:30] + "..." if len(item['product_name']) > 30 else item['product_name'],
                        format_number(item['pending_quantity']),
                        delta=f"ETD {format_date(item['etd'])}"
                    )
            else:
                st.info("No pending PO")
        except Exception as e:
            logger.error(f"Error loading PO data: {e}")
            st.error("Failed to load PO data")
    
    with tab4:
        try:
            wht_data = data_service.get_wht_summary(selected_product_id)
            if not wht_data.empty:
                for _, item in wht_data.iterrows():
                    st.metric(
                        item['product_name'][:30] + "..." if len(item['product_name']) > 30 else item['product_name'],
                        format_number(item['transfer_quantity']),
                        delta=item['status']
                    )
            else:
                st.info("No warehouse transfers")
        except Exception as e:
            logger.error(f"Error loading WHT data: {e}")
            st.error("Failed to load WHT data")

def show_oc_allocation_tab():
    """Display OC pending allocation interface"""
    col1, col2 = st.columns([7, 3])
    
    # Left column - OC List
    with col1:
        st.subheader("📋 Order Confirmations Pending Delivery")
        
        # Filters
        show_filters()
        
        # Load and display OC data
        if st.session_state.refresh_data:
            try:
                oc_df = data_service.get_oc_pending(st.session_state.filters)
                st.session_state.oc_data = oc_df
                st.session_state.refresh_data = False
            except Exception as e:
                logger.error(f"Error loading OC data: {e}")
                st.error("Failed to load OC data")
                oc_df = pd.DataFrame()
        else:
            oc_df = st.session_state.get('oc_data', pd.DataFrame())
        
        if not oc_df.empty:
            show_oc_list(oc_df)
        else:
            st.info("No pending OCs found with current filters")
    
    # Right column - Supply Summary
    with col2:
        show_supply_summary()

def show_inventory_details():
    """Show detailed inventory view"""
    try:
        inventory = data_service.get_inventory_details()
        if not inventory.empty:
            st.dataframe(
                inventory,
                column_config={
                    "product_name": "Product",
                    "batch_number": "Batch",
                    "location": "Location",
                    "remaining_quantity": st.column_config.NumberColumn("Available", format="%.0f"),
                    "expiry_date": st.column_config.DateColumn("Expiry"),
                    "warehouse_name": "Warehouse"
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("No inventory data available")
    except Exception as e:
        logger.error(f"Error loading inventory details: {e}")
        st.error("Failed to load inventory details")

def show_can_details():
    """Show detailed CAN view"""
    try:
        can_data = data_service.get_can_details()
        if not can_data.empty:
            st.dataframe(
                can_data,
                column_config={
                    "arrival_note_number": "CAN Number",
                    "product_name": "Product",
                    "available_quantity": st.column_config.NumberColumn("Pending Qty", format="%.0f"),
                    "arrival_date": st.column_config.DateColumn("Arrival Date"),
                    "days_since_arrival": st.column_config.NumberColumn("Days", format="%.0f"),
                    "vendor": "Vendor"  # Sửa từ vendor_name
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("No pending CAN data available")
    except Exception as e:
        logger.error(f"Error loading CAN details: {e}")
        st.error("Failed to load CAN details")

def show_po_details():
    """Show detailed PO view"""
    try:
        po_data = data_service.get_po_details()
        if not po_data.empty:
            st.dataframe(
                po_data,
                column_config={
                    "po_number": "PO Number",
                    "product_name": "Product",
                    "available_quantity": st.column_config.NumberColumn("Pending Qty", format="%.0f"),
                    "etd": st.column_config.DateColumn("ETD"),
                    "vendor_name": "Vendor",
                    "po_status": "Status"
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("No pending PO data available")
    except Exception as e:
        logger.error(f"Error loading PO details: {e}")
        st.error("Failed to load PO details")

def show_wht_details():
    """Show detailed warehouse transfer view"""
    try:
        wht_data = data_service.get_wht_details()
        if not wht_data.empty:
            st.dataframe(
                wht_data,
                column_config={
                    "product_name": "Product",
                    "transfer_quantity": st.column_config.NumberColumn("Transfer Qty", format="%.0f"),
                    "from_warehouse": "From",
                    "to_warehouse": "To",
                    "transfer_date": st.column_config.DateColumn("Date"),
                    "is_completed": st.column_config.CheckboxColumn("Completed")
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("No warehouse transfer data available")
    except Exception as e:
        logger.error(f"Error loading WHT details: {e}")
        st.error("Failed to load WHT details")

def show_supply_overview_tab():
    """Display supply overview tab"""
    st.subheader("📊 Supply Sources Overview")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        source_type = st.radio(
            "Select Source",
            ["Inventory", "Pending CAN", "Pending PO", "Warehouse Transfer"]
        )
    
    with col2:
        if source_type == "Inventory":
            show_inventory_details()
        elif source_type == "Pending CAN":
            show_can_details()
        elif source_type == "Pending PO":
            show_po_details()
        else:
            show_wht_details()

def show_analytics_tab():
    """Display analytics tab"""
    st.subheader("📈 Allocation Analytics")
    
    # Placeholder for analytics
    col1, col2 = st.columns(2)
    
    with col1:
        st.info("📊 Allocation by Customer - Coming Soon")
    
    with col2:
        st.info("📈 Allocation Trend - Coming Soon")
    
    st.info("💡 Analytics features will be available in the next release")

@st.dialog("Create/Update Allocation", width="large")
def show_allocation_modal():
    """Display allocation creation/update modal"""
    oc = st.session_state.selected_oc
    
    # Header
    st.subheader(f"📦 Allocate for {oc['oc_number']}")
    
    # OC Information
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Product", oc['product_name'][:20] + "..." if len(oc['product_name']) > 20 else oc['product_name'])
    col2.metric("Customer", oc['customer'])
    col3.metric("Required", format_number(oc['pending_quantity']))
    col4.metric("ETD", format_date(oc['etd']))
    
    st.divider()
    
    # Check existing allocations
    existing_allocations = data_service.get_existing_allocations(oc['ocd_id'])
    if not existing_allocations.empty:
        st.info(f"ℹ️ This OC has {len(existing_allocations)} existing allocation(s)")
        with st.expander("View existing allocations"):
            # Format display for existing allocations
            display_df = existing_allocations.copy()
            display_df['supply_source_display'] = display_df['supply_source_type'].apply(
                lambda x: 'No specific source (SOFT)' if x == 'No specific source' else x
            )
            
            st.dataframe(
                display_df[['allocation_number', 'supply_source_display', 'allocated_qty', 'allocation_mode', 'status']].rename(columns={
                    'supply_source_display': 'Supply Source'
                }),
                hide_index=True
            )
    
    # Check total available supply
    total_supply = data_service.get_total_available_supply(oc['product_id'])
    
    if not total_supply['has_supply']:
        st.error("❌ No available supply for this product")
        st.info("Cannot create allocation without available supply from any source")
        if st.button("Close"):
            st.session_state.show_allocation_modal = False
            st.rerun()
        return
    
    # Show available supply summary
    st.info(f"✅ Total available supply: **{format_number(total_supply['total_available'])}** units")
    with st.expander("View supply breakdown"):
        for source in total_supply['sources']:
            st.write(f"- {source['source']}: {format_number(source['quantity'])} units ({source['count']} items)")
    
    st.divider()
    
    # Supply source selection
    source_type = st.radio(
        "Select Allocation Type",
        ["SOFT Allocation (No specific source)", "HARD Allocation (Select specific source)"],
        help="""
        **SOFT**: Reserve quantity without locking to specific supply source. Flexible for later changes.
        **HARD**: Lock allocation to specific supply source. Cannot be changed after creation.
        """
    )
    
    # For SOFT allocation, just show quantity input
    if source_type == "SOFT Allocation (No specific source)":
        st.info("ℹ️ SOFT allocation reserves quantity without selecting specific supply source")
        
        allocations = []
        total_allocated = st.number_input(
            "Allocation Quantity",
            min_value=0.0,
            max_value=min(float(oc['pending_quantity']), float(total_supply['total_available'])),
            value=0.0,
            step=1.0,
            help=f"Enter quantity to allocate (max: {min(oc['pending_quantity'], total_supply['total_available'])})"
        )
        
        if total_allocated > 0:
            allocations.append({
                'source_type': None,
                'source_id': None,
                'quantity': total_allocated,
                'supply_info': {'type': 'SOFT', 'description': 'No specific source'}
            })
    else:
        # HARD allocation - select specific source
        supply_type_options = []
        if any(s['source'] == 'Inventory' for s in total_supply['sources']):
            supply_type_options.append("Inventory")
        if any(s['source'] == 'Pending CAN' for s in total_supply['sources']):
            supply_type_options.append("Pending CAN")
        if any(s['source'] == 'Pending PO' for s in total_supply['sources']):
            supply_type_options.append("Pending PO")
        if any(s['source'] == 'Warehouse Transfer' for s in total_supply['sources']):
            supply_type_options.append("Warehouse Transfer")
        
        selected_source = st.selectbox(
            "Select Supply Source",
            supply_type_options,
            help="Choose which supply source to allocate from"
        )
        
        # Get available supplies for selected source
        supplies = data_service.get_supply_details(oc['product_id'], selected_source)
        
        if supplies.empty:
            st.warning(f"No {selected_source} available")
            allocations = []
            total_allocated = 0
        else:
            # Supply selection and allocation
            st.subheader("Select Supply Items")
            
            allocations = []
            total_allocated = 0
            
            # Display supplies for selection
            for idx, supply in supplies.iterrows():
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        # Display supply information based on type
                        if selected_source == "Inventory":
                            info = f"Batch: {supply['batch_number']} | Location: {supply['location']} | Expiry: {format_date(supply['expiry_date'])}"
                        elif selected_source == "Pending CAN":
                            info = f"CAN: {supply.get('arrival_note_number', 'N/A')} | Arrival: {format_date(supply.get('arrival_date'))} | Days: {supply.get('days_since_arrival', 0)}"
                        elif selected_source == "Pending PO":
                            info = f"PO: {supply.get('po_number', 'N/A')} | Vendor: {supply.get('vendor_name', 'N/A')} | ETD: {format_date(supply.get('etd'))}"
                        else:  # Warehouse Transfer
                            info = f"From: {supply.get('from_warehouse', 'N/A')} | To: {supply.get('to_warehouse', 'N/A')} | Completed: {'Yes' if supply.get('is_completed') else 'No'}"
                        
                        st.text(info)
                        st.caption(f"Available: {format_number(supply['available_quantity'])} {supply.get('uom', 'pcs')}")
                    
                    with col2:
                        # Allocation quantity input
                        max_qty = min(supply['available_quantity'], oc['pending_quantity'] - total_allocated)
                        qty = st.number_input(
                            "Allocate",
                            min_value=0.0,
                            max_value=float(max_qty),
                            value=0.0,
                            step=1.0,
                            key=f"alloc_qty_{idx}"
                        )
                        
                        if qty > 0:
                            # Map source type for database
                            db_source_type = {
                                'Inventory': 'INVENTORY',
                                'Pending CAN': 'PENDING_CAN', 
                                'Pending PO': 'PENDING_PO',
                                'Warehouse Transfer': 'PENDING_WHT'
                            }.get(selected_source, selected_source)
                            
                            allocations.append({
                                'source_type': db_source_type,
                                'source_id': supply.get('source_id', supply.get('id')),
                                'quantity': qty,
                                'supply_info': supply.to_dict()
                            })
                            total_allocated += qty
    
    st.divider()
    
    # Allocation summary
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Allocated", format_number(total_allocated))
    col2.metric("Remaining", format_number(max(0, oc['pending_quantity'] - total_allocated)))
    col3.metric("Coverage", format_percentage(total_allocated / oc['pending_quantity'] * 100 if oc['pending_quantity'] > 0 else 0))
    
    # Allocation parameters
    col1, col2 = st.columns(2)
    with col1:
        # Set mode based on allocation type selected
        if source_type == "SOFT Allocation (No specific source)":
            allocation_mode = "SOFT"
            st.info("Mode: SOFT (flexible)")
        else:
            allocation_mode = st.selectbox(
                "Allocation Mode", 
                ["SOFT", "HARD"], 
                help="""
                **SOFT**: Can change supply source later
                **HARD**: Locked to selected supply source
                """,
                index=1  # Default to HARD for specific source
            )
    with col2:
        allocated_etd = st.date_input("Allocated ETD", value=oc['etd'])
    
    notes = st.text_area("Notes", placeholder="Optional notes about this allocation...")
    
    # Validation warnings
    if total_allocated > oc['pending_quantity']:
        st.warning(f"⚠️ Over-allocation: {format_number(total_allocated - oc['pending_quantity'])} units")
    
    # Action buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("💾 Save Allocation", type="primary", use_container_width=True, disabled=len(allocations) == 0):
            try:
                # Validate allocations
                validation_errors = validator.validate_allocations(allocations, oc, allocation_mode)
                if validation_errors:
                    for error in validation_errors:
                        st.error(error)
                else:
                    # Save allocation
                    result = allocation_service.create_allocation(
                        oc_detail_id=oc['ocd_id'],
                        allocations=allocations,
                        mode=allocation_mode,
                        etd=allocated_etd,
                        notes=notes,
                        user_id=st.session_state.user_id
                    )
                    
                    if result['success']:
                        st.success(f"✅ Allocation created successfully! ID: {result['allocation_number']}")
                        st.balloons()
                        
                        # Close modal and refresh
                        st.session_state.show_allocation_modal = False
                        st.session_state.refresh_data = True
                        st.rerun()
                    else:
                        st.error(f"❌ Failed to create allocation: {result['error']}")
                        
            except Exception as e:
                logger.error(f"Error creating allocation: {e}")
                st.error(f"❌ Error: {str(e)}")
    
    with col2:
        if st.button("🔄 Reset", use_container_width=True):
            st.rerun()
    
    with col3:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state.show_allocation_modal = False
            st.rerun()

# Metrics Row
show_metrics()

# Main content
tab1, tab2, tab3 = st.tabs(["📋 OC Pending Allocation", "📊 Supply Overview", "📈 Analytics"])

with tab1:
    show_oc_allocation_tab()

with tab2:
    show_supply_overview_tab()

with tab3:
    show_analytics_tab()

# Allocation Modal
if st.session_state.show_allocation_modal and st.session_state.selected_oc:
    show_allocation_modal()

# Auto-refresh data periodically
if st.session_state.get('auto_refresh', False):
    time.sleep(300)  # Refresh every 5 minutes
    st.session_state.refresh_data = True
    st.rerun()