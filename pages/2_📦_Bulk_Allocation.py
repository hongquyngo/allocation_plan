"""
Bulk Allocation Page
====================
Main page for bulk allocation with strategy-based allocation assistance.

Features:
- Scope selection (Brand, Customer, Legal Entity, ETD Range)
- Allocation status breakdown (NEW: Not Allocated, Partial, Fully Allocated)
- Strategy selection (FCFS, ETD Priority, Proportional, Revenue Priority, Hybrid)
- Simulation preview with fine-tuning
- Bulk commit with summary email

REFACTORED: 2024-12 - Added customer name display, deduplicated product display logic
REFACTORED: 2024-12 - Fixed CSS leak in allocation status chart
REFACTORED: 2024-12 - Added demands_dict to email notification for OC creator lookup
REFACTORED: 2024-12 - Improved email notification UI:
    - Separate spinner for email sending (vs DB commit)
    - Preview recipients before sending
    - Email metrics (Summary/Individual/Errors) in columns
    - Detailed success/warning/error messages
    - Expander for error details
FEATURE: 2024-12 - Added Supply Context UI in Step 3:
    - Supply summary panel showing Total/Committed/Available
    - Available supply column in fine-tuning table
    - Product supply detail expander
BUGFIX: 2024-12 - Fixed "Select All" button not re-selecting unchecked rows
FEATURE: 2024-12 - Added "Clear All" button to deselect all rows
    - Renamed "Include All" → "Select All" for clarity
    - Added force_include_all / force_clear_all session state flags
FEATURE: 2024-12 - Added navigation buttons after commit:
    - New Allocation (go home), Same Scope, Adjust Scope
    - Separate navigation for commit fail scenario
FEATURE: 2024-12 - Added Developer Tools (Clear Cache) for admin/GM/MD
BUGFIX: 2024-12 - Fixed checkbox edits resetting on rerun:
    - Store include states in allocation_include_states session state
    - Sync from widget state BEFORE building base_df
BUGFIX: 2024-12 - Fixed Split Allocation ETD not updating:
    - Root cause: Form widgets don't commit to session_state until submit
    - Buttons outside form couldn't read edited values
    - Solution: Replaced form with regular widgets (sync immediately)
    - Kept Save button for explicit save action
REFACTORED: 2024-12 - Split Allocation pending/saved separation:
    - Added pending_split_edits for unsaved changes
    - Add/Remove only updates pending state
    - Save button commits pending to split_allocations
    - Active Splits only shows saved configurations
    - Unsaved changes indicator
FEATURE: 2024-12 - Split Allocation UX improvements:
    - Added ocd_id column to allocation editor for easy reference
    - Full product display in dropdown (no truncation)
    - Dropdown shows [ID:xxx] prefix for quick identification
    - @st.fragment wrapper to prevent full page rerun on qty/etd changes
    - Note: Requires Streamlit >= 1.33.0 for fragment support
"""
import streamlit as st
import pandas as pd
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Import bulk allocation modules
from utils.bulk_allocation import (
    BulkAllocationData,
    StrategyEngine,
    BulkAllocationValidator,
    BulkAllocationService,
    BulkEmailService
)
from utils.bulk_allocation.strategy_engine import StrategyType, StrategyConfig
from utils.bulk_allocation.bulk_formatters import (
    format_number, format_percentage, format_date,
    format_coverage_badge, format_strategy_name, format_allocation_mode,
    format_etd_urgency, format_scope_summary, format_quantity_with_uom,
    # NEW: Imported formatter functions
    format_product_display,
    format_customer_display
)
from utils.bulk_allocation.bulk_tooltips import (
    SCOPE_TOOLTIPS, STRATEGY_TOOLTIPS, REVIEW_TOOLTIPS, FORMULA_TOOLTIPS
)
# NEW: Import Supply Context UI components
from utils.bulk_allocation.bulk_supply_context import (
    build_supply_context,
    render_supply_summary_panel,
    render_product_supply_detail,
    get_supply_tooltip
)
from utils.auth import AuthManager

# Page configuration
st.set_page_config(
    page_title="Bulk Allocation",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Authentication
auth = AuthManager()
if not auth.check_session():
    st.warning("⚠️ Please login to access this page")
    st.stop()

# Get current user from session state
user = st.session_state.get('user', {})
if not user or not user.get('id'):
    st.error("Please login to access this page")
    st.stop()

# Initialize services (no caching to avoid stale data after code updates)
def get_services():
    return {
        'data': BulkAllocationData(),
        'engine': StrategyEngine(),
        'validator': BulkAllocationValidator(),
        'service': BulkAllocationService(),
        'email': BulkEmailService()
    }

services = get_services()

# Developer option: Clear all caches (in sidebar)
with st.sidebar:
    if user.get('role') in ['admin', 'GM', 'MD']:
        with st.expander("🔧 Developer Tools", expanded=False):
            if st.button("🗑️ Clear Cache", help="Clear all cached data. Use after code updates."):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.success("✅ Cache cleared!")
                st.rerun()

# ==================== SESSION STATE INITIALIZATION ====================

def init_session_state():
    """Initialize session state variables"""
    defaults = {
        # Current step
        'bulk_step': 1,
        
        # Scope selection
        'scope_brand_ids': [],
        'scope_customer_codes': [],
        'scope_legal_entities': [],
        'scope_etd_from': None,
        'scope_etd_to': None,
        
        # ========== ALLOCATION STATUS FILTER ==========
        # Options: 'ALL_NEEDING', 'ONLY_UNALLOCATED', 'ONLY_PARTIAL', 'INCLUDE_ALL'
        'scope_allocation_status_filter': 'ALL_NEEDING',
        
        # ========== URGENCY FILTER ==========
        # Options: 'ALL_ETD', 'URGENT_ONLY', 'OVERDUE_ONLY', 'URGENT_AND_OVERDUE'
        'scope_urgency_filter': 'ALL_ETD',
        'scope_urgent_days': 7,  # Days threshold for "urgent"
        
        # ========== ADDITIONAL FILTERS ==========
        'scope_low_coverage_only': False,  # Only OCs with coverage < threshold
        'scope_low_coverage_threshold': 50,  # Coverage % threshold
        'scope_stock_available_only': False,  # Only products with available stock
        'scope_high_value_only': False,  # Only high value orders
        'scope_high_value_threshold': 10000,  # USD threshold
        # Note: exclude_over_allocated is auto-managed based on allocation_status_filter
        
        # DEPRECATED but kept for backward compatibility
        'scope_include_partial': True,
        'scope_exclude_fully_allocated': True,
        'scope_only_unallocated': False,
        
        # Strategy configuration
        'strategy_type': 'HYBRID',
        'allocation_mode': 'SOFT',
        'hybrid_phases': [
            {'name': 'MIN_GUARANTEE', 'weight': 30},
            {'name': 'ETD_PRIORITY', 'weight': 40},
            {'name': 'PROPORTIONAL', 'weight': 30}
        ],
        'min_guarantee_percent': 30,
        'urgent_threshold_days': 7,
        
        # Simulation results
        'simulation_results': None,
        'demands_df': None,
        'supply_df': None,
        
        # Fine-tuning
        'adjusted_allocations': {},
        'split_allocations': {},  # {ocd_id: [{'qty': X, 'etd': Y}, ...]} for SAVED splits
        'pending_split_edits': {},  # {ocd_id: [{'qty': X, 'etd': Y}, ...]} for UNSAVED edits
        'split_form_version': 0,  # Increment to force form widget recreation
        
        # Commit state
        'is_committing': False,
        'commit_result': None,
        
        # Fine-tuning quick actions
        'force_include_all': False,
        'force_clear_all': False
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# ==================== HELPER FUNCTIONS ====================

# Allocation status filter options
ALLOCATION_STATUS_OPTIONS = {
    'ALL_NEEDING': {
        'label': '🔄 All needing allocation',
        'description': 'Not Allocated + Partially Allocated',
        'include_not_allocated': True,
        'include_partial': True,
        'include_fully': False
    },
    'ONLY_UNALLOCATED': {
        'label': '🆕 Only unallocated OCs',
        'description': 'Fresh orders never allocated',
        'include_not_allocated': True,
        'include_partial': False,
        'include_fully': False
    },
    'ONLY_PARTIAL': {
        'label': '📈 Only partially allocated OCs',
        'description': 'Top-up existing allocations',
        'include_not_allocated': False,
        'include_partial': True,
        'include_fully': False
    },
    'INCLUDE_ALL': {
        'label': '📋 All OCs (include fully allocated)',
        'description': 'Review or re-allocate everything',
        'include_not_allocated': True,
        'include_partial': True,
        'include_fully': True
    },
    'OVER_ALLOCATED': {
        'label': '⚠️ Over-allocated OCs only',
        'description': 'OCs with allocation exceeding pending needs - review/fix',
        'include_not_allocated': False,
        'include_partial': False,
        'include_fully': False,
        'include_over_allocated': True
    }
}

# Urgency filter options
URGENCY_FILTER_OPTIONS = {
    'ALL_ETD': {
        'label': '📅 All ETDs',
        'description': 'No urgency filter'
    },
    'URGENT_ONLY': {
        'label': '🔴 Urgent only',
        'description': 'ETD within threshold days'
    },
    'OVERDUE_ONLY': {
        'label': '⚠️ Overdue only',
        'description': 'ETD already passed'
    },
    'URGENT_AND_OVERDUE': {
        'label': '🚨 Urgent + Overdue',
        'description': 'Both urgent and overdue OCs'
    }
}

def get_current_scope() -> Dict:
    """Build current scope from session state"""
    # Get allocation status filter and convert to old params for backward compatibility
    status_filter = st.session_state.get('scope_allocation_status_filter', 'ALL_NEEDING')
    filter_config = ALLOCATION_STATUS_OPTIONS.get(status_filter, ALLOCATION_STATUS_OPTIONS['ALL_NEEDING'])
    
    # Determine if we should exclude over-allocated OCs
    # Default True, but False when viewing OVER_ALLOCATED or INCLUDE_ALL
    exclude_over = status_filter not in ['OVER_ALLOCATED', 'INCLUDE_ALL']
    
    return {
        # Basic filters
        'brand_ids': st.session_state.scope_brand_ids,
        'customer_codes': st.session_state.scope_customer_codes,
        'legal_entities': st.session_state.scope_legal_entities,
        'etd_from': st.session_state.scope_etd_from,
        'etd_to': st.session_state.scope_etd_to,
        
        # Allocation status filter (converted to old params for backward compatibility)
        'include_partial_allocated': filter_config['include_partial'],
        'exclude_fully_allocated': not filter_config['include_fully'],
        'only_unallocated': status_filter == 'ONLY_UNALLOCATED',
        'only_partial': status_filter == 'ONLY_PARTIAL',
        'only_over_allocated': status_filter == 'OVER_ALLOCATED',
        'allocation_status_filter': status_filter,
        
        # Urgency filter
        'urgency_filter': st.session_state.get('scope_urgency_filter', 'ALL_ETD'),
        'urgent_days': st.session_state.get('scope_urgent_days', 7),
        
        # Additional filters
        'low_coverage_only': st.session_state.get('scope_low_coverage_only', False),
        'low_coverage_threshold': st.session_state.get('scope_low_coverage_threshold', 50),
        'stock_available_only': st.session_state.get('scope_stock_available_only', False),
        'high_value_only': st.session_state.get('scope_high_value_only', False),
        'high_value_threshold': st.session_state.get('scope_high_value_threshold', 10000),
        
        # Over-allocation protection - auto-managed based on status filter
        'exclude_over_allocated': exclude_over,
    }

def get_strategy_config() -> StrategyConfig:
    """Build strategy config from session state"""
    return StrategyConfig(
        strategy_type=StrategyType[st.session_state.strategy_type],
        allocation_mode=st.session_state.allocation_mode,
        phases=st.session_state.hybrid_phases if st.session_state.strategy_type == 'HYBRID' else [],
        min_guarantee_percent=st.session_state.min_guarantee_percent,
        urgent_threshold_days=st.session_state.urgent_threshold_days
    )

def clear_simulation():
    """Clear simulation results and widget state"""
    st.session_state.simulation_results = None
    st.session_state.demands_df = None
    st.session_state.supply_df = None
    st.session_state.adjusted_allocations = {}
    st.session_state.split_allocations = {}
    st.session_state.pending_split_edits = {}  # Clear pending edits too
    st.session_state.split_form_version = 0  # Reset form version
    # Clear data_editor widget state to prevent stale edits from applying to new simulation
    if 'bulk_allocation_editor' in st.session_state:
        del st.session_state['bulk_allocation_editor']
    # Clear include states to reset checkboxes for new simulation
    if 'allocation_include_states' in st.session_state:
        del st.session_state['allocation_include_states']
    # Clear split expander state
    if 'split_expander_open' in st.session_state:
        del st.session_state['split_expander_open']
    # Clear current split selection
    if 'split_current_ocd_id' in st.session_state:
        del st.session_state['split_current_ocd_id']
    # Clear commit confirmation state
    if 'show_commit_confirmation' in st.session_state:
        del st.session_state['show_commit_confirmation']

# ==================== PAGE HEADER ====================

st.title("📦 Bulk Allocation")
st.caption(f"Logged in as: **{user.get('username', 'Unknown')}** ({user.get('role', 'Unknown')})")

# Check permission
if not services['validator'].check_permission(user.get('role', ''), 'bulk_allocate'):
    st.error("❌ You don't have permission to perform bulk allocation")
    st.info("Required roles: admin, GM, MD, sales_manager, supply_chain")
    st.stop()

# ==================== STEP INDICATOR ====================

def render_step_indicator():
    """Render step indicator"""
    steps = ['1. Select Scope', '2. Choose Strategy', '3. Review & Commit']
    
    cols = st.columns(len(steps))
    for i, (col, step) in enumerate(zip(cols, steps)):
        step_num = i + 1
        if step_num < st.session_state.bulk_step:
            col.success(f"✅ {step}")
        elif step_num == st.session_state.bulk_step:
            col.info(f"🔵 {step}")
        else:
            col.markdown(f"⚪ {step}")

render_step_indicator()
st.divider()

# ==================== ALLOCATION STATUS CHART ====================

def render_allocation_status_chart(summary: Dict):
    """Render allocation status breakdown as horizontal stacked bar"""
    
    total = summary.get('total_ocs', 0)
    if total == 0:
        return
    
    not_alloc = summary.get('not_allocated_count', 0)
    partial = summary.get('partially_allocated_count', 0)
    fully = summary.get('fully_allocated_count', 0)
    
    not_alloc_pct = not_alloc / total * 100 if total > 0 else 0
    partial_pct = partial / total * 100 if total > 0 else 0
    fully_pct = fully / total * 100 if total > 0 else 0
    
    # Display values for bar segments
    not_alloc_display = str(not_alloc) if not_alloc_pct > 8 else ''
    partial_display = str(partial) if partial_pct > 8 else ''
    fully_display = str(fully) if fully_pct > 8 else ''
    
    # Build HTML as compact single-line strings to avoid Streamlit rendering issues
    bar_html = (
        '<div style="margin:15px 0">'
        '<div style="display:flex;height:28px;border-radius:6px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1)">'
        f'<div style="width:{not_alloc_pct}%;background:linear-gradient(135deg,#ef4444,#dc2626);display:flex;align-items:center;justify-content:center;color:white;font-size:11px;font-weight:600" title="Not Allocated: {not_alloc}">{not_alloc_display}</div>'
        f'<div style="width:{partial_pct}%;background:linear-gradient(135deg,#f59e0b,#d97706);display:flex;align-items:center;justify-content:center;color:white;font-size:11px;font-weight:600" title="Partially Allocated: {partial}">{partial_display}</div>'
        f'<div style="width:{fully_pct}%;background:linear-gradient(135deg,#22c55e,#16a34a);display:flex;align-items:center;justify-content:center;color:white;font-size:11px;font-weight:600" title="Fully Allocated: {fully}">{fully_display}</div>'
        '</div>'
        '</div>'
    )
    st.markdown(bar_html, unsafe_allow_html=True)
    
    # Legend using columns (avoids HTML rendering issues)
    col1, col2, col3 = st.columns(3)
    col1.caption(f"🔴 Not Allocated: **{not_alloc}** ({not_alloc_pct:.1f}%)")
    col2.caption(f"🟡 Partial: **{partial}** ({partial_pct:.1f}%)")
    col3.caption(f"🟢 Fully Allocated: **{fully}** ({fully_pct:.1f}%)")


# ==================== HELP PANEL ====================

def render_help_panel():
    """Render expandable help section"""
    with st.expander("❓ Guide & Formula Explanations", expanded=False):
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Scope & Metrics", "🎯 Strategies", "📐 Formulas", "🔍 Filters"])
        
        with tab1:
            st.markdown("""
            ### Allocation Status
            
            | Status | Condition | Action |
            |--------|-----------|--------|
            | 🔴 **Not Allocated** | `undelivered_allocated = 0` | Need new allocation |
            | 🟡 **Partially Allocated** | `0 < undelivered < pending` | Can be topped up |
            | 🟢 **Fully Allocated** | `undelivered >= pending` | No action needed |
            | ⚠️ **Over-Allocated** | `undelivered > pending` | Need to cancel excess |
            
            **Note**: `undelivered_allocated` = allocation that has been committed but not yet delivered.
            
            ### Key Metrics
            
            - **Need Allocation**: OCs that need allocation (Not Allocated + Partially Allocated)
            - **Allocatable Demand**: `Σ (pending_qty - undelivered_allocated)` for OCs needing allocation
            - **Available Supply**: Total supply minus committed quantity
            - **Committed**: `Σ MIN(pending_qty, undelivered_allocated)` - quantity locked for existing allocations
            
            ### Supply Sources
            
            | Source | Description | View |
            |--------|-------------|------|
            | 📦 **Inventory** | Physical stock on hand | `inventory_detailed_view.remaining_quantity` |
            | 🚢 **CAN Pending** | Arrived but not stocked-in | `can_pending_stockin_view.pending_quantity` |
            | 📋 **PO Pending** | Ordered but not arrived | `purchase_order_full_view.pending_standard_arrival_quantity` |
            | 🔄 **WHT Pending** | In-transit between warehouses | `warehouse_transfer_details_view.transfer_quantity` |
            """)
            
        with tab2:
            st.markdown("""
            ### Allocation Strategies
            
            | Strategy | Priority By | Best For |
            |----------|-------------|----------|
            | **FCFS** | OC creation date (old → new) | Fair by order sequence |
            | **ETD Priority** | ETD (near → far) | Meeting delivery dates |
            | **Proportional** | Demand ratio | Fair distribution |
            | **Revenue Priority** | Order value | Maximize revenue |
            | **Hybrid** ⭐ | Multi-phase | Balanced approach |
            
            ### Hybrid Phases (Default)
            1. **MIN_GUARANTEE (30%)**: Each OC receives at least 30%
            2. **ETD_PRIORITY (40%)**: Prioritize urgent (≤7 days)
            3. **PROPORTIONAL (30%)**: Distribute remaining fairly
            
            ### Allocation Modes
            
            | Mode | Description |
            |------|-------------|
            | **SOFT** | Flexible - can be adjusted before delivery |
            | **HARD** | Locked - requires approval to change |
            """)
            
        with tab3:
            st.markdown("""
            ### Max Allocatable Formula
            
            ```
            max_allocatable = MAX(0, pending_qty - undelivered_allocated)
            ```
            
            **Simplified**: How much more can be allocated = What's needed - What's already allocated
            
            ### Committed Quantity Formula
            
            ```
            committed = Σ MIN(pending_qty, undelivered_allocated)
            ```
            
            Uses MIN because:
            - If `pending < undelivered` → over-allocated, only need pending
            - If `undelivered < pending` → partial, only committed amount is locked
            
            ### Available Supply Formula
            
            ```
            Total Supply = Inventory + CAN + PO + WHT
            Available = Total Supply - Committed
            ```
            
            ### Coverage Formula
            
            ```
            Coverage % = (undelivered_allocated / pending_qty) × 100%
            ```
            """)
        
        with tab4:
            st.markdown("""
            ### Allocation Status Filter
            
            | Option | Shows OCs Where | Use Case |
            |--------|-----------------|----------|
            | 🔄 **All needing** | `undelivered < pending` | Daily allocation (default) |
            | 🆕 **Unallocated only** | `undelivered = 0` | New orders only |
            | 📈 **Partial only** | `0 < undelivered < pending` | Top-up shortage |
            | 📋 **All OCs** | Everything | Review all |
            | ⚠️ **Over-allocated** | `undelivered > pending` | Fix problems |
            
            ### Urgency Filter
            
            | Option | Condition | Use Case |
            |--------|-----------|----------|
            | 📅 **All ETDs** | No filter | See everything |
            | 🔴 **Urgent only** | `ETD ≤ today + X days` | Rush deliveries |
            | ⚠️ **Overdue only** | `ETD < today` | Late orders |
            | 🚨 **Urgent + Overdue** | Both above | Critical attention |
            
            ### Additional Filters
            
            | Filter | Condition | Use Case |
            |--------|-----------|----------|
            | **Low coverage** | `coverage < threshold%` | Focus on shortage |
            | **Stock only** | Product has available supply | Where we can fulfill |
            | **High value** | Order value ≥ threshold | Prioritize big orders |
            """)


# ==================== STEP 1: SELECT SCOPE ====================

def render_step1_scope():
    """Render scope selection step"""
    st.subheader("Step 1: Define Allocation Scope")
    
    # Help panel at top
    render_help_panel()
    
    # Load filter options
    brands = services['data'].get_brand_options()
    customers = services['data'].get_customer_options()
    legal_entities = services['data'].get_legal_entity_options()
    
    # Filter columns
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### 🏷️ Brand Filter")
        brand_options = {b['id']: f"{b['brand_name']} ({b['oc_count']} OCs)" for b in brands}
        selected_brands = st.multiselect(
            "Select Brands",
            options=list(brand_options.keys()),
            format_func=lambda x: brand_options.get(x, x),
            default=st.session_state.scope_brand_ids,
            key="brand_selector",
            help="Filter by product brand"
        )
        st.session_state.scope_brand_ids = selected_brands
        
        st.markdown("##### 👥 Customer Filter")
        customer_options = {c['customer_code']: f"{c['customer']} ({c['oc_count']} OCs)" for c in customers}
        selected_customers = st.multiselect(
            "Select Customers",
            options=list(customer_options.keys()),
            format_func=lambda x: customer_options.get(x, x),
            default=st.session_state.scope_customer_codes,
            key="customer_selector",
            help="Filter by customer"
        )
        st.session_state.scope_customer_codes = selected_customers
    
    with col2:
        st.markdown("##### 🏢 Legal Entity Filter")
        le_options = {le['legal_entity']: f"{le['legal_entity']} ({le['oc_count']} OCs)" for le in legal_entities}
        selected_les = st.multiselect(
            "Select Legal Entities",
            options=list(le_options.keys()),
            format_func=lambda x: le_options.get(x, x),
            default=st.session_state.scope_legal_entities,
            key="le_selector",
            help="Filter by legal entity (Prostech VN, SG...)"
        )
        st.session_state.scope_legal_entities = selected_les
        
        st.markdown("##### 📅 ETD Range")
        etd_col1, etd_col2 = st.columns(2)
        with etd_col1:
            etd_from = st.date_input(
                "From",
                value=st.session_state.scope_etd_from,
                key="etd_from_input",
                help="ETD start date"
            )
            st.session_state.scope_etd_from = etd_from
        with etd_col2:
            etd_to = st.date_input(
                "To",
                value=st.session_state.scope_etd_to or (date.today() + timedelta(days=30)),
                key="etd_to_input",
                help="ETD end date"
            )
            st.session_state.scope_etd_to = etd_to
    
    # ========== OPTIONS SECTION (REDESIGNED) ==========
    st.markdown("##### ⚙️ Filter Options")
    
    # ===== ROW 1: Allocation Status Filter =====
    st.markdown("###### 📦 Allocation Status")
    status_options = list(ALLOCATION_STATUS_OPTIONS.keys())
    current_filter = st.session_state.get('scope_allocation_status_filter', 'ALL_NEEDING')
    current_index = status_options.index(current_filter) if current_filter in status_options else 0
    
    selected_status = st.radio(
        "Select which OCs to include by allocation status",
        options=status_options,
        format_func=lambda x: ALLOCATION_STATUS_OPTIONS[x]['label'],
        index=current_index,
        key="allocation_status_radio",
        horizontal=True,
        label_visibility="collapsed"
    )
    st.session_state.scope_allocation_status_filter = selected_status
    st.caption(f"ℹ️ {ALLOCATION_STATUS_OPTIONS[selected_status]['description']}")
    
    # ===== ROW 2: Urgency Filter =====
    st.markdown("###### 📅 Urgency Filter")
    urg_col1, urg_col2 = st.columns([3, 1])
    
    with urg_col1:
        urgency_options = list(URGENCY_FILTER_OPTIONS.keys())
        current_urgency = st.session_state.get('scope_urgency_filter', 'ALL_ETD')
        current_urg_index = urgency_options.index(current_urgency) if current_urgency in urgency_options else 0
        
        selected_urgency = st.radio(
            "Filter by ETD urgency",
            options=urgency_options,
            format_func=lambda x: URGENCY_FILTER_OPTIONS[x]['label'],
            index=current_urg_index,
            key="urgency_filter_radio",
            horizontal=True,
            label_visibility="collapsed"
        )
        st.session_state.scope_urgency_filter = selected_urgency
    
    with urg_col2:
        if selected_urgency in ['URGENT_ONLY', 'URGENT_AND_OVERDUE']:
            urgent_days = st.number_input(
                "Urgent threshold (days)",
                min_value=1,
                max_value=30,
                value=st.session_state.get('scope_urgent_days', 7),
                key="urgent_days_input",
                help="OCs with ETD within this many days are considered urgent"
            )
            st.session_state.scope_urgent_days = urgent_days
    
    # ===== ROW 3: Additional Filters =====
    st.markdown("###### 🔍 Additional Filters")
    add_col1, add_col2, add_col3 = st.columns(3)
    
    with add_col1:
        low_coverage = st.checkbox(
            "Low coverage only",
            value=st.session_state.get('scope_low_coverage_only', False),
            key="low_coverage_check",
            help="Only include OCs with coverage below threshold"
        )
        st.session_state.scope_low_coverage_only = low_coverage
        
        if low_coverage:
            coverage_threshold = st.slider(
                "Coverage threshold %",
                min_value=10,
                max_value=90,
                value=st.session_state.get('scope_low_coverage_threshold', 50),
                key="coverage_threshold_slider",
                help="OCs with coverage below this % will be included"
            )
            st.session_state.scope_low_coverage_threshold = coverage_threshold
    
    with add_col2:
        stock_available = st.checkbox(
            "Products with stock only",
            value=st.session_state.get('scope_stock_available_only', False),
            key="stock_available_check",
            help="Only include OCs for products that have available supply"
        )
        st.session_state.scope_stock_available_only = stock_available
    
    with add_col3:
        high_value = st.checkbox(
            "High value orders only",
            value=st.session_state.get('scope_high_value_only', False),
            key="high_value_check",
            help="Only include orders above value threshold"
        )
        st.session_state.scope_high_value_only = high_value
        
        if high_value:
            value_threshold = st.number_input(
                "Value threshold (USD)",
                min_value=1000,
                max_value=1000000,
                value=st.session_state.get('scope_high_value_threshold', 10000),
                step=1000,
                key="value_threshold_input"
            )
            st.session_state.scope_high_value_threshold = value_threshold
    
    # ========== SCOPE PREVIEW (UPDATED) ==========
    st.divider()
    st.markdown("##### 📊 Scope Preview")
    
    scope = get_current_scope()
    
    # Initialize variables for navigation logic
    has_new_fields = False
    
    # Validate scope
    scope_errors = services['validator'].validate_scope(scope)
    if scope_errors:
        for error in scope_errors:
            st.warning(error)
        summary = {'total_ocs': 0}
    else:
        # Get scope summary
        with st.spinner("Loading scope summary..."):
            summary = services['data'].get_scope_summary(scope)
        
        # Check if new fields exist (backward compatibility)
        has_new_fields = 'need_allocation_count' in summary
        
        if summary.get('total_ocs', 0) == 0:
            st.info("No OCs found matching the selected scope. Please adjust your filters.")
        else:
            if has_new_fields:
                # ===== NEW UI: OC Status Breakdown =====
                st.markdown("###### 📋 OC Allocation Status")
                
                c1, c2, c3, c4 = st.columns(4)
                
                c1.metric(
                    "Total OCs in Scope",
                    format_number(summary.get('total_ocs', 0)),
                    help=SCOPE_TOOLTIPS.get('total_ocs', '')
                )
                c2.metric(
                    "Need Allocation",
                    format_number(summary.get('need_allocation_count', 0)),
                    delta=f"{summary.get('need_allocation_percent', 0):.1f}%",
                    help=SCOPE_TOOLTIPS.get('need_allocation', '')
                )
                c3.metric(
                    "Fully Allocated",
                    format_number(summary.get('fully_allocated_count', 0)),
                    delta=f"{summary.get('fully_allocated_percent', 0):.1f}%",
                    delta_color="off",
                    help=SCOPE_TOOLTIPS.get('fully_allocated', '')
                )
                c4.metric(
                    "Not Allocated",
                    format_number(summary.get('not_allocated_count', 0)),
                    help=SCOPE_TOOLTIPS.get('not_allocated', '')
                )
                
                # Visual chart
                render_allocation_status_chart(summary)
                
                # ===== ROW 2: Demand & Supply =====
                st.markdown("###### 📦 Demand & Supply")
                
                m1, m2, m3, m4, m5 = st.columns(5)
                
                m1.metric(
                    "Products",
                    format_number(summary.get('total_products', 0)),
                    help=SCOPE_TOOLTIPS.get('products', '')
                )
                m2.metric(
                    "Total Demand",
                    format_number(summary.get('total_demand', 0)),
                    help=SCOPE_TOOLTIPS.get('total_demand', '')
                )
                m3.metric(
                    "Allocatable Demand",
                    format_number(summary.get('total_allocatable', 0)),
                    help=SCOPE_TOOLTIPS.get('allocatable_demand', '')
                )
                m4.metric(
                    "Available Supply",
                    format_number(summary.get('available_supply', 0)),
                    help=SCOPE_TOOLTIPS.get('available_supply', '')
                )
                
                # Coverage based on allocatable demand
                allocatable_coverage = summary.get('allocatable_coverage_percent', 0)
                coverage_delta = "Sufficient" if allocatable_coverage >= 100 else "Shortage"
                m5.metric(
                    "Coverage",
                    format_percentage(allocatable_coverage),
                    delta=coverage_delta,
                    delta_color="normal" if allocatable_coverage >= 100 else "inverse",
                    help=SCOPE_TOOLTIPS.get('coverage', '')
                )
                
                # Info box for filter effect - based on new filters
                status_filter = st.session_state.get('scope_allocation_status_filter', 'ALL_NEEDING')
                urgency_filter = st.session_state.get('scope_urgency_filter', 'ALL_ETD')
                not_alloc = summary.get('not_allocated_count', 0)
                partial = summary.get('partially_allocated_count', 0)
                fully = summary.get('fully_allocated_count', 0)
                
                # Build filter summary messages
                filter_msgs = []
                
                # Allocation status message
                if status_filter == 'ALL_NEEDING':
                    filter_msgs.append(f"📦 **{not_alloc + partial}** OCs (Not Allocated + Partial)")
                elif status_filter == 'ONLY_UNALLOCATED':
                    filter_msgs.append(f"🆕 **{not_alloc}** unallocated OCs only")
                elif status_filter == 'ONLY_PARTIAL':
                    filter_msgs.append(f"📈 **{partial}** partially allocated OCs only")
                elif status_filter == 'INCLUDE_ALL':
                    filter_msgs.append(f"📋 ALL **{not_alloc + partial + fully}** OCs")
                
                # Urgency message
                if urgency_filter == 'URGENT_ONLY':
                    days = st.session_state.get('scope_urgent_days', 7)
                    filter_msgs.append(f"🔴 Urgent only (ETD ≤ {days} days)")
                elif urgency_filter == 'OVERDUE_ONLY':
                    filter_msgs.append("⚠️ Overdue only (ETD passed)")
                elif urgency_filter == 'URGENT_AND_OVERDUE':
                    days = st.session_state.get('scope_urgent_days', 7)
                    filter_msgs.append(f"🚨 Urgent ({days}d) + Overdue")
                
                # Additional filters
                if st.session_state.get('scope_low_coverage_only', False):
                    threshold = st.session_state.get('scope_low_coverage_threshold', 50)
                    filter_msgs.append(f"📉 Coverage < {threshold}%")
                if st.session_state.get('scope_stock_available_only', False):
                    filter_msgs.append("📦 With stock only")
                if st.session_state.get('scope_high_value_only', False):
                    threshold = st.session_state.get('scope_high_value_threshold', 10000)
                    filter_msgs.append(f"💰 Value ≥ ${threshold:,.0f}")
                
                # Display summary
                if len(filter_msgs) > 1:
                    st.info("ℹ️ **Active Filters:** " + " | ".join(filter_msgs))
                elif filter_msgs:
                    if status_filter == 'INCLUDE_ALL':
                        st.warning("⚠️ " + filter_msgs[0] + " - Use with caution.")
                    else:
                        st.info("ℹ️ " + filter_msgs[0])
            
            else:
                # ===== FALLBACK: Old UI (backward compatible) =====
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Products", format_number(summary.get('total_products', 0)))
                m2.metric("OCs", format_number(summary.get('total_ocs', 0)))
                m3.metric("Total Demand", format_number(summary.get('total_demand', 0)))
                m4.metric("Available Supply", format_number(summary.get('available_supply', 0)))
                m5.metric("Coverage", format_percentage(summary.get('coverage_percent', 0)))
    
    # Navigation
    st.divider()
    col1, col2, col3 = st.columns([1, 2, 1])
    with col3:
        # Determine if can proceed (backward compatible)
        if has_new_fields:
            can_proceed = (
                not bool(scope_errors) and 
                summary.get('total_ocs', 0) > 0 and
                summary.get('need_allocation_count', 0) > 0
            )
            no_allocation_needed = (
                summary.get('total_ocs', 0) > 0 and 
                summary.get('need_allocation_count', 0) == 0
            )
        else:
            # Fallback: just check total_ocs > 0
            can_proceed = (
                not bool(scope_errors) and 
                summary.get('total_ocs', 0) > 0
            )
            no_allocation_needed = False
        
        if st.button(
            "Next: Choose Strategy →", 
            type="primary", 
            disabled=not can_proceed,
            key="next_to_step2"
        ):
            clear_simulation()
            st.session_state.bulk_step = 2
            st.rerun()
        
        if no_allocation_needed:
            st.warning("⚠️ All OCs are fully allocated. Nothing to allocate.")


# ==================== STEP 2: CHOOSE STRATEGY ====================

def render_step2_strategy():
    """Render strategy selection step"""
    st.subheader("Step 2: Choose Allocation Strategy")
    
    # Show current scope summary
    scope = get_current_scope()
    summary = services['data'].get_scope_summary(scope)
    
    st.info(f"📋 Scope: {format_scope_summary(scope)} | **{summary['need_allocation_count']}** OCs to allocate")
    
    # Strategy selection
    st.markdown("##### 🎯 Select Strategy")
    
    strategy_info = services['engine'].get_all_strategies()
    
    # Strategy cards
    cols = st.columns(len(strategy_info))
    for col, (stype, info) in zip(cols, strategy_info.items()):
        with col:
            is_selected = st.session_state.strategy_type == stype.name
            
            # Card styling
            if is_selected:
                st.markdown(f"""
                <div style="background: #e3f2fd; border: 2px solid #2196f3; border-radius: 8px; padding: 15px; height: 180px;">
                    <h4>{info['icon']} {info['name']}</h4>
                    <p style="font-size: 12px; color: #666;">{info['description']}</p>
                    <p style="font-size: 11px; color: #888;"><b>Best for:</b> {info['best_for']}</p>
                    <p style="text-align: center;">✅ Selected</p>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="background: #f5f5f5; border: 1px solid #ddd; border-radius: 8px; padding: 15px; height: 180px;">
                    <h4>{info['icon']} {info['name']}</h4>
                    <p style="font-size: 12px; color: #666;">{info['description']}</p>
                    <p style="font-size: 11px; color: #888;"><b>Best for:</b> {info['best_for']}</p>
                </div>
                """, unsafe_allow_html=True)
            
            if st.button(f"Select {stype.name}", key=f"select_{stype.name}", disabled=is_selected):
                st.session_state.strategy_type = stype.name
                clear_simulation()
                st.rerun()
    
    # Strategy-specific settings
    st.markdown("##### ⚙️ Strategy Settings")
    
    settings_col1, settings_col2 = st.columns(2)
    
    with settings_col1:
        allocation_mode = st.selectbox(
            "Allocation Mode",
            options=['SOFT', 'HARD'],
            index=0 if st.session_state.allocation_mode == 'SOFT' else 1,
            key="mode_selector",
            help=STRATEGY_TOOLTIPS['allocation_mode']
        )
        st.session_state.allocation_mode = allocation_mode
    
    with settings_col2:
        if st.session_state.strategy_type == 'HYBRID':
            min_guarantee = st.slider(
                "Minimum Guarantee %",
                min_value=0,
                max_value=50,
                value=st.session_state.min_guarantee_percent,
                key="min_guarantee_slider",
                help=STRATEGY_TOOLTIPS['min_guarantee']
            )
            st.session_state.min_guarantee_percent = min_guarantee
    
    if st.session_state.strategy_type in ['ETD_PRIORITY', 'HYBRID']:
        urgent_days = st.slider(
            "Urgent Threshold (Days)",
            min_value=1,
            max_value=30,
            value=st.session_state.urgent_threshold_days,
            key="urgent_days_slider",
            help=STRATEGY_TOOLTIPS['urgent_threshold']
        )
        st.session_state.urgent_threshold_days = urgent_days
    
    # Simulation button
    st.divider()
    st.markdown("##### 🔬 Run Simulation")
    
    if st.button("▶️ Run Allocation Simulation", type="primary", key="run_simulation"):
        with st.spinner("Running allocation simulation..."):
            # Load demands
            demands_df = services['data'].get_demands_in_scope(scope)
            
            if demands_df.empty:
                st.error("No demands found in scope")
            else:
                # Load supply
                product_ids = demands_df['product_id'].unique().tolist()
                supply_df = services['data'].get_supply_by_products(product_ids)
                
                # ========== STOCK AVAILABLE FILTER ==========
                # Filter demands to only include products with available supply
                if scope.get('stock_available_only', False):
                    # Get products with available supply > 0
                    if not supply_df.empty:
                        products_with_stock = supply_df[supply_df['available_supply'] > 0]['product_id'].unique().tolist()
                        original_count = len(demands_df)
                        demands_df = demands_df[demands_df['product_id'].isin(products_with_stock)]
                        filtered_count = original_count - len(demands_df)
                        if filtered_count > 0:
                            st.info(f"ℹ️ Filtered out {filtered_count} OCs for products without available stock")
                    else:
                        st.warning("⚠️ No supply data available. Stock filter cannot be applied.")
                
                if demands_df.empty:
                    st.error("No demands remain after applying filters")
                else:
                    # Run simulation
                    config = get_strategy_config()
                    results = services['engine'].simulate(demands_df, supply_df, config)
                    
                    # Store in session
                    st.session_state.simulation_results = results
                    st.session_state.demands_df = demands_df
                    st.session_state.supply_df = supply_df
                    
                    st.success(f"✅ Simulation complete: {len(results)} OCs processed")
                    st.rerun()
    
    # Show simulation results preview
    if st.session_state.simulation_results:
        results = st.session_state.simulation_results
        demands_df = st.session_state.demands_df
        
        st.markdown("##### 📊 Simulation Results")
        
        # Summary metrics
        total_suggested = sum(r.suggested_qty for r in results)
        total_demand = sum(r.demand_qty for r in results)
        avg_coverage = (total_suggested / total_demand * 100) if total_demand > 0 else 0
        allocated_count = sum(1 for r in results if r.suggested_qty > 0)
        unallocated_count = len(results) - allocated_count
        
        sm1, sm2, sm3, sm4, sm5 = st.columns(5)
        sm1.metric("OCs with Allocation", allocated_count)
        sm2.metric("Total Suggested Qty", format_number(total_suggested))
        sm3.metric("Total Demand", format_number(total_demand))
        sm4.metric("Avg Coverage", format_percentage(avg_coverage))
        sm5.metric("Unallocated", unallocated_count)
        
        st.info(f"Strategy: **{format_strategy_name(st.session_state.strategy_type)}** | Mode: **{st.session_state.allocation_mode}**")
        
        # Details table in expander
        with st.expander("📋 View Allocation Details", expanded=False):
            # Build details dataframe
            details_data = []
            for r in results:
                oc_info = demands_df[demands_df['ocd_id'] == r.ocd_id].iloc[0].to_dict() if not demands_df[demands_df['ocd_id'] == r.ocd_id].empty else {}
                
                # REFACTORED: Use formatter functions
                product_display = format_product_display(oc_info)
                customer_display = format_customer_display(
                    r.customer_code,
                    oc_info.get('customer', '') or oc_info.get('customer_name', '')
                )
                
                details_data.append({
                    'OC Number': oc_info.get('oc_number', ''),
                    'Customer': customer_display,
                    'Product': product_display,
                    'ETD': oc_info.get('etd'),
                    'Demand': r.demand_qty,
                    'Already Allocated': r.current_allocated,
                    'Suggested': r.suggested_qty,
                    'Coverage %': round((r.suggested_qty / r.demand_qty * 100) if r.demand_qty > 0 else 0, 1)
                })
            
            details_df = pd.DataFrame(details_data)
            
            # Filter options
            filter_col1, filter_col2 = st.columns(2)
            with filter_col1:
                show_filter = st.selectbox(
                    "Filter by",
                    options=['All', 'With Allocation', 'Zero Allocation'],
                    key="sim_details_filter"
                )
            with filter_col2:
                sort_by = st.selectbox(
                    "Sort by",
                    options=['ETD', 'Demand', 'Suggested', 'Coverage %'],
                    key="sim_details_sort"
                )
            
            # Apply filter
            if show_filter == 'With Allocation':
                details_df = details_df[details_df['Suggested'] > 0]
            elif show_filter == 'Zero Allocation':
                details_df = details_df[details_df['Suggested'] == 0]
            
            # Apply sort
            if sort_by in details_df.columns:
                ascending = True if sort_by == 'ETD' else False
                details_df = details_df.sort_values(by=sort_by, ascending=ascending)
            
            # Display table
            st.dataframe(
                details_df,
                column_config={
                    'OC Number': st.column_config.TextColumn('OC Number', width="medium"),
                    'Customer': st.column_config.TextColumn('Customer', width="medium"),
                    'Product': st.column_config.TextColumn('Product', width="large"),
                    'ETD': st.column_config.DateColumn('ETD', width="small"),
                    'Demand': st.column_config.NumberColumn('Demand', format="%.0f"),
                    'Already Allocated': st.column_config.NumberColumn('Already Alloc', format="%.0f"),
                    'Suggested': st.column_config.NumberColumn('Suggested', format="%.0f"),
                    'Coverage %': st.column_config.NumberColumn('Coverage %', format="%.1f%%")
                },
                use_container_width=True,
                hide_index=True,
                height=400
            )
            
            # Export button
            csv = details_df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name="simulation_results.csv",
                mime="text/csv",
                key="download_sim_csv"
            )
    
    # Navigation
    st.divider()
    nav_col1, nav_col2, nav_col3 = st.columns([1, 1, 1])
    
    with nav_col1:
        if st.button("← Back to Scope", key="back_to_step1"):
            st.session_state.bulk_step = 1
            st.rerun()
    
    with nav_col3:
        has_results = st.session_state.simulation_results is not None
        if st.button(
            "Next: Review & Commit →", 
            type="primary", 
            disabled=not has_results,
            key="next_to_step3"
        ):
            st.session_state.bulk_step = 3
            st.rerun()


# ==================== STEP 3: REVIEW & COMMIT (WITH SUPPLY CONTEXT) ====================

def render_step3_commit():
    """
    Render review and commit step - WITH SUPPLY CONTEXT INTEGRATION
    
    Key changes from original:
    1. Added supply context panel above fine-tuning table
    2. Added "Available Supply" column in data editor
    3. Added product supply detail expander
    4. Supply indicators for low coverage products
    5. REFACTORED: Split allocation pending/saved separation
    """
    st.subheader("Step 3: Review & Commit")
    
    results = st.session_state.simulation_results
    demands_df = st.session_state.demands_df
    supply_df = st.session_state.supply_df
    
    if not results:
        st.warning("No simulation results. Please go back and run simulation.")
        if st.button("← Back to Strategy"):
            st.session_state.bulk_step = 2
            st.rerun()
        return
    
    # Show scope and strategy summary
    scope = get_current_scope()
    st.info(f"📋 Scope: {format_scope_summary(scope)}")
    st.info(f"🎯 Strategy: {format_strategy_name(st.session_state.strategy_type)} | Mode: {st.session_state.allocation_mode}")
    
    # ==================== NEW: SUPPLY CONTEXT PANEL ====================
    # Build supply context for all products in scope
    supply_context = build_supply_context(demands_df, supply_df, results)
    
    # Render collapsible supply summary panel
    render_supply_summary_panel(supply_context, expanded=False)
    
    # ==================== FINE-TUNING SECTION ====================
    st.markdown("##### ✏️ Fine-tune Allocations")
    st.caption("Uncheck rows to exclude from allocation. Adjust quantities and ETDs as needed.")
    
    # ==================== INCLUDE STATES MANAGEMENT ====================
    # Store include states separately to preserve user edits across reruns
    
    # Initialize include_states if not exists (first time or after new simulation)
    if 'allocation_include_states' not in st.session_state:
        st.session_state.allocation_include_states = {
            r.ocd_id: r.suggested_qty > 0 for r in results
        }
    
    # Build ocd_id lookup for syncing from widget edits
    ocd_id_by_idx = {idx: r.ocd_id for idx, r in enumerate(results)}
    
    # ===== CRITICAL: Sync from widget state BEFORE building base_df =====
    # This ensures user's checkbox edits are preserved across reruns
    if 'bulk_allocation_editor' in st.session_state:
        widget_edits = st.session_state.bulk_allocation_editor
        edited_rows = widget_edits.get('edited_rows', {})
        for row_idx_str, changes in edited_rows.items():
            row_idx = int(row_idx_str) if isinstance(row_idx_str, str) else row_idx_str
            if 'include' in changes and row_idx in ocd_id_by_idx:
                ocd_id = ocd_id_by_idx[row_idx]
                st.session_state.allocation_include_states[ocd_id] = changes['include']
    
    # Handle Select All / Clear All flags
    force_include_all = st.session_state.get('force_include_all', False)
    force_clear_all = st.session_state.get('force_clear_all', False)
    
    if force_include_all:
        # Update all include states to True
        st.session_state.allocation_include_states = {r.ocd_id: True for r in results}
        st.session_state.force_include_all = False
    elif force_clear_all:
        # Update all include states to False
        st.session_state.allocation_include_states = {r.ocd_id: False for r in results}
        st.session_state.force_clear_all = False
    
    # Build BASE data from simulation results using stored include states
    base_data = []
    for r in results:
        oc_info = demands_df[demands_df['ocd_id'] == r.ocd_id].iloc[0].to_dict() if not demands_df[demands_df['ocd_id'] == r.ocd_id].empty else {}
        
        oc_etd = oc_info.get('etd')
        
        product_display = format_product_display(oc_info)
        customer_display = format_customer_display(
            r.customer_code,
            oc_info.get('customer', '') or oc_info.get('customer_name', '')
        )
        
        # NEW: Get supply info for this product
        product_supply_info = supply_context.get('products', {}).get(r.product_id, {})
        available_supply = product_supply_info.get('available', 0)
        supply_coverage = product_supply_info.get('coverage_pct', 0)
        
        # Get include value from stored states (preserves user edits)
        include_row = st.session_state.allocation_include_states.get(r.ocd_id, r.suggested_qty > 0)
        
        # ========== SYNC FROM SAVED SPLITS ==========
        # Check if this OC has saved split allocations
        saved_splits = st.session_state.split_allocations.get(r.ocd_id, [])
        has_saved_split = len(saved_splits) > 1
        
        if has_saved_split:
            # Sync final_qty from saved splits total
            split_total_qty = sum(s['qty'] for s in saved_splits)
            # Build split_info string: "✂️ 2026-01-05 (2), 2026-01-07 (2)"
            split_etd_parts = [f"{s['etd']} ({s['qty']:.0f})" for s in saved_splits]
            split_info = "✂️ " + ", ".join(split_etd_parts)
            # Use first ETD for allocated_etd column (for date sorting)
            first_split_etd = saved_splits[0]['etd'] if saved_splits else oc_etd
            # Coverage based on split total
            coverage_pct = (split_total_qty / r.demand_qty * 100) if r.demand_qty > 0 else 0
        else:
            split_total_qty = r.suggested_qty
            split_info = ""
            first_split_etd = oc_etd
            coverage_pct = (r.suggested_qty / r.demand_qty * 100) if r.demand_qty > 0 else 0
        
        base_data.append({
            'ocd_id': r.ocd_id,
            'product_id': r.product_id,  # NEW: needed for supply detail
            'include': include_row,
            'oc_number': oc_info.get('oc_number', ''),
            'customer_code': r.customer_code,
            'customer': oc_info.get('customer', ''),
            'customer_display': customer_display,
            'product_display': product_display,
            'pt_code': oc_info.get('pt_code', ''),
            'product_name': oc_info.get('product_name', ''),
            'package_size': oc_info.get('package_size', ''),
            'allocation_status': oc_info.get('allocation_status', ''),
            'oc_etd': oc_etd,
            'allocated_etd': first_split_etd,  # Use first split ETD if has splits
            
            # ===== QUANTITY COLUMNS (Refactored v3.0) =====
            'demand_qty': r.demand_qty,  # pending_standard_delivery_quantity
            
            # Undelivered allocated (committed but not shipped)
            'undelivered_allocated': r.undelivered_allocated,  # RENAMED for clarity
            
            # NEW: Allocatable quantity (max can allocate from view)
            'allocatable_qty': r.allocatable_qty,  # NEW COLUMN
            
            'suggested_qty': r.suggested_qty,
            'final_qty': split_total_qty,  # Sync from saved splits if exists
            'coverage_pct': coverage_pct,
            # NEW: Split info column
            'split_info': split_info,
            'has_split': has_saved_split,
            # Supply context fields
            'available_supply': available_supply,
            'supply_coverage': supply_coverage,
            'standard_uom': oc_info.get('standard_uom', '')
        })
    
    base_df = pd.DataFrame(base_data)
    
    # Convert dates properly for data_editor
    if 'oc_etd' in base_df.columns:
        base_df['oc_etd'] = pd.to_datetime(base_df['oc_etd']).dt.date
    if 'allocated_etd' in base_df.columns:
        base_df['allocated_etd'] = pd.to_datetime(base_df['allocated_etd']).dt.date
    
    # ==================== NEW: LOW COVERAGE WARNING ====================
    low_coverage_products = [
        p for pid, p in supply_context.get('products', {}).items()
        if p.get('coverage_pct', 100) < 100
    ]
    
    if low_coverage_products:
        critical_count = len([p for p in low_coverage_products if p.get('coverage_pct', 100) < 50])
        if critical_count > 0:
            st.warning(f"⚠️ **{critical_count}** product(s) have critical supply shortage (<50% coverage). "
                      f"Check the Supply Context panel above.")
        elif len(low_coverage_products) > 0:
            st.info(f"ℹ️ **{len(low_coverage_products)}** product(s) have supply constraints.")
    
    # ==================== DATA EDITOR ====================
    # Quick actions
    action_col1, action_col2, action_col3, action_col4 = st.columns([1, 1, 1, 3])
    with action_col1:
        if st.button("☑️ Select All", key="select_all_btn", help="Select all rows for allocation"):
            # Set flag to force include all rows
            st.session_state.force_include_all = True
            st.session_state.force_clear_all = False
            if 'bulk_allocation_editor' in st.session_state:
                del st.session_state['bulk_allocation_editor']
            st.rerun()
    with action_col2:
        if st.button("☐ Clear All", key="clear_all_btn", help="Deselect all rows"):
            # Set flag to force exclude all rows
            st.session_state.force_clear_all = True
            st.session_state.force_include_all = False
            if 'bulk_allocation_editor' in st.session_state:
                del st.session_state['bulk_allocation_editor']
            st.rerun()
    with action_col3:
        # NEW: Toggle supply column visibility
        show_supply_col = st.checkbox("📦 Show Supply", value=True, 
                                      help="Show available supply column")
    
    # Build display columns - Added ocd_id for reference when selecting split, split_info for split indicator
    # REFACTORED v3.0: Added allocatable_qty, renamed current_allocated to undelivered_allocated
    display_columns = ['include', 'ocd_id', 'oc_number', 'customer_display', 'product_display']
    if show_supply_col:
        display_columns.append('available_supply')
    display_columns.extend(['allocation_status', 'oc_etd', 'demand_qty', 'undelivered_allocated', 
                           'allocatable_qty', 'suggested_qty', 'final_qty', 'split_info', 'allocated_etd', 'coverage_pct'])
    
    # Build column config
    column_config = {
        'include': st.column_config.CheckboxColumn('✓', width="small", default=True,
            help="Uncheck to exclude this OC from allocation"),
        'ocd_id': st.column_config.NumberColumn('ID', disabled=True, width="small",
            help="OC Detail ID - use this to find OC in Split Allocation dropdown"),
        'oc_number': st.column_config.TextColumn('OC Number', disabled=True, width="medium"),
        'customer_display': st.column_config.TextColumn('Customer', disabled=True, width="medium",
            help="Customer Code - Customer Name"),
        'product_display': st.column_config.TextColumn('Product', disabled=True, width="large", 
            help="PT Code | Product Name | Package Size"),
        'allocation_status': st.column_config.TextColumn('Status', disabled=True, width="small",
            help="Allocation status: NOT_ALLOCATED / PARTIALLY_ALLOCATED / FULLY_ALLOCATED / OVER_ALLOCATED"),
        'oc_etd': st.column_config.DateColumn('OC ETD', disabled=True, width="small",
            help="Original ETD from OC"),
        'demand_qty': st.column_config.NumberColumn('Demand', disabled=True, format="%.0f", width="small",
            help=REVIEW_TOOLTIPS['demand_qty']),
        # RENAMED: current_allocated -> undelivered_allocated
        'undelivered_allocated': st.column_config.NumberColumn('Undeliv Alloc', disabled=True, format="%.0f", width="small",
            help="= undelivered_allocated_qty_standard\n\n"
                 "Quantity previously allocated but not yet delivered.\n"
                 "This quantity has goods 'committed' and will be delivered when shipment occurs."),
        # NEW: allocatable_qty column
        'allocatable_qty': st.column_config.NumberColumn('Allocatable', disabled=True, format="%.0f", width="small",
            help="= allocatable_qty_standard\n\n"
                 "Maximum quantity that can be allocated.\n"
                 "Formula: MIN(Demand - Undelivered, OC Quota Remaining)"),
        'suggested_qty': st.column_config.NumberColumn('Suggested', disabled=True, format="%.0f", width="small",
            help=REVIEW_TOOLTIPS['suggested_qty']),
        'final_qty': st.column_config.NumberColumn('Final Qty ✏️', format="%.0f", width="small",
            help="Final allocation quantity (editable).\n"
                 "Cannot exceed Allocatable quantity."),
        'split_info': st.column_config.TextColumn('Split ETDs', disabled=True, width="medium",
            help="✂️ Split allocation info: ETD (qty) for each split.\nEmpty = regular allocation (single ETD).\nEdit splits in the Split Allocation section below."),
        'allocated_etd': st.column_config.DateColumn('Alloc ETD ✏️', width="small",
            help="Allocated ETD - defaults to OC ETD. For split allocations, this shows the first ETD."),
        'coverage_pct': st.column_config.NumberColumn('Coverage %', disabled=True, format="%.1f%%", width="small",
            help=REVIEW_TOOLTIPS['coverage_pct'])
    }
    
    # NEW: Add supply column config if shown
    if show_supply_col:
        column_config['available_supply'] = st.column_config.NumberColumn(
            '📦 Avail', 
            disabled=True, 
            format="%.0f", 
            width="small",
            help="Available supply for this product.\n"
                 "Available = Total Supply - Committed\n\n"
                 "🟢 ≥100% coverage | 🟡 50-99% | 🔴 <50%"
        )
    
    edited_df = st.data_editor(
        base_df[display_columns],
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key="bulk_allocation_editor"
    )
    
    # ==================== NEW v3.0: OVER-ALLOCATION VALIDATION ====================
    # Validate that final_qty does not exceed allocatable_qty
    if 'allocatable_qty' in edited_df.columns and 'final_qty' in edited_df.columns:
        over_allocated_mask = edited_df['final_qty'] > edited_df['allocatable_qty']
        over_allocated_rows = edited_df[over_allocated_mask]
        
        if not over_allocated_rows.empty:
            st.error(f"⚠️ **{len(over_allocated_rows)} row(s)** have Final Qty exceeding Allocatable limit!")
            
            # Show details of over-allocated rows
            for idx, row in over_allocated_rows.iterrows():
                st.warning(
                    f"OC **{row.get('oc_number', 'N/A')}**: "
                    f"Final Qty ({row['final_qty']:.0f}) > "
                    f"Allocatable ({row['allocatable_qty']:.0f})"
                )
    
    # ==================== NEW: PRODUCT SUPPLY DETAIL EXPANDER ====================
    unique_products = base_df[['product_id', 'product_display', 'pt_code']].drop_duplicates()
    
    if len(unique_products) > 0:
        with st.expander("🔍 View Detailed Supply by Product", expanded=False):
            selected_product = st.selectbox(
                "Select Product",
                options=unique_products['product_id'].tolist(),
                format_func=lambda x: unique_products[unique_products['product_id'] == x]['product_display'].iloc[0][:60] if len(unique_products[unique_products['product_id'] == x]['product_display'].iloc[0]) > 60 else unique_products[unique_products['product_id'] == x]['product_display'].iloc[0],
                key="supply_detail_product_selector"
            )
            
            if selected_product:
                # Get detailed supply breakdown
                try:
                    supply_details = services['data'].get_supply_details_by_product(selected_product)
                except Exception as e:
                    logger.warning(f"Could not get supply details: {e}")
                    supply_details = None
                render_product_supply_detail(selected_product, supply_context, supply_details)
    
    # ==================== SPLIT ALLOCATION FEATURE (REFACTORED WITH FRAGMENT) ====================
    st.divider()
    
    # Count active (SAVED) splits for header
    active_split_count = sum(1 for v in st.session_state.split_allocations.values() if len(v) > 1)
    split_header = "✂️ Advanced: Split Allocation (Multiple ETDs)"
    if active_split_count > 0:
        split_header += f" — **{active_split_count} saved**"
    
    # Track expander state - keep open after add/remove/save actions
    split_expander_open = st.session_state.get('split_expander_open', False)
    
    # ========== FRAGMENT FUNCTION FOR SPLIT ALLOCATION ==========
    # Using @st.fragment to prevent full page rerun when editing qty/etd
    # Requires Streamlit >= 1.33.0
    @st.fragment
    def render_split_allocation_fragment(split_candidates_data, default_results):
        """
        Render split allocation UI in a fragment to prevent full page rerun.
        Changes to qty/etd inputs only rerun this fragment, not the entire page.
        """
        if not split_candidates_data:
            st.info("No OCs with allocation to split. Adjust quantities above first.")
            return
        
        # Build lookup dict for quick access
        candidates_by_ocd_id = {c['ocd_id']: c for c in split_candidates_data}
        ocd_id_list = [c['ocd_id'] for c in split_candidates_data]
        
        # Format function with visual indicator - takes ocd_id as input
        def format_split_option(oid):
            oc = candidates_by_ocd_id.get(oid)
            if not oc:
                return f"[ID:{oid}] Unknown"
            # Show different indicators for saved vs pending
            if oc['has_saved_splits']:
                indicator = "✅ "
                splits = st.session_state.split_allocations.get(oc['ocd_id'], [])
                splits_info = f" [{len(splits)} saved]"
            elif oc['has_pending_edits']:
                indicator = "⚠️ "
                splits = st.session_state.pending_split_edits.get(oc['ocd_id'], [])
                splits_info = f" [{len(splits)} unsaved]"
            else:
                indicator = ""
                splits_info = ""
            # Full display with ocd_id for easy reference
            return f"{indicator}[ID:{oc['ocd_id']}] {oc['oc_number']} | {oc['product']} | Qty: {oc['final_qty']:.0f}{splits_info}"
        
        # ========== DETERMINE DEFAULT VALUE ==========
        # Use ocd_id as value (not index) - this prevents wrong selection when list order changes
        default_ocd_id = ocd_id_list[0] if ocd_id_list else None
        
        # Check if Edit button was clicked (highest priority)
        if 'split_edit_target' in st.session_state:
            target_ocd_id = st.session_state.split_edit_target
            if target_ocd_id in candidates_by_ocd_id:
                default_ocd_id = target_ocd_id
            del st.session_state['split_edit_target']
            # Force recreate selectbox with new value
            if 'split_oc_selector' in st.session_state:
                del st.session_state['split_oc_selector']
        # Otherwise, try to restore current selection
        elif 'split_current_ocd_id' in st.session_state:
            current_ocd_id = st.session_state.split_current_ocd_id
            if current_ocd_id in candidates_by_ocd_id:
                default_ocd_id = current_ocd_id
        
        # Find index for default value
        default_idx = 0
        if default_ocd_id in ocd_id_list:
            default_idx = ocd_id_list.index(default_ocd_id)
        
        # Selectbox with ocd_id as options (not index)
        selected_ocd_id = st.selectbox(
            "Select OC to split",
            options=ocd_id_list,
            index=default_idx,
            format_func=format_split_option,
            key="split_oc_selector"
        )
        
        selected_oc = candidates_by_ocd_id[selected_ocd_id]
        ocd_id = selected_ocd_id
        max_qty = selected_oc['max_allocatable']
        default_etd = selected_oc['oc_etd']
        
        # ========== SAVE CURRENT SELECTION ==========
        # This preserves the selection when fragment reruns (e.g., when editing qty/etd)
        st.session_state.split_current_ocd_id = ocd_id
        
        # Show current status with visual indicator
        status_col1, status_col2 = st.columns([2, 1])
        with status_col1:
            st.markdown(f"**Max allocatable:** {max_qty:.0f} | **OC ETD:** {default_etd}")
        with status_col2:
            if selected_oc['has_saved_splits']:
                splits = st.session_state.split_allocations.get(ocd_id, [])
                st.success(f"✅ {len(splits)} splits saved")
            elif selected_oc['has_pending_edits']:
                splits = st.session_state.pending_split_edits.get(ocd_id, [])
                st.warning(f"⚠️ {len(splits)} splits unsaved")
        
        # ========== INITIALIZE PENDING EDITS ==========
        if ocd_id not in st.session_state.pending_split_edits:
            if ocd_id in st.session_state.split_allocations:
                st.session_state.pending_split_edits[ocd_id] = [
                    {'qty': s['qty'], 'etd': s['etd']} 
                    for s in st.session_state.split_allocations[ocd_id]
                ]
            else:
                st.session_state.pending_split_edits[ocd_id] = [
                    {'qty': selected_oc['final_qty'], 'etd': default_etd}
                ]
        
        pending_splits = st.session_state.pending_split_edits[ocd_id]
        
        # Add/Remove buttons
        btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 1])
        with btn_col1:
            add_clicked = st.button("➕ Add Split", key=f"add_split_{ocd_id}")
        with btn_col3:
            remove_clicked = st.button("🗑️ Remove Last", key=f"remove_last_{ocd_id}", 
                                      disabled=len(pending_splits) <= 1)
        
        # ========== SPLIT ENTRIES ==========
        st.markdown("**Split Entries:**")
        
        for idx, split in enumerate(pending_splits):
            col1, col2 = st.columns([2, 2])
            
            with col1:
                split_value = float(split.get('qty', 0))
                max_qty_float = float(max(max_qty, 0.0))
                clamped_value = min(max(split_value, 0.0), max_qty_float) if max_qty_float > 0 else 0.0
                
                st.number_input(
                    f"Qty #{idx+1}",
                    min_value=0.0,
                    max_value=max_qty_float if max_qty_float > 0 else 1.0,
                    value=clamped_value,
                    step=1.0,
                    key=f"split_qty_{ocd_id}_{idx}",
                    disabled=max_qty_float <= 0
                )
            
            with col2:
                split_etd_value = split.get('etd') if split.get('etd') else default_etd
                st.date_input(
                    f"ETD #{idx+1}",
                    value=split_etd_value,
                    key=f"split_etd_{ocd_id}_{idx}"
                )
        
        if max_qty <= 0:
            st.warning("⚠️ This OC has no remaining allocatable quantity.")
        
        # ========== SAVE BUTTON ==========
        save_clicked = st.button("💾 Save Splits", key=f"save_split_{ocd_id}", type="primary")
        
        # ========== HELPER: Read current values from widget state ==========
        def read_current_splits_from_widgets():
            current = []
            for idx in range(len(pending_splits)):
                qty_key = f"split_qty_{ocd_id}_{idx}"
                etd_key = f"split_etd_{ocd_id}_{idx}"
                current_qty = st.session_state.get(qty_key, pending_splits[idx]['qty'])
                current_etd = st.session_state.get(etd_key, pending_splits[idx]['etd'])
                current.append({'qty': float(current_qty), 'etd': current_etd})
            return current
        
        # ========== HANDLE SAVE ==========
        if save_clicked:
            current_splits = read_current_splits_from_widgets()
            valid_splits = [s for s in current_splits if s['qty'] > 0]
            
            if valid_splits:
                st.session_state.split_allocations[ocd_id] = valid_splits
                if ocd_id in st.session_state.pending_split_edits:
                    del st.session_state.pending_split_edits[ocd_id]
                st.session_state.split_save_success = ocd_id
            else:
                if ocd_id in st.session_state.split_allocations:
                    del st.session_state.split_allocations[ocd_id]
                if ocd_id in st.session_state.pending_split_edits:
                    del st.session_state.pending_split_edits[ocd_id]
            
            st.session_state.split_expander_open = True
            st.session_state.split_edit_target = ocd_id
            st.rerun()
        
        # ========== HANDLE ADD ==========
        if add_clicked:
            current_splits = read_current_splits_from_widgets()
            current_total = sum(s['qty'] for s in current_splits)
            new_count = len(current_splits) + 1
            qty_per_split = current_total / new_count
            
            new_splits = []
            for s in current_splits:
                new_splits.append({'qty': qty_per_split, 'etd': s['etd']})
            new_splits.append({'qty': qty_per_split, 'etd': default_etd})
            
            st.session_state.pending_split_edits[ocd_id] = new_splits
            st.session_state.split_expander_open = True
            st.session_state.split_edit_target = ocd_id
            st.rerun()
        
        # ========== HANDLE REMOVE ==========
        if remove_clicked and len(pending_splits) > 1:
            current_splits = read_current_splits_from_widgets()
            removed_qty = current_splits[-1]['qty']
            remaining_splits = current_splits[:-1]
            if remaining_splits and removed_qty > 0:
                add_per_split = removed_qty / len(remaining_splits)
                for s in remaining_splits:
                    s['qty'] += add_per_split
            
            st.session_state.pending_split_edits[ocd_id] = remaining_splits
            st.session_state.split_expander_open = True
            st.session_state.split_edit_target = ocd_id
            st.rerun()
        
        # Show persistent success message
        if st.session_state.get('split_save_success') == ocd_id:
            st.success("✅ Splits saved successfully!")
            del st.session_state['split_save_success']
        
        # Total validation with visual feedback
        current_total_qty = sum(
            float(st.session_state.get(f"split_qty_{ocd_id}_{idx}", pending_splits[idx]['qty']))
            for idx in range(len(pending_splits))
        )
        if max_qty <= 0:
            if current_total_qty > 0:
                st.error(f"⚠️ No allocatable quantity available for this OC")
        elif current_total_qty > max_qty:
            st.error(f"⚠️ Total split qty ({current_total_qty:.0f}) exceeds max allocatable ({max_qty:.0f})")
        elif current_total_qty > 0 and current_total_qty < max_qty:
            st.warning(f"ℹ️ Remaining unallocated: {max_qty - current_total_qty:.0f}")
        elif current_total_qty > 0:
            st.success(f"✅ Total: {current_total_qty:.0f} / {max_qty:.0f}")
        
        # ========== UNSAVED CHANGES WARNING ==========
        saved_splits = st.session_state.split_allocations.get(ocd_id, [])
        has_unsaved = False
        
        if len(pending_splits) != len(saved_splits):
            has_unsaved = True
        else:
            for i, (p, s) in enumerate(zip(pending_splits, saved_splits)):
                p_qty = float(st.session_state.get(f"split_qty_{ocd_id}_{i}", p['qty']))
                p_etd = st.session_state.get(f"split_etd_{ocd_id}_{i}", p['etd'])
                if abs(p_qty - s['qty']) > 0.01 or str(p_etd) != str(s['etd']):
                    has_unsaved = True
                    break
        
        if has_unsaved and len(pending_splits) > 1:
            st.warning("⚠️ **Unsaved changes!** Click **Save Splits** to apply.")
    
    # ========== BUILD SPLIT CANDIDATES AND CALL FRAGMENT ==========
    with st.expander(split_header, expanded=split_expander_open):
        st.caption("Split one OC into multiple allocation records with different delivery dates")
        
        # Get OCs with allocation > 0 for split options - NO TRUNCATION for full display
        split_candidates = [
            {
                'ocd_id': base_df.iloc[i]['ocd_id'],
                'oc_number': base_df.iloc[i]['oc_number'],
                'product': base_df.iloc[i]['product_display'],  # Full product display - no truncation
                'pt_code': base_df.iloc[i]['pt_code'],
                'final_qty': edited_df.iloc[i]['final_qty'],
                'oc_etd': base_df.iloc[i]['oc_etd'],
                'max_allocatable': results[i].demand_qty - results[i].current_allocated,
                'has_saved_splits': base_df.iloc[i]['ocd_id'] in st.session_state.split_allocations and len(st.session_state.split_allocations.get(base_df.iloc[i]['ocd_id'], [])) > 1,
                'has_pending_edits': base_df.iloc[i]['ocd_id'] in st.session_state.pending_split_edits
            }
            for i in range(len(results))
            if edited_df.iloc[i]['final_qty'] > 0 
               and edited_df.iloc[i].get('include', True)
               and (results[i].demand_qty - results[i].current_allocated) > 0
        ]
        
        # Call fragment function - changes inside only rerun the fragment
        render_split_allocation_fragment(split_candidates, results)
        
        # ==================== ACTIVE SPLITS SUMMARY (SAVED ONLY) ====================
        active_splits = {k: v for k, v in st.session_state.split_allocations.items() if len(v) > 1}
        if active_splits:
            st.markdown("---")
            st.markdown(f"**📋 Saved Splits ({len(active_splits)} OCs):**")
            st.caption("Click ✏️ to edit or 🗑️ to remove split configuration")
            
            for ocd_id, splits in active_splits.items():
                oc_match = base_df[base_df['ocd_id'] == ocd_id]
                if len(oc_match) > 0:
                    oc_info = oc_match.iloc[0]
                    total_qty = sum(s['qty'] for s in splits)
                    
                    # Display as a card with action buttons
                    card_col, edit_col, remove_col = st.columns([6, 1, 1])
                    
                    with card_col:
                        st.markdown(f"""
                        <div style="background: #e8f5e9; padding: 8px 12px; border-radius: 6px; border-left: 4px solid #4caf50;">
                            <strong>✅ {oc_info['oc_number']}</strong><br/>
                            <span style="color: #666; font-size: 0.85em;">
                                {len(splits)} splits → Total: {total_qty:.0f} | 
                                ETDs: {', '.join(str(s['etd']) for s in splits)}
                            </span>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with edit_col:
                        if st.button("✏️", key=f"edit_split_{ocd_id}", 
                                    help=f"Edit split for {oc_info['oc_number']}"):
                            # Set target to auto-select this OC in the selectbox
                            st.session_state.split_edit_target = ocd_id
                            st.session_state.split_expander_open = True
                            # Copy saved to pending for editing
                            st.session_state.pending_split_edits[ocd_id] = [
                                {'qty': s['qty'], 'etd': s['etd']} for s in splits
                            ]
                            st.rerun()
                    
                    with remove_col:
                        if st.button("🗑️", key=f"remove_split_{ocd_id}", 
                                    help=f"Remove split for {oc_info['oc_number']}"):
                            # Reset to single allocation with total qty
                            default_etd = oc_info.get('oc_etd')
                            # Remove from saved splits
                            del st.session_state.split_allocations[ocd_id]
                            # Also clear pending if exists
                            if ocd_id in st.session_state.pending_split_edits:
                                del st.session_state.pending_split_edits[ocd_id]
                            st.session_state.split_expander_open = True
                            st.rerun()
    
    # ==================== SUMMARY METRICS ====================
    # Only count rows where include = True
    final_total = 0
    allocated_count = 0
    excluded_count = 0
    etd_adjustments = 0
    qty_adjustments = 0
    
    for i, row in edited_df.iterrows():
        ocd_id = base_df.iloc[i]['ocd_id']
        oc_etd = base_df.iloc[i]['oc_etd']
        suggested_qty = results[i].suggested_qty
        
        # Check if row is included
        is_included = row.get('include', True)
        
        if not is_included:
            excluded_count += 1
            continue
        
        if ocd_id in st.session_state.split_allocations and len(st.session_state.split_allocations[ocd_id]) > 1:
            split_total = sum(s['qty'] for s in st.session_state.split_allocations[ocd_id])
            final_total += split_total
            if split_total > 0:
                allocated_count += 1
        else:
            final_total += row['final_qty']
            if row['final_qty'] > 0:
                allocated_count += 1
        
        # Count adjustments (only for included rows)
        if abs(float(row['final_qty']) - float(suggested_qty)) > 0.001:
            qty_adjustments += 1
        if row['allocated_etd'] and oc_etd and row['allocated_etd'] != oc_etd:
            etd_adjustments += 1
    
    # Calculate totals for included rows only
    included_demand = sum(r.demand_qty for i, r in enumerate(results) if edited_df.iloc[i].get('include', True))
    final_coverage = (final_total / included_demand * 100) if included_demand > 0 else 0
    split_count = sum(1 for splits in st.session_state.split_allocations.values() if len(splits) > 1)
    
    st.divider()
    
    # Show excluded warning only if some (but not all) are excluded
    if excluded_count > 0 and allocated_count > 0:
        st.warning(f"⚠️ **{excluded_count}** OC(s) excluded from allocation (unchecked)")
    
    st.markdown("##### 📊 Final Summary")
    m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
    m1.metric("Total to Allocate", format_number(final_total))
    m2.metric("OCs Included", allocated_count)
    m3.metric("OCs Excluded", excluded_count)
    m4.metric("Avg Coverage", format_percentage(final_coverage))
    m5.metric("Qty Adjustments", qty_adjustments)
    m6.metric("ETD Adjustments", etd_adjustments, help="OCs with allocated ETD different from OC ETD")
    m7.metric("Split Allocations", split_count, help="OCs split into multiple allocation records")
    
    # ==================== VALIDATION ====================
    # Check if any rows are selected
    included_count = sum(1 for i in range(len(edited_df)) if edited_df.iloc[i].get('include', True))
    
    if included_count == 0:
        # No rows selected - show info instead of validation error
        st.info("ℹ️ No OCs selected for allocation. Use **☑️ Select All** or tick individual rows to include them.")
        validation_result = {'valid': False, 'errors': [], 'warnings': []}
    else:
        # Only validate included rows
        validation_data = [
            {
                'ocd_id': base_df.iloc[i]['ocd_id'], 
                'product_id': results[i].product_id, 
                'final_qty': edited_df.iloc[i]['final_qty'],
                'allocated_etd': edited_df.iloc[i]['allocated_etd'],
                'oc_etd': base_df.iloc[i]['oc_etd']
            } 
            for i in range(len(results))
            if edited_df.iloc[i].get('include', True)  # Only validate included rows
        ]
        
        validation_result = services['validator'].validate_bulk_allocation(
            validation_data,
            demands_df,
            supply_df,
            user.get('role', '')
        )
        
        etd_delay_warnings = []
        for i, row in edited_df.iterrows():
            # Skip excluded rows
            if not row.get('include', True):
                continue
            oc_etd = base_df.iloc[i]['oc_etd']
            alloc_etd = row['allocated_etd']
            if oc_etd and alloc_etd and alloc_etd > oc_etd:
                days_delay = (alloc_etd - oc_etd).days
                oc_number = base_df.iloc[i]['oc_number']
                etd_delay_warnings.append(f"{oc_number}: Allocated ETD is {days_delay} days after OC ETD")
        
        if not validation_result['valid']:
            st.error("❌ Validation Failed")
            st.text(services['validator'].generate_validation_summary(validation_result))
        elif validation_result['warnings'] or etd_delay_warnings:
            st.warning("⚠️ Warnings")
            for warning in validation_result['warnings']:
                st.caption(f"  • {warning}")
            if etd_delay_warnings:
                with st.expander(f"📅 ETD Delay Warnings ({len(etd_delay_warnings)})", expanded=False):
                    for warning in etd_delay_warnings[:10]:
                        st.caption(f"  • {warning}")
                    if len(etd_delay_warnings) > 10:
                        st.caption(f"  ... and {len(etd_delay_warnings) - 10} more")
    
    # ==================== COMMIT SECTION ====================
    st.divider()
    st.markdown("##### 💾 Commit Allocation")
    
    # Check if already committed
    already_committed = st.session_state.get('commit_result') is not None
    show_confirmation = st.session_state.get('show_commit_confirmation', False)
    
    notes = st.text_area(
        "Notes (optional)",
        placeholder="Add any notes about this bulk allocation...",
        key="commit_notes",
        disabled=already_committed or show_confirmation
    )
    
    # ==================== CONFIRMATION DIALOG ====================
    if show_confirmation and not already_committed:
        st.markdown("---")
        st.markdown("### 📋 Review Allocation Plan")
        st.info("Please review the allocation details below before confirming.")
        
        # Build allocation details for confirmation
        regular_allocations = []
        split_allocation_details = []
        
        for i, row in edited_df.iterrows():
            if not row.get('include', True):
                continue
            
            ocd_id = base_df.iloc[i]['ocd_id']
            oc_number = base_df.iloc[i]['oc_number']
            product = base_df.iloc[i]['product_display'][:50]
            # BUGFIX: Changed 'customer_name' to 'customer_display' - field name mismatch
            customer = base_df.iloc[i].get('customer_display', 'N/A')[:30]
            
            if ocd_id in st.session_state.split_allocations and len(st.session_state.split_allocations[ocd_id]) > 1:
                # Split allocation
                splits = st.session_state.split_allocations[ocd_id]
                split_allocation_details.append({
                    'ocd_id': ocd_id,  # Added ocd_id
                    'oc_number': oc_number,
                    'product': product,
                    'customer': customer,
                    'splits': splits,
                    'total_qty': sum(s['qty'] for s in splits)
                })
            else:
                # Regular allocation
                regular_allocations.append({
                    'ocd_id': ocd_id,  # Added ocd_id
                    'oc_number': oc_number,
                    'product': product,
                    'customer': customer,
                    'qty': row['final_qty'],
                    'etd': row['allocated_etd']
                })
        
        # Summary metrics in confirmation
        conf_col1, conf_col2, conf_col3, conf_col4 = st.columns(4)
        conf_col1.metric("🎯 Total Quantity", format_number(final_total))
        conf_col2.metric("📦 Regular Allocations", len(regular_allocations))
        conf_col3.metric("✂️ Split Allocations", len(split_allocation_details))
        conf_col4.metric("❌ Excluded OCs", excluded_count)
        
        # Regular allocations table - with ocd_id column
        if regular_allocations:
            with st.expander(f"📦 Regular Allocations ({len(regular_allocations)} OCs)", expanded=True):
                reg_df = pd.DataFrame(regular_allocations)
                reg_df.columns = ['ID', 'OC Number', 'Product', 'Customer', 'Qty', 'ETD']
                st.dataframe(reg_df, use_container_width=True, hide_index=True)
        
        # Split allocations detail - with ocd_id
        if split_allocation_details:
            with st.expander(f"✂️ Split Allocations ({len(split_allocation_details)} OCs)", expanded=True):
                for detail in split_allocation_details:
                    st.markdown(f"**[ID:{detail['ocd_id']}] {detail['oc_number']}** - {detail['product']}")
                    st.caption(f"Customer: {detail['customer']} | Total: {detail['total_qty']:.0f}")
                    
                    split_rows = []
                    for idx, s in enumerate(detail['splits'], 1):
                        split_rows.append({
                            'Split #': idx,
                            'Quantity': f"{s['qty']:.2f}",
                            'ETD': str(s['etd'])
                        })
                    st.dataframe(pd.DataFrame(split_rows), use_container_width=True, hide_index=True)
                    st.markdown("---")
        
        # Warnings reminder
        if validation_result.get('warnings') or (included_count > 0 and 'etd_delay_warnings' in dir() and etd_delay_warnings):
            st.warning("⚠️ There are warnings for this allocation. Please review them above.")
        
        # Confirmation buttons
        st.markdown("---")
        conf_btn_col1, conf_btn_col2, conf_btn_col3 = st.columns([1, 1, 1])
        
        with conf_btn_col1:
            if st.button("❌ Cancel", key="cancel_commit", type="secondary"):
                st.session_state.show_commit_confirmation = False
                st.rerun()
        
        with conf_btn_col3:
            if st.button("✅ Confirm & Commit", key="confirm_commit", type="primary"):
                st.session_state.show_commit_confirmation = False
                commit_bulk_allocation(edited_df, base_df, notes)
    
    # ==================== MAIN BUTTONS ====================
    elif not already_committed:
        nav_col1, nav_col2, nav_col3 = st.columns([1, 1, 1])
        
        with nav_col1:
            if st.button("← Back to Strategy", key="back_to_step2"):
                st.session_state.bulk_step = 2
                st.rerun()
        
        with nav_col3:
            # Disable button if: validation failed or no allocations
            commit_disabled = (
                not validation_result['valid'] or 
                allocated_count == 0
            )
            
            if st.button("📋 Review & Commit", type="primary", 
                        disabled=commit_disabled,
                        key="review_commit_btn",
                        help="Review allocation plan before committing"):
                st.session_state.show_commit_confirmation = True
                st.rerun()
    
    # ==================== DISPLAY COMMIT RESULT (after rerun) ====================
    if already_committed:
        result = st.session_state.commit_result
        
        st.divider()
        st.success(f"✅ Bulk allocation committed successfully!")
        st.info(f"Allocation Number: **{result.get('allocation_number', 'N/A')}**")
        
        # Commit metrics
        col_r1, col_r2, col_r3, col_r4 = st.columns(4)
        col_r1.metric("OCs Allocated", result.get('detail_count', 0))
        col_r2.metric("Total Quantity", format_number(result.get('total_allocated', 0)))
        col_r3.metric("Products", result.get('products_affected', 0))
        col_r4.metric("Customers", result.get('customers_affected', 0))
        
        # Show excluded OCs if any
        excluded_ocs = result.get('excluded_ocs', [])
        if excluded_ocs:
            excluded_display = ', '.join(excluded_ocs[:5])
            if len(excluded_ocs) > 5:
                excluded_display += f"... (+{len(excluded_ocs) - 5} more)"
            st.warning(f"⚠️ {len(excluded_ocs)} OC(s) excluded from allocation: {excluded_display}")
        
        # Show split count if any
        if result.get('split_count', 0) > 0:
            st.info(f"✂️ {result['split_count']} OC(s) have split allocations (multiple ETDs)")
        
        # ===== EMAIL RESULTS =====
        email_result = result.get('email_result')
        if email_result:
            st.divider()
            st.markdown("##### 📧 Email Notifications")
            
            if email_result.get('success'):
                em1, em2, em3 = st.columns(3)
                
                summary_sent = email_result.get('summary_sent', False)
                em1.metric("Summary Email", "✓ Sent" if summary_sent else "✗ Failed",
                          help="Email to allocator with all OCs")
                
                individual_sent = email_result.get('individual_sent', 0)
                individual_total = email_result.get('individual_total', 0)
                em2.metric("Individual Emails", f"{individual_sent}/{individual_total}",
                          help="Emails to individual OC creators")
                
                all_sent = summary_sent and (individual_sent == individual_total)
                em3.metric("Status", "✓ Complete" if all_sent else "⚠️ Partial",
                          delta="all sent" if all_sent else f"{individual_total - individual_sent} failed")
                
                if all_sent:
                    st.success("✅ All email notifications sent successfully!")
                elif email_result.get('errors'):
                    st.warning(f"⚠️ Some emails failed ({len(email_result['errors'])} errors)")
            else:
                st.warning("⚠️ Email notifications failed or unavailable")
        
        # ===== NAVIGATION BUTTONS =====
        st.divider()
        st.markdown("##### 🧭 What's next?")
        nav_col1, nav_col2, nav_col3 = st.columns(3)
        
        with nav_col1:
            if st.button("🔄 New Allocation", key="new_allocation_btn", type="primary", 
                        help="Go back to home page and start fresh"):
                keys_to_delete = [
                    'commit_result', 'is_committing',
                    'simulation_results', 'demands_df', 'supply_df',
                    'adjusted_allocations', 'split_allocations', 'pending_split_edits',
                    'allocation_include_states', 'split_expander_open', 'split_save_success',
                    'show_commit_confirmation', 'split_current_ocd_id',
                ]
                for key in keys_to_delete:
                    if key in st.session_state:
                        del st.session_state[key]
                for key in list(st.session_state.keys()):
                    if key.startswith('bulk_') or key.startswith('scope_') or key.startswith('strategy_') or key.startswith('force_') or key.startswith('split_'):
                        del st.session_state[key]
                st.session_state.bulk_step = 1  # Explicitly set step to 1
                init_session_state()
                st.rerun()
        
        with nav_col2:
            if st.button("📋 Same Scope", key="same_scope_btn",
                        help="Re-run with same filters, choose new strategy"):
                st.session_state.commit_result = None
                st.session_state.simulation_results = None
                st.session_state.demands_df = None
                st.session_state.supply_df = None
                st.session_state.adjusted_allocations = {}
                st.session_state.split_allocations = {}
                st.session_state.pending_split_edits = {}
                if 'allocation_include_states' in st.session_state:
                    del st.session_state['allocation_include_states']
                if 'bulk_allocation_editor' in st.session_state:
                    del st.session_state['bulk_allocation_editor']
                if 'split_expander_open' in st.session_state:
                    del st.session_state['split_expander_open']
                if 'split_current_ocd_id' in st.session_state:
                    del st.session_state['split_current_ocd_id']
                if 'show_commit_confirmation' in st.session_state:
                    del st.session_state['show_commit_confirmation']
                st.session_state.bulk_step = 2
                st.rerun()
        
        with nav_col3:
            if st.button("🔧 Adjust Scope", key="adjust_scope_btn",
                        help="Modify filters before allocating more"):
                st.session_state.commit_result = None
                st.session_state.simulation_results = None
                st.session_state.demands_df = None
                st.session_state.supply_df = None
                st.session_state.adjusted_allocations = {}
                st.session_state.split_allocations = {}
                st.session_state.pending_split_edits = {}
                if 'allocation_include_states' in st.session_state:
                    del st.session_state['allocation_include_states']
                if 'bulk_allocation_editor' in st.session_state:
                    del st.session_state['bulk_allocation_editor']
                if 'split_expander_open' in st.session_state:
                    del st.session_state['split_expander_open']
                if 'split_current_ocd_id' in st.session_state:
                    del st.session_state['split_current_ocd_id']
                if 'show_commit_confirmation' in st.session_state:
                    del st.session_state['show_commit_confirmation']
                st.session_state.bulk_step = 1
                st.rerun()


def commit_bulk_allocation(edited_df: pd.DataFrame, original_df: pd.DataFrame, notes: str):
    """Commit bulk allocation to database"""
    results = st.session_state.simulation_results
    demands_df = st.session_state.demands_df
    
    with st.spinner("Committing bulk allocation..."):
        allocation_results = []
        excluded_ocs = []
        
        for i, row in edited_df.iterrows():
            # Skip excluded rows
            if not row.get('include', True):
                excluded_ocs.append(original_df.iloc[i]['oc_number'])
                continue
            
            ocd_id = original_df.iloc[i]['ocd_id']
            result = results[i]
            
            oc_info = demands_df[demands_df['ocd_id'] == ocd_id].iloc[0].to_dict() if not demands_df[demands_df['ocd_id'] == ocd_id].empty else {}
            
            final_qty = row['final_qty']
            allocated_etd = row['allocated_etd']
            coverage_pct = (final_qty / result.demand_qty * 100) if result.demand_qty > 0 else 0
            product_display = format_product_display(oc_info)
            
            allocation_results.append({
                'ocd_id': ocd_id,
                'product_id': result.product_id,
                'customer_code': result.customer_code,
                'demand_qty': result.demand_qty,
                'suggested_qty': result.suggested_qty,
                'final_qty': final_qty,
                'coverage_percent': coverage_pct,
                'oc_number': oc_info.get('oc_number', ''),
                'pt_code': oc_info.get('pt_code', ''),
                'product_name': oc_info.get('product_name', ''),
                'package_size': oc_info.get('package_size', ''),
                'product_display': product_display,
                'oc_etd': oc_info.get('etd'),
                'allocated_etd': allocated_etd
            })
        
        if not allocation_results:
            st.error("❌ No OCs selected for allocation. Please include at least one OC.")
            return
        
        demands_dict = {int(row['ocd_id']): row.to_dict() for _, row in demands_df.iterrows()}
        
        strategy_config = {
            'strategy_type': st.session_state.strategy_type,
            'allocation_mode': st.session_state.allocation_mode,
            'phases': st.session_state.hybrid_phases,
            'min_guarantee_percent': st.session_state.min_guarantee_percent,
            'urgent_threshold_days': st.session_state.urgent_threshold_days
        }
        
        # Filter split_allocations to only include checked OCs
        included_ocd_ids = [r['ocd_id'] for r in allocation_results]
        filtered_split_allocations = {
            k: v for k, v in st.session_state.split_allocations.items() 
            if k in included_ocd_ids
        }
        
        result = services['service'].commit_bulk_allocation(
            allocation_results=allocation_results,
            demands_dict=demands_dict,
            scope=get_current_scope(),
            strategy_config=strategy_config,
            user_id=user.get('id'),
            notes=notes,
            split_allocations=filtered_split_allocations
        )
        
        if result['success']:
            st.session_state.commit_result = result
            st.session_state.commit_result['excluded_ocs'] = excluded_ocs
            
            # ===== SEND EMAIL NOTIFICATIONS =====
            email_result = None
            with st.spinner("📧 Sending email notifications..."):
                try:
                    email_result = services['email'].send_bulk_allocation_emails(
                        commit_result=result,
                        allocation_results=allocation_results,
                        scope=get_current_scope(),
                        strategy_config=strategy_config,
                        allocator_user_id=user.get('id'),
                        demands_dict=demands_dict,
                        split_allocations=filtered_split_allocations
                    )
                except Exception as e:
                    logger.warning(f"Email notification failed: {e}")
                    email_result = {'success': False, 'errors': [str(e)]}
            
            # Save email result and rerun to update UI
            st.session_state.commit_result['email_result'] = email_result
            st.rerun()
        
        else:
            st.error(f"❌ Failed to commit: {result.get('error', 'Unknown error')}")
            if result.get('technical_error'):
                with st.expander("Technical details", expanded=False):
                    st.code(result['technical_error'])
            
            # Navigation after failed commit
            st.divider()
            fail_col1, fail_col2, fail_col3 = st.columns(3)
            with fail_col1:
                if st.button("🔄 Retry Commit", key="retry_commit_btn", type="primary",
                            help="Try committing again"):
                    st.rerun()
            with fail_col2:
                if st.button("✏️ Review & Edit", key="review_edit_btn",
                            help="Go back to fine-tuning table"):
                    st.session_state.commit_result = None
                    st.rerun()
            with fail_col3:
                if st.button("🏠 Back to Home", key="back_home_btn",
                            help="Go back to home page and start fresh"):
                    # Reset all state
                    keys_to_delete = [
                        'commit_result', 'is_committing',
                        'simulation_results', 'demands_df', 'supply_df',
                        'adjusted_allocations', 'split_allocations', 'pending_split_edits',
                        'allocation_include_states', 'split_expander_open', 'split_save_success',
                        'show_commit_confirmation', 'split_current_ocd_id',
                    ]
                    for key in keys_to_delete:
                        if key in st.session_state:
                            del st.session_state[key]
                    for key in list(st.session_state.keys()):
                        if key.startswith('bulk_') or key.startswith('scope_') or key.startswith('strategy_') or key.startswith('force_') or key.startswith('split_'):
                            del st.session_state[key]
                    st.session_state.bulk_step = 1  # Explicitly set step to 1
                    init_session_state()
                    st.rerun()




# ==================== MAIN RENDER ====================

if st.session_state.bulk_step == 1:
    render_step1_scope()
elif st.session_state.bulk_step == 2:
    render_step2_strategy()
elif st.session_state.bulk_step == 3:
    render_step3_commit()