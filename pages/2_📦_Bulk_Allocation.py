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
    format_etd_urgency, format_scope_summary, format_quantity_with_uom
)
from utils.bulk_allocation.bulk_tooltips import (
    SCOPE_TOOLTIPS, STRATEGY_TOOLTIPS, REVIEW_TOOLTIPS, FORMULA_TOOLTIPS
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

# Initialize services
@st.cache_resource
def get_services():
    return {
        'data': BulkAllocationData(),
        'engine': StrategyEngine(),
        'validator': BulkAllocationValidator(),
        'service': BulkAllocationService(),
        'email': BulkEmailService()
    }

services = get_services()

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
        'scope_include_partial': True,
        
        # NEW: Allocation status filters
        'scope_exclude_fully_allocated': True,  # Default: exclude fully allocated
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
        'split_allocations': {},  # {ocd_id: [{'qty': X, 'etd': Y}, ...]} for multi-ETD splits
        
        # Commit state
        'is_committing': False,
        'commit_result': None
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# ==================== HELPER FUNCTIONS ====================

def get_current_scope() -> Dict:
    """Build current scope from session state"""
    return {
        'brand_ids': st.session_state.scope_brand_ids,
        'customer_codes': st.session_state.scope_customer_codes,
        'legal_entities': st.session_state.scope_legal_entities,
        'etd_from': st.session_state.scope_etd_from,
        'etd_to': st.session_state.scope_etd_to,
        'include_partial_allocated': st.session_state.scope_include_partial,
        # NEW filters
        'exclude_fully_allocated': st.session_state.scope_exclude_fully_allocated,
        'only_unallocated': st.session_state.scope_only_unallocated,
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
    """Clear simulation results"""
    st.session_state.simulation_results = None
    st.session_state.demands_df = None
    st.session_state.supply_df = None
    st.session_state.adjusted_allocations = {}
    st.session_state.split_allocations = {}

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
    
    st.markdown(f"""
    <div style="margin: 15px 0;">
        <div style="display: flex; height: 28px; border-radius: 6px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
            <div style="width: {not_alloc_pct}%; background: linear-gradient(135deg, #ef4444, #dc2626); display: flex; align-items: center; justify-content: center; color: white; font-size: 11px; font-weight: 600;" title="Not Allocated: {not_alloc}">
                {not_alloc if not_alloc_pct > 8 else ''}
            </div>
            <div style="width: {partial_pct}%; background: linear-gradient(135deg, #f59e0b, #d97706); display: flex; align-items: center; justify-content: center; color: white; font-size: 11px; font-weight: 600;" title="Partially Allocated: {partial}">
                {partial if partial_pct > 8 else ''}
            </div>
            <div style="width: {fully_pct}%; background: linear-gradient(135deg, #22c55e, #16a34a); display: flex; align-items: center; justify-content: center; color: white; font-size: 11px; font-weight: 600;" title="Fully Allocated: {fully}">
                {fully if fully_pct > 8 else ''}
            </div>
        </div>
        <div style="display: flex; justify-content: space-between; font-size: 12px; color: #666; margin-top: 8px;">
            <span>🔴 Not Allocated: <b>{not_alloc}</b> ({not_alloc_pct:.1f}%)</span>
            <span>🟡 Partial: <b>{partial}</b> ({partial_pct:.1f}%)</span>
            <span>🟢 Fully Allocated: <b>{fully}</b> ({fully_pct:.1f}%)</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ==================== HELP PANEL ====================

def render_help_panel():
    """Render expandable help section"""
    with st.expander("❓ Guide & Formula Explanations", expanded=False):
        tab1, tab2, tab3 = st.tabs(["📋 Scope & Metrics", "🎯 Strategies", "📐 Formulas"])
        
        with tab1:
            st.markdown("""
            ### Allocation Status
            
            | Status | Condition | Action |
            |--------|-----------|--------|
            | 🔴 **Not Allocated** | `undelivered_allocated = 0` | Need new allocation |
            | 🟡 **Partially Allocated** | `0 < undelivered < pending` | Can be topped up |
            | 🟢 **Fully Allocated** | `undelivered >= pending` | No action needed |
            
            ### Key Metrics
            
            - **Need Allocation**: OCs that need allocation (Not Allocated + Partially Allocated with room)
            - **Allocatable Demand**: Total quantity that can still be allocated
            - **Available Supply**: Supply after deducting committed quantity
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
            """)
            
        with tab3:
            st.markdown(FORMULA_TOOLTIPS['max_allocatable'])
            st.divider()
            st.markdown(FORMULA_TOOLTIPS['committed_qty'])
            st.divider()
            st.markdown(FORMULA_TOOLTIPS['available_supply'])


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
    
    # ========== OPTIONS SECTION (UPDATED) ==========
    st.markdown("##### ⚙️ Options")
    
    opt_col1, opt_col2 = st.columns(2)
    
    with opt_col1:
        exclude_fully = st.checkbox(
            "Exclude fully allocated OCs",
            value=st.session_state.scope_exclude_fully_allocated,
            key="exclude_fully_check",
            help=SCOPE_TOOLTIPS['exclude_fully_allocated']
        )
        st.session_state.scope_exclude_fully_allocated = exclude_fully
        
        include_partial = st.checkbox(
            "Include partially allocated OCs (top-up)",
            value=st.session_state.scope_include_partial,
            key="include_partial_check",
            help=SCOPE_TOOLTIPS['include_partial'],
            disabled=st.session_state.scope_only_unallocated  # Disable if only_unallocated is checked
        )
        st.session_state.scope_include_partial = include_partial
    
    with opt_col2:
        only_unallocated = st.checkbox(
            "Only unallocated OCs",
            value=st.session_state.scope_only_unallocated,
            key="only_unallocated_check",
            help=SCOPE_TOOLTIPS['only_unallocated']
        )
        st.session_state.scope_only_unallocated = only_unallocated
        
        # If only_unallocated is checked, auto-enable exclude_fully
        if only_unallocated:
            st.session_state.scope_include_partial = False
    
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
                
                # Info box for filter effect
                if st.session_state.scope_exclude_fully_allocated:
                    filtered_count = summary.get('fully_allocated_count', 0)
                    st.info(f"ℹ️ **{filtered_count}** fully allocated OCs will be excluded from allocation.")
            
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
                
                # Build product display
                product_display = oc_info.get('product_display', '')
                if not product_display:
                    parts = [oc_info.get('pt_code', '')]
                    if oc_info.get('product_name'):
                        parts.append(oc_info.get('product_name'))
                    if oc_info.get('package_size'):
                        parts.append(oc_info.get('package_size'))
                    product_display = ' | '.join(filter(None, parts))
                    if oc_info.get('brand_name'):
                        product_display += f" ({oc_info.get('brand_name')})"
                
                details_data.append({
                    'OC Number': oc_info.get('oc_number', ''),
                    'Customer': r.customer_code,
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
                    'Customer': st.column_config.TextColumn('Customer', width="small"),
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


# ==================== STEP 3: REVIEW & COMMIT ====================

def render_step3_commit():
    """Render review and commit step"""
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
    
    # Fine-tuning section
    st.markdown("##### ✏️ Fine-tune Allocations")
    st.caption("Adjust quantities if needed before committing")
    
    # Build editable data
    edit_data = []
    for r in results:
        oc_info = demands_df[demands_df['ocd_id'] == r.ocd_id].iloc[0].to_dict() if not demands_df[demands_df['ocd_id'] == r.ocd_id].empty else {}
        
        # Check for adjustments (qty and etd)
        adjusted_qty = st.session_state.adjusted_allocations.get(r.ocd_id, {}).get('qty', r.final_qty) if isinstance(st.session_state.adjusted_allocations.get(r.ocd_id), dict) else st.session_state.adjusted_allocations.get(r.ocd_id, r.final_qty)
        
        # Get OC ETD as default for allocated_etd
        oc_etd = oc_info.get('etd')
        adjusted_etd = st.session_state.adjusted_allocations.get(r.ocd_id, {}).get('etd', oc_etd) if isinstance(st.session_state.adjusted_allocations.get(r.ocd_id), dict) else oc_etd
        
        # Build product display: pt_code | product_name | package_size (brand)
        product_display = oc_info.get('product_display', '')
        if not product_display:
            # Fallback if product_display not available
            parts = [oc_info.get('pt_code', '')]
            if oc_info.get('product_name'):
                parts.append(oc_info.get('product_name'))
            if oc_info.get('package_size'):
                parts.append(oc_info.get('package_size'))
            product_display = ' | '.join(filter(None, parts))
            if oc_info.get('brand_name'):
                product_display += f" ({oc_info.get('brand_name')})"
        
        edit_data.append({
            'ocd_id': r.ocd_id,
            'oc_number': oc_info.get('oc_number', ''),
            'customer_code': r.customer_code,
            'customer': oc_info.get('customer', ''),
            'product_display': product_display,
            'pt_code': oc_info.get('pt_code', ''),
            'product_name': oc_info.get('product_name', ''),
            'package_size': oc_info.get('package_size', ''),
            'allocation_status': oc_info.get('allocation_status', ''),
            'oc_etd': oc_etd,
            'allocated_etd': adjusted_etd,
            'demand_qty': r.demand_qty,
            'current_allocated': r.current_allocated,
            'suggested_qty': r.suggested_qty,
            'final_qty': adjusted_qty,
            'coverage_pct': (adjusted_qty / r.demand_qty * 100) if r.demand_qty > 0 else 0
        })
    
    edit_df = pd.DataFrame(edit_data)
    
    # Convert dates properly for data_editor
    if 'oc_etd' in edit_df.columns:
        edit_df['oc_etd'] = pd.to_datetime(edit_df['oc_etd']).dt.date
    if 'allocated_etd' in edit_df.columns:
        edit_df['allocated_etd'] = pd.to_datetime(edit_df['allocated_etd']).dt.date
    
    # Editable columns with product_display and allocated_etd
    edited_df = st.data_editor(
        edit_df[['oc_number', 'customer_code', 'product_display', 'allocation_status', 'oc_etd', 'demand_qty', 'current_allocated', 'suggested_qty', 'final_qty', 'allocated_etd', 'coverage_pct']],
        column_config={
            'oc_number': st.column_config.TextColumn('OC Number', disabled=True, width="medium"),
            'customer_code': st.column_config.TextColumn('Customer', disabled=True, width="small"),
            'product_display': st.column_config.TextColumn('Product', disabled=True, width="large", 
                help="PT Code | Product Name | Package Size"),
            'allocation_status': st.column_config.TextColumn('Status', disabled=True, width="small",
                help="Current allocation status"),
            'oc_etd': st.column_config.DateColumn('OC ETD', disabled=True, width="small",
                help="Original ETD from OC"),
            'demand_qty': st.column_config.NumberColumn('Demand', disabled=True, format="%.0f", width="small",
                help=REVIEW_TOOLTIPS['demand_qty']),
            'current_allocated': st.column_config.NumberColumn('Already Alloc', disabled=True, format="%.0f", width="small",
                help=REVIEW_TOOLTIPS['current_allocated']),
            'suggested_qty': st.column_config.NumberColumn('Suggested', disabled=True, format="%.0f", width="small",
                help=REVIEW_TOOLTIPS['suggested_qty']),
            'final_qty': st.column_config.NumberColumn('Final Qty ✏️', format="%.0f", width="small",
                help=REVIEW_TOOLTIPS['final_qty']),
            'allocated_etd': st.column_config.DateColumn('Alloc ETD ✏️', width="small",
                help="Allocated ETD - defaults to OC ETD. Adjust if needed."),
            'coverage_pct': st.column_config.NumberColumn('Coverage %', disabled=True, format="%.1f%%", width="small",
                help=REVIEW_TOOLTIPS['coverage_pct'])
        },
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key="bulk_allocation_editor"  # Key to stabilize state
    )
    
    # Save adjustments (both qty and etd)
    for i, row in edited_df.iterrows():
        ocd_id = edit_df.iloc[i]['ocd_id']
        original_qty = results[i].suggested_qty
        original_etd = edit_df.iloc[i]['oc_etd']
        
        final_qty = row['final_qty']
        final_etd = row['allocated_etd']
        
        # Skip if this OC has split allocations (handled separately)
        if ocd_id in st.session_state.split_allocations:
            continue
        
        # Check if either qty or etd was adjusted
        qty_changed = final_qty != original_qty
        etd_changed = final_etd != original_etd
        
        if qty_changed or etd_changed:
            st.session_state.adjusted_allocations[ocd_id] = {
                'qty': final_qty,
                'etd': final_etd
            }
        elif ocd_id in st.session_state.adjusted_allocations:
            # Remove if reverted to original
            del st.session_state.adjusted_allocations[ocd_id]
    
    # ==================== SPLIT ALLOCATION FEATURE ====================
    st.divider()
    with st.expander("✂️ Advanced: Split Allocation (Multiple ETDs)", expanded=False):
        st.caption("Split one OC into multiple allocation records with different delivery dates")
        
        # Get OCs with allocation > 0 for split options
        split_candidates = [
            {
                'ocd_id': edit_df.iloc[i]['ocd_id'],
                'oc_number': edit_df.iloc[i]['oc_number'],
                'product': edit_df.iloc[i]['product_display'][:40] + '...' if len(edit_df.iloc[i]['product_display']) > 40 else edit_df.iloc[i]['product_display'],
                'final_qty': edited_df.iloc[i]['final_qty'],
                'oc_etd': edit_df.iloc[i]['oc_etd'],
                'max_allocatable': results[i].demand_qty - results[i].current_allocated
            }
            for i in range(len(results))
            if edited_df.iloc[i]['final_qty'] > 0
        ]
        
        if not split_candidates:
            st.info("No OCs with allocation to split. Run simulation first.")
        else:
            # Select OC to split - outside form
            selected_idx = st.selectbox(
                "Select OC to split",
                options=range(len(split_candidates)),
                format_func=lambda i: f"{split_candidates[i]['oc_number']} | {split_candidates[i]['product']} | Qty: {split_candidates[i]['final_qty']:.0f}",
                key="split_oc_selector"
            )
            
            selected_oc = split_candidates[selected_idx]
            ocd_id = selected_oc['ocd_id']
            max_qty = selected_oc['max_allocatable']
            default_etd = selected_oc['oc_etd']
            
            st.markdown(f"**Max allocatable:** {max_qty:.0f} | **OC ETD:** {default_etd}")
            
            # Initialize split if not exists
            if ocd_id not in st.session_state.split_allocations:
                st.session_state.split_allocations[ocd_id] = [
                    {'qty': selected_oc['final_qty'], 'etd': default_etd}
                ]
            
            splits = st.session_state.split_allocations[ocd_id]
            
            # Use form to batch changes
            with st.form(key=f"split_form_{ocd_id}"):
                st.markdown("**Split Entries:**")
                
                form_splits = []
                for idx, split in enumerate(splits):
                    col1, col2 = st.columns([2, 2])
                    
                    with col1:
                        split_qty = st.number_input(
                            f"Qty #{idx+1}",
                            min_value=0.0,
                            max_value=float(max_qty),
                            value=float(split.get('qty', 0)),
                            step=1.0,
                            key=f"form_split_qty_{ocd_id}_{idx}"
                        )
                    
                    with col2:
                        split_etd = st.date_input(
                            f"ETD #{idx+1}",
                            value=split.get('etd') if split.get('etd') else default_etd,
                            key=f"form_split_etd_{ocd_id}_{idx}"
                        )
                    
                    form_splits.append({'qty': split_qty, 'etd': split_etd})
                
                # Form submit buttons
                col_save, col_add, col_remove = st.columns([1, 1, 1])
                with col_save:
                    save_clicked = st.form_submit_button("💾 Save Splits", type="primary")
                with col_add:
                    add_clicked = st.form_submit_button("➕ Add Split")
                with col_remove:
                    remove_clicked = st.form_submit_button("🗑️ Remove Last")
            
            # Handle form submissions
            if save_clicked:
                # Filter out zero qty splits
                valid_splits = [s for s in form_splits if s['qty'] > 0]
                st.session_state.split_allocations[ocd_id] = valid_splits if valid_splits else [{'qty': 0, 'etd': default_etd}]
                st.success("Splits saved!")
                st.rerun()
            
            if add_clicked:
                # Add to session state, then form will reload
                total_so_far = sum(s['qty'] for s in form_splits)
                remaining = max(0, max_qty - total_so_far)
                st.session_state.split_allocations[ocd_id] = form_splits + [{'qty': remaining, 'etd': default_etd}]
                st.rerun()
            
            if remove_clicked and len(splits) > 1:
                st.session_state.split_allocations[ocd_id] = form_splits[:-1] if len(form_splits) > 1 else [form_splits[0]]
                st.rerun()
            
            # Show validation
            total_split_qty = sum(s['qty'] for s in st.session_state.split_allocations[ocd_id])
            if total_split_qty > max_qty:
                st.error(f"⚠️ Total split qty ({total_split_qty:.0f}) exceeds max allocatable ({max_qty:.0f})")
            elif total_split_qty > 0 and total_split_qty < max_qty:
                st.warning(f"ℹ️ Remaining unallocated: {max_qty - total_split_qty:.0f}")
            elif total_split_qty > 0:
                st.success(f"✅ Total: {total_split_qty:.0f} / {max_qty:.0f}")
            
            # Remove from simple adjustments if using splits
            if len(st.session_state.split_allocations.get(ocd_id, [])) > 1:
                if ocd_id in st.session_state.adjusted_allocations:
                    del st.session_state.adjusted_allocations[ocd_id]
        
        # Show summary of all active splits
        active_splits = {k: v for k, v in st.session_state.split_allocations.items() if len(v) > 1}
        if active_splits:
            st.markdown("---")
            st.markdown("**Active Splits:**")
            for ocd_id, splits in active_splits.items():
                oc_match = edit_df[edit_df['ocd_id'] == ocd_id]
                if len(oc_match) > 0:
                    oc_info = oc_match.iloc[0]
                    st.caption(f"• {oc_info['oc_number']}: {len(splits)} splits → {sum(s['qty'] for s in splits):.0f} total")
    
    # Recalculate metrics with adjustments (including splits)
    final_total = 0
    for i, row in edited_df.iterrows():
        ocd_id = edit_df.iloc[i]['ocd_id']
        if ocd_id in st.session_state.split_allocations:
            # Use split total
            final_total += sum(s['qty'] for s in st.session_state.split_allocations[ocd_id])
        else:
            final_total += row['final_qty']
    
    total_demand = sum(r.demand_qty for r in results)
    final_coverage = (final_total / total_demand * 100) if total_demand > 0 else 0
    
    # Count OCs with allocation (including splits)
    allocated_count = 0
    for i, row in edited_df.iterrows():
        ocd_id = edit_df.iloc[i]['ocd_id']
        if ocd_id in st.session_state.split_allocations:
            if sum(s['qty'] for s in st.session_state.split_allocations[ocd_id]) > 0:
                allocated_count += 1
        elif row['final_qty'] > 0:
            allocated_count += 1
    
    # Count splits
    split_count = sum(1 for splits in st.session_state.split_allocations.values() if len(splits) > 1)
    
    # Count ETD adjustments
    etd_adjustments = 0
    for ocd_id, adj in st.session_state.adjusted_allocations.items():
        if isinstance(adj, dict) and 'etd' in adj:
            # Find original OC ETD
            oc_rows = edit_df[edit_df['ocd_id'] == ocd_id]
            if len(oc_rows) > 0:
                original_etd = oc_rows['oc_etd'].values[0]
                if adj['etd'] != original_etd:
                    etd_adjustments += 1
    
    st.divider()
    
    # Final summary
    st.markdown("##### 📊 Final Summary")
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total to Allocate", format_number(final_total))
    m2.metric("OCs to Allocate", allocated_count)
    m3.metric("Avg Coverage", format_percentage(final_coverage))
    m4.metric("Qty Adjustments", len(st.session_state.adjusted_allocations))
    m5.metric("ETD Adjustments", etd_adjustments, help="OCs with allocated ETD different from OC ETD")
    m6.metric("Split Allocations", split_count, help="OCs split into multiple allocation records")
    
    # Validation
    validation_result = services['validator'].validate_bulk_allocation(
        [{'ocd_id': edit_df.iloc[i]['ocd_id'], 
          'product_id': results[i].product_id, 
          'final_qty': edited_df.iloc[i]['final_qty'],
          'allocated_etd': edited_df.iloc[i]['allocated_etd'],
          'oc_etd': edit_df.iloc[i]['oc_etd']} 
         for i in range(len(results))],
        demands_df,
        supply_df,
        user.get('role', '')
    )
    
    # Check for ETD delays
    etd_delay_warnings = []
    for i, row in edited_df.iterrows():
        oc_etd = edit_df.iloc[i]['oc_etd']
        alloc_etd = row['allocated_etd']
        if oc_etd and alloc_etd and alloc_etd > oc_etd:
            days_delay = (alloc_etd - oc_etd).days
            oc_number = edit_df.iloc[i]['oc_number']
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
                for warning in etd_delay_warnings[:10]:  # Show first 10
                    st.caption(f"  • {warning}")
                if len(etd_delay_warnings) > 10:
                    st.caption(f"  ... and {len(etd_delay_warnings) - 10} more")
    
    # Commit section
    st.divider()
    st.markdown("##### 💾 Commit Allocation")
    
    notes = st.text_area(
        "Notes (optional)",
        placeholder="Add any notes about this bulk allocation...",
        key="commit_notes"
    )
    
    # Navigation and commit
    nav_col1, nav_col2, nav_col3 = st.columns([1, 1, 1])
    
    with nav_col1:
        if st.button("← Back to Strategy", key="back_to_step2"):
            st.session_state.bulk_step = 2
            st.rerun()
    
    with nav_col3:
        if st.button("💾 Commit Allocation", type="primary", 
                    disabled=not validation_result['valid'] or allocated_count == 0,
                    key="commit_btn"):
            commit_bulk_allocation(edited_df, edit_df, notes)


def commit_bulk_allocation(edited_df: pd.DataFrame, original_df: pd.DataFrame, notes: str):
    """Commit bulk allocation to database"""
    results = st.session_state.simulation_results
    demands_df = st.session_state.demands_df
    
    with st.spinner("Committing bulk allocation..."):
        # Build allocation results with final quantities and ETDs
        allocation_results = []
        for i, row in edited_df.iterrows():
            ocd_id = original_df.iloc[i]['ocd_id']
            result = results[i]
            
            oc_info = demands_df[demands_df['ocd_id'] == ocd_id].iloc[0].to_dict() if not demands_df[demands_df['ocd_id'] == ocd_id].empty else {}
            
            # Get final qty and allocated_etd from edited data
            final_qty = row['final_qty']
            allocated_etd = row['allocated_etd']
            
            # Build product display for email/logging
            product_display = original_df.iloc[i].get('product_display', '')
            if not product_display:
                parts = [oc_info.get('pt_code', '')]
                if oc_info.get('product_name'):
                    parts.append(oc_info.get('product_name'))
                if oc_info.get('package_size'):
                    parts.append(oc_info.get('package_size'))
                product_display = ' | '.join(filter(None, parts))
                if oc_info.get('brand_name'):
                    product_display += f" ({oc_info.get('brand_name')})"
            
            allocation_results.append({
                'ocd_id': ocd_id,
                'product_id': result.product_id,
                'customer_code': result.customer_code,
                'demand_qty': result.demand_qty,
                'suggested_qty': result.suggested_qty,
                'final_qty': final_qty,
                'coverage_percent': row['coverage_pct'],
                'oc_number': oc_info.get('oc_number', ''),
                'pt_code': oc_info.get('pt_code', ''),
                'product_name': oc_info.get('product_name', ''),
                'package_size': oc_info.get('package_size', ''),
                'product_display': product_display,
                'oc_etd': oc_info.get('etd'),
                'allocated_etd': allocated_etd  # Use edited ETD
            })
        
        # Build demands dict
        demands_dict = {int(row['ocd_id']): row.to_dict() for _, row in demands_df.iterrows()}
        
        # Build strategy config dict
        strategy_config = {
            'strategy_type': st.session_state.strategy_type,
            'allocation_mode': st.session_state.allocation_mode,
            'phases': st.session_state.hybrid_phases,
            'min_guarantee_percent': st.session_state.min_guarantee_percent,
            'urgent_threshold_days': st.session_state.urgent_threshold_days
        }
        
        # Commit with split allocations
        result = services['service'].commit_bulk_allocation(
            allocation_results=allocation_results,
            demands_dict=demands_dict,
            scope=get_current_scope(),
            strategy_config=strategy_config,
            user_id=user.get('id'),
            notes=notes,
            split_allocations=st.session_state.split_allocations  # Pass split data
        )
        
        if result['success']:
            st.session_state.commit_result = result
            st.success(f"✅ Bulk allocation committed successfully!")
            st.info(f"Allocation Number: **{result['allocation_number']}**")
            st.metric("OCs Allocated", result['detail_count'])
            st.metric("Total Quantity", format_number(result['total_allocated']))
            
            # Send email notifications
            # 1. Summary email to allocator
            # 2. Individual emails to each OC creator
            try:
                email_result = services['email'].send_bulk_allocation_emails(
                    commit_result=result,
                    allocation_results=allocation_results,
                    scope=get_current_scope(),
                    strategy_config=strategy_config,
                    allocator_user_id=user.get('id'),
                    split_allocations=st.session_state.split_allocations
                )
                
                if email_result.get('success'):
                    summary_status = "✓" if email_result.get('summary_sent') else "✗"
                    individual_sent = email_result.get('individual_sent', 0)
                    individual_total = email_result.get('individual_total', 0)
                    
                    st.success(f"📧 Emails sent: Summary {summary_status} | Individual: {individual_sent}/{individual_total} OC creators")
                    
                    if email_result.get('errors'):
                        with st.expander("⚠️ Email errors", expanded=False):
                            for err in email_result['errors']:
                                st.caption(f"• {err}")
                else:
                    st.warning(f"📧 Email: {email_result.get('message', 'Failed')}")
                    
            except Exception as e:
                logger.warning(f"Email notification failed: {e}")
                st.warning(f"Email notification failed: {e}")
            
            # Clear session and offer new allocation
            if st.button("🔄 Start New Bulk Allocation", key="new_allocation_btn"):
                for key in list(st.session_state.keys()):
                    if key.startswith('bulk_') or key.startswith('scope_') or key.startswith('strategy_'):
                        del st.session_state[key]
                init_session_state()
                st.rerun()
        else:
            st.error(f"❌ Failed to commit: {result.get('error', 'Unknown error')}")


# ==================== MAIN RENDER ====================

if st.session_state.bulk_step == 1:
    render_step1_scope()
elif st.session_state.bulk_step == 2:
    render_step2_strategy()
elif st.session_state.bulk_step == 3:
    render_step3_commit()