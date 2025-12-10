"""
Bulk Allocation Page
====================
Main page for bulk allocation with strategy-based allocation assistance.

Features:
- Scope selection (Brand, Customer, Legal Entity, ETD Range)
- Strategy selection (FCFS, ETD Priority, Proportional, Revenue Priority, Hybrid)
- Simulation preview with fine-tuning
- Bulk commit with summary email
"""
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional

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
        'include_partial_allocated': st.session_state.scope_include_partial
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

# ==================== STEP 1: SELECT SCOPE ====================

def render_step1_scope():
    """Render scope selection step"""
    st.subheader("Step 1: Define Allocation Scope")
    
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
            key="brand_selector"
        )
        st.session_state.scope_brand_ids = selected_brands
        
        st.markdown("##### 👥 Customer Filter")
        customer_options = {c['customer_code']: f"{c['customer']} ({c['oc_count']} OCs)" for c in customers}
        selected_customers = st.multiselect(
            "Select Customers",
            options=list(customer_options.keys()),
            format_func=lambda x: customer_options.get(x, x),
            default=st.session_state.scope_customer_codes,
            key="customer_selector"
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
            key="le_selector"
        )
        st.session_state.scope_legal_entities = selected_les
        
        st.markdown("##### 📅 ETD Range")
        etd_col1, etd_col2 = st.columns(2)
        with etd_col1:
            etd_from = st.date_input(
                "From",
                value=st.session_state.scope_etd_from,
                key="etd_from_input"
            )
            st.session_state.scope_etd_from = etd_from
        with etd_col2:
            etd_to = st.date_input(
                "To",
                value=st.session_state.scope_etd_to or (date.today() + timedelta(days=30)),
                key="etd_to_input"
            )
            st.session_state.scope_etd_to = etd_to
    
    # Options
    st.markdown("##### ⚙️ Options")
    include_partial = st.checkbox(
        "Include partially allocated OCs (top-up)",
        value=st.session_state.scope_include_partial,
        key="include_partial_check"
    )
    st.session_state.scope_include_partial = include_partial
    
    # Scope preview
    st.divider()
    st.markdown("##### 📊 Scope Preview")
    
    scope = get_current_scope()
    
    # Validate scope
    scope_errors = services['validator'].validate_scope(scope)
    if scope_errors:
        for error in scope_errors:
            st.warning(error)
    else:
        # Get scope summary
        with st.spinner("Loading scope summary..."):
            summary = services['data'].get_scope_summary(scope)
        
        # Display metrics
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Products", format_number(summary['total_products']))
        m2.metric("OCs", format_number(summary['total_ocs']))
        m3.metric("Total Demand", format_number(summary['total_demand']))
        m4.metric("Available Supply", format_number(summary['available_supply']))
        m5.metric("Coverage", format_percentage(summary['coverage_percent']))
        
        if summary['total_ocs'] == 0:
            st.info("No OCs found matching the selected scope. Please adjust your filters.")
    
    # Navigation
    st.divider()
    col1, col2, col3 = st.columns([1, 2, 1])
    with col3:
        if st.button("Next: Choose Strategy →", type="primary", disabled=bool(scope_errors) or summary.get('total_ocs', 0) == 0):
            clear_simulation()
            st.session_state.bulk_step = 2
            st.rerun()

# ==================== STEP 2: CHOOSE STRATEGY ====================

def render_step2_strategy():
    """Render strategy selection step"""
    st.subheader("Step 2: Choose Allocation Strategy")
    
    # Show current scope summary
    scope = get_current_scope()
    st.info(f"📋 Scope: {format_scope_summary(scope)}")
    
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
            
            if st.button(f"Select {stype.name}", key=f"select_{stype.name}", 
                        disabled=is_selected, use_container_width=True):
                st.session_state.strategy_type = stype.name
                clear_simulation()
                st.rerun()
    
    # Strategy configuration
    st.divider()
    st.markdown("##### ⚙️ Strategy Configuration")
    
    conf_col1, conf_col2 = st.columns(2)
    
    with conf_col1:
        allocation_mode = st.radio(
            "Allocation Mode",
            options=['SOFT', 'HARD'],
            index=0 if st.session_state.allocation_mode == 'SOFT' else 1,
            horizontal=True,
            help="SOFT: Flexible allocation without specific source. HARD: Link to specific supply source."
        )
        st.session_state.allocation_mode = allocation_mode
    
    with conf_col2:
        if st.session_state.strategy_type == 'HYBRID':
            min_guarantee = st.slider(
                "Minimum Guarantee %",
                min_value=0, max_value=50, value=st.session_state.min_guarantee_percent,
                help="Minimum percentage each OC is guaranteed to receive"
            )
            st.session_state.min_guarantee_percent = min_guarantee
    
    # HYBRID phase configuration
    if st.session_state.strategy_type == 'HYBRID':
        st.markdown("##### 📊 Hybrid Phases")
        
        phase_cols = st.columns(4)
        total_weight = 0
        new_phases = []
        
        phase_options = ['MIN_GUARANTEE', 'ETD_PRIORITY', 'FCFS', 'PROPORTIONAL', 'REVENUE_PRIORITY']
        
        for i, phase_col in enumerate(phase_cols[:3]):
            with phase_col:
                current_phase = st.session_state.hybrid_phases[i] if i < len(st.session_state.hybrid_phases) else {'name': 'PROPORTIONAL', 'weight': 0}
                
                phase_name = st.selectbox(
                    f"Phase {i+1}",
                    options=phase_options,
                    index=phase_options.index(current_phase['name']) if current_phase['name'] in phase_options else 0,
                    key=f"phase_{i}_name"
                )
                
                phase_weight = st.number_input(
                    "Weight %",
                    min_value=0, max_value=100,
                    value=current_phase['weight'],
                    key=f"phase_{i}_weight"
                )
                
                new_phases.append({'name': phase_name, 'weight': phase_weight})
                total_weight += phase_weight
        
        with phase_cols[3]:
            st.metric("Total Weight", f"{total_weight}%")
            if total_weight != 100:
                st.error("Must equal 100%")
        
        st.session_state.hybrid_phases = new_phases
    
    # Run simulation button
    st.divider()
    
    run_col1, run_col2, run_col3 = st.columns([1, 2, 1])
    with run_col2:
        if st.button("🔄 Run Simulation", type="primary", use_container_width=True):
            with st.spinner("Running allocation simulation..."):
                run_simulation()
            st.rerun()
    
    # Show simulation results if available
    if st.session_state.simulation_results is not None:
        render_simulation_preview()
    
    # Navigation
    st.divider()
    nav_col1, nav_col2, nav_col3 = st.columns([1, 2, 1])
    with nav_col1:
        if st.button("← Back to Scope"):
            st.session_state.bulk_step = 1
            st.rerun()
    with nav_col3:
        can_proceed = st.session_state.simulation_results is not None and len(st.session_state.simulation_results) > 0
        if st.button("Next: Review & Commit →", type="primary", disabled=not can_proceed):
            st.session_state.bulk_step = 3
            st.rerun()

def run_simulation():
    """Run allocation simulation"""
    scope = get_current_scope()
    config = get_strategy_config()
    
    # Load demands
    demands_df = services['data'].get_demands_in_scope(scope)
    if demands_df.empty:
        st.error("No demands found in scope")
        return
    
    # Load supply
    product_ids = demands_df['product_id'].unique().tolist()
    supply_df = services['data'].get_supply_by_products(product_ids)
    
    # Run simulation
    results = services['engine'].simulate(demands_df, supply_df, config)
    
    # Store results
    st.session_state.simulation_results = results
    st.session_state.demands_df = demands_df
    st.session_state.supply_df = supply_df
    st.session_state.adjusted_allocations = {}

def render_simulation_preview():
    """Render simulation results preview"""
    results = st.session_state.simulation_results
    demands_df = st.session_state.demands_df
    
    if not results:
        return
    
    st.markdown("##### 📋 Simulation Results")
    
    # Summary metrics
    total_allocated = sum(r.final_qty for r in results)
    total_demand = sum(r.demand_qty for r in results)
    allocated_count = sum(1 for r in results if r.final_qty > 0)
    avg_coverage = (total_allocated / total_demand * 100) if total_demand > 0 else 0
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Allocated", format_number(total_allocated))
    m2.metric("OCs with Allocation", f"{allocated_count}/{len(results)}")
    m3.metric("Avg Coverage", format_percentage(avg_coverage))
    m4.metric("Unallocated", format_number(len(results) - allocated_count))
    
    # Results table
    with st.expander("📊 View Allocation Details", expanded=True):
        # Convert to DataFrame for display
        display_data = []
        for r in results:
            # Get OC info from demands_df
            oc_info = demands_df[demands_df['ocd_id'] == r.ocd_id].iloc[0] if not demands_df[demands_df['ocd_id'] == r.ocd_id].empty else {}
            
            display_data.append({
                'OC Number': oc_info.get('oc_number', ''),
                'Customer': r.customer_code,
                'Product': oc_info.get('pt_code', ''),
                'ETD': format_date(oc_info.get('etd')),
                'Demand': r.demand_qty,
                'Already Allocated': r.current_allocated,
                'Suggested': r.suggested_qty,
                'Coverage %': r.coverage_percent,
                'ocd_id': r.ocd_id
            })
        
        display_df = pd.DataFrame(display_data)
        
        # Show as editable dataframe (for fine-tuning in Step 3)
        st.dataframe(
            display_df.drop(columns=['ocd_id']),
            use_container_width=True,
            hide_index=True
        )

# ==================== STEP 3: REVIEW & COMMIT ====================

def render_step3_commit():
    """Render review and commit step"""
    st.subheader("Step 3: Review & Commit")
    
    results = st.session_state.simulation_results
    demands_df = st.session_state.demands_df
    supply_df = st.session_state.supply_df
    
    if results is None or len(results) == 0:
        st.warning("No simulation results. Please go back and run simulation.")
        if st.button("← Back to Strategy"):
            st.session_state.bulk_step = 2
            st.rerun()
        return
    
    # Summary
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
        
        # Check for adjustments
        adjusted = st.session_state.adjusted_allocations.get(r.ocd_id, r.final_qty)
        
        edit_data.append({
            'ocd_id': r.ocd_id,
            'oc_number': oc_info.get('oc_number', ''),
            'customer_code': r.customer_code,
            'customer': oc_info.get('customer', ''),
            'pt_code': oc_info.get('pt_code', ''),
            'product_name': oc_info.get('product_name', ''),
            'etd': oc_info.get('etd'),
            'demand_qty': r.demand_qty,
            'current_allocated': r.current_allocated,
            'suggested_qty': r.suggested_qty,
            'final_qty': adjusted,
            'coverage_pct': (adjusted / r.demand_qty * 100) if r.demand_qty > 0 else 0
        })
    
    edit_df = pd.DataFrame(edit_data)
    
    # Editable columns
    edited_df = st.data_editor(
        edit_df[['oc_number', 'customer_code', 'pt_code', 'etd', 'demand_qty', 'current_allocated', 'suggested_qty', 'final_qty', 'coverage_pct']],
        column_config={
            'oc_number': st.column_config.TextColumn('OC Number', disabled=True),
            'customer_code': st.column_config.TextColumn('Customer', disabled=True),
            'pt_code': st.column_config.TextColumn('Product', disabled=True),
            'etd': st.column_config.DateColumn('ETD', disabled=True),
            'demand_qty': st.column_config.NumberColumn('Demand', disabled=True, format="%.0f"),
            'current_allocated': st.column_config.NumberColumn('Already Alloc', disabled=True, format="%.0f"),
            'suggested_qty': st.column_config.NumberColumn('Suggested', disabled=True, format="%.0f"),
            'final_qty': st.column_config.NumberColumn('Final Qty', format="%.0f"),
            'coverage_pct': st.column_config.NumberColumn('Coverage %', disabled=True, format="%.1f%%")
        },
        use_container_width=True,
        hide_index=True,
        num_rows="fixed"
    )
    
    # Save adjustments
    for i, row in edited_df.iterrows():
        ocd_id = edit_df.iloc[i]['ocd_id']
        original = results[i].suggested_qty
        final = row['final_qty']
        if final != original:
            st.session_state.adjusted_allocations[ocd_id] = final
    
    # Recalculate metrics with adjustments
    final_total = sum(edited_df['final_qty'])
    total_demand = sum(r.demand_qty for r in results)
    final_coverage = (final_total / total_demand * 100) if total_demand > 0 else 0
    allocated_count = sum(1 for qty in edited_df['final_qty'] if qty > 0)
    
    st.divider()
    
    # Final summary
    st.markdown("##### 📊 Final Summary")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total to Allocate", format_number(final_total))
    m2.metric("OCs to Allocate", allocated_count)
    m3.metric("Avg Coverage", format_percentage(final_coverage))
    m4.metric("Adjustments Made", len(st.session_state.adjusted_allocations))
    
    # Validation
    validation_result = services['validator'].validate_bulk_allocation(
        [{'ocd_id': edit_df.iloc[i]['ocd_id'], 'product_id': results[i].product_id, 'final_qty': edited_df.iloc[i]['final_qty']} 
         for i in range(len(results))],
        demands_df,
        supply_df,
        user.get('role', '')
    )
    
    if not validation_result['valid']:
        st.error("❌ Validation Failed")
        st.text(services['validator'].generate_validation_summary(validation_result))
    elif validation_result['warnings']:
        st.warning("⚠️ Warnings")
        for warning in validation_result['warnings']:
            st.caption(f"  • {warning}")
    
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
        if st.button("← Back to Strategy"):
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
        # Build allocation results with final quantities
        allocation_results = []
        for i, row in edited_df.iterrows():
            ocd_id = original_df.iloc[i]['ocd_id']
            result = results[i]
            
            oc_info = demands_df[demands_df['ocd_id'] == ocd_id].iloc[0].to_dict() if not demands_df[demands_df['ocd_id'] == ocd_id].empty else {}
            
            allocation_results.append({
                'ocd_id': ocd_id,
                'product_id': result.product_id,
                'customer_code': result.customer_code,
                'demand_qty': result.demand_qty,
                'suggested_qty': result.suggested_qty,
                'final_qty': row['final_qty'],
                'coverage_percent': row['coverage_pct'],
                'oc_number': oc_info.get('oc_number', ''),
                'pt_code': oc_info.get('pt_code', ''),
                'allocated_etd': oc_info.get('etd')
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
        
        # Commit
        result = services['service'].commit_bulk_allocation(
            allocation_results=allocation_results,
            demands_dict=demands_dict,
            scope=get_current_scope(),
            strategy_config=strategy_config,
            user_id=user.get('id'),
            notes=notes
        )
        
        if result['success']:
            st.session_state.commit_result = result
            st.success(f"✅ Bulk allocation committed successfully!")
            st.info(f"Allocation Number: **{result['allocation_number']}**")
            st.metric("OCs Allocated", result['detail_count'])
            st.metric("Total Quantity", format_number(result['total_allocated']))
            
            # Send email
            try:
                recipients = services['email'].get_recipients_for_scope(
                    get_current_scope(),
                    user.get('email')
                )
                if recipients:
                    services['email'].send_bulk_allocation_email(
                        commit_result=result,
                        allocation_results=allocation_results,
                        scope=get_current_scope(),
                        strategy_config=strategy_config,
                        recipients=recipients
                    )
            except Exception as e:
                st.warning(f"Email notification failed: {e}")
            
            # Clear session and offer new allocation
            if st.button("🔄 Start New Bulk Allocation"):
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