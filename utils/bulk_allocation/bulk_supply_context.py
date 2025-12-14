"""
Bulk Allocation Supply Context Components
==========================================
Provides supply context information during fine-tuning in Step 3.

Features:
1. Collapsible Supply Summary Panel - overview of all products in scope
2. Product Supply Detail Expander - detailed breakdown per product
3. Available Supply column in fine-tuning table
4. Supply tooltips and indicators

CREATED: 2024-12 - Improve UX for allocation fine-tuning decisions
UPDATED: 2024-12 - Show all items in Supply Sources Breakdown (removed 3-item limit)
LOCATION: utils/bulk_allocation/bulk_supply_context.py
"""
import streamlit as st
import pandas as pd
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


# ==================== SUPPLY CONTEXT DATA BUILDER ====================

def build_supply_context(
    demands_df: pd.DataFrame,
    supply_df: pd.DataFrame,
    simulation_results: List[Any] = None
) -> Dict[str, Any]:
    """
    Build comprehensive supply context for all products in scope.
    
    This function aggregates demand and supply data to provide context
    for each product, helping users make informed allocation decisions.
    
    Args:
        demands_df: DataFrame with OC demands from get_demands_in_scope()
        supply_df: DataFrame with supply data from get_supply_by_products()
        simulation_results: Optional list of AllocationResult from simulation
    
    Returns:
        Dict with:
        - products: Dict[product_id] -> product supply context
        - summary: Overall supply summary across all products
    
    Example:
        supply_context = build_supply_context(demands_df, supply_df, results)
        render_supply_summary_panel(supply_context)
    """
    if demands_df is None or demands_df.empty:
        return {'products': {}, 'summary': _empty_summary()}
    
    # Build supply lookup from supply_df
    supply_lookup = {}
    if supply_df is not None and not supply_df.empty:
        for _, row in supply_df.iterrows():
            supply_lookup[int(row['product_id'])] = {
                'total_supply': float(row.get('total_supply', 0) or 0),
                'committed': float(row.get('total_committed', 0) or 0),
                'available': float(row.get('available', 0) or 0)
            }
    
    # Aggregate demand per product
    product_demands = {}
    for _, row in demands_df.iterrows():
        pid = int(row['product_id'])
        if pid not in product_demands:
            product_demands[pid] = {
                'product_id': pid,
                'pt_code': row.get('pt_code', ''),
                'product_name': row.get('product_name', ''),
                'package_size': row.get('package_size', ''),
                'brand_name': row.get('brand_name', ''),
                'standard_uom': row.get('standard_uom', ''),
                'product_display': row.get('product_display', ''),
                'oc_count': 0,
                'total_demand': 0,
                'total_undelivered_allocated': 0,
                'total_max_allocatable': 0,
            }
        
        product_demands[pid]['oc_count'] += 1
        product_demands[pid]['total_demand'] += float(row.get('pending_qty', 0) or 0)
        product_demands[pid]['total_undelivered_allocated'] += float(row.get('undelivered_allocated', 0) or 0)
        product_demands[pid]['total_max_allocatable'] += float(row.get('max_allocatable', 0) or 0)
    
    # Merge supply info and calculate coverage
    products = {}
    totals = {
        'supply': 0, 'committed': 0, 'available': 0, 
        'demand': 0, 'allocatable': 0
    }
    
    for pid, demand_info in product_demands.items():
        supply_info = supply_lookup.get(pid, {
            'total_supply': 0, 'committed': 0, 'available': 0
        })
        
        max_allocatable = demand_info['total_max_allocatable']
        available = supply_info['available']
        
        products[pid] = {
            **demand_info,
            'total_supply': supply_info['total_supply'],
            'committed': supply_info['committed'],
            'available': available,
            'coverage_pct': (available / max_allocatable * 100) if max_allocatable > 0 else 100
        }
        
        # Accumulate totals
        totals['supply'] += supply_info['total_supply']
        totals['committed'] += supply_info['committed']
        totals['available'] += available
        totals['demand'] += demand_info['total_demand']
        totals['allocatable'] += max_allocatable
    
    # Build summary
    summary = {
        'product_count': len(products),
        'total_supply': totals['supply'],
        'total_committed': totals['committed'],
        'total_available': totals['available'],
        'total_demand': totals['demand'],
        'total_allocatable': totals['allocatable'],
        'overall_coverage_pct': (totals['available'] / totals['allocatable'] * 100) 
                                if totals['allocatable'] > 0 else 100
    }
    
    return {
        'products': products,
        'summary': summary
    }


def _empty_summary() -> Dict[str, Any]:
    """Return empty summary structure."""
    return {
        'product_count': 0,
        'total_supply': 0,
        'total_committed': 0,
        'total_available': 0,
        'total_demand': 0,
        'total_allocatable': 0,
        'overall_coverage_pct': 0
    }


# ==================== UI: SUPPLY SUMMARY PANEL ====================

def render_supply_summary_panel(
    supply_context: Dict[str, Any],
    expanded: bool = False
) -> None:
    """
    Render collapsible supply summary panel showing all products in scope.
    
    Displays:
    - Overall supply metrics (Total, Committed, Available)
    - Per-product breakdown table with coverage indicators
    - Formula reference for committed/available calculations
    
    Args:
        supply_context: Output from build_supply_context()
        expanded: Whether to expand the panel by default
    """
    summary = supply_context.get('summary', {})
    products = supply_context.get('products', {})
    
    if not products:
        return
    
    with st.expander("📦 Supply Context for Fine-tuning", expanded=expanded):
        # ========== Overall Summary Metrics ==========
        st.markdown("##### 📊 Overall Supply Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        col1.metric(
            "Total Supply",
            f"{summary.get('total_supply', 0):,.0f}",
            help="Sum of all supply sources:\nInventory + Pending CAN + Pending PO + WH Transfer"
        )
        
        col2.metric(
            "Committed",
            f"{summary.get('total_committed', 0):,.0f}",
            help="Already allocated but not yet delivered.\n\n"
                 "Formula: Σ MIN(pending_delivery, undelivered_allocated)\n\n"
                 "This prevents over-blocking supply when delivery data is incomplete."
        )
        
        available = summary.get('total_available', 0)
        coverage = summary.get('overall_coverage_pct', 0)
        col3.metric(
            "🟡 Available",
            f"{available:,.0f}",
            delta=f"{coverage:.0f}% of allocatable",
            delta_color="normal" if coverage >= 100 else "inverse",
            help="Supply available for new allocation.\n\nAvailable = Total Supply - Committed"
        )
        
        col4.metric(
            "Products",
            summary.get('product_count', 0),
            help="Number of unique products with pending OCs in scope"
        )
        
        st.divider()
        
        # ========== Product-level Breakdown Table ==========
        st.markdown("##### 📋 Supply by Product")
        
        # Build product summary table (sorted by coverage ascending - worst first)
        product_data = []
        for pid, pinfo in sorted(products.items(), key=lambda x: x[1].get('coverage_pct', 100)):
            cov = pinfo.get('coverage_pct', 0)
            # Color-coded status icon
            status_icon = "🟢" if cov >= 100 else "🟡" if cov >= 50 else "🔴"
            
            product_data.append({
                'product_id': pid,
                'Product': pinfo.get('product_display', pinfo.get('pt_code', '')),
                'OCs': pinfo.get('oc_count', 0),
                'Allocatable': pinfo.get('total_max_allocatable', 0),
                'Total Supply': pinfo.get('total_supply', 0),
                'Committed': pinfo.get('committed', 0),
                'Available': pinfo.get('available', 0),
                'Coverage': f"{status_icon} {cov:.0f}%",
                'UOM': pinfo.get('standard_uom', '')
            })
        
        if product_data:
            df = pd.DataFrame(product_data)
            
            st.dataframe(
                df[['Product', 'OCs', 'Allocatable', 'Total Supply', 'Committed', 'Available', 'Coverage', 'UOM']],
                column_config={
                    'Product': st.column_config.TextColumn('Product', width="large"),
                    'OCs': st.column_config.NumberColumn('OCs', width="small"),
                    'Allocatable': st.column_config.NumberColumn('Allocatable', format="%.0f"),
                    'Total Supply': st.column_config.NumberColumn('Total Supply', format="%.0f"),
                    'Committed': st.column_config.NumberColumn('Committed', format="%.0f"),
                    'Available': st.column_config.NumberColumn('Available', format="%.0f"),
                    'Coverage': st.column_config.TextColumn('Coverage', width="small"),
                    'UOM': st.column_config.TextColumn('UOM', width="small")
                },
                hide_index=True,
                use_container_width=True
            )
        
        # ========== Formula Reference ==========
        st.markdown("""
        <div style="background: #f8f9fa; padding: 12px; border-radius: 8px; font-size: 12px; margin-top: 10px;">
        <b>📐 Formula Reference:</b><br>
        • <b>Committed</b> = Σ MIN(pending_delivery, undelivered_allocated) — prevents over-blocking when delivery data is incomplete<br>
        • <b>Available</b> = Total Supply - Committed<br>
        • <b>Coverage %</b> = Available / Allocatable Demand × 100%<br>
        <br>
        <b>Legend:</b> 🟢 ≥100% coverage | 🟡 50-99% coverage | 🔴 <50% coverage (shortage)
        </div>
        """, unsafe_allow_html=True)


# ==================== UI: PRODUCT SUPPLY DETAIL ====================

def render_product_supply_detail(
    product_id: int,
    supply_context: Dict[str, Any],
    supply_details: Optional[Dict] = None
) -> None:
    """
    Render detailed supply breakdown for a specific product.
    
    Args:
        product_id: Product ID to show details for
        supply_context: Output from build_supply_context()
        supply_details: Optional detailed breakdown from get_supply_details_by_product()
    """
    pinfo = supply_context.get('products', {}).get(product_id, {})
    
    if not pinfo:
        st.warning(f"No supply data for product {product_id}")
        return
    
    uom = pinfo.get('standard_uom', '')
    
    # ========== Summary Metrics ==========
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric(
        "Total Supply",
        f"{pinfo.get('total_supply', 0):,.0f} {uom}",
        help="Sum of all supply sources"
    )
    
    col2.metric(
        "Committed",
        f"{pinfo.get('committed', 0):,.0f} {uom}",
        help="Allocated but not yet delivered\nFormula: Σ MIN(pending, undelivered_allocated)"
    )
    
    available = pinfo.get('available', 0)
    coverage = pinfo.get('coverage_pct', 0)
    col3.metric(
        "🟡 Available",
        f"{available:,.0f} {uom}",
        delta=f"{coverage:.0f}% coverage",
        delta_color="normal" if coverage >= 100 else "inverse"
    )
    
    col4.metric(
        "OCs in Scope",
        pinfo.get('oc_count', 0),
        help=f"Allocatable demand: {pinfo.get('total_max_allocatable', 0):,.0f} {uom}"
    )
    
    # ========== Source Breakdown ==========
    if supply_details:
        st.markdown("---")
        st.markdown("##### 📦 Supply Sources Breakdown")
        
        src_cols = st.columns(4)
        
        # Inventory
        with src_cols[0]:
            st.markdown("**🏭 Inventory**")
            inv_list = supply_details.get('inventory', [])
            if inv_list:
                inv_total = sum(i.get('remaining_quantity', 0) for i in inv_list)
                st.metric("Total", f"{inv_total:,.0f} {uom}")
                for batch in inv_list:
                    exp = batch.get('expiry_date', 'N/A')
                    qty = batch.get('remaining_quantity', 0)
                    batch_no = str(batch.get('batch_number', 'N/A'))[:12]
                    st.caption(f"• {batch_no}: {qty:,.0f}")
                    st.caption(f"  Exp: {exp}")
            else:
                st.caption("No stock on hand")
        
        # Pending CAN
        with src_cols[1]:
            st.markdown("**📋 Pending CAN**")
            can_list = supply_details.get('pending_can', [])
            if can_list:
                can_total = sum(c.get('pending_quantity', 0) for c in can_list)
                st.metric("Total", f"{can_total:,.0f} {uom}")
                for can in can_list:
                    qty = can.get('pending_quantity', 0)
                    can_no = str(can.get('arrival_note_number', 'N/A'))[:15]
                    st.caption(f"• {can_no}")
                    st.caption(f"  {qty:,.0f} {uom}")
            else:
                st.caption("No pending CAN")
        
        # Pending PO
        with src_cols[2]:
            st.markdown("**📄 Pending PO**")
            po_list = supply_details.get('pending_po', [])
            if po_list:
                po_total = sum(p.get('pending_standard_arrival_quantity', 0) for p in po_list)
                st.metric("Total", f"{po_total:,.0f} {uom}")
                for po in po_list:
                    qty = po.get('pending_standard_arrival_quantity', 0)
                    eta = po.get('eta', 'N/A')
                    po_no = str(po.get('po_number', 'N/A'))[:15]
                    st.caption(f"• {po_no}")
                    st.caption(f"  {qty:,.0f} | ETA: {eta}")
            else:
                st.caption("No pending PO")
        
        # WH Transfer
        with src_cols[3]:
            st.markdown("**🚚 WH Transfer**")
            wht_list = supply_details.get('wh_transfer', [])
            if wht_list:
                wht_total = sum(w.get('transfer_quantity', 0) for w in wht_list)
                st.metric("Total", f"{wht_total:,.0f} {uom}")
                for wht in wht_list:
                    qty = wht.get('transfer_quantity', 0)
                    from_wh = str(wht.get('from_warehouse', ''))[:10]
                    to_wh = str(wht.get('to_warehouse', ''))[:10]
                    st.caption(f"• {from_wh} → {to_wh}")
                    st.caption(f"  {qty:,.0f} {uom}")
            else:
                st.caption("No transfers")
    
    # Formula reference
    st.markdown("---")
    st.markdown("""
    <div style="background: #f0f7ff; padding: 10px; border-radius: 6px; font-size: 11px;">
    <b>📐 Formula:</b> Committed = Σ MIN(pending_delivery, undelivered_allocated) — 
    prevents over-blocking supply when delivery data is incomplete
    </div>
    """, unsafe_allow_html=True)


# ==================== TOOLTIP GENERATORS ====================

def get_supply_tooltip(product_id: int, supply_context: Dict[str, Any]) -> str:
    """
    Generate tooltip text for a product's supply information.
    
    Use this for help text in table columns or other UI elements.
    
    Args:
        product_id: Product ID
        supply_context: Output from build_supply_context()
    
    Returns:
        Multi-line tooltip text string
    """
    pinfo = supply_context.get('products', {}).get(product_id, {})
    
    if not pinfo:
        return "No supply data available"
    
    uom = pinfo.get('standard_uom', '')
    
    lines = [
        f"📦 Supply: {pinfo.get('pt_code', '')}",
        "",
        f"Total Supply: {pinfo.get('total_supply', 0):,.0f} {uom}",
        f"Committed: {pinfo.get('committed', 0):,.0f} {uom}",
        f"🟡 Available: {pinfo.get('available', 0):,.0f} {uom}",
        "",
        f"Coverage: {pinfo.get('coverage_pct', 0):.0f}%",
        f"OCs: {pinfo.get('oc_count', 0)} | Allocatable: {pinfo.get('total_max_allocatable', 0):,.0f} {uom}",
        "",
        "Formula:",
        "Committed = Σ MIN(pending, undelivered_allocated)",
        "Available = Total Supply - Committed"
    ]
    
    return "\n".join(lines)


def get_supply_indicator(product_id: int, supply_context: Dict[str, Any]) -> str:
    """
    Get supply status indicator icon for a product.
    
    Args:
        product_id: Product ID
        supply_context: Output from build_supply_context()
    
    Returns:
        Status icon: 🟢 (sufficient), 🟡 (moderate), 🔴 (shortage)
    """
    pinfo = supply_context.get('products', {}).get(product_id, {})
    coverage = pinfo.get('coverage_pct', 100)
    
    if coverage >= 100:
        return "🟢"
    elif coverage >= 50:
        return "🟡"
    else:
        return "🔴"


# ==================== HELPER FUNCTIONS ====================

def _truncate(text: str, max_len: int = 50) -> str:
    """Truncate text with ellipsis."""
    if not text:
        return ""
    return text[:max_len-3] + "..." if len(text) > max_len else text


def _format_number(value: Any, decimals: int = 0) -> str:
    """Format number with thousand separators."""
    try:
        return f"{float(value):,.{decimals}f}"
    except:
        return str(value)