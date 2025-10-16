"""
Tooltip Helpers for Allocation Module
Provides rich, informative tooltips for OC and Allocation details
Extracted from original code to maintain full tooltip functionality
"""
import pandas as pd
import logging
from typing import Union, Dict, Any

from .formatters import format_number, format_date, format_allocation_mode

logger = logging.getLogger(__name__)


def create_oc_tooltip(oc: Union[pd.Series, Dict]) -> str:
    """
    Create comprehensive tooltip for OC (Order Confirmation) details
    
    Shows:
    - OC quantity breakdown (Original → Effective → Pending)
    - Allocation summary (if any allocations exist)
    - Current allocation status
    
    Handles both pandas Series and dict inputs safely
    
    Args:
        oc: OC data as pandas Series or dict
        
    Returns:
        Formatted multi-line tooltip string
    """
    tooltip_lines = []
    
    # Helper function to safely get value from dict or Series
    def get_value(key, default=0):
        try:
            if isinstance(oc, pd.Series):
                if key in oc.index:
                    value = oc[key]
                    # Handle numpy types and ensure scalar
                    if hasattr(value, 'item'):
                        return value.item()
                    elif isinstance(value, pd.Series):
                        return value.iloc[0] if len(value) > 0 else default
                    return value
                else:
                    return default
            else:
                return oc.get(key, default)
        except Exception as e:
            logger.debug(f"Error getting value for key '{key}': {e}")
            return default
    
    # Get values with proper defaults and type conversion
    original_qty = float(get_value('original_standard_quantity', 0))
    oc_cancelled = float(get_value('total_oc_cancelled_qty', 0))
    effective_oc = float(get_value('standard_quantity', 0))
    delivered = float(get_value('total_delivered_standard_quantity', 0))
    pending = float(get_value('pending_standard_delivery_quantity', 0))
    standard_uom = str(get_value('standard_uom', ''))
    
    # ===== OC DETAILS SECTION =====
    tooltip_lines.append("📄 OC Details")
    tooltip_lines.append("")
    tooltip_lines.append(f"• Original Quantity: {format_number(original_qty)} {standard_uom}")
    
    if oc_cancelled > 0:
        tooltip_lines.append(f"• OC Cancelled: {format_number(oc_cancelled)} {standard_uom}")
    
    tooltip_lines.append(f"• Effective OC: {format_number(effective_oc)} {standard_uom}")
    
    if delivered > 0:
        tooltip_lines.append(f"• Delivered: {format_number(delivered)} {standard_uom}")
    
    tooltip_lines.append(f"• Pending Delivery: {format_number(pending)} {standard_uom}")
    
    # ===== ALLOCATION SUMMARY SECTION =====
    if get_value('allocation_count', 0) > 0:
        tooltip_lines.append("")
        tooltip_lines.append("📦 Allocation Summary")
        tooltip_lines.append("")
        
        total_allocated = float(get_value('total_allocated_qty_standard', 0))
        alloc_cancelled = float(get_value('total_allocation_cancelled_qty_standard', 0))
        effective_allocated = float(get_value('total_effective_allocated_qty_standard', 0))
        alloc_delivered = float(get_value('total_allocation_delivered_qty_standard', 0))
        undelivered = float(get_value('undelivered_allocated_qty_standard', 0))
        
        tooltip_lines.append(f"• Total Allocated: {format_number(total_allocated)} {standard_uom}")
        
        if alloc_cancelled > 0:
            tooltip_lines.append(f"• Allocation Cancelled: {format_number(alloc_cancelled)} {standard_uom}")
        
        tooltip_lines.append(f"• Effective Allocated: {format_number(effective_allocated)} {standard_uom}")
        
        if alloc_delivered > 0:
            tooltip_lines.append(f"• Delivered from Allocation: {format_number(alloc_delivered)} {standard_uom}")
        
        tooltip_lines.append(f"• Undelivered: {format_number(undelivered)} {standard_uom}")
    
    # ===== STATUS =====
    status = get_oc_allocation_status(oc)
    tooltip_lines.append("")
    tooltip_lines.append(f"Status: {status}")
    
    return "\n".join(tooltip_lines)


def get_oc_allocation_status(oc: Union[pd.Series, Dict]) -> str:
    """
    Get allocation status text for OC with emoji indicators
    
    Status levels:
    - ❌ Over-Committed: Effective allocation exceeds OC quantity
    - ⚠️ Pending Over-Allocated: Undelivered allocation exceeds pending delivery
    - ⏳ Not Allocated: No allocations made
    - ✅ Fully Allocated: Undelivered allocation equals pending delivery
    - 🟡 Partially Allocated: Some allocation but not complete
    
    Args:
        oc: OC data as pandas Series or dict
        
    Returns:
        Status string with emoji
    """
    # Helper function to safely get value
    def get_value(key, default=0):
        try:
            if isinstance(oc, pd.Series):
                if key in oc.index:
                    value = oc[key]
                    # Handle numpy types and ensure scalar
                    if hasattr(value, 'item'):
                        return value.item()
                    elif isinstance(value, pd.Series):
                        return value.iloc[0] if len(value) > 0 else default
                    return value
                else:
                    return default
            else:
                return oc.get(key, default)
        except Exception:
            return default
    
    pending = float(get_value('pending_standard_delivery_quantity', 0))
    undelivered = float(get_value('undelivered_allocated_qty_standard', 0))
    over_type = get_value('over_allocation_type', 'Normal')
    
    # Check for over-allocation conditions first
    if over_type == 'Over-Committed':
        return "❌ Over-Committed"
    elif over_type == 'Pending-Over-Allocated':
        return "⚠️ Pending Over-Allocated"
    
    # Check allocation coverage
    if undelivered == 0:
        return "⏳ Not Allocated"
    elif undelivered >= pending:
        return "✅ Fully Allocated"
    else:
        coverage = (undelivered / pending * 100) if pending > 0 else 0
        return f"🟡 Partially Allocated ({coverage:.0f}%)"


def get_allocation_status_color(pending: float, undelivered: float) -> str:
    """
    Get color indicator emoji based on allocation status
    
    Color coding:
    - 🔴 Red: Over-allocated (undelivered > pending)
    - 🟢 Green: Fully allocated (undelivered == pending)
    - 🟡 Yellow: Partially allocated (0 < undelivered < pending)
    - ⚪ White: Not allocated (undelivered == 0)
    
    Args:
        pending: Pending delivery quantity
        undelivered: Undelivered allocated quantity
        
    Returns:
        Emoji string representing allocation status
    """
    if undelivered > pending:
        return "🔴"  # Over-allocated
    elif undelivered == pending:
        return "🟢"  # Fully allocated
    elif undelivered > 0:
        return "🟡"  # Partially allocated
    else:
        return "⚪"  # Not allocated


def create_allocation_tooltip(alloc: Union[pd.Series, Dict], 
                              oc_info: Dict) -> str:
    """
    Create comprehensive tooltip for allocation details
    
    Shows:
    - Allocation quantities breakdown
    - Metadata (date, creator, mode, source)
    
    Args:
        alloc: Allocation data as pandas Series or dict
        oc_info: OC information dict containing UOM details
        
    Returns:
        Formatted multi-line tooltip string
    """
    tooltip_lines = []
    
    # Helper function to safely get value
    def get_value(obj, key, default=0):
        try:
            if isinstance(obj, pd.Series):
                if key in obj.index:
                    value = obj[key]
                    # Handle numpy types and ensure scalar
                    if hasattr(value, 'item'):
                        return value.item()
                    elif isinstance(value, pd.Series):
                        return value.iloc[0] if len(value) > 0 else default
                    return value
                else:
                    return default
            else:
                return obj.get(key, default)
        except Exception:
            return default
    
    # Get values
    allocated_qty = float(get_value(alloc, 'allocated_qty', 0))
    cancelled_qty = float(get_value(alloc, 'cancelled_qty', 0))
    effective_qty = float(get_value(alloc, 'effective_qty', 0))
    delivered_qty = float(get_value(alloc, 'delivered_qty', 0))
    pending_qty = float(get_value(alloc, 'pending_qty', 0))
    standard_uom = str(get_value(oc_info, 'standard_uom', ''))
    
    # ===== HEADER =====
    tooltip_lines.append(f"📦 Allocation {get_value(alloc, 'allocation_number', '')}")
    tooltip_lines.append("")
    
    # ===== QUANTITIES =====
    tooltip_lines.append(f"• Allocated Quantity: {format_number(allocated_qty)} {standard_uom}")
    
    if cancelled_qty > 0:
        tooltip_lines.append(f"• Cancelled: {format_number(cancelled_qty)} {standard_uom}")
    
    tooltip_lines.append(f"• Effective: {format_number(effective_qty)} {standard_uom}")
    
    if delivered_qty > 0:
        tooltip_lines.append(f"• Delivered: {format_number(delivered_qty)} {standard_uom}")
    
    tooltip_lines.append(f"• Pending: {format_number(pending_qty)} {standard_uom}")
    
    # ===== METADATA =====
    tooltip_lines.append("")
    tooltip_lines.append(f"• Created: {format_date(get_value(alloc, 'allocation_date'))}")
    tooltip_lines.append(f"• By: {str(get_value(alloc, 'created_by', ''))}")
    tooltip_lines.append(f"• Mode: {format_allocation_mode(get_value(alloc, 'allocation_mode', ''))}")
    
    if get_value(alloc, 'supply_source_type'):
        tooltip_lines.append(f"• Source: {str(get_value(alloc, 'supply_source_type', ''))}")
    
    return "\n".join(tooltip_lines)