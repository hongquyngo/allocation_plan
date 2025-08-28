"""
Formatting utilities for Allocation module - Enhanced Version
Added UOM-aware formatting functions
"""
import pandas as pd
from datetime import datetime, date
from typing import Union, Optional
import logging

logger = logging.getLogger(__name__)


def format_number(value: Union[int, float, None], decimals: int = 0) -> str:
    """
    Format number with thousand separator
    
    Args:
        value: Number to format
        decimals: Number of decimal places
        
    Returns:
        Formatted string
    """
    try:
        if value is None or pd.isna(value):
            return "-"
        
        if decimals == 0:
            return f"{int(value):,}"
        else:
            return f"{float(value):,.{decimals}f}"
            
    except (ValueError, TypeError):
        return "-"


def format_date(value: Union[str, datetime, date, None], 
                format_str: str = "%d/%m/%Y") -> str:
    """
    Format date consistently
    
    Args:
        value: Date value to format
        format_str: Output format string
        
    Returns:
        Formatted date string
    """
    try:
        if value is None or pd.isna(value):
            return "-"
        
        # Handle different input types
        if isinstance(value, str):
            if value.strip() == "":
                return "-"
            # Try common formats
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
                try:
                    dt = datetime.strptime(value.split('.')[0], fmt)  # Remove microseconds
                    return dt.strftime(format_str)
                except ValueError:
                    continue
            # Try pandas parsing as last resort
            try:
                dt = pd.to_datetime(value)
                return dt.strftime(format_str)
            except:
                return value  # Return as-is if can't parse
            
        elif isinstance(value, datetime):
            return value.strftime(format_str)
            
        elif isinstance(value, date):
            return value.strftime(format_str)
            
        elif isinstance(value, pd.Timestamp):
            return value.strftime(format_str)
            
        else:
            return str(value)
            
    except Exception as e:
        logger.debug(f"Error formatting date {value}: {e}")
        return "-"


def format_percentage(value: Union[int, float, None], decimals: int = 1) -> str:
    """
    Format percentage value
    
    Args:
        value: Percentage value (0-100)
        decimals: Number of decimal places
        
    Returns:
        Formatted percentage string
    """
    try:
        if value is None or pd.isna(value):
            return "-"
        
        return f"{float(value):.{decimals}f}%"
        
    except (ValueError, TypeError):
        return "-"


def format_allocation_mode(mode: str) -> str:
    """
    Format allocation mode with icon
    
    Args:
        mode: SOFT or HARD
        
    Returns:
        Formatted mode string
    """
    mode_map = {
        'SOFT': '📄 SOFT',
        'HARD': '🔒 HARD'
    }
    
    return mode_map.get(mode, mode)


def format_reason_category(category: str) -> str:
    """
    Format cancellation reason category
    
    Args:
        category: Reason category
        
    Returns:
        Formatted category string
    """
    category_map = {
        'CUSTOMER_REQUEST': '👤 Customer Request',
        'SUPPLY_ISSUE': '⚠️ Supply Issue',
        'QUALITY_ISSUE': '❌ Quality Issue',
        'BUSINESS_DECISION': '💼 Business Decision',
        'OTHER': '📝 Other'
    }
    
    return category_map.get(category, category)


def format_quantity_with_uom(quantity: Union[int, float, None], 
                            uom: str,
                            decimals: int = 0,
                            show_zero: bool = True) -> str:
    """
    Format quantity with UOM suffix
    
    Args:
        quantity: Quantity value
        uom: Unit of measure
        decimals: Number of decimal places
        show_zero: Whether to show "0 UOM" or just "-"
        
    Returns:
        Formatted string like "1,234 pcs" or "-"
    """
    try:
        if quantity is None or pd.isna(quantity):
            return "-"
        
        if quantity == 0 and not show_zero:
            return "-"
        
        formatted_qty = format_number(quantity, decimals)
        
        if uom:
            return f"{formatted_qty} {uom}"
        else:
            return formatted_qty
            
    except Exception as e:
        logger.debug(f"Error formatting quantity with UOM: {e}")
        return "-"


def format_quantity_range_with_uom(min_qty: Union[int, float, None],
                                  max_qty: Union[int, float, None],
                                  uom: str,
                                  decimals: int = 0) -> str:
    """
    Format quantity range with UOM
    
    Args:
        min_qty: Minimum quantity
        max_qty: Maximum quantity  
        uom: Unit of measure
        decimals: Number of decimal places
        
    Returns:
        Formatted string like "100-500 pcs"
    """
    try:
        if min_qty is None and max_qty is None:
            return "-"
        
        if min_qty == max_qty:
            return format_quantity_with_uom(min_qty, uom, decimals)
        
        min_str = format_number(min_qty, decimals) if min_qty is not None else "?"
        max_str = format_number(max_qty, decimals) if max_qty is not None else "?"
        
        if uom:
            return f"{min_str}-{max_str} {uom}"
        else:
            return f"{min_str}-{max_str}"
            
    except Exception:
        return "-"


def format_uom_conversion(ratio: str, from_uom: str, to_uom: str) -> str:
    """
    Format UOM conversion ratio for display
    
    Args:
        ratio: Conversion ratio (e.g., "100/1" or "100")
        from_uom: Source UOM
        to_uom: Target UOM
        
    Returns:
        Formatted string like "1 carton = 100 pcs"
    """
    try:
        if not ratio or ratio == "1":
            return f"1 {from_uom} = 1 {to_uom}"
        
        # Parse ratio
        if "/" in ratio:
            parts = ratio.split("/")
            if len(parts) == 2:
                numerator = float(parts[0].strip())
                denominator = float(parts[1].strip())
                
                if denominator == 1:
                    return f"1 {from_uom} = {format_number(numerator)} {to_uom}"
                else:
                    return f"{format_number(denominator)} {from_uom} = {format_number(numerator)} {to_uom}"
        else:
            ratio_value = float(ratio)
            return f"1 {from_uom} = {format_number(ratio_value)} {to_uom}"
            
    except Exception as e:
        logger.debug(f"Error formatting UOM conversion: {e}")
        return f"{from_uom} → {to_uom}"


def format_supply_status_icon(supply_percent: float) -> str:
    """
    Get status icon based on supply coverage percentage
    
    Args:
        supply_percent: Supply coverage percentage (0-100+)
        
    Returns:
        Status icon
    """
    if supply_percent >= 100:
        return "🟢"  # Sufficient
    elif supply_percent >= 50:
        return "🟡"  # Partial
    elif supply_percent > 0:
        return "🔴"  # Low
    else:
        return "⚫"  # No Supply