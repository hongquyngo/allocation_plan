"""
Bulk Allocation Formatters
==========================
Formatting utilities for bulk allocation UI.

CHANGELOG:
- 2024-12: Added format_product_display() to eliminate code duplication
           across UI components (was repeated 3+ times)
"""
from typing import Any, Optional, Union, Dict
from datetime import datetime, date
from decimal import Decimal
import pandas as pd


def format_number(value: Any, decimals: int = 0, prefix: str = "", suffix: str = "") -> str:
    """
    Format number with thousands separator
    
    Args:
        value: Number to format
        decimals: Number of decimal places
        prefix: Prefix string (e.g., "$")
        suffix: Suffix string (e.g., " pcs")
    
    Returns:
        Formatted string
    """
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "-"
        
        num = float(value)
        
        if decimals == 0:
            formatted = f"{num:,.0f}"
        else:
            formatted = f"{num:,.{decimals}f}"
        
        return f"{prefix}{formatted}{suffix}"
        
    except (ValueError, TypeError):
        return str(value) if value is not None else "-"


def format_percentage(value: Any, decimals: int = 1) -> str:
    """
    Format value as percentage
    
    Args:
        value: Number to format (already in percentage form, e.g., 75.5 for 75.5%)
        decimals: Number of decimal places
    
    Returns:
        Formatted percentage string
    """
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "-"
        
        num = float(value)
        return f"{num:.{decimals}f}%"
        
    except (ValueError, TypeError):
        return "-"


def format_date(value: Any, format_str: str = "%Y-%m-%d") -> str:
    """
    Format date value
    
    Args:
        value: Date to format
        format_str: strftime format string
    
    Returns:
        Formatted date string
    """
    try:
        if value is None:
            return "-"
        
        if isinstance(value, str):
            dt = pd.to_datetime(value)
        elif isinstance(value, (datetime, date)):
            dt = value
        else:
            dt = pd.to_datetime(value)
        
        if isinstance(dt, datetime):
            return dt.strftime(format_str)
        elif isinstance(dt, date):
            return dt.strftime(format_str)
        else:
            return str(value)
            
    except:
        return str(value) if value is not None else "-"


def format_datetime(value: Any, format_str: str = "%Y-%m-%d %H:%M") -> str:
    """Format datetime value"""
    return format_date(value, format_str)


def format_currency(value: Any, currency: str = "USD", decimals: int = 2) -> str:
    """
    Format value as currency
    
    Args:
        value: Amount to format
        currency: Currency code
        decimals: Number of decimal places
    """
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "-"
        
        num = float(value)
        
        if currency == "USD":
            return f"${num:,.{decimals}f}"
        elif currency == "VND":
            return f"{num:,.0f} ₫"
        else:
            return f"{num:,.{decimals}f} {currency}"
            
    except (ValueError, TypeError):
        return "-"


def format_quantity_with_uom(value: Any, uom: str = "", decimals: int = 0) -> str:
    """
    Format quantity with UOM
    
    Args:
        value: Quantity to format
        uom: Unit of measure
        decimals: Number of decimal places
    
    Returns:
        Formatted string like "1,000 pcs"
    """
    formatted = format_number(value, decimals)
    if formatted == "-":
        return formatted
    
    if uom:
        return f"{formatted} {uom}"
    return formatted


def format_coverage_badge(coverage_percent: float) -> str:
    """
    Format coverage percentage as colored badge
    
    Returns HTML-safe string for Streamlit
    """
    try:
        pct = float(coverage_percent)
        
        if pct >= 100:
            color = "🟢"
            label = "Full"
        elif pct >= 80:
            color = "🟢"
            label = f"{pct:.0f}%"
        elif pct >= 50:
            color = "🟡"
            label = f"{pct:.0f}%"
        elif pct > 0:
            color = "🟠"
            label = f"{pct:.0f}%"
        else:
            color = "⚪"
            label = "0%"
        
        return f"{color} {label}"
        
    except:
        return "⚪ -"


def format_strategy_name(strategy_type: str) -> str:
    """Format strategy type to display name"""
    names = {
        'FCFS': '📅 First Come First Serve',
        'ETD_PRIORITY': '🚨 ETD Priority',
        'PROPORTIONAL': '⚖️ Proportional',
        'REVENUE_PRIORITY': '💰 Revenue Priority',
        'HYBRID': '🎯 Hybrid (Recommended)'
    }
    return names.get(strategy_type.upper(), strategy_type)


def format_allocation_mode(mode: str) -> str:
    """Format allocation mode for display"""
    modes = {
        'SOFT': '🔵 SOFT (Flexible)',
        'HARD': '🔴 HARD (Fixed Source)'
    }
    return modes.get(mode.upper(), mode)


def format_etd_urgency(etd_date: Any, today: date = None) -> str:
    """
    Format ETD with urgency indicator
    
    Returns:
        String with emoji indicator
    """
    try:
        if etd_date is None:
            return "⚪ No ETD"
        
        if today is None:
            today = date.today()
        
        if isinstance(etd_date, str):
            etd = pd.to_datetime(etd_date).date()
        elif isinstance(etd_date, datetime):
            etd = etd_date.date()
        elif isinstance(etd_date, date):
            etd = etd_date
        else:
            return str(etd_date)
        
        days = (etd - today).days
        
        if days < 0:
            return f"🔴 Overdue ({abs(days)}d)"
        elif days <= 3:
            return f"🔴 Urgent ({days}d)"
        elif days <= 7:
            return f"🟠 Soon ({days}d)"
        elif days <= 14:
            return f"🟡 Normal ({days}d)"
        else:
            return f"🟢 {etd.strftime('%Y-%m-%d')}"
            
    except:
        return str(etd_date) if etd_date else "-"


def format_scope_summary(scope: dict) -> str:
    """Format scope configuration for display"""
    parts = []
    
    if scope.get('brand_ids'):
        count = len(scope['brand_ids'])
        parts.append(f"🏷️ {count} brand{'s' if count > 1 else ''}")
    
    if scope.get('customer_codes'):
        count = len(scope['customer_codes'])
        parts.append(f"👥 {count} customer{'s' if count > 1 else ''}")
    
    if scope.get('legal_entities'):
        count = len(scope['legal_entities'])
        parts.append(f"🏢 {count} entity/ies")
    
    if scope.get('etd_from') or scope.get('etd_to'):
        etd_from = scope.get('etd_from', 'Any')
        etd_to = scope.get('etd_to', 'Any')
        parts.append(f"📅 {etd_from} → {etd_to}")
    
    return " | ".join(parts) if parts else "No filters"


def format_diff(old_value: float, new_value: float, suffix: str = "") -> str:
    """
    Format difference between two values
    
    Returns:
        String with arrow indicator (e.g., "↑ +100 pcs")
    """
    try:
        diff = new_value - old_value
        
        if abs(diff) < 0.01:
            return f"→ {suffix}".strip()
        elif diff > 0:
            return f"↑ +{diff:,.0f} {suffix}".strip()
        else:
            return f"↓ {diff:,.0f} {suffix}".strip()
            
    except:
        return "-"


def truncate_text(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """Truncate text to max length"""
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def format_list_summary(items: list, max_items: int = 3) -> str:
    """Format list with count summary"""
    if not items:
        return "-"
    
    if len(items) <= max_items:
        return ", ".join(str(i) for i in items)
    
    shown = ", ".join(str(i) for i in items[:max_items])
    remaining = len(items) - max_items
    return f"{shown} (+{remaining} more)"


# ==================== NEW: Product Display Formatter ====================

def format_product_display(
    oc_info: Dict,
    include_brand: bool = True,
    max_length: Optional[int] = None
) -> str:
    """
    Build consistent product display string from OC info.
    
    This function consolidates the product display logic that was previously
    duplicated across multiple UI components.
    
    Format: "PT Code | Product Name | Package Size (Brand)"
    
    Args:
        oc_info: Dictionary containing product information. Expected keys:
            - product_display (optional): Pre-formatted display string
            - pt_code: Product code
            - product_name: Product name
            - package_size: Package size
            - brand_name: Brand name (optional)
        include_brand: Whether to append brand name in parentheses
        max_length: Optional max length for truncation
    
    Returns:
        Formatted product display string
        
    Examples:
        >>> format_product_display({'pt_code': 'P025000563', 'product_name': '3M Tape', 'package_size': '19mmx33m', 'brand_name': 'Vietape'})
        'P025000563 | 3M Tape | 19mmx33m (Vietape)'
        
        >>> format_product_display({'product_display': 'Already Formatted'})
        'Already Formatted'
    """
    # If pre-formatted display exists, use it
    if display := oc_info.get('product_display'):
        if max_length and len(display) > max_length:
            return truncate_text(display, max_length)
        return display
    
    # Build display from components
    parts = []
    
    # PT Code (required)
    pt_code = oc_info.get('pt_code', '')
    if pt_code:
        parts.append(pt_code)
    
    # Product name (optional)
    product_name = oc_info.get('product_name', '')
    if product_name:
        parts.append(product_name)
    
    # Package size (optional)
    package_size = oc_info.get('package_size', '')
    if package_size:
        parts.append(package_size)
    
    # Join parts with separator
    result = ' | '.join(filter(None, parts))
    
    # Append brand if available and requested
    if include_brand:
        brand_name = oc_info.get('brand_name', '')
        if brand_name:
            result += f" ({brand_name})"
    
    # Handle empty result
    if not result:
        result = "Unknown Product"
    
    # Truncate if needed
    if max_length and len(result) > max_length:
        return truncate_text(result, max_length)
    
    return result


def format_product_display_short(oc_info: Dict, max_length: int = 50) -> str:
    """
    Format product display for constrained spaces (tables, lists).
    
    Prioritizes PT code and truncates if needed.
    
    Args:
        oc_info: Dictionary containing product information
        max_length: Maximum string length
    
    Returns:
        Short formatted product display
    """
    pt_code = oc_info.get('pt_code', '')
    product_name = oc_info.get('product_name', '')
    
    if pt_code and product_name:
        combined = f"{pt_code} | {product_name}"
        if len(combined) <= max_length:
            return combined
        # Truncate product name, keep full pt_code
        available = max_length - len(pt_code) - 7  # " | " + "..."
        if available > 10:
            return f"{pt_code} | {product_name[:available]}..."
        return pt_code
    
    return pt_code or product_name or "Unknown"


def build_product_display_from_row(row: Union[Dict, pd.Series]) -> str:
    """
    Build product display from a DataFrame row or dictionary.
    
    Convenience wrapper for format_product_display that handles
    both dict and pandas Series.
    
    Args:
        row: DataFrame row (Series) or dictionary
        
    Returns:
        Formatted product display string
    """
    if isinstance(row, pd.Series):
        oc_info = row.to_dict()
    else:
        oc_info = row
    
    return format_product_display(oc_info)