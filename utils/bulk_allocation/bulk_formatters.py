"""
Bulk Allocation Formatters
==========================
Formatting utilities for displaying data in bulk allocation UI.

REFACTORED: 2024-12 - Added format_product_display, format_customer_display
"""
import pandas as pd
from typing import Dict, Any, Optional, Union
from datetime import datetime, date
from decimal import Decimal


# ==================== Number Formatting ====================

def format_number(value: Any, decimal_places: int = 0, prefix: str = '', suffix: str = '') -> str:
    """
    Format number with thousand separators.
    
    Args:
        value: Number to format
        decimal_places: Number of decimal places
        prefix: Optional prefix (e.g., '$')
        suffix: Optional suffix (e.g., 'kg')
    
    Returns:
        Formatted string like "1,234,567" or "$1,234.56"
    """
    if value is None:
        return '-'
    
    try:
        num = float(value)
        if decimal_places == 0:
            formatted = f"{num:,.0f}"
        else:
            formatted = f"{num:,.{decimal_places}f}"
        return f"{prefix}{formatted}{suffix}"
    except (ValueError, TypeError):
        return str(value)


def format_percentage(value: Any, decimal_places: int = 1) -> str:
    """
    Format number as percentage.
    
    Args:
        value: Number to format (already as percentage, not decimal)
        decimal_places: Number of decimal places
    
    Returns:
        Formatted string like "85.5%"
    """
    if value is None:
        return '-'
    
    try:
        num = float(value)
        return f"{num:.{decimal_places}f}%"
    except (ValueError, TypeError):
        return str(value)


def format_currency(value: Any, currency: str = 'USD', decimal_places: int = 2) -> str:
    """
    Format number as currency.
    
    Args:
        value: Number to format
        currency: Currency code
        decimal_places: Number of decimal places
    
    Returns:
        Formatted string like "USD 1,234.56"
    """
    if value is None:
        return '-'
    
    try:
        num = float(value)
        formatted = f"{num:,.{decimal_places}f}"
        return f"{currency} {formatted}"
    except (ValueError, TypeError):
        return str(value)


def format_quantity_with_uom(qty: Any, uom: str = '') -> str:
    """
    Format quantity with unit of measure.
    
    Args:
        qty: Quantity value
        uom: Unit of measure
    
    Returns:
        Formatted string like "1,234 pcs"
    """
    if qty is None:
        return '-'
    
    try:
        num = float(qty)
        formatted = f"{num:,.0f}"
        if uom:
            return f"{formatted} {uom}"
        return formatted
    except (ValueError, TypeError):
        return str(qty)


# ==================== Date Formatting ====================

def format_date(value: Any, format_str: str = '%Y-%m-%d') -> str:
    """
    Format date value.
    
    Args:
        value: Date to format (datetime, date, or string)
        format_str: strftime format string
    
    Returns:
        Formatted date string
    """
    if value is None:
        return '-'
    
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return value
    
    if isinstance(value, datetime):
        return value.strftime(format_str)
    elif isinstance(value, date):
        return value.strftime(format_str)
    
    return str(value)


def format_datetime(value: Any, format_str: str = '%Y-%m-%d %H:%M') -> str:
    """
    Format datetime value.
    
    Args:
        value: Datetime to format
        format_str: strftime format string
    
    Returns:
        Formatted datetime string
    """
    return format_date(value, format_str)


# ==================== Status Formatting ====================

def format_coverage_badge(coverage_pct: float) -> str:
    """
    Format coverage percentage as colored badge.
    
    Args:
        coverage_pct: Coverage percentage (0-100+)
    
    Returns:
        HTML badge string
    """
    if coverage_pct >= 100:
        color = '#22c55e'  # Green
        label = 'Full'
    elif coverage_pct >= 70:
        color = '#f59e0b'  # Amber
        label = 'Partial'
    elif coverage_pct > 0:
        color = '#ef4444'  # Red
        label = 'Low'
    else:
        color = '#6b7280'  # Gray
        label = 'None'
    
    return f'<span style="background:{color}; color:white; padding:2px 8px; border-radius:4px; font-size:11px;">{label} ({coverage_pct:.0f}%)</span>'


def format_etd_urgency(etd: Any, threshold_days: int = 7) -> str:
    """
    Format ETD with urgency indicator.
    
    Args:
        etd: ETD date
        threshold_days: Days threshold for urgent status
    
    Returns:
        Formatted string with urgency indicator
    """
    if etd is None:
        return '-'
    
    if isinstance(etd, str):
        try:
            etd = datetime.fromisoformat(etd.replace('Z', '+00:00')).date()
        except ValueError:
            return etd
    
    if isinstance(etd, datetime):
        etd = etd.date()
    
    today = date.today()
    days_until = (etd - today).days
    
    date_str = etd.strftime('%Y-%m-%d')
    
    if days_until < 0:
        return f"🔴 {date_str} (Overdue)"
    elif days_until <= threshold_days:
        return f"🟡 {date_str} ({days_until}d)"
    else:
        return f"🟢 {date_str}"


# ==================== Strategy Formatting ====================

def format_strategy_name(strategy_type: str) -> str:
    """
    Format strategy type to display name.
    
    Args:
        strategy_type: Strategy type code (e.g., 'FCFS', 'ETD_PRIORITY')
    
    Returns:
        Formatted display name
    """
    names = {
        'FCFS': 'First Come First Served',
        'ETD_PRIORITY': 'ETD Priority',
        'PROPORTIONAL': 'Proportional',
        'REVENUE_PRIORITY': 'Revenue Priority',
        'HYBRID': 'Hybrid (Multi-Phase)'
    }
    return names.get(strategy_type, strategy_type)


def format_allocation_mode(mode: str) -> str:
    """
    Format allocation mode to display name.
    
    Args:
        mode: Mode code ('SOFT' or 'HARD')
    
    Returns:
        Formatted display name with description
    """
    modes = {
        'SOFT': 'Soft (Allow partial)',
        'HARD': 'Hard (All or nothing)'
    }
    return modes.get(mode, mode)


# ==================== Scope Formatting ====================

# Allocation status filter labels for display
ALLOCATION_STATUS_LABELS = {
    'ALL_NEEDING': 'All needing',
    'ONLY_UNALLOCATED': 'Unallocated only',
    'ONLY_PARTIAL': 'Partial only',
    'INCLUDE_ALL': 'Include all',
    'OVER_ALLOCATED': '⚠️ Over-allocated'
}

URGENCY_LABELS = {
    'ALL_ETD': '',
    'URGENT_ONLY': 'Urgent',
    'OVERDUE_ONLY': 'Overdue',
    'URGENT_AND_OVERDUE': 'Urgent+Overdue'
}

def format_scope_summary(scope: Dict) -> str:
    """
    Format scope dictionary as summary string.
    
    Args:
        scope: Scope dictionary with filters
    
    Returns:
        Summary string like "2 Brands, 3 Customers, ETD: 2024-01-01 to 2024-01-31 | Unallocated only"
    """
    parts = []
    
    if scope.get('brand_ids'):
        count = len(scope['brand_ids'])
        parts.append(f"{count} Brand{'s' if count > 1 else ''}")
    
    if scope.get('customer_codes'):
        count = len(scope['customer_codes'])
        parts.append(f"{count} Customer{'s' if count > 1 else ''}")
    
    if scope.get('legal_entities'):
        count = len(scope['legal_entities'])
        parts.append(f"{count} Legal Entity" if count == 1 else f"{count} Legal Entities")
    
    if scope.get('etd_from') or scope.get('etd_to'):
        etd_from = format_date(scope.get('etd_from')) if scope.get('etd_from') else 'Any'
        etd_to = format_date(scope.get('etd_to')) if scope.get('etd_to') else 'Any'
        parts.append(f"ETD: {etd_from} → {etd_to}")
    
    # Allocation status filter
    status_filter = scope.get('allocation_status_filter', 'ALL_NEEDING')
    status_label = ALLOCATION_STATUS_LABELS.get(status_filter, status_filter)
    
    # Urgency filter
    urgency_filter = scope.get('urgency_filter', 'ALL_ETD')
    urgency_label = URGENCY_LABELS.get(urgency_filter, '')
    
    # Build filter tag
    filter_parts = [status_label]
    if urgency_label:
        filter_parts.append(urgency_label)
    if scope.get('low_coverage_only'):
        filter_parts.append(f"<{scope.get('low_coverage_threshold', 50)}%")
    if scope.get('stock_available_only'):
        filter_parts.append('Stock')
    if scope.get('high_value_only'):
        filter_parts.append(f"≥${scope.get('high_value_threshold', 10000):,.0f}")
    
    parts.append(f"[{' | '.join(filter_parts)}]")
    
    if not parts:
        return "All (No filters)"
    
    return " | ".join(parts)


# ==================== Diff Formatting ====================

def format_diff(original: Any, adjusted: Any, show_arrow: bool = True) -> str:
    """
    Format difference between original and adjusted values.
    
    Args:
        original: Original value
        adjusted: Adjusted value
        show_arrow: Whether to show arrow indicator
    
    Returns:
        Formatted diff string like "100 → 150 (+50)"
    """
    if original is None or adjusted is None:
        return '-'
    
    try:
        orig = float(original)
        adj = float(adjusted)
        diff = adj - orig
        
        if diff == 0:
            return format_number(orig)
        
        diff_str = f"+{format_number(diff)}" if diff > 0 else format_number(diff)
        
        if show_arrow:
            return f"{format_number(orig)} → {format_number(adj)} ({diff_str})"
        else:
            return f"{format_number(adj)} ({diff_str})"
    except (ValueError, TypeError):
        return f"{original} → {adjusted}"


# ==================== Text Formatting ====================

def truncate_text(text: str, max_length: int = 50, suffix: str = '...') -> str:
    """
    Truncate text to max length with suffix.
    
    Args:
        text: Text to truncate
        max_length: Maximum length including suffix
        suffix: Suffix to add when truncated
    
    Returns:
        Truncated text
    """
    if not text:
        return ''
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def format_list_summary(items: list, max_items: int = 3, separator: str = ', ') -> str:
    """
    Format list as summary with overflow.
    
    Args:
        items: List of items
        max_items: Max items to show before overflow
        separator: Separator between items
    
    Returns:
        Summary string like "A, B, C, +2 more"
    """
    if not items:
        return '-'
    
    if len(items) <= max_items:
        return separator.join(str(item) for item in items)
    
    shown = separator.join(str(item) for item in items[:max_items])
    overflow = len(items) - max_items
    return f"{shown}, +{overflow} more"


# ==================== NEW: Product Display Formatting ====================

def format_product_display(
    oc_info: Dict,
    include_brand: bool = True,
    max_length: Optional[int] = None
) -> str:
    """
    Build consistent product display string from OC info.
    
    Format: "PT Code | Product Name | Package Size (Brand)"
    
    This function consolidates the duplicated product display building
    logic that was previously scattered across multiple locations.
    
    Args:
        oc_info: Dict with keys: product_display, pt_code, product_name, 
                 package_size, brand_name
        include_brand: Whether to append brand in parentheses
        max_length: Optional truncation length
    
    Returns:
        Formatted string like "P025000563 | 3M Tape | 19mmx33m (Vietape)"
    
    Examples:
        >>> format_product_display({
        ...     'pt_code': 'P025000563',
        ...     'product_name': '3M Glass Cloth Tape',
        ...     'package_size': '19mmx33m',
        ...     'brand_name': 'Vietape'
        ... })
        'P025000563 | 3M Glass Cloth Tape | 19mmx33m (Vietape)'
        
        >>> format_product_display({'pt_code': 'P001'})
        'P001'
    """
    # Use pre-formatted if exists and not empty
    if display := oc_info.get('product_display'):
        if max_length and len(display) > max_length:
            return truncate_text(display, max_length)
        return display
    
    # Build from components
    parts = []
    
    if pt_code := oc_info.get('pt_code', ''):
        parts.append(pt_code)
    
    if product_name := oc_info.get('product_name', ''):
        parts.append(product_name)
    
    if package_size := oc_info.get('package_size', ''):
        parts.append(package_size)
    
    result = ' | '.join(filter(None, parts))
    
    # Append brand if requested and available
    if include_brand:
        if brand := oc_info.get('brand_name', ''):
            result += f" ({brand})"
    
    # Apply max_length if specified
    if max_length and len(result) > max_length:
        result = truncate_text(result, max_length)
    
    return result or "Unknown Product"


def format_product_display_short(oc_info: Dict, max_length: int = 50) -> str:
    """
    Format product display for constrained spaces (tables, dropdowns).
    
    Args:
        oc_info: Dict with product info
        max_length: Maximum length (default 50)
    
    Returns:
        Truncated product display string
    """
    return format_product_display(oc_info, include_brand=False, max_length=max_length)


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


# ==================== NEW: Customer Display Formatting ====================

def format_customer_display(
    customer_code: str,
    customer_name: str = None,
    max_name_length: int = 40
) -> str:
    """
    Format customer display string combining code and name.
    
    Format: "Code - Customer Name" or just "Code" if name not available.
    
    This function provides consistent customer display formatting
    across all UI components.
    
    Args:
        customer_code: Customer code (e.g., "C000587")
        customer_name: Customer name (optional)
        max_name_length: Max length for customer name before truncation
    
    Returns:
        Formatted string
    
    Examples:
        >>> format_customer_display("C000587", "Samsung Electronics Vietnam")
        'C000587 - Samsung Electronics Vietnam'
        
        >>> format_customer_display("C000587", None)
        'C000587'
        
        >>> format_customer_display("C000587", "Very Long Customer Name That Exceeds Limit", 20)
        'C000587 - Very Long Customer ...'
    """
    if not customer_code:
        return customer_name or ""
    
    if not customer_name:
        return customer_code
    
    # Truncate long names
    if len(customer_name) > max_name_length:
        customer_name = customer_name[:max_name_length-3] + "..."
    
    return f"{customer_code} - {customer_name}"


def format_customer_display_from_dict(oc_info: Dict, max_name_length: int = 40) -> str:
    """
    Format customer display from OC info dictionary.
    
    Looks for customer code and name in common field names.
    
    Args:
        oc_info: Dictionary containing customer information
        max_name_length: Max length for customer name
    
    Returns:
        Formatted customer display string
    
    Examples:
        >>> format_customer_display_from_dict({
        ...     'customer_code': 'C000587',
        ...     'customer': 'Samsung Electronics'
        ... })
        'C000587 - Samsung Electronics'
    """
    customer_code = oc_info.get('customer_code', '')
    
    # Try multiple possible field names for customer name
    customer_name = (
        oc_info.get('customer') or 
        oc_info.get('customer_name') or 
        oc_info.get('customer_english_name') or
        ''
    )
    
    return format_customer_display(customer_code, customer_name, max_name_length)


# ==================== Allocation Status Formatting ====================

def format_allocation_status(status: str) -> str:
    """
    Format allocation status with icon.
    
    Args:
        status: Status code
    
    Returns:
        Formatted status with icon
    """
    status_map = {
        'NOT_ALLOCATED': '🔴 Not Allocated',
        'PARTIALLY_ALLOCATED': '🟡 Partial',
        'FULLY_ALLOCATED': '🟢 Full',
        'OVER_ALLOCATED': '🔵 Over'
    }
    return status_map.get(status, status)


def format_allocation_status_badge(status: str) -> str:
    """
    Format allocation status as HTML badge.
    
    Args:
        status: Status code
    
    Returns:
        HTML badge string
    """
    colors = {
        'NOT_ALLOCATED': '#ef4444',
        'PARTIALLY_ALLOCATED': '#f59e0b',
        'FULLY_ALLOCATED': '#22c55e',
        'OVER_ALLOCATED': '#3b82f6'
    }
    labels = {
        'NOT_ALLOCATED': 'Not Allocated',
        'PARTIALLY_ALLOCATED': 'Partial',
        'FULLY_ALLOCATED': 'Full',
        'OVER_ALLOCATED': 'Over'
    }
    
    color = colors.get(status, '#6b7280')
    label = labels.get(status, status)
    
    return f'<span style="background:{color}; color:white; padding:2px 8px; border-radius:4px; font-size:11px;">{label}</span>'