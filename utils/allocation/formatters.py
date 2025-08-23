"""
Formatting utilities for Allocation module
Handles display formatting for UI
"""
import pandas as pd
from datetime import datetime, date
from typing import Union, Optional, Any
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


def format_currency(value: Union[int, float, None], currency: str = "USD", 
                   decimals: int = 2) -> str:
    """
    Format currency value
    
    Args:
        value: Amount to format
        currency: Currency code (USD, VND, etc.)
        decimals: Number of decimal places
        
    Returns:
        Formatted currency string
    """
    try:
        if value is None or pd.isna(value):
            return "-"
        
        # Currency symbols
        symbols = {
            "USD": "$",
            "VND": "₫",
            "EUR": "€",
            "GBP": "£"
        }
        
        symbol = symbols.get(currency, currency + " ")
        
        if currency == "VND":
            # VND typically doesn't use decimals
            return f"{symbol}{int(value):,}"
        else:
            return f"{symbol}{float(value):,.{decimals}f}"
            
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
            # Try to parse string date
            if value.strip() == "":
                return "-"
            # Try common formats
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.strftime(format_str)
                except ValueError:
                    continue
            return value  # Return as-is if can't parse
            
        elif isinstance(value, datetime):
            return value.strftime(format_str)
            
        elif isinstance(value, date):
            return value.strftime(format_str)
            
        else:
            return str(value)
            
    except Exception as e:
        logger.debug(f"Error formatting date {value}: {e}")
        return "-"


def format_datetime(value: Union[str, datetime, None], 
                   format_str: str = "%d/%m/%Y %H:%M") -> str:
    """
    Format datetime with time
    
    Args:
        value: Datetime value to format
        format_str: Output format string
        
    Returns:
        Formatted datetime string
    """
    try:
        if value is None or pd.isna(value):
            return "-"
        
        if isinstance(value, str):
            if value.strip() == "":
                return "-"
            # Try to parse
            dt = pd.to_datetime(value)
            return dt.strftime(format_str)
        elif isinstance(value, datetime):
            return value.strftime(format_str)
        else:
            return str(value)
            
    except Exception:
        return "-"


def format_status(status: str) -> str:
    """
    Format allocation status with emoji
    
    Args:
        status: Status string
        
    Returns:
        Status with emoji prefix
    """
    status_map = {
        'Not Allocated': '⚪ Not Allocated',
        'Partially Allocated': '🟡 Partially Allocated',
        'Fully Allocated': '🟢 Fully Allocated',
        'Over Allocated': '🔴 Over Allocated',
        'ALLOCATED': '✅ Allocated',
        'DRAFT': '📝 Draft',
        'CANCELLED': '❌ Cancelled'
    }
    
    return status_map.get(status, f"❓ {status}")


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


def format_quantity_with_uom(quantity: Union[int, float, None], 
                            uom: Optional[str] = None) -> str:
    """
    Format quantity with unit of measure
    
    Args:
        quantity: Quantity value
        uom: Unit of measure
        
    Returns:
        Formatted string with UOM
    """
    qty_str = format_number(quantity, 2 if quantity and quantity != int(quantity) else 0)
    
    if qty_str == "-":
        return "-"
    
    if uom:
        return f"{qty_str} {uom}"
    else:
        return qty_str


def format_allocation_mode(mode: str) -> str:
    """
    Format allocation mode with icon
    
    Args:
        mode: SOFT or HARD
        
    Returns:
        Formatted mode string
    """
    mode_map = {
        'SOFT': '🔄 SOFT',
        'HARD': '🔒 HARD'
    }
    
    return mode_map.get(mode, mode)


def format_etd_variance(oc_etd: Union[date, None], 
                       allocated_etd: Union[date, None]) -> str:
    """
    Format ETD variance between OC and allocation
    
    Args:
        oc_etd: Original OC ETD
        allocated_etd: Allocated ETD
        
    Returns:
        Formatted variance string
    """
    try:
        if not oc_etd or not allocated_etd or pd.isna(oc_etd) or pd.isna(allocated_etd):
            return "-"
        
        # Ensure date objects
        if isinstance(oc_etd, str):
            oc_etd = pd.to_datetime(oc_etd).date()
        if isinstance(allocated_etd, str):
            allocated_etd = pd.to_datetime(allocated_etd).date()
        
        # Calculate difference
        diff = (allocated_etd - oc_etd).days
        
        if diff == 0:
            return "✅ On time"
        elif diff < 0:
            return f"⚡ {abs(diff)} days earlier"
        else:
            return f"⚠️ {diff} days later"
            
    except Exception:
        return "-"


def format_days_ago(value: Union[int, float, None]) -> str:
    """
    Format days ago in human-readable format
    
    Args:
        value: Number of days
        
    Returns:
        Formatted string
    """
    try:
        if value is None or pd.isna(value):
            return "-"
        
        days = int(value)
        
        if days == 0:
            return "Today"
        elif days == 1:
            return "Yesterday"
        elif days < 7:
            return f"{days} days ago"
        elif days < 30:
            weeks = days // 7
            return f"{weeks} week{'s' if weeks > 1 else ''} ago"
        elif days < 365:
            months = days // 30
            return f"{months} month{'s' if months > 1 else ''} ago"
        else:
            years = days // 365
            return f"{years} year{'s' if years > 1 else ''} ago"
            
    except (ValueError, TypeError):
        return "-"


def format_file_size(size_bytes: Union[int, float, None]) -> str:
    """
    Format file size in human-readable format
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    try:
        if size_bytes is None or pd.isna(size_bytes):
            return "-"
        
        size = float(size_bytes)
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        
        return f"{size:.1f} PB"
        
    except (ValueError, TypeError):
        return "-"


def format_supply_source(source_type: Optional[str]) -> str:
    """
    Format supply source type with icon
    
    Args:
        source_type: Supply source type (can be None for SOFT allocation)
        
    Returns:
        Formatted source string
    """
    if source_type is None or source_type == 'No specific source':
        return '🔄 No specific source (SOFT)'
    
    source_map = {
        'INVENTORY': '📦 Inventory',
        'PENDING_CAN': '🚢 Pending CAN',
        'PENDING_PO': '📋 Pending PO',
        'PENDING_WHT': '🚚 WH Transfer'
    }
    
    return source_map.get(source_type, source_type)


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


def truncate_text(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """
    Truncate text to maximum length
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated text
    """
    if not text:
        return ""
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def format_list_display(items: list, max_items: int = 3, 
                       separator: str = ", ") -> str:
    """
    Format list for display with truncation
    
    Args:
        items: List of items
        max_items: Maximum items to show
        separator: Item separator
        
    Returns:
        Formatted list string
    """
    if not items:
        return "-"
    
    if len(items) <= max_items:
        return separator.join(str(item) for item in items)
    else:
        displayed = separator.join(str(item) for item in items[:max_items])
        remaining = len(items) - max_items
        return f"{displayed} (+{remaining} more)"


def get_status_color(status: str) -> str:
    """
    Get color for status display
    
    Args:
        status: Status string
        
    Returns:
        Color name for Streamlit
    """
    color_map = {
        'Not Allocated': 'gray',
        'Partially Allocated': 'orange',
        'Fully Allocated': 'green',
        'Over Allocated': 'red',
        'COMPLETED': 'green',
        'IN_PROCESS': 'blue',
        'PENDING': 'gray',
        'CANCELLED': 'red'
    }
    
    return color_map.get(status, 'gray')


def format_comparison(actual: Union[int, float], 
                     target: Union[int, float], 
                     format_type: str = "number") -> str:
    """
    Format comparison between actual and target
    
    Args:
        actual: Actual value
        target: Target value
        format_type: Type of formatting (number, currency, percentage)
        
    Returns:
        Formatted comparison string
    """
    try:
        if pd.isna(actual) or pd.isna(target):
            return "-"
        
        if format_type == "currency":
            actual_str = format_currency(actual)
            target_str = format_currency(target)
        elif format_type == "percentage":
            actual_str = format_percentage(actual)
            target_str = format_percentage(target)
        else:
            actual_str = format_number(actual)
            target_str = format_number(target)
        
        return f"{actual_str} / {target_str}"
        
    except Exception:
        return "-"