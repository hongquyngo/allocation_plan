"""
Formatting utilities for Allocation module - Cleaned Version
Only formatters used in the UI
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