"""
Allocation Management Formatters
==================================
Display utilities for allocation management UI.

This module is INDEPENDENT - no imports from allocation/ or bulk_allocation/
"""

from datetime import datetime, date
from typing import Dict, Any, Optional
import pandas as pd


class AllocationManagementFormatters:
    """Display formatters for allocation management"""
    
    # ================================================================
    # STATUS FORMATTERS
    # ================================================================
    
    @staticmethod
    def format_delivery_status(status: str) -> str:
        """Format delivery status with emoji"""
        status_map = {
            'PENDING': '🔵 Pending',
            'PARTIALLY_DELIVERED': '🟡 Partial',
            'FULLY_DELIVERED': '✅ Delivered',
            'PARTIALLY_CANCELLED': '🟠 Part. Cancelled',
            'FULLY_CANCELLED': '❌ Cancelled'
        }
        return status_map.get(status, status)
    
    @staticmethod
    def get_status_color(status: str) -> str:
        """Get color for status"""
        color_map = {
            'PENDING': '#0066cc',
            'PARTIALLY_DELIVERED': '#ffc107',
            'FULLY_DELIVERED': '#28a745',
            'PARTIALLY_CANCELLED': '#fd7e14',
            'FULLY_CANCELLED': '#dc3545'
        }
        return color_map.get(status, '#6c757d')
    
    @staticmethod
    def format_allocation_mode(mode: str) -> str:
        """Format allocation mode"""
        if mode == 'HARD':
            return '🔒 HARD'
        return '🔓 SOFT'
    
    @staticmethod
    def format_supply_source(source_type: str) -> str:
        """Format supply source type"""
        source_map = {
            'INVENTORY': '🏭 Inventory',
            'PENDING_CAN': '📋 Pending CAN',
            'PENDING_PO': '📄 Pending PO',
            'PENDING_WHT': '🚚 WH Transfer'
        }
        return source_map.get(source_type, source_type or 'N/A')
    
    # ================================================================
    # QUANTITY FORMATTERS
    # ================================================================
    
    @staticmethod
    def format_quantity(qty: float, decimals: int = 2) -> str:
        """Format quantity with thousand separator"""
        if qty is None:
            return '-'
        return f"{qty:,.{decimals}f}"
    
    @staticmethod
    def format_quantity_change(old_qty: float, new_qty: float) -> str:
        """Format quantity change with indicator"""
        change = new_qty - old_qty
        if change > 0:
            return f"📈 +{change:,.2f}"
        elif change < 0:
            return f"📉 {change:,.2f}"
        return "No change"
    
    @staticmethod
    def format_progress(delivered: float, allocated: float, cancelled: float = 0) -> str:
        """Format delivery progress"""
        effective = allocated - cancelled
        if effective <= 0:
            return "N/A"
        percent = (delivered / effective) * 100
        return f"{percent:.0f}%"
    
    @staticmethod
    def format_undelivered(allocation: Dict) -> str:
        """Format undelivered quantity"""
        allocated = float(allocation.get('allocated_qty', 0))
        delivered = float(allocation.get('delivered_qty', 0))
        cancelled = float(allocation.get('cancelled_qty', 0))
        undelivered = allocated - cancelled - delivered
        
        if undelivered <= 0:
            return "✓ Complete"
        return f"{undelivered:,.2f}"
    
    # ================================================================
    # DATE FORMATTERS
    # ================================================================
    
    @staticmethod
    def format_date(dt: Any, format_str: str = '%d %b %Y') -> str:
        """Format date for display"""
        if dt is None:
            return '-'
        if isinstance(dt, str):
            try:
                dt = datetime.strptime(dt, '%Y-%m-%d').date()
            except:
                return dt
        if isinstance(dt, datetime):
            dt = dt.date()
        return dt.strftime(format_str)
    
    @staticmethod
    def format_datetime(dt: Any, format_str: str = '%d %b %Y %H:%M') -> str:
        """Format datetime for display"""
        if dt is None:
            return '-'
        if isinstance(dt, str):
            try:
                dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
            except:
                return dt
        return dt.strftime(format_str)
    
    @staticmethod
    def format_etd_with_diff(allocated_etd: Any, original_etd: Any) -> str:
        """Format ETD with difference from original"""
        if allocated_etd is None:
            return '-'
        
        allocated_str = AllocationManagementFormatters.format_date(allocated_etd)
        
        if original_etd is None:
            return allocated_str
        
        # Calculate difference
        try:
            if isinstance(allocated_etd, str):
                allocated_etd = datetime.strptime(allocated_etd, '%Y-%m-%d').date()
            if isinstance(original_etd, str):
                original_etd = datetime.strptime(original_etd, '%Y-%m-%d').date()
            
            diff = (allocated_etd - original_etd).days
            
            if diff == 0:
                return allocated_str
            elif diff > 0:
                return f"{allocated_str} (+{diff}d)"
            else:
                return f"{allocated_str} ({diff}d)"
        except:
            return allocated_str
    
    @staticmethod
    def days_until(dt: Any) -> Optional[int]:
        """Calculate days until a date"""
        if dt is None:
            return None
        if isinstance(dt, str):
            try:
                dt = datetime.strptime(dt, '%Y-%m-%d').date()
            except:
                return None
        if isinstance(dt, datetime):
            dt = dt.date()
        return (dt - date.today()).days
    
    # ================================================================
    # TEXT FORMATTERS
    # ================================================================
    
    @staticmethod
    def truncate(text: str, max_length: int = 30) -> str:
        """Truncate text with ellipsis"""
        if text is None:
            return '-'
        text = str(text)
        if len(text) <= max_length:
            return text
        return text[:max_length-3] + '...'
    
    @staticmethod
    def format_product_display(code: str, name: str, max_length: int = 50) -> str:
        """Format product for display"""
        if code and name:
            full = f"{code} | {name}"
        elif code:
            full = code
        elif name:
            full = name
        else:
            return 'N/A'
        
        return AllocationManagementFormatters.truncate(full, max_length)
    
    @staticmethod
    def format_customer_display(code: str, name: str, max_length: int = 40) -> str:
        """Format customer for display"""
        if code and name:
            full = f"{code} - {name}"
        elif code:
            full = code
        elif name:
            full = name
        else:
            return 'N/A'
        
        return AllocationManagementFormatters.truncate(full, max_length)
    
    @staticmethod
    def format_reason_category(category: str) -> str:
        """Format reason category for display"""
        if not category:
            return 'Other'
        return category.replace('_', ' ').title()
    
    # ================================================================
    # DATAFRAME FORMATTERS
    # ================================================================
    
    @staticmethod
    def format_allocation_df(df: pd.DataFrame) -> pd.DataFrame:
        """Format allocation dataframe for display"""
        if df.empty:
            return df
        
        display_df = df.copy()
        
        # Format status
        if 'delivery_status' in display_df.columns:
            display_df['status_display'] = display_df['delivery_status'].apply(
                AllocationManagementFormatters.format_delivery_status
            )
        
        # Format quantities
        qty_cols = ['allocated_qty', 'delivered_qty', 'cancelled_qty', 
                    'effective_allocated_qty', 'undelivered_qty', 'requested_qty']
        for col in qty_cols:
            if col in display_df.columns:
                display_df[f'{col}_display'] = display_df[col].apply(
                    lambda x: AllocationManagementFormatters.format_quantity(x) if pd.notna(x) else '-'
                )
        
        # Format dates
        date_cols = ['allocation_date', 'allocated_etd', 'original_etd', 'created_date']
        for col in date_cols:
            if col in display_df.columns:
                display_df[f'{col}_display'] = display_df[col].apply(
                    AllocationManagementFormatters.format_date
                )
        
        # Format supply source
        if 'supply_source_type' in display_df.columns:
            display_df['supply_display'] = display_df['supply_source_type'].apply(
                AllocationManagementFormatters.format_supply_source
            )
        
        return display_df
    
    # ================================================================
    # SUMMARY FORMATTERS
    # ================================================================
    
    @staticmethod
    def format_allocation_summary(allocation: Dict) -> Dict[str, str]:
        """Create formatted summary dict for display"""
        return {
            'Allocation #': allocation.get('allocation_number', 'N/A'),
            'OC #': allocation.get('demand_number', 'N/A'),
            'Customer': AllocationManagementFormatters.format_customer_display(
                allocation.get('customer_code'),
                allocation.get('customer_name')
            ),
            'Product': AllocationManagementFormatters.format_product_display(
                allocation.get('product_code'),
                allocation.get('product_name')
            ),
            'Allocated Qty': AllocationManagementFormatters.format_quantity(
                allocation.get('effective_allocated_qty')
            ),
            'Delivered': AllocationManagementFormatters.format_quantity(
                allocation.get('delivered_qty')
            ),
            'Undelivered': AllocationManagementFormatters.format_quantity(
                allocation.get('undelivered_qty')
            ),
            'Status': AllocationManagementFormatters.format_delivery_status(
                allocation.get('delivery_status')
            ),
            'ETD': AllocationManagementFormatters.format_date(
                allocation.get('allocated_etd')
            ),
            'Source': AllocationManagementFormatters.format_supply_source(
                allocation.get('supply_source_type')
            )
        }
    
    # ================================================================
    # TOOLTIP HELPERS
    # ================================================================
    
    @staticmethod
    def get_status_tooltip(status: str) -> str:
        """Get tooltip text for status"""
        tooltips = {
            'PENDING': 'Allocation created but no delivery yet',
            'PARTIALLY_DELIVERED': 'Some quantity has been delivered',
            'FULLY_DELIVERED': 'All allocated quantity has been delivered',
            'PARTIALLY_CANCELLED': 'Part of allocation was cancelled',
            'FULLY_CANCELLED': 'Entire allocation was cancelled'
        }
        return tooltips.get(status, '')
    
    @staticmethod
    def get_action_tooltip(action: str) -> str:
        """Get tooltip for action buttons"""
        tooltips = {
            'update_qty': 'Change the allocated quantity. Cannot reduce below delivered amount.',
            'update_etd': 'Change the expected delivery date.',
            'cancel': 'Cancel part or all of the undelivered allocation.',
            'reverse': 'Reverse a delivery that was already made (e.g., customer return).'
        }
        return tooltips.get(action, '')
