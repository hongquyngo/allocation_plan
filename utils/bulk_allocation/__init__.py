"""
Bulk Allocation Module
======================
Complete bulk allocation system with strategy-based allocation assistance.

Components:
- bulk_data: Data queries for scope selection and supply/demand
- strategy_engine: Allocation algorithms (FCFS, Proportional, ETD Priority, Hybrid)
- bulk_validator: Validation rules for bulk allocation
- bulk_service: Business logic and database operations
- bulk_email: Email notification service
- bulk_formatters: Formatting utilities
- bulk_tooltips: UI tooltip definitions

REFACTORED: 2024-12 - Added formatter exports for product/customer display
"""

from .bulk_data import BulkAllocationData
from .strategy_engine import StrategyEngine, AllocationStrategy
from .bulk_validator import BulkAllocationValidator
from .bulk_service import BulkAllocationService
from .bulk_email import BulkEmailService
from .bulk_tooltips import (
    SCOPE_TOOLTIPS,
    STRATEGY_TOOLTIPS,
    REVIEW_TOOLTIPS,
    FORMULA_TOOLTIPS,
    STATUS_TOOLTIPS,
    get_tooltip,
    get_all_tooltips
)

# NEW: Import formatter functions for convenience
from .bulk_formatters import (
    format_number,
    format_percentage,
    format_date,
    format_datetime,
    format_currency,
    format_quantity_with_uom,
    format_coverage_badge,
    format_strategy_name,
    format_allocation_mode,
    format_etd_urgency,
    format_scope_summary,
    format_diff,
    truncate_text,
    format_list_summary,
    # NEW: Product and Customer display formatters
    format_product_display,
    format_product_display_short,
    build_product_display_from_row,
    format_customer_display,
    format_customer_display_from_dict,
    format_allocation_status,
    format_allocation_status_badge
)

__all__ = [
    # Services
    'BulkAllocationData',
    'StrategyEngine',
    'AllocationStrategy',
    'BulkAllocationValidator',
    'BulkAllocationService',
    'BulkEmailService',
    
    # Tooltips
    'SCOPE_TOOLTIPS',
    'STRATEGY_TOOLTIPS',
    'REVIEW_TOOLTIPS',
    'FORMULA_TOOLTIPS',
    'STATUS_TOOLTIPS',
    'get_tooltip',
    'get_all_tooltips',
    
    # Formatters - Numbers
    'format_number',
    'format_percentage',
    'format_currency',
    'format_quantity_with_uom',
    
    # Formatters - Dates
    'format_date',
    'format_datetime',
    
    # Formatters - Status/Strategy
    'format_coverage_badge',
    'format_strategy_name',
    'format_allocation_mode',
    'format_etd_urgency',
    'format_allocation_status',
    'format_allocation_status_badge',
    
    # Formatters - Scope
    'format_scope_summary',
    
    # Formatters - Text/Diff
    'format_diff',
    'truncate_text',
    'format_list_summary',
    
    # NEW: Formatters - Product Display
    'format_product_display',
    'format_product_display_short',
    'build_product_display_from_row',
    
    # NEW: Formatters - Customer Display
    'format_customer_display',
    'format_customer_display_from_dict'
]