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
- bulk_formatters: Formatting utilities (including format_product_display)
- bulk_tooltips: UI tooltip definitions

CHANGELOG:
- 2024-12: Added format_product_display to bulk_formatters exports
"""

from .bulk_data import BulkAllocationData
from .strategy_engine import StrategyEngine, AllocationStrategy
from .bulk_validator import BulkAllocationValidator
from .bulk_service import BulkAllocationService
from .bulk_email import BulkEmailService
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
    format_product_display,            # NEW
    format_product_display_short,      # NEW
    build_product_display_from_row,    # NEW
    format_customer_display,           # NEW - for customer code + name
    format_customer_display_from_dict  # NEW - convenience wrapper
)
from .bulk_tooltips import (
    SCOPE_TOOLTIPS,
    STRATEGY_TOOLTIPS,
    REVIEW_TOOLTIPS,
    FORMULA_TOOLTIPS,
    STATUS_TOOLTIPS,
    get_tooltip,
    get_all_tooltips
)

__all__ = [
    # Services
    'BulkAllocationData',
    'StrategyEngine',
    'AllocationStrategy',
    'BulkAllocationValidator',
    'BulkAllocationService',
    'BulkEmailService',
    
    # Formatters
    'format_number',
    'format_percentage',
    'format_date',
    'format_datetime',
    'format_currency',
    'format_quantity_with_uom',
    'format_coverage_badge',
    'format_strategy_name',
    'format_allocation_mode',
    'format_etd_urgency',
    'format_scope_summary',
    'format_diff',
    'truncate_text',
    'format_list_summary',
    'format_product_display',            # NEW
    'format_product_display_short',      # NEW
    'build_product_display_from_row',    # NEW
    'format_customer_display',           # NEW
    'format_customer_display_from_dict', # NEW
    
    # Tooltips
    'SCOPE_TOOLTIPS',
    'STRATEGY_TOOLTIPS',
    'REVIEW_TOOLTIPS',
    'FORMULA_TOOLTIPS',
    'STATUS_TOOLTIPS',
    'get_tooltip',
    'get_all_tooltips'
]