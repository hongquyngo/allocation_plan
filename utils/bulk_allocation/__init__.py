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
- bulk_tooltips: UI tooltip definitions (NEW)
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
    'get_all_tooltips'
]