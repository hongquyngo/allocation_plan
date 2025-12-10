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
"""

from .bulk_data import BulkAllocationData
from .strategy_engine import StrategyEngine, AllocationStrategy
from .bulk_validator import BulkAllocationValidator
from .bulk_service import BulkAllocationService
from .bulk_email import BulkEmailService

__all__ = [
    'BulkAllocationData',
    'StrategyEngine',
    'AllocationStrategy',
    'BulkAllocationValidator',
    'BulkAllocationService',
    'BulkEmailService'
]
