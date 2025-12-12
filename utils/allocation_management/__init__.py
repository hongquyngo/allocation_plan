"""
Allocation Management Module
=============================
Centralized management for existing allocations.

Features:
- Search & View allocations
- Update allocated quantity
- Update ETD
- Cancel allocations (full/partial)
- Reverse deliveries
- Bulk operations
- Audit trail

This module is INDEPENDENT and does not import from:
- utils/allocation/
- utils/bulk_allocation/

Author: Prostech
Created: 2024-12
"""

from .mgmt_data import AllocationManagementData
from .mgmt_service import AllocationManagementService
from .mgmt_validators import AllocationManagementValidator
from .mgmt_email import AllocationManagementEmail
from .mgmt_supply import AllocationSupplyData
from .mgmt_formatters import AllocationManagementFormatters

__all__ = [
    'AllocationManagementData',
    'AllocationManagementService', 
    'AllocationManagementValidator',
    'AllocationManagementEmail',
    'AllocationSupplyData',
    'AllocationManagementFormatters',
]

__version__ = '1.0.0'
