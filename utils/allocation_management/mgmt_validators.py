"""
Allocation Management Validators
=================================
Validation rules for allocation management operations.

This module is INDEPENDENT - no imports from allocation/ or bulk_allocation/
"""

import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from datetime import date, datetime

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of validation check"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def add_error(self, message: str):
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str):
        self.warnings.append(message)
    
    def merge(self, other: 'ValidationResult'):
        """Merge another validation result into this one"""
        if not other.is_valid:
            self.is_valid = False
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)


class AllocationManagementValidator:
    """Validator for allocation management operations"""
    
    # ================================================================
    # UPDATE QUANTITY VALIDATION
    # ================================================================
    
    def validate_quantity_update(
        self,
        allocation: Dict,
        new_qty: float,
        supply_available: float = None
    ) -> ValidationResult:
        """
        Validate quantity update request.
        
        Rules:
        1. new_qty must be >= delivered_qty (cannot reduce below delivered)
        2. new_qty must be <= requested_qty (cannot exceed original demand)
        3. If increasing, supply must be available
        4. new_qty must be > 0
        
        Args:
            allocation: Current allocation data (from view or raw)
            new_qty: New quantity to set
            supply_available: Available supply (for increase validation)
        """
        result = ValidationResult(is_valid=True)
        
        if allocation is None:
            result.add_error("Allocation not found")
            return result
        
        # Extract quantities
        allocated_qty = float(allocation.get('allocated_qty', 0))
        delivered_qty = float(allocation.get('delivered_qty', 0))
        cancelled_qty = float(allocation.get('cancelled_qty', 0))
        requested_qty = float(allocation.get('requested_qty', 0))
        
        # Effective current = allocated - cancelled
        effective_current = allocated_qty - cancelled_qty
        
        # Rule 1: Cannot be negative or zero
        if new_qty <= 0:
            result.add_error("New quantity must be greater than 0")
        
        # Rule 2: Cannot reduce below delivered
        if new_qty < delivered_qty:
            result.add_error(
                f"Cannot reduce quantity below delivered amount. "
                f"Delivered: {delivered_qty:,.2f}, New qty: {new_qty:,.2f}"
            )
        
        # Rule 3: Cannot exceed original demand
        if new_qty > requested_qty:
            result.add_error(
                f"Cannot exceed original demand. "
                f"Requested: {requested_qty:,.2f}, New qty: {new_qty:,.2f}"
            )
        
        # Rule 4: If increasing, check supply availability
        increase_amount = new_qty - effective_current
        if increase_amount > 0 and supply_available is not None:
            if increase_amount > supply_available:
                result.add_error(
                    f"Insufficient supply available. "
                    f"Need: {increase_amount:,.2f}, Available: {supply_available:,.2f}"
                )
            elif increase_amount > supply_available * 0.9:
                result.add_warning(
                    f"This will use most of available supply. "
                    f"Need: {increase_amount:,.2f}, Available: {supply_available:,.2f}"
                )
        
        # Warning if significant change
        if effective_current > 0:
            change_percent = abs(new_qty - effective_current) / effective_current * 100
            if change_percent > 50:
                result.add_warning(
                    f"Large quantity change: {change_percent:.0f}% "
                    f"({effective_current:,.2f} → {new_qty:,.2f})"
                )
        
        return result
    
    # ================================================================
    # UPDATE ETD VALIDATION
    # ================================================================
    
    def validate_etd_update(
        self,
        allocation: Dict,
        new_etd: date
    ) -> ValidationResult:
        """
        Validate ETD update request.
        
        Rules:
        1. new_etd cannot be in the past (with 1-day tolerance)
        2. Warning if ETD is very far in future (>6 months)
        3. Warning if this is multiple ETD updates
        
        Args:
            allocation: Current allocation data
            new_etd: New ETD date
        """
        result = ValidationResult(is_valid=True)
        
        if allocation is None:
            result.add_error("Allocation not found")
            return result
        
        if new_etd is None:
            result.add_error("New ETD is required")
            return result
        
        # Convert to date if datetime
        if isinstance(new_etd, datetime):
            new_etd = new_etd.date()
        
        today = date.today()
        
        # Rule 1: Cannot be in the past (with 1-day tolerance for timezone issues)
        from datetime import timedelta
        if new_etd < today - timedelta(days=1):
            result.add_error(
                f"ETD cannot be in the past. Today: {today}, New ETD: {new_etd}"
            )
        
        # Rule 2: Warning if very far in future
        days_ahead = (new_etd - today).days
        if days_ahead > 180:
            result.add_warning(
                f"ETD is {days_ahead} days in the future (over 6 months)"
            )
        
        # Rule 3: Warning if multiple updates
        etd_update_count = int(allocation.get('etd_update_count', 0))
        if etd_update_count >= 2:
            result.add_warning(
                f"This allocation has been updated {etd_update_count} times already"
            )
        
        # Info: Compare with original ETD
        original_etd = allocation.get('original_etd')
        if original_etd:
            if isinstance(original_etd, str):
                original_etd = datetime.strptime(original_etd, '%Y-%m-%d').date()
            elif isinstance(original_etd, datetime):
                original_etd = original_etd.date()
            
            days_diff = (new_etd - original_etd).days
            if days_diff > 30:
                result.add_warning(
                    f"New ETD is {days_diff} days later than original ETD"
                )
            elif days_diff < -7:
                result.add_warning(
                    f"New ETD is {abs(days_diff)} days earlier than original ETD"
                )
        
        return result
    
    # ================================================================
    # CANCEL VALIDATION
    # ================================================================
    
    def validate_cancel(
        self,
        allocation: Dict,
        cancel_qty: float,
        reason: str
    ) -> ValidationResult:
        """
        Validate cancellation request.
        
        Rules:
        1. cancel_qty must be > 0
        2. cancel_qty cannot exceed undelivered quantity
        3. Reason is required
        4. Warning if cancelling large portion
        
        Args:
            allocation: Current allocation data
            cancel_qty: Quantity to cancel
            reason: Cancellation reason
        """
        result = ValidationResult(is_valid=True)
        
        if allocation is None:
            result.add_error("Allocation not found")
            return result
        
        # Extract quantities
        allocated_qty = float(allocation.get('allocated_qty', 0))
        delivered_qty = float(allocation.get('delivered_qty', 0))
        cancelled_qty = float(allocation.get('cancelled_qty', 0))
        
        # Undelivered = allocated - cancelled - delivered
        undelivered = allocated_qty - cancelled_qty - delivered_qty
        
        # Rule 1: cancel_qty must be positive
        if cancel_qty <= 0:
            result.add_error("Cancel quantity must be greater than 0")
        
        # Rule 2: Cannot cancel more than undelivered
        if cancel_qty > undelivered:
            result.add_error(
                f"Cannot cancel more than undelivered quantity. "
                f"Undelivered: {undelivered:,.2f}, Cancel qty: {cancel_qty:,.2f}"
            )
        
        # Rule 3: Reason is required
        if not reason or not reason.strip():
            result.add_error("Cancellation reason is required")
        elif len(reason.strip()) < 10:
            result.add_warning("Please provide a more detailed reason")
        
        # Rule 4: Warning if cancelling large portion
        if undelivered > 0:
            cancel_percent = cancel_qty / undelivered * 100
            if cancel_percent >= 100:
                result.add_warning(
                    "This will cancel the entire remaining allocation"
                )
            elif cancel_percent >= 50:
                result.add_warning(
                    f"This will cancel {cancel_percent:.0f}% of remaining allocation"
                )
        
        # Check delivery status
        delivery_status = allocation.get('delivery_status', '')
        if delivery_status == 'FULLY_DELIVERED':
            result.add_error("Cannot cancel a fully delivered allocation")
        elif delivery_status in ('FULLY_CANCELLED', 'PARTIALLY_CANCELLED'):
            if cancel_qty > undelivered:
                result.add_error("Allocation already partially/fully cancelled")
        
        return result
    
    # ================================================================
    # REVERSE VALIDATION
    # ================================================================
    
    def validate_reverse(
        self,
        allocation: Dict,
        delivery_link: Dict,
        reverse_qty: float,
        reason: str
    ) -> ValidationResult:
        """
        Validate reversal request.
        
        Rules:
        1. reverse_qty must be > 0
        2. reverse_qty cannot exceed delivered quantity for this link
        3. Reason is required
        4. Warning for large reversals
        
        Args:
            allocation: Current allocation data
            delivery_link: Delivery link data
            reverse_qty: Quantity to reverse
            reason: Reversal reason
        """
        result = ValidationResult(is_valid=True)
        
        if allocation is None:
            result.add_error("Allocation not found")
            return result
        
        if delivery_link is None:
            result.add_error("Delivery link not found")
            return result
        
        delivered_qty = float(delivery_link.get('delivered_qty', 0))
        
        # Rule 1: reverse_qty must be positive
        if reverse_qty <= 0:
            result.add_error("Reverse quantity must be greater than 0")
        
        # Rule 2: Cannot reverse more than delivered
        if reverse_qty > delivered_qty:
            result.add_error(
                f"Cannot reverse more than delivered quantity. "
                f"Delivered: {delivered_qty:,.2f}, Reverse qty: {reverse_qty:,.2f}"
            )
        
        # Rule 3: Reason is required
        if not reason or not reason.strip():
            result.add_error("Reversal reason is required")
        elif len(reason.strip()) < 10:
            result.add_warning("Please provide a more detailed reason")
        
        # Rule 4: Warning for large reversals
        if delivered_qty > 0:
            reverse_percent = reverse_qty / delivered_qty * 100
            if reverse_percent >= 100:
                result.add_warning(
                    "This will reverse the entire delivery"
                )
            elif reverse_percent >= 50:
                result.add_warning(
                    f"This will reverse {reverse_percent:.0f}% of this delivery"
                )
        
        return result
    
    # ================================================================
    # BULK OPERATIONS VALIDATION
    # ================================================================
    
    def validate_bulk_etd_update(
        self,
        allocations: List[Dict],
        new_etd: date
    ) -> ValidationResult:
        """
        Validate bulk ETD update.
        
        Rules:
        1. All individual ETD validations must pass
        2. Warning if updating different products
        """
        result = ValidationResult(is_valid=True)
        
        if not allocations:
            result.add_error("No allocations selected")
            return result
        
        # Check each allocation
        for alloc in allocations:
            individual_result = self.validate_etd_update(alloc, new_etd)
            if not individual_result.is_valid:
                alloc_id = alloc.get('allocation_detail_id', 'Unknown')
                for error in individual_result.errors:
                    result.add_error(f"Allocation {alloc_id}: {error}")
        
        # Warning for mixed products
        products = set(alloc.get('product_id') for alloc in allocations)
        if len(products) > 1:
            result.add_warning(
                f"Updating ETD for {len(products)} different products"
            )
        
        return result
    
    def validate_bulk_cancel(
        self,
        allocations: List[Dict],
        reason: str
    ) -> ValidationResult:
        """
        Validate bulk cancellation (full cancel for each).
        
        Rules:
        1. Reason is required
        2. All allocations must have something to cancel
        """
        result = ValidationResult(is_valid=True)
        
        if not allocations:
            result.add_error("No allocations selected")
            return result
        
        # Reason check
        if not reason or not reason.strip():
            result.add_error("Cancellation reason is required")
        
        # Check each allocation has something to cancel
        cancellable_count = 0
        for alloc in allocations:
            allocated_qty = float(alloc.get('allocated_qty', 0))
            delivered_qty = float(alloc.get('delivered_qty', 0))
            cancelled_qty = float(alloc.get('cancelled_qty', 0))
            undelivered = allocated_qty - cancelled_qty - delivered_qty
            
            if undelivered > 0:
                cancellable_count += 1
            else:
                alloc_id = alloc.get('allocation_detail_id', 'Unknown')
                result.add_warning(
                    f"Allocation {alloc_id} has nothing to cancel "
                    f"(fully delivered or cancelled)"
                )
        
        if cancellable_count == 0:
            result.add_error("No allocations have remaining quantity to cancel")
        
        return result
    
    # ================================================================
    # HELPER METHODS
    # ================================================================
    
    def get_cancellable_qty(self, allocation: Dict) -> float:
        """Calculate maximum quantity that can be cancelled"""
        allocated_qty = float(allocation.get('allocated_qty', 0))
        delivered_qty = float(allocation.get('delivered_qty', 0))
        cancelled_qty = float(allocation.get('cancelled_qty', 0))
        return max(0, allocated_qty - cancelled_qty - delivered_qty)
    
    def get_quantity_limits(self, allocation: Dict) -> Dict[str, float]:
        """Get min/max limits for quantity update"""
        delivered_qty = float(allocation.get('delivered_qty', 0))
        requested_qty = float(allocation.get('requested_qty', 0))
        
        return {
            'min': delivered_qty,  # Cannot go below delivered
            'max': requested_qty,   # Cannot exceed demand
            'delivered': delivered_qty,
            'requested': requested_qty
        }
