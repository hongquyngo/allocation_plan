"""
Validation utilities for Allocation module - Updated Version
Core validation logic for allocation operations with partial delivery support
"""
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class AllocationValidator:
    """Validator for allocation operations"""
    
    def __init__(self):
        # Configuration constants
        self.MAX_OVER_ALLOCATION_PERCENT = 110  # Allow max 10% over-allocation
        self.MIN_ALLOCATION_QTY = 0.01
        self.MAX_ETD_DAYS_FUTURE = 365
        self.MIN_REASON_LENGTH = 10
        self.MAX_STRING_LENGTH = 500
        
        # Valid values
        self.VALID_ALLOCATION_MODES = ['SOFT', 'HARD']
        self.VALID_REASON_CATEGORIES = [
            'CUSTOMER_REQUEST', 
            'SUPPLY_ISSUE', 
            'QUALITY_ISSUE', 
            'BUSINESS_DECISION', 
            'OTHER'
        ]
        
        # Permission matrix (based on users table role field)
        self.PERMISSIONS = {
            'admin': ['create', 'update', 'cancel', 'reverse', 'delete', 'view'],
            'GM': ['create', 'update', 'cancel', 'reverse', 'view'],
            'MD': ['create', 'update', 'cancel', 'reverse', 'view'],
            'sales_manager': ['create', 'update', 'cancel', 'view'],
            'sales': ['create', 'update', 'cancel', 'view'],
            'supply_chain': ['create', 'update', 'cancel', 'view'],
            'viewer': ['view'],
            'customer': ['view'],
            'vendor': ['view']
        }
    
    # ==================== Create Allocation Validation ====================
    
    def validate_create_allocation(self, 
                                 allocations: List[Dict],
                                 oc_info: Dict,
                                 mode: str,
                                 user_role: str = 'viewer') -> List[str]:
        """
        Validate allocation creation request
        
        Returns:
            List of error messages (empty if valid)
        """
        errors = []
        
        # 1. Check permission
        if not self.check_permission(user_role, 'create'):
            errors.append("You don't have permission to create allocations")
            return errors
        
        # 2. Validate allocation mode
        if mode not in self.VALID_ALLOCATION_MODES:
            errors.append(f"Invalid allocation mode. Must be {' or '.join(self.VALID_ALLOCATION_MODES)}")
        
        # 3. Check allocations not empty
        if not allocations:
            errors.append("No allocation items provided")
            return errors
        
        # 4. Validate each allocation item
        total_quantity = 0
        source_keys = set()
        
        for idx, alloc in enumerate(allocations):
            # Check quantity
            qty = alloc.get('quantity', 0)
            if qty <= 0:
                errors.append(f"Item {idx + 1}: Quantity must be positive")
            elif qty < self.MIN_ALLOCATION_QTY:
                errors.append(f"Item {idx + 1}: Minimum quantity is {self.MIN_ALLOCATION_QTY}")
            
            total_quantity += qty
            
            # For HARD allocation, check source info
            if mode == 'HARD':
                if not alloc.get('source_type'):
                    errors.append(f"Item {idx + 1}: Source type required for HARD allocation")
                if not alloc.get('source_id'):
                    errors.append(f"Item {idx + 1}: Source ID required for HARD allocation")
                
                # Check for duplicate sources
                source_key = f"{alloc.get('source_type')}_{alloc.get('source_id')}"
                if source_key in source_keys:
                    errors.append(f"Item {idx + 1}: Duplicate allocation from same source")
                source_keys.add(source_key)
        
        # 5. Check over-allocation
        if total_quantity > 0 and oc_info.get('pending_quantity'):
            pending_qty = float(oc_info['pending_quantity'])
            max_allowed = pending_qty * (self.MAX_OVER_ALLOCATION_PERCENT / 100)
            
            if total_quantity > max_allowed:
                errors.append(
                    f"Total allocation ({total_quantity:.0f}) exceeds maximum allowed "
                    f"({max_allowed:.0f} = {self.MAX_OVER_ALLOCATION_PERCENT}% of {pending_qty:.0f})"
                )
        
        # 6. Warning for over-allocation (not an error, just a warning)
        if total_quantity > 0 and oc_info.get('pending_quantity'):
            pending_qty = float(oc_info['pending_quantity'])
            if total_quantity > pending_qty and total_quantity <= max_allowed:
                logger.warning(
                    f"Over-allocating by {total_quantity - pending_qty:.0f} "
                    f"({((total_quantity - pending_qty) / pending_qty * 100):.1f}%)"
                )
        
        return errors
    
    # ==================== Update Allocation Validation ====================
    
    def validate_update_etd(self,
                          allocation_detail: Dict,
                          new_etd: Any,
                          user_role: str = 'viewer') -> Tuple[bool, str]:
        """
        Validate ETD update request with partial delivery support
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check permission
        if not self.check_permission(user_role, 'update'):
            return False, "You don't have permission to update allocations"
        
        # Check allocation mode
        if allocation_detail.get('allocation_mode') == 'HARD':
            # For HARD allocation, need manager approval
            if user_role not in ['GM', 'MD', 'admin', 'sales_manager']:
                return False, "HARD allocation ETD update requires manager approval"
        
        # Check status
        if allocation_detail.get('status') != 'ALLOCATED':
            return False, "Can only update ETD for ALLOCATED status"
        
        # NEW: Check if there's pending quantity (not yet delivered)
        pending_qty = allocation_detail.get('pending_allocated_qty', 0)
        if pending_qty <= 0:
            return False, "Cannot update ETD - all quantity has been delivered"
        
        # Validate ETD date
        if not new_etd:
            return False, "ETD is required"
        
        # Convert to date object if needed
        try:
            if isinstance(new_etd, str):
                new_etd_date = datetime.strptime(new_etd, "%Y-%m-%d").date()
            elif isinstance(new_etd, datetime):
                new_etd_date = new_etd.date()
            elif isinstance(new_etd, date):
                new_etd_date = new_etd
            else:
                return False, "Invalid ETD format"
        except ValueError:
            return False, "Invalid ETD format. Use YYYY-MM-DD"
        
        # Check not too far in the past (30 days)
        min_date = date.today() - timedelta(days=30)
        if new_etd_date < min_date:
            return False, "ETD cannot be more than 30 days in the past"
        
        # Check not too far in the future
        max_date = date.today() + timedelta(days=self.MAX_ETD_DAYS_FUTURE)
        if new_etd_date > max_date:
            return False, f"ETD cannot be more than {self.MAX_ETD_DAYS_FUTURE} days in the future"
        
        # Check if new ETD is different from current
        current_etd = allocation_detail.get('allocated_etd')
        if current_etd:
            if isinstance(current_etd, str):
                current_etd = pd.to_datetime(current_etd).date()
            elif isinstance(current_etd, datetime):
                current_etd = current_etd.date()
            
            if current_etd == new_etd_date:
                return False, "New ETD is the same as current ETD"
        
        # NEW: Add info message about partial delivery
        delivered_qty = allocation_detail.get('delivered_qty', 0)
        if delivered_qty > 0:
            logger.info(
                f"ETD update for partially delivered allocation: "
                f"{delivered_qty:.0f} already delivered, "
                f"{pending_qty:.0f} pending"
            )
        
        return True, ""
    
    # ==================== Cancel Allocation Validation ====================
    
    def validate_cancel_allocation(self,
                                 allocation_detail: Dict,
                                 cancel_qty: float,
                                 reason: str,
                                 reason_category: str,
                                 user_role: str = 'viewer') -> List[str]:
        """
        Validate cancellation request with partial delivery support
        
        Returns:
            List of error messages
        """
        errors = []
        
        # Check permission
        if not self.check_permission(user_role, 'cancel'):
            errors.append("You don't have permission to cancel allocations")
            return errors
        
        # Check quantity
        if cancel_qty <= 0:
            errors.append("Cancel quantity must be positive")
        
        # NEW: Use pending_allocated_qty instead of effective_qty
        pending_qty = allocation_detail.get('pending_allocated_qty', 0)
        if cancel_qty > pending_qty:
            errors.append(
                f"Cannot cancel {cancel_qty:.0f}. "
                f"Only {pending_qty:.0f} pending (not yet delivered)"
            )
        
        # Check if all has been delivered
        if pending_qty <= 0:
            errors.append("Cannot cancel - all quantity has been delivered")
        
        # Check allocation mode
        if allocation_detail.get('allocation_mode') == 'HARD':
            # For HARD allocation, need manager approval
            if user_role not in ['GM', 'MD', 'admin', 'sales_manager']:
                errors.append("HARD allocation cancellation requires manager approval")
        
        # Validate reason
        if not reason or len(reason.strip()) < self.MIN_REASON_LENGTH:
            errors.append(f"Please provide detailed reason (minimum {self.MIN_REASON_LENGTH} characters)")
        
        # Validate reason category
        if reason_category not in self.VALID_REASON_CATEGORIES:
            errors.append(
                f"Invalid reason category. Must be one of: {', '.join(self.VALID_REASON_CATEGORIES)}"
            )
        
        # NEW: Add info about partial delivery
        delivered_qty = allocation_detail.get('delivered_qty', 0)
        if delivered_qty > 0 and not errors:
            logger.info(
                f"Cancelling from partially delivered allocation: "
                f"{delivered_qty:.0f} already delivered, "
                f"cancelling {cancel_qty:.0f} of {pending_qty:.0f} pending"
            )
        
        return errors
    
    # ==================== Reverse Cancellation Validation ====================
    
    def validate_reverse_cancellation(self,
                                    cancellation: Dict,
                                    reversal_reason: str,
                                    user_role: str = 'viewer') -> Tuple[bool, str]:
        """
        Validate cancellation reversal
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check permission - only GM, MD, and admin
        if not self.check_permission(user_role, 'reverse'):
            return False, "Only managers and admins can reverse cancellations"
        
        # Check cancellation status
        if cancellation.get('status') != 'ACTIVE':
            return False, "Cancellation has already been reversed"
        
        # Check reversal reason
        if not reversal_reason or len(reversal_reason.strip()) < self.MIN_REASON_LENGTH:
            return False, f"Please provide reversal reason (minimum {self.MIN_REASON_LENGTH} characters)"
        
        # Validate reason length
        if len(reversal_reason) > self.MAX_STRING_LENGTH:
            return False, f"Reversal reason too long (maximum {self.MAX_STRING_LENGTH} characters)"
        
        return True, ""
    
    # ==================== Helper Methods ====================
    
    def check_permission(self, user_role: str, action: str) -> bool:
        """Check if user role has permission for action"""
        allowed_actions = self.PERMISSIONS.get(user_role.lower(), [])
        return action in allowed_actions
    
    def validate_allocation_from_view_data(self, 
                                         oc_data: Dict,
                                         action: str) -> Dict[str, Any]:
        """
        Validate allocation action based on view data (with new flags)
        
        Args:
            oc_data: Data from outbound_oc_pending_delivery_view
            action: 'update_etd' or 'cancel'
            
        Returns:
            Dict with 'valid' boolean and 'message' if invalid
        """
        # Use the new flags from view
        if action == 'update_etd':
            if oc_data.get('can_update_etd') == 'Yes':
                return {'valid': True}
            else:
                if oc_data.get('pending_allocated_qty_standard', 0) <= 0:
                    return {
                        'valid': False,
                        'message': 'Cannot update ETD - all quantity has been delivered'
                    }
                elif not oc_data.get('has_soft_allocation'):
                    return {
                        'valid': False,
                        'message': 'Cannot update ETD - no SOFT allocation found'
                    }
                else:
                    return {
                        'valid': False,
                        'message': 'Cannot update ETD for this allocation'
                    }
        
        elif action == 'cancel':
            if oc_data.get('can_cancel') == 'Yes':
                return {
                    'valid': True,
                    'max_qty': oc_data.get('max_cancellable_qty', 0)
                }
            else:
                return {
                    'valid': False,
                    'message': 'Cannot cancel - all quantity has been delivered'
                }
        
        return {'valid': False, 'message': 'Unknown action'}