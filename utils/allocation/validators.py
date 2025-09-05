"""
Validation utilities for Allocation module - Fixed UOM Handling
Improved validation with clear UOM context in error messages
"""
from datetime import datetime, date
from typing import Dict, List, Any, Tuple
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class AllocationValidator:
    """Validator for allocation operations with UOM-aware error messages"""
    
    def __init__(self):
        # Configuration constants
        self.MAX_OVER_ALLOCATION_PERCENT = 100
        self.MIN_ALLOCATION_QTY = 0.01
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
        Validate allocation creation request with correct UOM context
        
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
        
        # 5. Check over-allocation với logic ĐÚNG
        if total_quantity > 0:
            # Sử dụng effective quantity từ view (đã trừ OC cancellation)
            effective_qty = float(oc_info.get('standard_quantity', 0))
            
            # Lấy phân bổ hiệu lực hiện tại
            current_effective_allocated = float(oc_info.get('total_effective_allocated_qty_standard', 0))
            
            standard_uom = oc_info.get('standard_uom', '')
            
            # Tính tổng phân bổ hiệu lực mới
            new_total_effective = current_effective_allocated + total_quantity
            
            # Giới hạn cho phép
            max_allowed = effective_qty * (self.MAX_OVER_ALLOCATION_PERCENT / 100)
            
            if new_total_effective > max_allowed:
                errors.append(
                    f"Total allocation would be {new_total_effective:.0f} {standard_uom} "
                    f"(current effective: {current_effective_allocated:.0f} + new: {total_quantity:.0f}). "
                    f"Maximum allowed is {max_allowed:.0f} {standard_uom} "
                    f"({self.MAX_OVER_ALLOCATION_PERCENT}% of effective OC quantity {effective_qty:.0f} {standard_uom})"
                )
                
                # Add context về pending quantity
                pending_qty = float(oc_info.get('pending_standard_delivery_quantity', 0))
                if pending_qty < effective_qty:
                    delivered_qty = effective_qty - pending_qty
                    errors.append(
                        f"Note: {delivered_qty:.0f} {standard_uom} already delivered, "
                        f"only {pending_qty:.0f} {standard_uom} pending delivery"
                    )
                
                # Add helpful context if selling UOM is different
                if oc_info.get('selling_uom') and oc_info.get('selling_uom') != standard_uom:
                    from .uom_converter import UOMConverter
                    converter = UOMConverter()
                    
                    if converter.needs_conversion(oc_info.get('uom_conversion', '1')):
                        # Convert to selling UOM for user reference
                        total_selling = converter.convert_quantity(
                            new_total_effective,
                            'standard',
                            'selling',
                            oc_info.get('uom_conversion', '1')
                        )
                        max_selling = converter.convert_quantity(
                            max_allowed,
                            'standard',
                            'selling',
                            oc_info.get('uom_conversion', '1')
                        )
                        effective_selling = oc_info.get('selling_quantity', effective_qty)
                        
                        errors.append(
                            f"For reference: {total_selling:.0f} {oc_info['selling_uom']} exceeds "
                            f"{max_selling:.0f} {oc_info['selling_uom']} "
                            f"(110% of {effective_selling:.0f} {oc_info['selling_uom']})"
                        )
        
        # 6. Warning for over-allocation (not an error, just a warning)
        if total_quantity > 0 and len(errors) == 0:
            effective_qty = float(oc_info.get('standard_quantity', 0))
            current_effective_allocated = float(oc_info.get('total_effective_allocated_qty_standard', 0))
            new_total_effective = current_effective_allocated + total_quantity
            
            standard_uom = oc_info.get('standard_uom', '')
            
            if effective_qty > 0 and new_total_effective > effective_qty:
                over_qty = new_total_effective - effective_qty
                over_pct = (over_qty / effective_qty * 100)
                logger.warning(
                    f"Over-allocating by {over_qty:.0f} {standard_uom} ({over_pct:.1f}%) "
                    f"vs effective OC quantity"
                )
        
        return errors

    # ==================== Update Allocation Validation ====================
        
    def validate_update_etd(self,
                        allocation_detail: Dict,
                        new_etd: Any,
                        user_role: str = 'viewer') -> Tuple[bool, str]:
        """
        Validate ETD update request
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check permission
        if not self.check_permission(user_role, 'update'):
            return False, "You don't have permission to update allocations"
        
        # REMOVED: Check for HARD allocation mode - now both HARD and SOFT can be updated
        
        # Check status
        if allocation_detail.get('status') != 'ALLOCATED':
            return False, "Can only update ETD for ALLOCATED status"
        
        # Check if there's pending quantity (not yet delivered)
        pending_qty = allocation_detail.get('pending_allocated_qty', 0)
        if pending_qty <= 0:
            return False, "Cannot update ETD - all quantity has been delivered"
        
        # Validate ETD date format
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
        
        # Check if new ETD is different from current
        current_etd = allocation_detail.get('allocated_etd')
        if current_etd:
            if isinstance(current_etd, str):
                current_etd = pd.to_datetime(current_etd).date()
            elif isinstance(current_etd, datetime):
                current_etd = current_etd.date()
            
            if current_etd == new_etd_date:
                return False, "New ETD is the same as current ETD"
        
        # Add info message about partial delivery
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
        Validate cancellation request with UOM context
        
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
        
        # Use pending_allocated_qty instead of effective_qty
        pending_qty = allocation_detail.get('pending_allocated_qty', 0)
        
        # Get UOM context for better error messages
        uom = allocation_detail.get('uom', '')
        
        if cancel_qty > pending_qty:
            errors.append(
                f"Cannot cancel {cancel_qty:.0f}{' ' + uom if uom else ''}. "
                f"Only {pending_qty:.0f}{' ' + uom if uom else ''} pending (not yet delivered)"
            )
        
        # Check if all has been delivered
        if pending_qty <= 0:
            errors.append("Cannot cancel - all quantity has been delivered")
        
        # REMOVED: Check for HARD allocation mode - now both can be cancelled with same rules
        
        # Validate reason
        if not reason or len(reason.strip()) < self.MIN_REASON_LENGTH:
            errors.append(f"Please provide detailed reason (minimum {self.MIN_REASON_LENGTH} characters)")
        
        if reason and len(reason) > self.MAX_STRING_LENGTH:
            errors.append(f"Reason too long (maximum {self.MAX_STRING_LENGTH} characters)")
        
        # Validate reason category
        if reason_category not in self.VALID_REASON_CATEGORIES:
            errors.append(
                f"Invalid reason category. Must be one of: {', '.join(self.VALID_REASON_CATEGORIES)}"
            )
        
        # Add info about partial delivery
        delivered_qty = allocation_detail.get('delivered_qty', 0)
        if delivered_qty > 0 and not errors:
            logger.info(
                f"Cancelling from partially delivered allocation: "
                f"{delivered_qty:.0f}{' ' + uom if uom else ''} already delivered, "
                f"cancelling {cancel_qty:.0f}{' ' + uom if uom else ''} of "
                f"{pending_qty:.0f}{' ' + uom if uom else ''} pending"
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
    