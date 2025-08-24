"""
Validation utilities for Allocation module - Simplified but Complete
Focuses on core business rules with clear error messages
"""
import re
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging

logger = logging.getLogger(__name__)


class AllocationValidator:
    """Simplified validator for allocation operations"""
    
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
        
        # Permission matrix
        self.PERMISSIONS = {
            'admin': ['create', 'update', 'cancel', 'reverse', 'delete'],
            'manager': ['create', 'update', 'cancel', 'reverse'],
            'user': ['create', 'update', 'cancel'],
            'viewer': ['view']
        }
    
    # ==================== Create Allocation Validation ====================
    
    def validate_create_allocation(self, 
                                 allocations: List[Dict],
                                 oc_info: Dict,
                                 mode: str,
                                 user_role: str = 'user') -> List[str]:
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
        
        return errors
    
    # ==================== Update Allocation Validation ====================
    
    def validate_update_etd(self,
                          allocation_detail: Dict,
                          new_etd: Any,
                          user_role: str = 'user') -> Tuple[bool, str]:
        """
        Validate ETD update request
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check permission
        if not self.check_permission(user_role, 'update'):
            return False, "You don't have permission to update allocations"
        
        # Check allocation mode
        if allocation_detail.get('allocation_mode') == 'HARD':
            return False, "Cannot update ETD for HARD allocation"
        
        # Check status
        if allocation_detail.get('status') != 'ALLOCATED':
            return False, "Can only update ETD for ALLOCATED status"
        
        # Check if already delivered
        if allocation_detail.get('delivered_qty', 0) > 0:
            return False, "Cannot update ETD for delivered allocation"
        
        # Validate ETD date
        valid, error = self.validate_date(new_etd, 'ETD')
        if not valid:
            return False, error
        
        return True, ""
    
    # ==================== Cancel Allocation Validation ====================
    
    def validate_cancel_allocation(self,
                                 allocation_detail: Dict,
                                 cancel_qty: float,
                                 reason: str,
                                 reason_category: str,
                                 user_role: str = 'user') -> List[str]:
        """
        Validate cancellation request
        
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
        
        effective_qty = allocation_detail.get('effective_qty', 0)
        if cancel_qty > effective_qty:
            errors.append(f"Cannot cancel {cancel_qty:.0f}. Only {effective_qty:.0f} available")
        
        # Check if already delivered
        delivered_qty = allocation_detail.get('delivered_qty', 0)
        if delivered_qty > 0:
            max_cancellable = effective_qty - delivered_qty
            if cancel_qty > max_cancellable:
                errors.append(
                    f"Cannot cancel delivered quantity. Maximum cancellable: {max_cancellable:.0f}"
                )
        
        # Check allocation mode
        if allocation_detail.get('allocation_mode') == 'HARD':
            errors.append("Cannot cancel HARD allocation. Please contact manager")
        
        # Validate reason
        if not reason or len(reason.strip()) < self.MIN_REASON_LENGTH:
            errors.append(f"Please provide detailed reason (minimum {self.MIN_REASON_LENGTH} characters)")
        
        # Validate reason category
        if reason_category not in self.VALID_REASON_CATEGORIES:
            errors.append(
                f"Invalid reason category. Must be one of: {', '.join(self.VALID_REASON_CATEGORIES)}"
            )
        
        return errors
    
    # ==================== Reverse Cancellation Validation ====================
    
    def validate_reverse_cancellation(self,
                                    cancellation: Dict,
                                    reversal_reason: str,
                                    user_role: str = 'user') -> Tuple[bool, str]:
        """
        Validate cancellation reversal
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check permission - only manager and admin
        if not self.check_permission(user_role, 'reverse'):
            return False, "Only managers and admins can reverse cancellations"
        
        # Check cancellation status
        if cancellation.get('status') != 'ACTIVE':
            return False, "Cancellation has already been reversed"
        
        # Check reversal reason
        if not reversal_reason or len(reversal_reason.strip()) < self.MIN_REASON_LENGTH:
            return False, f"Please provide reversal reason (minimum {self.MIN_REASON_LENGTH} characters)"
        
        return True, ""
    
    # ==================== Delete Allocation Validation ====================
    
    def validate_delete_allocation(self,
                                 allocation_detail: Dict,
                                 user_role: str = 'user') -> Tuple[bool, str]:
        """
        Validate allocation deletion
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check permission - only admin
        if not self.check_permission(user_role, 'delete'):
            return False, "Only admins can delete allocations"
        
        # Check status - only DRAFT can be deleted
        if allocation_detail.get('status') != 'DRAFT':
            return False, "Can only delete DRAFT allocations"
        
        # Check if any delivery linked
        if allocation_detail.get('delivered_qty', 0) > 0:
            return False, "Cannot delete allocation with delivery history"
        
        return True, ""
    
    # ==================== Common Validation Methods ====================
    
    def validate_quantity(self, quantity: Any, max_quantity: float = None) -> Tuple[bool, str]:
        """Validate quantity input"""
        # Check if quantity is provided
        if quantity is None or quantity == "":
            return False, "Quantity is required"
        
        # Convert to float
        try:
            qty = float(quantity)
        except (ValueError, TypeError):
            return False, "Quantity must be a number"
        
        # Check if positive
        if qty <= 0:
            return False, "Quantity must be positive"
        
        # Check minimum
        if qty < self.MIN_ALLOCATION_QTY:
            return False, f"Minimum quantity is {self.MIN_ALLOCATION_QTY}"
        
        # Check maximum if provided
        if max_quantity is not None and qty > max_quantity:
            return False, f"Quantity cannot exceed {max_quantity:.0f}"
        
        return True, ""
    
    def validate_date(self, date_value: Any, field_name: str = "Date") -> Tuple[bool, str]:
        """Validate date input"""
        if not date_value:
            return False, f"{field_name} is required"
        
        # Convert to date object if needed
        try:
            if isinstance(date_value, str):
                date_obj = datetime.strptime(date_value, "%Y-%m-%d").date()
            elif isinstance(date_value, datetime):
                date_obj = date_value.date()
            elif isinstance(date_value, date):
                date_obj = date_value
            else:
                return False, f"Invalid {field_name} format"
        except ValueError:
            return False, f"Invalid {field_name} format. Use YYYY-MM-DD"
        
        # Check not too far in the past (30 days)
        min_date = date.today() - timedelta(days=30)
        if date_obj < min_date:
            return False, f"{field_name} cannot be more than 30 days in the past"
        
        # Check not too far in the future
        max_date = date.today() + timedelta(days=self.MAX_ETD_DAYS_FUTURE)
        if date_obj > max_date:
            return False, f"{field_name} cannot be more than {self.MAX_ETD_DAYS_FUTURE} days in the future"
        
        return True, ""
    
    def check_permission(self, user_role: str, action: str) -> bool:
        """Check if user role has permission for action"""
        allowed_actions = self.PERMISSIONS.get(user_role.lower(), [])
        return action in allowed_actions
    
    def sanitize_input(self, value: str) -> str:
        """Sanitize user input to prevent injection attacks"""
        if not value:
            return ""
        
        # Convert to string
        value = str(value)
        
        # Remove potentially dangerous characters
        # Keep alphanumeric, spaces, and common punctuation
        sanitized = re.sub(r'[<>\"\'%;()&+\-=]', '', value)
        
        # Trim whitespace
        sanitized = sanitized.strip()
        
        # Limit length
        if len(sanitized) > self.MAX_STRING_LENGTH:
            sanitized = sanitized[:self.MAX_STRING_LENGTH]
        
        return sanitized
    
    # ==================== Supply Validation Methods ====================
    
    def validate_supply_availability(self,
                                   supply_info: Dict,
                                   requested_qty: float) -> Tuple[bool, str]:
        """Validate if supply is available for allocation"""
        available_qty = supply_info.get('available_qty', 0)
        
        if available_qty <= 0:
            return False, "Supply source is not available"
        
        if requested_qty > available_qty:
            return False, f"Insufficient supply. Available: {available_qty:.0f}, Requested: {requested_qty:.0f}"
        
        # Check expiry for inventory
        if supply_info.get('source_type') == 'INVENTORY':
            expiry_date = supply_info.get('expiry_date')
            if expiry_date:
                try:
                    if isinstance(expiry_date, str):
                        expiry = datetime.strptime(expiry_date, "%Y-%m-%d").date()
                    else:
                        expiry = expiry_date
                    
                    if expiry < date.today():
                        return False, "Cannot allocate expired inventory"
                    elif expiry < date.today() + timedelta(days=30):
                        logger.warning(f"Inventory expires soon: {expiry}")
                except:
                    pass
        
        return True, ""
    
    # ==================== Batch Operation Validation ====================
    
    def validate_batch_allocations(self, batch_data: List[Dict]) -> Dict[str, Any]:
        """
        Validate batch allocation data
        
        Returns:
            Dictionary with validation results
        """
        results = {
            'valid_count': 0,
            'error_count': 0,
            'errors': []
        }
        
        # Check batch size
        if not batch_data:
            results['errors'].append("No data provided for batch allocation")
            return results
        
        if len(batch_data) > 100:
            results['errors'].append("Batch size cannot exceed 100 items")
            return results
        
        # Validate each item
        for idx, item in enumerate(batch_data):
            item_errors = []
            
            # Check required fields
            if not item.get('oc_detail_id'):
                item_errors.append("Missing OC detail ID")
            
            if not item.get('allocations'):
                item_errors.append("Missing allocation items")
            else:
                # Validate allocations
                alloc_errors = self.validate_create_allocation(
                    item['allocations'],
                    item.get('oc_info', {}),
                    item.get('mode', 'SOFT')
                )
                item_errors.extend(alloc_errors)
            
            # Add to results
            if item_errors:
                results['error_count'] += 1
                results['errors'].append({
                    'index': idx,
                    'oc_number': item.get('oc_info', {}).get('oc_number', 'Unknown'),
                    'errors': item_errors
                })
            else:
                results['valid_count'] += 1
        
        return results
    
    # ==================== Helper Methods ====================
    
    def format_validation_errors(self, errors: List[str]) -> str:
        """Format validation errors for display"""
        if not errors:
            return ""
        
        if len(errors) == 1:
            return errors[0]
        
        return "Please fix the following errors:\n" + "\n".join(f"• {error}" for error in errors)
    
    def get_validation_summary(self, errors: List[str]) -> Dict[str, Any]:
        """Get summary of validation results"""
        return {
            'is_valid': len(errors) == 0,
            'error_count': len(errors),
            'errors': errors,
            'message': self.format_validation_errors(errors)
        }