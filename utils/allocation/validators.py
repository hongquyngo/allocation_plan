"""
Validation utilities for Allocation module
Handles input validation and business rules
"""
import re
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging

logger = logging.getLogger(__name__)


class AllocationValidator:
    """Validator for allocation operations"""
    
    def __init__(self):
        # Business rule constants
        self.MAX_OVER_ALLOCATION_PERCENT = 110  # Allow max 10% over-allocation
        self.MIN_ALLOCATION_QTY = 0.01
        self.MAX_ALLOCATION_DAYS_FUTURE = 365
        self.VALID_ALLOCATION_MODES = ['SOFT', 'HARD']
        self.VALID_REASON_CATEGORIES = [
            'CUSTOMER_REQUEST', 'SUPPLY_ISSUE', 'QUALITY_ISSUE', 
            'BUSINESS_DECISION', 'OTHER'
        ]
    
    def validate_allocations(self, allocations: List[Dict], 
                           oc_info: Dict, mode: str = 'SOFT') -> List[str]:
        """
        Validate allocation items before creation
        
        Args:
            allocations: List of allocation items
            oc_info: OC detail information
            mode: Allocation mode (SOFT or HARD)
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check if allocations list is empty
        if not allocations:
            errors.append("No allocation items provided")
            return errors
        
        # Validate each allocation item
        total_allocated = 0
        source_ids_used = set()
        
        for idx, alloc in enumerate(allocations):
            # For SOFT allocation, supply source is optional
            if mode == 'HARD':
                # HARD allocation must have supply source
                if not alloc.get('source_type'):
                    errors.append(f"Item {idx + 1}: Missing source type (required for HARD allocation)")
                
                if not alloc.get('source_id'):
                    errors.append(f"Item {idx + 1}: Missing source ID (required for HARD allocation)")
                
                # Check for duplicate source usage
                source_key = f"{alloc.get('source_type')}_{alloc.get('source_id')}"
                if source_key in source_ids_used:
                    errors.append(f"Item {idx + 1}: Duplicate allocation from same source")
                source_ids_used.add(source_key)
            
            # Validate quantity (for both SOFT and HARD)
            qty = alloc.get('quantity', 0)
            if qty <= 0:
                errors.append(f"Item {idx + 1}: Quantity must be positive")
            elif qty < self.MIN_ALLOCATION_QTY:
                errors.append(f"Item {idx + 1}: Quantity must be at least {self.MIN_ALLOCATION_QTY}")
            
            total_allocated += qty
        
        # Validate total allocation vs OC quantity
        if total_allocated > 0:
            oc_qty = oc_info.get('pending_quantity', 0)
            if oc_qty > 0:
                allocation_percent = (total_allocated / oc_qty) * 100
                if allocation_percent > self.MAX_OVER_ALLOCATION_PERCENT:
                    errors.append(
                        f"Total allocation ({total_allocated}) exceeds maximum allowed "
                        f"({oc_qty * self.MAX_OVER_ALLOCATION_PERCENT / 100:.2f})"
                    )
        
        return errors
    
    def validate_allocation_mode(self, mode: str) -> Tuple[bool, str]:
        """
        Validate allocation mode
        
        Args:
            mode: Allocation mode to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not mode:
            return False, "Allocation mode is required"
        
        if mode not in self.VALID_ALLOCATION_MODES:
            return False, f"Invalid allocation mode. Must be one of: {', '.join(self.VALID_ALLOCATION_MODES)}"
        
        return True, ""
    
    def validate_etd(self, etd: Any, oc_etd: Any = None) -> Tuple[bool, str]:
        """
        Validate ETD date
        
        Args:
            etd: ETD to validate
            oc_etd: Original OC ETD for comparison
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not etd:
            return False, "ETD is required"
        
        # Convert to date object if needed
        try:
            if isinstance(etd, str):
                etd_date = datetime.strptime(etd, "%Y-%m-%d").date()
            elif isinstance(etd, datetime):
                etd_date = etd.date()
            elif isinstance(etd, date):
                etd_date = etd
            else:
                return False, "Invalid ETD format"
        except ValueError:
            return False, "Invalid ETD date format"
        
        # Check if ETD is not too far in the past
        min_date = date.today() - timedelta(days=30)
        if etd_date < min_date:
            return False, "ETD cannot be more than 30 days in the past"
        
        # Check if ETD is not too far in the future
        max_date = date.today() + timedelta(days=self.MAX_ALLOCATION_DAYS_FUTURE)
        if etd_date > max_date:
            return False, f"ETD cannot be more than {self.MAX_ALLOCATION_DAYS_FUTURE} days in the future"
        
        # If OC ETD provided, validate variance
        if oc_etd:
            try:
                if isinstance(oc_etd, str):
                    oc_etd_date = datetime.strptime(oc_etd, "%Y-%m-%d").date()
                elif isinstance(oc_etd, datetime):
                    oc_etd_date = oc_etd.date()
                elif isinstance(oc_etd, date):
                    oc_etd_date = oc_etd
                else:
                    return True, ""  # Skip comparison if invalid format
                
                # Warning if ETD variance is more than 30 days
                variance_days = abs((etd_date - oc_etd_date).days)
                if variance_days > 30:
                    logger.warning(f"Large ETD variance: {variance_days} days")
                    
            except Exception as e:
                logger.debug(f"Error comparing ETD dates: {e}")
        
        return True, ""
    
    def validate_cancellation(self, allocation_detail: Dict, 
                            cancel_qty: float, reason: str, 
                            reason_category: str) -> List[str]:
        """
        Validate cancellation request
        
        Args:
            allocation_detail: Allocation detail information
            cancel_qty: Quantity to cancel
            reason: Cancellation reason
            reason_category: Reason category
            
        Returns:
            List of validation errors
        """
        errors = []
        
        # Validate quantity
        if cancel_qty <= 0:
            errors.append("Cancel quantity must be positive")
        
        effective_qty = allocation_detail.get('effective_qty', 0)
        if cancel_qty > effective_qty:
            errors.append(f"Cannot cancel {cancel_qty}. Only {effective_qty} available")
        
        # Validate reason
        if not reason or len(reason.strip()) < 10:
            errors.append("Please provide a detailed reason (at least 10 characters)")
        
        # Validate reason category
        if reason_category not in self.VALID_REASON_CATEGORIES:
            errors.append(f"Invalid reason category. Must be one of: {', '.join(self.VALID_REASON_CATEGORIES)}")
        
        # Check if allocation is already delivered
        delivered_qty = allocation_detail.get('delivered_qty', 0)
        if delivered_qty > 0:
            if cancel_qty > (effective_qty - delivered_qty):
                errors.append(f"Cannot cancel delivered quantity. Maximum cancellable: {effective_qty - delivered_qty}")
        
        # Check allocation mode
        if allocation_detail.get('allocation_mode') == 'HARD':
            errors.append("Cannot cancel HARD allocation. Please contact manager for approval")
        
        return errors
    
    def validate_quantity(self, quantity: Any, max_quantity: float = None) -> Tuple[bool, str]:
        """
        Validate quantity input
        
        Args:
            quantity: Quantity to validate
            max_quantity: Maximum allowed quantity
            
        Returns:
            Tuple of (is_valid, error_message)
        """
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
            return False, f"Quantity cannot exceed {max_quantity}"
        
        # Check decimal places
        if qty != int(qty) and len(str(qty).split('.')[-1]) > 2:
            return False, "Maximum 2 decimal places allowed"
        
        return True, ""
    
    def validate_notes(self, notes: str, max_length: int = 500) -> Tuple[bool, str]:
        """
        Validate notes/comments
        
        Args:
            notes: Notes text to validate
            max_length: Maximum allowed length
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not notes:
            return True, ""  # Notes are optional
        
        # Check length
        if len(notes) > max_length:
            return False, f"Notes cannot exceed {max_length} characters"
        
        # Check for invalid characters (basic SQL injection prevention)
        if any(char in notes for char in ['<', '>', '"', "'"]):
            return False, "Notes contain invalid characters"
        
        return True, ""
    
    def validate_supply_source(self, source_type: str, source_id: Any) -> Tuple[bool, str]:
        """
        Validate supply source reference
        
        Args:
            source_type: Type of supply source
            source_id: ID of the source
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        valid_source_types = ['INVENTORY', 'PENDING_CAN', 'PENDING_PO', 'PENDING_WHT']
        
        if not source_type:
            return False, "Source type is required"
        
        if source_type not in valid_source_types:
            return False, f"Invalid source type. Must be one of: {', '.join(valid_source_types)}"
        
        if not source_id:
            return False, "Source ID is required"
        
        # Validate source ID is numeric
        try:
            int(source_id)
        except (ValueError, TypeError):
            return False, "Source ID must be numeric"
        
        return True, ""
    
    def validate_user_permissions(self, user_role: str, action: str) -> Tuple[bool, str]:
        """
        Validate user permissions for action
        
        Args:
            user_role: User's role
            action: Action to perform
            
        Returns:
            Tuple of (is_allowed, error_message)
        """
        # Define permission matrix
        permissions = {
            'admin': ['create', 'update', 'cancel', 'reverse', 'delete'],
            'manager': ['create', 'update', 'cancel', 'reverse'],
            'user': ['create', 'update'],
            'viewer': []
        }
        
        allowed_actions = permissions.get(user_role.lower(), [])
        
        if action not in allowed_actions:
            return False, f"Your role ({user_role}) does not have permission to {action} allocations"
        
        return True, ""
    
    def validate_batch_allocations(self, batch_data: List[Dict]) -> Dict[str, Any]:
        """
        Validate batch allocation data
        
        Args:
            batch_data: List of allocation requests
            
        Returns:
            Dictionary with validation results
        """
        results = {
            'valid_count': 0,
            'error_count': 0,
            'errors': [],
            'warnings': []
        }
        
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
            
            # Validate allocations
            if item.get('allocations'):
                alloc_errors = self.validate_allocations(
                    item['allocations'],
                    item.get('oc_info', {})
                )
                item_errors.extend(alloc_errors)
            
            # Validate mode
            mode_valid, mode_error = self.validate_allocation_mode(
                item.get('mode', 'SOFT')
            )
            if not mode_valid:
                item_errors.append(mode_error)
            
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
    
    def sanitize_input(self, value: str) -> str:
        """
        Sanitize user input to prevent injection attacks
        
        Args:
            value: Input value to sanitize
            
        Returns:
            Sanitized value
        """
        if not value:
            return ""
        
        # Remove potentially dangerous characters
        sanitized = re.sub(r'[<>\"\'%;()&+]', '', str(value))
        
        # Trim whitespace
        sanitized = sanitized.strip()
        
        # Limit length
        max_length = 1000
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length]
        
        return sanitized