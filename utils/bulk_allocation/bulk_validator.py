"""
Bulk Allocation Validator
=========================
Validation rules for bulk allocation operations.
Ensures data integrity and business rule compliance.

REFACTORED v3.0: 2024-12 - Simplified validation using allocatable_qty from view
    - Primary constraint: final_qty <= allocatable_qty
    - Clear error messages identifying which rule was violated
"""
import logging
from typing import Dict, List, Any, Tuple, Optional
from decimal import Decimal
from datetime import datetime, date
import pandas as pd

logger = logging.getLogger(__name__)


class BulkAllocationValidator:
    """Validator for bulk allocation operations"""
    
    def __init__(self):
        # Configuration constants
        self.MAX_OVER_ALLOCATION_PERCENT = 100
        self.MIN_ALLOCATION_QTY = 0.01
        self.MIN_REASON_LENGTH = 10
        self.MAX_STRING_LENGTH = 500
        
        # Valid values
        self.VALID_ALLOCATION_MODES = ['SOFT', 'HARD']
        self.VALID_STRATEGY_TYPES = ['FCFS', 'ETD_PRIORITY', 'PROPORTIONAL', 'REVENUE_PRIORITY', 'HYBRID']
        
        # Permission matrix (based on users table role field)
        self.PERMISSIONS = {
            'admin': ['create', 'update', 'cancel', 'reverse', 'delete', 'view', 'bulk_allocate'],
            'GM': ['create', 'update', 'cancel', 'reverse', 'view', 'bulk_allocate'],
            'MD': ['create', 'update', 'cancel', 'reverse', 'view', 'bulk_allocate'],
            'sales_manager': ['create', 'update', 'cancel', 'view', 'bulk_allocate'],
            'supply_chain': ['create', 'update', 'cancel', 'view', 'bulk_allocate'],
            'sales': ['create', 'update', 'view'],
            'viewer': ['view'],
            'customer': ['view'],
            'vendor': ['view']
        }
    
    # ==================== Permission Check ====================
    
    def check_permission(self, user_role: str, action: str) -> bool:
        """Check if user role has permission for action"""
        allowed_actions = self.PERMISSIONS.get(user_role.lower(), [])
        return action in allowed_actions
    
    def validate_user_permission(self, user_role: str) -> Tuple[bool, str]:
        """Validate user has permission for bulk allocation"""
        if not self.check_permission(user_role, 'bulk_allocate'):
            return False, "You don't have permission to perform bulk allocation"
        return True, ""
    
    # ==================== Scope Validation ====================
    
    def validate_scope(self, scope: Dict) -> List[str]:
        """
        Validate allocation scope selection
        
        Args:
            scope: Dict with keys: brand_ids, customer_codes, legal_entities,
                   etd_from, etd_to, include_partial_allocated
        
        Returns:
            List of error messages (empty if valid)
        """
        errors = []
        
        # Must have at least one filter
        has_filter = (
            (scope.get('brand_ids') and len(scope['brand_ids']) > 0) or
            (scope.get('customer_codes') and len(scope['customer_codes']) > 0) or
            (scope.get('legal_entities') and len(scope['legal_entities']) > 0) or
            scope.get('etd_from') or
            scope.get('etd_to')
        )
        
        if not has_filter:
            errors.append("Please select at least one filter (Brand, Customer, Legal Entity, or ETD range)")
        
        # Validate ETD range
        if scope.get('etd_from') and scope.get('etd_to'):
            try:
                etd_from = pd.to_datetime(scope['etd_from']).date()
                etd_to = pd.to_datetime(scope['etd_to']).date()
                
                if etd_from > etd_to:
                    errors.append("ETD From date cannot be after ETD To date")
            except Exception:
                errors.append("Invalid ETD date format")
        
        return errors
    
    # ==================== Strategy Validation ====================
    
    def validate_strategy_config(self, strategy_type: str, phases: List[Dict] = None,
                                 allocation_mode: str = 'SOFT') -> List[str]:
        """
        Validate strategy configuration
        
        Args:
            strategy_type: One of VALID_STRATEGY_TYPES
            phases: List of phase configs for HYBRID strategy
            allocation_mode: SOFT or HARD
        
        Returns:
            List of error messages
        """
        errors = []
        
        # Validate strategy type
        if strategy_type.upper() not in self.VALID_STRATEGY_TYPES:
            errors.append(f"Invalid strategy type. Must be one of: {', '.join(self.VALID_STRATEGY_TYPES)}")
        
        # Validate allocation mode
        if allocation_mode not in self.VALID_ALLOCATION_MODES:
            errors.append(f"Invalid allocation mode. Must be {' or '.join(self.VALID_ALLOCATION_MODES)}")
        
        # Validate HYBRID phases
        if strategy_type.upper() == 'HYBRID' and phases:
            total_weight = sum(p.get('weight', 0) for p in phases)
            if abs(total_weight - 100) > 0.01:
                errors.append(f"Phase weights must sum to 100%. Current sum: {total_weight}%")
            
            valid_phase_names = ['MIN_GUARANTEE', 'FCFS', 'ETD_PRIORITY', 'PROPORTIONAL', 'REVENUE_PRIORITY']
            for phase in phases:
                if phase.get('name') not in valid_phase_names:
                    errors.append(f"Invalid phase name: {phase.get('name')}. Valid options: {', '.join(valid_phase_names)}")
                
                if phase.get('weight', 0) < 0 or phase.get('weight', 0) > 100:
                    errors.append(f"Phase weight must be between 0 and 100. Got: {phase.get('weight')}")
        
        return errors
    
    # ==================== Allocation Row Validation ====================
    
    def validate_allocation_row(self, row_data: Dict, oc_info: Dict, 
                                supply_available: float) -> Tuple[bool, List[str]]:
        """
        Validate a single allocation row.
        
        Uses allocatable_qty from view as primary constraint.
        Additional checks for minimum qty and supply availability.
        
        Args:
            row_data: Dict with keys: ocd_id, product_id, final_qty
            oc_info: Dict with OC information from view
            supply_available: Available supply for the product
        
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        warnings = []
        
        final_qty = float(row_data.get('final_qty', 0))
        standard_uom = oc_info.get('standard_uom', '')
        
        # Skip if no allocation
        if final_qty <= 0:
            return True, []
        
        # ===== CHECK 1: Minimum allocation quantity =====
        if final_qty < self.MIN_ALLOCATION_QTY:
            errors.append(f"Minimum allocation is {self.MIN_ALLOCATION_QTY} {standard_uom}")
        
        # ===== CHECK 2: Primary constraint - allocatable_qty from view =====
        # This field already incorporates both rules:
        # - Rule 1: effective_qty - total_effective_allocated (OC quota)
        # - Rule 2: pending_qty - undelivered_allocated (delivery need)
        allocatable_qty = float(oc_info.get('allocatable_qty', 0))
        
        if final_qty > allocatable_qty:
            # Determine which constraint is violated for better error message
            effective_qty = float(oc_info.get('effective_qty', 0) or oc_info.get('standard_quantity', 0))
            total_effective_allocated = float(oc_info.get('total_effective_allocated', 0) or 
                                             oc_info.get('total_effective_allocated_qty_standard', 0))
            pending_qty = float(oc_info.get('pending_qty', 0) or 
                               oc_info.get('pending_standard_delivery_quantity', 0))
            undelivered = float(oc_info.get('undelivered_allocated', 0) or 
                               oc_info.get('undelivered_allocated_qty_standard', 0))
            
            # Calculate individual constraints
            oc_quota_remaining = effective_qty - total_effective_allocated
            pending_remaining = pending_qty - undelivered
            
            if oc_quota_remaining <= 0:
                errors.append(
                    f"Over-commitment: OC quota exhausted. "
                    f"Total allocated ({total_effective_allocated:.0f}) = OC quantity ({effective_qty:.0f}) {standard_uom}. "
                    f"Max allocatable: {allocatable_qty:.0f} {standard_uom}"
                )
            elif pending_remaining <= 0:
                errors.append(
                    f"Pending over-allocation: Delivery need fully covered. "
                    f"Undelivered ({undelivered:.0f}) >= Pending ({pending_qty:.0f}) {standard_uom}. "
                    f"Max allocatable: {allocatable_qty:.0f} {standard_uom}"
                )
            elif final_qty > oc_quota_remaining and oc_quota_remaining <= pending_remaining:
                # OC quota is the tighter constraint
                errors.append(
                    f"Over-commitment: Allocating {final_qty:.0f} {standard_uom} would exceed OC quota. "
                    f"Total would be {total_effective_allocated + final_qty:.0f} vs "
                    f"OC quantity {effective_qty:.0f} {standard_uom}. "
                    f"Max allocatable: {allocatable_qty:.0f} {standard_uom}"
                )
            elif final_qty > pending_remaining:
                # Pending delivery is the tighter constraint
                errors.append(
                    f"Pending over-allocation: Allocating {final_qty:.0f} {standard_uom} would exceed delivery need. "
                    f"Undelivered would be {undelivered + final_qty:.0f} vs "
                    f"pending {pending_qty:.0f} {standard_uom}. "
                    f"Max allocatable: {allocatable_qty:.0f} {standard_uom}"
                )
            else:
                # Generic error
                errors.append(
                    f"Exceeds allocatable quantity: {final_qty:.0f} > {allocatable_qty:.0f} {standard_uom}"
                )
        
        # ===== CHECK 3: Supply availability (WARNING only) =====
        if final_qty > supply_available * 1.5:
            warnings.append(
                f"Large allocation: requesting {final_qty:.0f} {standard_uom} "
                f"(total product supply: {supply_available:.0f} {standard_uom})"
            )
        
        # Combine errors and warnings
        all_messages = errors + [f"⚠️ {w}" for w in warnings]
        
        return len(errors) == 0, all_messages
    
    # ==================== Bulk Validation ====================
    
    def validate_bulk_allocation(self, allocation_results: List[Dict],
                                 demands_df: pd.DataFrame,
                                 supply_df: pd.DataFrame,
                                 user_role: str) -> Dict[str, Any]:
        """
        Validate entire bulk allocation before commit
        
        Args:
            allocation_results: List of allocation results with final_qty
            demands_df: Original demand DataFrame
            supply_df: Supply DataFrame
            user_role: User's role
        
        Returns:
            Dict with:
            - valid: bool
            - errors: List of global errors
            - row_errors: Dict mapping ocd_id -> list of errors
            - warnings: List of warnings
        """
        result = {
            'valid': True,
            'errors': [],
            'row_errors': {},
            'warnings': []
        }
        
        # Permission check
        is_valid, error = self.validate_user_permission(user_role)
        if not is_valid:
            result['valid'] = False
            result['errors'].append(error)
            return result
        
        # Check if any allocations
        total_allocated = sum(float(r.get('final_qty', 0)) for r in allocation_results)
        if total_allocated <= 0:
            result['valid'] = False
            result['errors'].append("No quantities to allocate. Please adjust allocation amounts.")
            return result
        
        # Build supply dict
        supply_dict = {}
        if not supply_df.empty:
            for _, row in supply_df.iterrows():
                supply_dict[int(row['product_id'])] = float(row['available'])
        
        # Build demands lookup
        demands_lookup = {}
        if not demands_df.empty:
            for _, row in demands_df.iterrows():
                demands_lookup[int(row['ocd_id'])] = row.to_dict()
        
        # Track supply consumption per product
        supply_consumed = {}
        
        # First pass: Calculate total consumption per product
        for alloc in allocation_results:
            product_id = int(alloc.get('product_id', 0))
            final_qty = float(alloc.get('final_qty', 0))
            if final_qty > 0:
                supply_consumed[product_id] = supply_consumed.get(product_id, 0) + final_qty
        
        # Check total consumption vs supply (as warning, not blocking error)
        for product_id, consumed in supply_consumed.items():
            available = supply_dict.get(product_id, 0)
            if consumed > available + 0.01:  # Small tolerance for floating point
                result['warnings'].append(
                    f"Product {product_id}: Total allocation ({consumed:.0f}) exceeds available supply ({available:.0f})"
                )
        
        # Second pass: Validate each row (OC-level rules only, not supply)
        for alloc in allocation_results:
            ocd_id = int(alloc.get('ocd_id', 0))
            product_id = int(alloc.get('product_id', 0))
            final_qty = float(alloc.get('final_qty', 0))
            
            if final_qty <= 0:
                continue
            
            # Get OC info
            oc_info = demands_lookup.get(ocd_id, {})
            if not oc_info:
                result['row_errors'][ocd_id] = [f"OC not found in scope"]
                continue
            
            # Validate row (pass full supply - supply check is at product level, not row level)
            available_for_product = supply_dict.get(product_id, 0)
            is_valid, errors = self.validate_allocation_row(
                {'ocd_id': ocd_id, 'product_id': product_id, 'final_qty': final_qty},
                oc_info,
                available_for_product  # Full supply for OC-level check
            )
            
            if not is_valid:
                result['row_errors'][ocd_id] = errors
        
        # Check for any row errors
        if result['row_errors']:
            result['valid'] = False
            result['errors'].append(f"{len(result['row_errors'])} OC(s) have validation errors")
        
        # Warning for low coverage
        allocated_ocs = sum(1 for a in allocation_results if float(a.get('final_qty', 0)) > 0)
        total_ocs = len(allocation_results)
        if allocated_ocs < total_ocs:
            result['warnings'].append(
                f"{total_ocs - allocated_ocs} OC(s) will receive no allocation"
            )
        
        return result
    
    # ==================== ETD Validation ====================
    
    def validate_allocated_etd(self, allocated_etd: Any, oc_etd: Any) -> Tuple[bool, str]:
        """
        Validate allocated ETD date
        
        Returns:
            Tuple of (is_valid, warning_message)
        """
        try:
            if allocated_etd is None:
                return False, "Allocated ETD is required"
            
            # Convert to date
            if isinstance(allocated_etd, str):
                alloc_date = pd.to_datetime(allocated_etd).date()
            elif isinstance(allocated_etd, datetime):
                alloc_date = allocated_etd.date()
            elif isinstance(allocated_etd, date):
                alloc_date = allocated_etd
            else:
                return False, "Invalid allocated ETD format"
            
            # Compare with OC ETD
            if oc_etd:
                if isinstance(oc_etd, str):
                    oc_date = pd.to_datetime(oc_etd).date()
                elif isinstance(oc_etd, datetime):
                    oc_date = oc_etd.date()
                elif isinstance(oc_etd, date):
                    oc_date = oc_etd
                else:
                    oc_date = None
                
                if oc_date and alloc_date > oc_date:
                    days_delay = (alloc_date - oc_date).days
                    return True, f"Allocated ETD is {days_delay} days after requested ETD"
            
            return True, ""
            
        except Exception as e:
            return False, f"ETD validation error: {str(e)}"
    
    # ==================== Summary Validation ====================
    
    def generate_validation_summary(self, validation_result: Dict) -> str:
        """Generate human-readable validation summary"""
        lines = []
        
        if validation_result['valid']:
            lines.append("✅ Validation passed")
        else:
            lines.append("❌ Validation failed")
            
            if validation_result['errors']:
                lines.append("\nErrors:")
                for error in validation_result['errors']:
                    lines.append(f"  • {error}")
            
            if validation_result['row_errors']:
                lines.append(f"\nRow errors ({len(validation_result['row_errors'])} OCs):")
                for ocd_id, errors in list(validation_result['row_errors'].items())[:5]:
                    lines.append(f"  • OC {ocd_id}: {'; '.join(errors)}")
                if len(validation_result['row_errors']) > 5:
                    lines.append(f"  ... and {len(validation_result['row_errors']) - 5} more")
        
        if validation_result['warnings']:
            lines.append("\nWarnings:")
            for warning in validation_result['warnings']:
                lines.append(f"  ⚠️ {warning}")
        
        return "\n".join(lines)