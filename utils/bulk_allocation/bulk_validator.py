"""
Bulk Allocation Validator
=========================
Validation rules for bulk allocation operations.
Ensures data integrity and business rule compliance.
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
        Validate a single allocation row
        
        Args:
            row_data: Dict with keys: ocd_id, product_id, final_qty
            oc_info: Dict with OC information from view
            supply_available: Available supply for the product
        
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        
        final_qty = float(row_data.get('final_qty', 0))
        
        # Skip if no allocation
        if final_qty <= 0:
            return True, []
        
        # Check minimum allocation
        if final_qty < self.MIN_ALLOCATION_QTY:
            errors.append(f"Minimum allocation is {self.MIN_ALLOCATION_QTY}")
        
        # Get OC quantities
        effective_qty = float(oc_info.get('effective_qty', 0) or oc_info.get('standard_quantity', 0))
        pending_qty = float(oc_info.get('pending_qty', 0) or oc_info.get('pending_standard_delivery_quantity', 0))
        current_allocated = float(oc_info.get('allocated_qty', 0) or oc_info.get('total_effective_allocated_qty_standard', 0))
        undelivered = float(oc_info.get('undelivered_allocated', 0) or oc_info.get('undelivered_allocated_qty_standard', 0))
        
        standard_uom = oc_info.get('standard_uom', '')
        
        # Check 1: Total commitment vs effective OC quantity
        new_total_effective = current_allocated + final_qty
        max_allowed = effective_qty * (self.MAX_OVER_ALLOCATION_PERCENT / 100)
        
        if new_total_effective > max_allowed:
            errors.append(
                f"Over-commitment: {new_total_effective:.0f} {standard_uom} exceeds max {max_allowed:.0f} {standard_uom} "
                f"(100% of effective OC qty)"
            )
        
        # Check 2: Pending over-allocation
        new_undelivered = undelivered + final_qty
        if new_undelivered > pending_qty:
            errors.append(
                f"Pending over-allocation: undelivered would be {new_undelivered:.0f} {standard_uom}, "
                f"but only {pending_qty:.0f} {standard_uom} pending delivery"
            )
        
        # Check 3: Supply availability
        if final_qty > supply_available:
            errors.append(
                f"Insufficient supply: requesting {final_qty:.0f} {standard_uom} "
                f"but only {supply_available:.0f} {standard_uom} available"
            )
        
        return len(errors) == 0, errors
    
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
        
        # Validate each row
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
            
            # Calculate remaining supply after previous allocations
            consumed = supply_consumed.get(product_id, 0)
            remaining_supply = supply_dict.get(product_id, 0) - consumed
            
            # Validate row
            is_valid, errors = self.validate_allocation_row(
                {'ocd_id': ocd_id, 'product_id': product_id, 'final_qty': final_qty},
                oc_info,
                remaining_supply
            )
            
            if not is_valid:
                result['row_errors'][ocd_id] = errors
            else:
                # Track supply consumption
                supply_consumed[product_id] = consumed + final_qty
        
        # Check for any row errors
        if result['row_errors']:
            result['valid'] = False
            result['errors'].append(f"{len(result['row_errors'])} OC(s) have validation errors")
        
        # Add warnings for edge cases
        for product_id, consumed in supply_consumed.items():
            available = supply_dict.get(product_id, 0)
            if consumed > available:
                result['warnings'].append(
                    f"Product {product_id}: Total allocation ({consumed:.0f}) exceeds available supply ({available:.0f})"
                )
        
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
