"""
Allocation Service for Business Logic - Complete Fixed Version
Core business logic for allocation operations with proper UOM handling
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import json
from sqlalchemy import text
from contextlib import contextmanager
import streamlit as st
from threading import local

from ..db import get_db_engine
from ..config import config
from .data_service import AllocationDataService
from .uom_converter import UOMConverter

logger = logging.getLogger(__name__)

# ==================== CUSTOM EXCEPTIONS ====================
class AllocationError(Exception):
    """Base exception for allocation errors"""
    pass

class OverAllocationError(AllocationError):
    """Raised when allocation exceeds limits"""
    def __init__(self, requested: float, maximum: float, uom: str = ''):
        self.requested = requested
        self.maximum = maximum
        self.uom = uom
        super().__init__(
            f"Requested quantity {requested:.0f} {uom} exceeds maximum allowed {maximum:.0f} {uom}"
        )

class InsufficientSupplyError(AllocationError):
    """Raised when supply is insufficient"""
    def __init__(self, available: float, requested: float, uom: str = ''):
        self.available = available
        self.requested = requested
        self.uom = uom
        super().__init__(
            f"Insufficient supply. Available: {available:.0f} {uom}, Requested: {requested:.0f} {uom}"
        )

class InvalidAllocationModeError(AllocationError):
    """Raised when allocation mode is invalid for operation"""
    pass

class AllocationNotFoundError(AllocationError):
    """Raised when allocation is not found"""
    pass

# ==================== ALLOCATION SERVICE ====================
class AllocationService:
    """Service for handling allocation business logic with improved error handling"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.data_service = AllocationDataService()
        self.uom_converter = UOMConverter()
        
        # Configuration from settings
        self.MAX_OVER_ALLOCATION_PERCENT = config.get_app_setting('MAX_OVER_ALLOCATION_PERCENT', 110)
        self.MIN_ALLOCATION_QTY = config.get_app_setting('MIN_ALLOCATION_QTY', 0.01)
        
        # Thread local storage for nested transactions
        self._local = local()
    
    @property
    def _current_transaction(self):
        """Get current transaction from thread local storage"""
        return getattr(self._local, 'transaction', None)
    
    @_current_transaction.setter
    def _current_transaction(self, value):
        """Set current transaction in thread local storage"""
        self._local.transaction = value
    
    @contextmanager
    def db_transaction(self, savepoint: bool = False):
        """
        Improved context manager for database transactions with savepoint support
        
        Args:
            savepoint: Whether to use savepoint for nested transactions
        """
        # Check if we're in a nested transaction
        if self._current_transaction:
            if savepoint:
                # Use savepoint for nested transaction
                sp = self._current_transaction.begin_nested()
                try:
                    yield self._current_transaction
                    sp.commit()
                except Exception:
                    sp.rollback()
                    raise
            else:
                # Use existing transaction without savepoint
                yield self._current_transaction
        else:
            # New transaction
            conn = self.engine.connect()
            trans = conn.begin()
            self._current_transaction = conn
            try:
                yield conn
                trans.commit()
                logger.debug("Transaction committed successfully")
            except Exception as e:
                trans.rollback()
                logger.error(f"Transaction rolled back due to error: {e}")
                raise
            finally:
                self._current_transaction = None
                conn.close()
    
    def _log_action(self, action: str, entity_type: str, entity_id: Any, 
                   user_id: int, details: Dict = None):
        """
        Log action for audit trail
        
        Args:
            action: Action performed (CREATE, UPDATE, CANCEL, etc.)
            entity_type: Type of entity (ALLOCATION, OC, etc.)
            entity_id: ID of the entity
            user_id: User performing the action
            details: Additional details
        """
        try:
            with self.db_transaction() as conn:
                query = text("""
                    INSERT INTO audit_logs 
                    (action, entity_type, entity_id, user_id, details, created_at)
                    VALUES (:action, :entity_type, :entity_id, :user_id, :details, NOW())
                """)
                
                conn.execute(query, {
                    'action': action,
                    'entity_type': entity_type,
                    'entity_id': str(entity_id),
                    'user_id': user_id,
                    'details': json.dumps(details) if details else None
                })
        except Exception as e:
            # Don't fail main operation if audit log fails
            logger.error(f"Failed to create audit log: {e}")
    
    def create_allocation(self, oc_detail_id: int, allocations: List[Dict], 
                         mode: str, etd: datetime, notes: str, 
                         user_id: int) -> Dict[str, Any]:
        """
        Create new allocation with improved error handling
        
        Args:
            oc_detail_id: OC detail ID to allocate for
            allocations: List of allocation items with source info and quantity
            mode: SOFT or HARD
            etd: Allocated ETD date
            notes: Optional notes
            user_id: User creating the allocation
            
        Returns:
            Dictionary with success status and result
        """
        try:
            with self.db_transaction() as conn:
                # Get OC detail information
                oc_info = self._get_oc_detail_info(conn, oc_detail_id)
                if not oc_info:
                    raise AllocationNotFoundError(f"Order confirmation {oc_detail_id} not found")
                
                # Validate allocations before processing
                validation_result = self._validate_allocation_request(
                    conn, oc_info, allocations, mode
                )
                
                if not validation_result['valid']:
                    raise AllocationError(validation_result['error'])
                
                # Generate allocation number
                allocation_number = self._generate_allocation_number(conn)
                
                # Create allocation context with correct field names and UOM info
                allocation_context = self._create_allocation_context(
                    oc_info, allocations, mode, user_id
                )
                
                # Create allocation plan
                plan_query = text("""
                    INSERT INTO allocation_plans 
                    (allocation_number, allocation_date, creator_id, notes, allocation_context)
                    VALUES (:allocation_number, NOW(), :creator_id, :notes, :allocation_context)
                """)
                
                result = conn.execute(plan_query, {
                    'allocation_number': allocation_number,
                    'creator_id': user_id,
                    'notes': notes,
                    'allocation_context': json.dumps(allocation_context)
                })
                
                allocation_plan_id = result.lastrowid
                
                # Create allocation details
                detail_ids = []
                total_allocated = 0
                
                for alloc in allocations:
                    detail_id, allocated_qty = self._create_allocation_detail(
                        conn, allocation_plan_id, oc_info, alloc, mode, etd
                    )
                    detail_ids.append(detail_id)
                    total_allocated += allocated_qty
                
                # Log action
                self._log_action(
                    'CREATE_ALLOCATION',
                    'ALLOCATION',
                    allocation_number,
                    user_id,
                    {
                        'oc_detail_id': oc_detail_id,
                        'mode': mode,
                        'total_allocated': total_allocated,
                        'allocation_count': len(allocations)
                    }
                )
                
                # Clear cache to reflect new data
                st.cache_data.clear()
                
                logger.info(
                    f"Created allocation {allocation_number} with {len(allocations)} items, "
                    f"total qty: {total_allocated}"
                )
                
                return {
                    'success': True,
                    'allocation_number': allocation_number,
                    'allocation_plan_id': allocation_plan_id,
                    'detail_ids': detail_ids,
                    'total_allocated': total_allocated
                }
                
        except AllocationError as e:
            logger.warning(f"Allocation validation error: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error creating allocation: {e}", exc_info=True)
            return {
                'success': False,
                'error': "Unable to create allocation. Please try again or contact support.",
                'technical_error': str(e)
            }
    
    def cancel_allocation(self, allocation_detail_id: int, cancelled_qty: float,
                         reason: str, reason_category: str, user_id: int) -> Dict[str, Any]:
        """Cancel allocation with improved error handling"""
        try:
            with self.db_transaction() as conn:
                # Get allocation detail info with pending quantity
                detail = self._get_allocation_detail_with_pending(conn, allocation_detail_id)
                
                if not detail:
                    raise AllocationNotFoundError(f"Allocation {allocation_detail_id} not found")
                
                # Validate cancellation
                pending_qty = detail.get('pending_qty', 0)
                
                if cancelled_qty <= 0:
                    raise AllocationError("Cancel quantity must be positive")
                
                if cancelled_qty > pending_qty:
                    raise AllocationError(
                        f"Cannot cancel {cancelled_qty:.0f}. "
                        f"Only {pending_qty:.0f} pending (not yet delivered)"
                    )
                
                if pending_qty <= 0:
                    raise AllocationError("Cannot cancel - all quantity has been delivered")
                
                # Check allocation mode
                if detail['allocation_mode'] == 'HARD':
                    raise InvalidAllocationModeError(
                        "Cannot cancel HARD allocation. Please contact manager for approval"
                    )
                
                # Insert cancellation record
                cancel_query = text("""
                    INSERT INTO allocation_cancellations (
                        allocation_detail_id, allocation_plan_id, cancelled_qty,
                        reason, reason_category, cancelled_by_user_id, cancelled_date
                    ) VALUES (
                        :allocation_detail_id, :allocation_plan_id, :cancelled_qty,
                        :reason, :reason_category, :cancelled_by_user_id, NOW()
                    )
                """)
                
                result = conn.execute(cancel_query, {
                    'allocation_detail_id': allocation_detail_id,
                    'allocation_plan_id': detail['allocation_plan_id'],
                    'cancelled_qty': cancelled_qty,
                    'reason': reason,
                    'reason_category': reason_category,
                    'cancelled_by_user_id': user_id
                })
                
                cancellation_id = result.lastrowid
                
                # Log action
                self._log_action(
                    'CANCEL_ALLOCATION',
                    'ALLOCATION_DETAIL',
                    allocation_detail_id,
                    user_id,
                    {
                        'cancelled_qty': cancelled_qty,
                        'reason_category': reason_category,
                        'pending_before': pending_qty,
                        'pending_after': pending_qty - cancelled_qty
                    }
                )
                
                # Clear cache
                st.cache_data.clear()
                
                delivered_qty = detail.get('delivered_qty', 0) or 0
                logger.info(
                    f"Cancelled {cancelled_qty} from allocation detail {allocation_detail_id}. "
                    f"Delivered: {delivered_qty}, Remaining pending: {pending_qty - cancelled_qty}"
                )
                
                return {
                    'success': True,
                    'cancellation_id': cancellation_id,
                    'cancelled_qty': cancelled_qty,
                    'remaining_pending_qty': pending_qty - cancelled_qty,
                    'delivered_qty': delivered_qty
                }
                
        except AllocationError as e:
            logger.warning(f"Allocation cancellation error: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error cancelling allocation: {e}", exc_info=True)
            return {
                'success': False,
                'error': "Unable to cancel allocation. Please try again or contact support.",
                'technical_error': str(e)
            }
    
    def update_allocation_etd(self, allocation_detail_id: int, new_etd: datetime,
                             user_id: int) -> Dict[str, Any]:
        """Update allocated ETD with improved error handling"""
        try:
            with self.db_transaction() as conn:
                # Check allocation details with pending quantity
                detail = self._get_allocation_detail_for_update(conn, allocation_detail_id)
                
                if not detail:
                    raise AllocationNotFoundError(f"Allocation {allocation_detail_id} not found")
                
                # Validation
                if detail['allocation_mode'] == 'HARD':
                    raise InvalidAllocationModeError(
                        "Cannot update ETD for HARD allocation without manager approval"
                    )
                
                if detail['status'] != 'ALLOCATED':
                    raise AllocationError("Can only update ETD for ALLOCATED status")
                
                pending_qty = detail.get('pending_qty', 0)
                if pending_qty <= 0:
                    raise AllocationError("Cannot update ETD - all quantity has been delivered")
                
                # Store old ETD for logging
                old_etd = detail['allocated_etd']
                
                # Update ETD with tracking fields
                update_query = text("""
                    UPDATE allocation_details 
                    SET 
                        allocated_etd = :new_etd,
                        last_updated_etd_date = NOW(),
                        etd_update_count = etd_update_count + 1
                    WHERE id = :detail_id
                """)
                
                conn.execute(update_query, {
                    'new_etd': new_etd,
                    'detail_id': allocation_detail_id
                })
                
                # Log action
                self._log_action(
                    'UPDATE_ALLOCATION_ETD',
                    'ALLOCATION_DETAIL',
                    allocation_detail_id,
                    user_id,
                    {
                        'old_etd': str(old_etd),
                        'new_etd': str(new_etd),
                        'update_count': detail.get('etd_update_count', 0) + 1,
                        'pending_qty_affected': pending_qty
                    }
                )
                
                # Clear cache
                st.cache_data.clear()
                
                delivered_qty = detail.get('delivered_qty', 0) or 0
                update_count = (detail.get('etd_update_count', 0) or 0) + 1
                logger.info(
                    f"Updated ETD for allocation detail {allocation_detail_id} "
                    f"from {old_etd} to {new_etd}. "
                    f"Update #{update_count}. Delivered: {delivered_qty}, Pending: {pending_qty}"
                )
                
                return {
                    'success': True,
                    'new_etd': new_etd,
                    'update_count': update_count,
                    'pending_qty_affected': pending_qty
                }
                
        except AllocationError as e:
            logger.warning(f"ETD update error: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error updating ETD: {e}", exc_info=True)
            return {
                'success': False,
                'error': "Unable to update ETD. Please try again or contact support.",
                'technical_error': str(e)
            }
    
    def reverse_cancellation(self, cancellation_id: int, reversal_reason: str,
                           user_id: int) -> Dict[str, Any]:
        """Reverse a cancellation with improved error handling"""
        try:
            with self.db_transaction() as conn:
                # Check if cancellation exists and is active
                check_query = text("""
                    SELECT 
                        ac.status,
                        ac.allocation_detail_id,
                        ac.cancelled_qty,
                        ad.delivered_qty
                    FROM allocation_cancellations ac
                    INNER JOIN allocation_details ad ON ac.allocation_detail_id = ad.id
                    WHERE ac.id = :cancellation_id
                """)
                
                result = conn.execute(check_query, {'cancellation_id': cancellation_id}).fetchone()
                
                if not result:
                    raise AllocationNotFoundError(f"Cancellation {cancellation_id} not found")
                
                if result._mapping['status'] != 'ACTIVE':
                    raise AllocationError("Cancellation has already been reversed")
                
                cancelled_qty = result._mapping['cancelled_qty']
                
                # Update cancellation status
                update_query = text("""
                    UPDATE allocation_cancellations 
                    SET 
                        status = 'REVERSED',
                        reversed_by_user_id = :user_id,
                        reversed_date = NOW(),
                        reversal_reason = :reason
                    WHERE id = :cancellation_id
                """)
                
                conn.execute(update_query, {
                    'user_id': user_id,
                    'reason': reversal_reason,
                    'cancellation_id': cancellation_id
                })
                
                # Log action
                self._log_action(
                    'REVERSE_CANCELLATION',
                    'CANCELLATION',
                    cancellation_id,
                    user_id,
                    {
                        'restored_qty': cancelled_qty,
                        'reversal_reason': reversal_reason
                    }
                )
                
                # Clear cache
                st.cache_data.clear()
                
                logger.info(
                    f"Reversed cancellation {cancellation_id}. "
                    f"Restored quantity: {cancelled_qty}"
                )
                
                return {
                    'success': True,
                    'restored_qty': cancelled_qty
                }
                
        except AllocationError as e:
            logger.warning(f"Cancellation reversal error: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error reversing cancellation: {e}", exc_info=True)
            return {
                'success': False,
                'error': "Unable to reverse cancellation. Please try again or contact support.",
                'technical_error': str(e)
            }
    
    # ==================== HELPER METHODS ====================
    
    def _create_allocation_context(self, oc_info: Dict, allocations: List[Dict],
                                  mode: str, user_id: int) -> Dict:
        """Create allocation context for audit trail"""
        return {
            'oc_detail': {
                'id': oc_info['ocd_id'],
                'oc_number': oc_info['oc_number'],
                'customer': oc_info['customer_name'],
                'product': oc_info['product_name'],
                'pending_qty_standard': float(oc_info.get('pending_standard_delivery_quantity', oc_info['pending_quantity'])),
                'pending_qty_selling': float(oc_info.get('pending_quantity', 
                                                        oc_info['pending_quantity'])),
                'standard_uom': oc_info['standard_uom'],
                'selling_uom': oc_info.get('selling_uom', oc_info['standard_uom']),
                'uom_conversion': oc_info.get('uom_conversion', '1')
            },
            'allocations': [
                {
                    'source_type': alloc.get('source_type'),
                    'source_id': alloc.get('source_id'),
                    'quantity': float(alloc['quantity']),  # Always in standard UOM
                    'source_info': {
                        k: v for k, v in alloc.get('supply_info', {}).items() 
                        if k in ['buying_uom', 'standard_uom', 'uom_conversion', 'reference',
                                'batch_number', 'arrival_note_number', 'po_number']
                    }
                } for alloc in allocations
            ],
            'mode': mode,
            'created_by': user_id,
            'created_at': datetime.now().isoformat()
        }
    
    def _create_allocation_detail(self, conn, allocation_plan_id: int, oc_info: Dict,
                                 alloc: Dict, mode: str, etd: datetime) -> Tuple[int, float]:
        """Create single allocation detail record"""
        # Determine supply source info
        if mode == 'SOFT' or not alloc.get('source_type'):
            supply_source_type = None
            supply_source_id = None
            source_description = "Not specified (SOFT allocation)"
        else:
            supply_source_type = alloc['source_type']
            supply_source_id = alloc['source_id']
            source_description = self._get_source_description(alloc)
        
        # Insert allocation detail
        detail_query = text("""
            INSERT INTO allocation_details (
                allocation_plan_id, allocation_mode, demand_type, 
                demand_reference_id, demand_number, product_id, pt_code,
                customer_code, customer_name, legal_entity_name,
                requested_qty, allocated_qty, delivered_qty,
                etd, allocated_etd, status, notes,
                supply_source_type, supply_source_id,
                etd_update_count, last_updated_etd_date
            ) VALUES (
                :allocation_plan_id, :allocation_mode, 'OC',
                :demand_reference_id, :demand_number, :product_id, :pt_code,
                :customer_code, :customer_name, :legal_entity_name,
                :requested_qty, :allocated_qty, 0,
                :etd, :allocated_etd, 'ALLOCATED', :notes,
                :supply_source_type, :supply_source_id,
                0, NULL
            )
        """)
        
        # Use standard UOM for storage
        requested_qty_standard = oc_info.get('pending_standard_delivery_quantity', oc_info['pending_quantity'])
        
        result = conn.execute(detail_query, {
            'allocation_plan_id': allocation_plan_id,
            'allocation_mode': mode,
            'demand_reference_id': oc_info['ocd_id'],
            'demand_number': oc_info['oc_number'],
            'product_id': oc_info['product_id'],
            'pt_code': oc_info['pt_code'],
            'customer_code': oc_info['customer_code'],
            'customer_name': oc_info['customer_name'],
            'legal_entity_name': oc_info['legal_entity'],
            'requested_qty': requested_qty_standard,  # Standard UOM
            'allocated_qty': alloc['quantity'],  # Standard UOM
            'etd': oc_info['etd'],
            'allocated_etd': etd,
            'notes': f"Source: {source_description}",
            'supply_source_type': supply_source_type,
            'supply_source_id': supply_source_id
        })
        
        return result.lastrowid, alloc['quantity']
    
    def _validate_allocation_request(self, conn, oc_info: Dict, 
                                   allocations: List[Dict], mode: str) -> Dict[str, Any]:
        """
        Comprehensive validation for allocation request with improved error messages
        
        Returns:
            Dict with 'valid' boolean and 'error' message if invalid
        """
        # Check if allocations list is empty
        if not allocations:
            return {
                'valid': False,
                'error': 'Please select at least one supply source'
            }
        
        # Calculate total to be allocated (in standard UOM)
        total_to_allocate = sum(alloc['quantity'] for alloc in allocations)
        
        # Basic quantity validation
        if total_to_allocate <= 0:
            return {
                'valid': False,
                'error': 'Total allocation quantity must be greater than zero'
            }
        
        # Check over-allocation - FIX: Use pending_standard_delivery_quantity
        pending_qty_standard = float(oc_info.get('pending_standard_delivery_quantity', oc_info['pending_quantity']))
        max_allowed = pending_qty_standard * (self.MAX_OVER_ALLOCATION_PERCENT / 100)
        standard_uom = oc_info.get('standard_uom', '')
        
        if total_to_allocate > max_allowed:
            over_percent = ((total_to_allocate - pending_qty_standard) / pending_qty_standard * 100)
            
            # Build error message with UOM context
            error_msg = (
                f"Over-allocation limit exceeded. "
                f"Requested: {total_to_allocate:.0f} {standard_uom} "
                f"({over_percent:.1f}% over). "
                f"Maximum allowed: {max_allowed:.0f} {standard_uom} "
                f"({self.MAX_OVER_ALLOCATION_PERCENT - 100}% over)"
            )
            
            # Add selling UOM reference if different
            if oc_info.get('selling_uom') and oc_info.get('selling_uom') != standard_uom:
                if self.uom_converter.needs_conversion(oc_info.get('uom_conversion', '1')):
                    # Convert to selling UOM for user reference
                    total_selling = self.uom_converter.convert_quantity(
                        total_to_allocate,
                        'standard',
                        'selling',
                        oc_info.get('uom_conversion', '1')
                    )
                    max_selling = self.uom_converter.convert_quantity(
                        max_allowed,
                        'standard',
                        'selling',
                        oc_info.get('uom_conversion', '1')
                    )
                    pending_selling = oc_info.get('pending_quantity', pending_qty_standard)
                    
                    error_msg += (
                        f"\n\nFor reference in selling UOM: "
                        f"{total_selling:.0f} {oc_info['selling_uom']} exceeds "
                        f"{max_selling:.0f} {oc_info['selling_uom']} "
                        f"(110% of {pending_selling:.0f} {oc_info['selling_uom']})"
                    )
            
            return {
                'valid': False,
                'error': error_msg
            }
        
        # For SOFT allocation, check total supply availability
        if mode == 'SOFT':
            total_supply = self.data_service.get_total_available_supply(oc_info['product_id'])
            if not total_supply['has_supply']:
                return {
                    'valid': False,
                    'error': 'No available supply found for this product'
                }
            
            if total_to_allocate > total_supply['total_available']:
                return {
                    'valid': False,
                    'error': (
                        f"Insufficient supply. "
                        f"Available: {total_supply['total_available']:.0f} {standard_uom}, "
                        f"Requested: {total_to_allocate:.0f} {standard_uom}"
                    )
                }
        else:
            # For HARD allocation, validate each specific source
            for idx, alloc in enumerate(allocations):
                if not alloc.get('source_type') or not alloc.get('source_id'):
                    return {
                        'valid': False,
                        'error': 'HARD allocation requires specific supply source for all items'
                    }
                
                # Check availability
                availability = self.data_service.check_supply_availability(
                    alloc['source_type'],
                    alloc['source_id'],
                    oc_info['product_id']
                )
                
                if not availability['available']:
                    source_name = alloc.get('supply_info', {}).get('reference', alloc['source_type'])
                    return {
                        'valid': False,
                        'error': f'{source_name} is no longer available'
                    }
                
                if alloc['quantity'] > availability['available_qty']:
                    source_name = alloc.get('supply_info', {}).get('reference', alloc['source_type'])
                    return {
                        'valid': False,
                        'error': (
                            f"Insufficient quantity in {source_name}. "
                            f"Available: {availability['available_qty']:.0f} {standard_uom}, "
                            f"Requested: {alloc['quantity']:.0f} {standard_uom}"
                        )
                    }
        
        # Check for duplicate allocations in the same request
        if mode == 'HARD':
            source_keys = []
            for alloc in allocations:
                key = f"{alloc['source_type']}_{alloc['source_id']}"
                if key in source_keys:
                    return {
                        'valid': False,
                        'error': 'Cannot allocate from the same source multiple times in one allocation'
                    }
                source_keys.append(key)
        
        return {'valid': True}
    
    def _generate_allocation_number(self, conn) -> str:
        """Generate unique allocation number"""
        try:
            # Get current year and month
            now = datetime.now()
            year_month = now.strftime('%Y%m')
            
            # Get last allocation number for this month
            query = text("""
                SELECT allocation_number 
                FROM allocation_plans 
                WHERE allocation_number LIKE :prefix
                ORDER BY id DESC 
                LIMIT 1
            """)
            
            prefix = f"ALL-{year_month}-%"
            result = conn.execute(query, {'prefix': prefix}).fetchone()
            
            if result:
                # Extract sequence number and increment
                last_number = result[0]
                sequence = int(last_number.split('-')[-1]) + 1
            else:
                sequence = 1
            
            return f"ALL-{year_month}-{sequence:04d}"
            
        except Exception as e:
            logger.error(f"Error generating allocation number: {e}")
            # Fallback to timestamp-based number
            return f"ALL-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    def _get_oc_detail_info(self, conn, oc_detail_id: int) -> Optional[Dict]:
        """Get OC detail information for allocation with full UOM info"""
        try:
            query = text("""
                SELECT 
                    ocd_id,
                    oc_number,
                    customer_code,
                    customer as customer_name,
                    legal_entity,
                    product_id,
                    product_name,
                    pt_code,
                    etd,
                    pending_standard_delivery_quantity,
                    pending_selling_delivery_quantity as pending_quantity,
                    selling_uom,
                    standard_uom,
                    uom_conversion
                FROM outbound_oc_pending_delivery_view
                WHERE ocd_id = :oc_detail_id
            """)
            
            result = conn.execute(query, {'oc_detail_id': oc_detail_id}).fetchone()
            
            if result:
                return dict(result._mapping)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting OC detail info: {e}")
            return None
    
    def _get_allocation_detail_with_pending(self, conn, allocation_detail_id: int) -> Optional[Dict]:
        """Get allocation detail with pending quantity calculation"""
        detail_query = text("""
            SELECT 
                ad.*,
                (ad.allocated_qty - COALESCE(ac.cancelled_qty, 0)) as effective_qty,
                (ad.allocated_qty - COALESCE(ac.cancelled_qty, 0) - COALESCE(adl.delivered_qty, 0)) as pending_qty
            FROM allocation_details ad
            LEFT JOIN (
                SELECT 
                    allocation_detail_id,
                    SUM(CASE WHEN status = 'ACTIVE' THEN cancelled_qty ELSE 0 END) as cancelled_qty
                FROM allocation_cancellations
                GROUP BY allocation_detail_id
            ) ac ON ad.id = ac.allocation_detail_id
            LEFT JOIN (
                SELECT 
                    allocation_detail_id,
                    SUM(delivered_qty) as delivered_qty
                FROM allocation_delivery_links
                GROUP BY allocation_detail_id
            ) adl ON ad.id = adl.allocation_detail_id
            WHERE ad.id = :detail_id
        """)
        
        result = conn.execute(detail_query, {'detail_id': allocation_detail_id}).fetchone()
        
        if result:
            return dict(result._mapping)
        return None
    
    def _get_allocation_detail_for_update(self, conn, allocation_detail_id: int) -> Optional[Dict]:
        """Get allocation detail for ETD update"""
        check_query = text("""
            SELECT 
                ad.allocation_mode,
                ad.status,
                ad.delivered_qty,
                ad.etd_update_count,
                ad.allocated_etd,
                (ad.allocated_qty - COALESCE(ac.cancelled_qty, 0) - COALESCE(adl.delivered_qty, 0)) as pending_qty
            FROM allocation_details ad
            LEFT JOIN (
                SELECT 
                    allocation_detail_id,
                    SUM(CASE WHEN status = 'ACTIVE' THEN cancelled_qty ELSE 0 END) as cancelled_qty
                FROM allocation_cancellations
                GROUP BY allocation_detail_id
            ) ac ON ad.id = ac.allocation_detail_id
            LEFT JOIN (
                SELECT 
                    allocation_detail_id,
                    SUM(delivered_qty) as delivered_qty
                FROM allocation_delivery_links
                GROUP BY allocation_detail_id
            ) adl ON ad.id = adl.allocation_detail_id
            WHERE ad.id = :detail_id
        """)
        
        result = conn.execute(check_query, {'detail_id': allocation_detail_id}).fetchone()
        
        if result:
            return dict(result._mapping)
        return None
    
    def _get_source_description(self, allocation: Dict) -> str:
        """Get human-readable description for supply source"""
        source_type = allocation.get('source_type', '')
        supply_info = allocation.get('supply_info', {})
        
        if source_type == 'INVENTORY':
            return f"Inventory Batch {supply_info.get('batch_number', 'N/A')}"
        elif source_type == 'PENDING_CAN':
            desc = f"CAN {supply_info.get('arrival_note_number', 'N/A')}"
            if supply_info.get('buying_uom') and self.uom_converter.needs_conversion(supply_info.get('uom_conversion', '1')):
                desc += f" (Buying: {supply_info['buying_uom']})"
            return desc
        elif source_type == 'PENDING_PO':
            desc = f"PO {supply_info.get('po_number', 'N/A')}"
            if supply_info.get('buying_uom') and self.uom_converter.needs_conversion(supply_info.get('uom_conversion', '1')):
                desc += f" (Buying: {supply_info['buying_uom']})"
            return desc
        elif source_type == 'PENDING_WHT':
            return f"Transfer {supply_info.get('from_warehouse', 'N/A')} → {supply_info.get('to_warehouse', 'N/A')}"
        else:
            return source_type