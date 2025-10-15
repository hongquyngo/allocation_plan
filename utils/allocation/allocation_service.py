"""
Allocation Service for Business Logic - REFACTORED
Core business logic with improved committed quantity calculation using MIN logic
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, Union
from decimal import Decimal
import json
from sqlalchemy import text
from contextlib import contextmanager
import streamlit as st
from threading import local

from ..db import get_db_engine
from ..config import config
from .supply_data import SupplyData
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
            f"Cannot allocate {requested:.0f} {uom}. "
            f"Maximum allowed is {maximum:.0f} {uom} (100% of pending quantity). "
            f"Please reduce the allocation amount or contact a manager for approval."
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

class AllocationNotFoundError(AllocationError):
    """Raised when allocation is not found"""
    pass

class InvalidUserError(AllocationError):
    """Raised when user validation fails"""
    pass

# ==================== ALLOCATION SERVICE ====================
class AllocationService:
    """Service for handling allocation business logic with proper user validation"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.supply_data = SupplyData()
        self.uom_converter = UOMConverter()
        
        # Configuration
        self.MAX_OVER_ALLOCATION_PERCENT = config.get_app_setting('MAX_OVER_ALLOCATION_PERCENT', 100)
        self.MIN_ALLOCATION_QTY = config.get_app_setting('MIN_ALLOCATION_QTY', 0.01)
        
        # Thread local storage for nested transactions
        self._local = local()
    
    # ==================== TYPE CONVERSION HELPERS ====================
    def _to_decimal(self, value: Union[None, int, float, str, Decimal]) -> Decimal:
        """
        Safely convert any value to Decimal
        Handles: float, int, str, Decimal, None, pandas/numpy types
        """
        if value is None:
            return Decimal('0')
        elif isinstance(value, Decimal):
            return value
        elif isinstance(value, (int, float)):
            # Convert to string first to avoid float precision issues
            return Decimal(str(value))
        elif isinstance(value, str):
            return Decimal(value.strip() if value.strip() else '0')
        else:
            # Handle numpy/pandas types
            try:
                if hasattr(value, 'item'):  # numpy scalar
                    return Decimal(str(value.item()))
                return Decimal(str(value))
            except:
                return Decimal('0')
    
    def _to_float(self, value: Union[None, int, float, str, Decimal]) -> float:
        """
        Safely convert any value to float
        Handles: Decimal, float, int, str, None, pandas/numpy types
        """
        if value is None:
            return 0.0
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, float):
            return value
        elif isinstance(value, (int, str)):
            return float(value)
        else:
            # Handle numpy/pandas types
            try:
                if hasattr(value, 'item'):  # numpy scalar
                    return float(value.item())
                return float(str(value))
            except:
                return 0.0
    
    # ==================== TRANSACTION MANAGEMENT ====================
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
        """Context manager for database transactions with savepoint support"""
        if self._current_transaction:
            if savepoint:
                sp = self._current_transaction.begin_nested()
                try:
                    yield self._current_transaction
                    sp.commit()
                except Exception:
                    sp.rollback()
                    raise
            else:
                yield self._current_transaction
        else:
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
    
    # ==================== USER VALIDATION HELPER ====================
    def _validate_user_id(self, conn, user_id: int) -> Tuple[bool, Optional[str], Optional[Dict]]:
        """
        Validate that user exists and is active
        
        Returns:
            Tuple of (is_valid, error_message, user_info)
        """
        if not user_id:
            return False, "User ID is required for this operation", None
        
        try:
            query = text("""
                SELECT 
                    u.id,
                    u.username,
                    u.role,
                    u.is_active,
                    u.email,
                    CONCAT(COALESCE(e.first_name, u.username), ' ', COALESCE(e.last_name, '')) as full_name
                FROM users u
                LEFT JOIN employees e ON u.employee_id = e.id
                WHERE u.id = :user_id
                AND u.delete_flag = 0
            """)
            
            result = conn.execute(query, {'user_id': user_id}).fetchone()
            
            if not result:
                logger.error(f"User ID {user_id} not found in database")
                return False, f"User account not found. Please login again.", None
            
            user_data = dict(result._mapping)
            
            if not user_data['is_active']:
                logger.error(f"User ID {user_id} ({user_data['username']}) is inactive")
                return False, "Your account is inactive. Please contact an administrator.", None
            
            logger.debug(f"User validated: {user_data['username']} (ID: {user_id}, Role: {user_data['role']})")
            return True, None, user_data
            
        except Exception as e:
            logger.error(f"Error validating user {user_id}: {e}")
            return False, "Unable to validate user session. Please login again.", None
    
    # ==================== CREATE ALLOCATION ====================
    def create_allocation(self, oc_detail_id: int, allocations: List[Dict], 
                         mode: str, etd: datetime, notes: str, 
                         user_id: int) -> Dict[str, Any]:
        """Create new allocation with proper user validation"""
        try:
            # Early validation of user_id
            if not user_id:
                logger.error("create_allocation called without user_id")
                return {
                    'success': False,
                    'error': "Session error. Please login again."
                }
            
            with self.db_transaction() as conn:
                # Validate user exists and is active
                user_valid, user_error, user_info = self._validate_user_id(conn, user_id)
                if not user_valid:
                    return {
                        'success': False,
                        'error': user_error
                    }
                
                # Get OC detail information
                oc_info = self._get_oc_detail_info(conn, oc_detail_id)
                if not oc_info:
                    raise AllocationNotFoundError(f"Order confirmation {oc_detail_id} not found")
                
                # Validate allocations
                validation_result = self._validate_allocation_request(
                    conn, oc_info, allocations, mode
                )
                
                if not validation_result['valid']:
                    raise AllocationError(validation_result['error'])
                
                # Generate allocation number
                allocation_number = self._generate_allocation_number(conn)
                
                # Create allocation context with user info
                allocation_context = self._create_allocation_context(
                    oc_info, allocations, mode, user_id, user_info
                )
                
                # Create allocation plan with validated user_id
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
                
                # Log allocation creation with user info
                logger.info(
                    f"User {user_info['username']} (ID: {user_id}, Role: {user_info['role']}) "
                    f"created allocation plan {allocation_number} (Plan ID: {allocation_plan_id})"
                )
                
                # Create allocation details
                detail_ids = []
                total_allocated = Decimal('0')
                
                for alloc in allocations:
                    detail_id, allocated_qty = self._create_allocation_detail(
                        conn, allocation_plan_id, oc_info, alloc, mode, etd
                    )
                    detail_ids.append(detail_id)
                    total_allocated += allocated_qty
                
                # Clear cache
                st.cache_data.clear()
                
                logger.info(
                    f"Successfully created allocation {allocation_number} by user {user_info['username']} "
                    f"with {len(allocations)} items, total qty: {self._to_float(total_allocated):.0f}"
                )
                
                return {
                    'success': True,
                    'allocation_number': allocation_number,
                    'allocation_plan_id': allocation_plan_id,
                    'detail_ids': detail_ids,
                    'total_allocated': self._to_float(total_allocated),
                    'creator_id': user_id,
                    'creator_username': user_info['username']
                }
                
        except AllocationError as e:
            logger.warning(f"Allocation validation error for user {user_id}: {e}")
            return {'success': False, 'error': str(e)}
        except Exception as e:
            # Check for foreign key constraint violation
            if "foreign key constraint" in str(e).lower() and "creator_id" in str(e).lower():
                logger.error(f"Foreign key violation for creator_id {user_id}: {e}")
                return {
                    'success': False,
                    'error': "Session error. Your user account may have been deactivated. Please login again."
                }
            
            logger.error(f"Unexpected error creating allocation for user {user_id}: {e}", exc_info=True)
            return {
                'success': False,
                'error': "Unable to create allocation. Please try again or contact support.",
                'technical_error': str(e)
            }
    
    # ==================== CANCEL ALLOCATION ====================
    def cancel_allocation(self, allocation_detail_id: int, cancelled_qty: float,
                         reason: str, reason_category: str, user_id: int) -> Dict[str, Any]:
        """Cancel allocation with proper user validation"""
        try:
            # Early validation of user_id
            if not user_id:
                logger.error("cancel_allocation called without user_id")
                return {
                    'success': False,
                    'error': "Session error. Please login again."
                }
            
            with self.db_transaction() as conn:
                # Validate user exists and is active
                user_valid, user_error, user_info = self._validate_user_id(conn, user_id)
                if not user_valid:
                    return {
                        'success': False,
                        'error': user_error
                    }
                
                # Convert input to Decimal for database operations
                cancelled_qty_decimal = self._to_decimal(cancelled_qty)
                
                # Get allocation detail with delivery info
                detail = self._get_allocation_detail_with_pending(conn, allocation_detail_id)
                
                if not detail:
                    raise AllocationNotFoundError(f"Allocation {allocation_detail_id} not found")
                
                # Convert database values to Decimal
                pending_qty_decimal = self._to_decimal(detail.get('pending_qty', 0))
                delivered_qty_decimal = self._to_decimal(detail.get('delivered_qty', 0))
                
                # Validate cancellation
                if cancelled_qty_decimal <= 0:
                    raise AllocationError("Cancel quantity must be positive")
                
                if cancelled_qty_decimal > pending_qty_decimal:
                    raise AllocationError(
                        f"Cannot cancel {self._to_float(cancelled_qty_decimal):.0f}. "
                        f"Only {self._to_float(pending_qty_decimal):.0f} pending (not yet delivered)"
                    )
                
                if pending_qty_decimal <= 0:
                    raise AllocationError("Cannot cancel - all quantity has been delivered")
                
                # Calculate remaining after cancellation
                remaining_pending = pending_qty_decimal - cancelled_qty_decimal
                
                # Insert cancellation record with validated user_id
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
                    'cancelled_qty': cancelled_qty_decimal,
                    'reason': reason,
                    'reason_category': reason_category,
                    'cancelled_by_user_id': user_id
                })
                
                cancellation_id = result.lastrowid
                
                # Clear cache
                st.cache_data.clear()
                
                logger.info(
                    f"User {user_info['username']} (ID: {user_id}) cancelled "
                    f"{self._to_float(cancelled_qty_decimal):.0f} from allocation detail {allocation_detail_id}. "
                    f"Delivered: {self._to_float(delivered_qty_decimal):.0f}, "
                    f"Remaining pending: {self._to_float(remaining_pending):.0f}"
                )
                
                return {
                    'success': True,
                    'cancellation_id': cancellation_id,
                    'cancelled_qty': self._to_float(cancelled_qty_decimal),
                    'remaining_pending_qty': self._to_float(remaining_pending),
                    'delivered_qty': self._to_float(delivered_qty_decimal),
                    'cancelled_by': user_info['username']
                }
                
        except AllocationError as e:
            logger.warning(f"Allocation cancellation error for user {user_id}: {e}")
            return {'success': False, 'error': str(e)}
        except Exception as e:
            if "foreign key constraint" in str(e).lower() and "cancelled_by_user_id" in str(e).lower():
                logger.error(f"Foreign key violation for cancelled_by_user_id {user_id}: {e}")
                return {
                    'success': False,
                    'error': "Session error. Your user account may have been deactivated. Please login again."
                }
            
            logger.error(f"Unexpected error cancelling allocation for user {user_id}: {e}", exc_info=True)
            return {
                'success': False,
                'error': "Unable to cancel allocation. Please try again or contact support.",
                'technical_error': str(e)
            }
    
    # ==================== UPDATE ALLOCATION ETD ====================
    def update_allocation_etd(self, allocation_detail_id: int, new_etd: datetime,
                             user_id: int) -> Dict[str, Any]:
        """Update allocated ETD with user validation"""
        try:
            # Early validation of user_id
            if not user_id:
                logger.error("update_allocation_etd called without user_id")
                return {
                    'success': False,
                    'error': "Session error. Please login again."
                }
            
            with self.db_transaction() as conn:
                # Validate user exists and is active
                user_valid, user_error, user_info = self._validate_user_id(conn, user_id)
                if not user_valid:
                    return {
                        'success': False,
                        'error': user_error
                    }
                
                # Check allocation details
                detail = self._get_allocation_detail_for_update(conn, allocation_detail_id)
                
                if not detail:
                    raise AllocationNotFoundError(f"Allocation {allocation_detail_id} not found")
                
                # Validation
                if detail['status'] != 'ALLOCATED':
                    raise AllocationError("Can only update ETD for ALLOCATED status")
                
                pending_qty = self._to_decimal(detail.get('pending_qty', 0))
                if pending_qty <= 0:
                    raise AllocationError("Cannot update ETD - all quantity has been delivered")
                
                # Store old ETD for logging
                old_etd = detail['allocated_etd']
                
                # Update ETD
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
                
                # Clear cache
                st.cache_data.clear()
                
                update_count = (detail.get('etd_update_count', 0) or 0) + 1
                
                logger.info(
                    f"User {user_info['username']} (ID: {user_id}) updated ETD "
                    f"for allocation detail {allocation_detail_id} "
                    f"from {old_etd} to {new_etd}. Update #{update_count}"
                )
                
                return {
                    'success': True,
                    'new_etd': new_etd,
                    'update_count': update_count,
                    'pending_qty_affected': self._to_float(pending_qty),
                    'updated_by': user_info['username']
                }
                
        except AllocationError as e:
            logger.warning(f"ETD update error for user {user_id}: {e}")
            return {'success': False, 'error': str(e)}
        except Exception as e:
            logger.error(f"Unexpected error updating ETD for user {user_id}: {e}", exc_info=True)
            return {
                'success': False,
                'error': "Unable to update ETD. Please try again or contact support.",
                'technical_error': str(e)
            }
    
    # ==================== REVERSE CANCELLATION ====================
    def reverse_cancellation(self, cancellation_id: int, reversal_reason: str,
                           user_id: int) -> Dict[str, Any]:
        """Reverse a cancellation with user validation"""
        try:
            # Early validation of user_id
            if not user_id:
                logger.error("reverse_cancellation called without user_id")
                return {
                    'success': False,
                    'error': "Session error. Please login again."
                }
            
            with self.db_transaction() as conn:
                # Validate user exists and is active
                user_valid, user_error, user_info = self._validate_user_id(conn, user_id)
                if not user_valid:
                    return {
                        'success': False,
                        'error': user_error
                    }
                
                # Check if cancellation exists and is active
                check_query = text("""
                    SELECT 
                        ac.status,
                        ac.allocation_detail_id,
                        ac.cancelled_qty,
                        ad.allocated_qty
                    FROM allocation_cancellations ac
                    INNER JOIN allocation_details ad ON ac.allocation_detail_id = ad.id
                    WHERE ac.id = :cancellation_id
                """)
                
                result = conn.execute(check_query, {'cancellation_id': cancellation_id}).fetchone()
                
                if not result:
                    raise AllocationNotFoundError(f"Cancellation {cancellation_id} not found")
                
                if result._mapping['status'] != 'ACTIVE':
                    raise AllocationError("Cancellation has already been reversed")
                
                cancelled_qty = self._to_decimal(result._mapping['cancelled_qty'])
                
                # Update cancellation status with validated user_id
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
                
                # Clear cache
                st.cache_data.clear()
                
                logger.info(
                    f"User {user_info['username']} (ID: {user_id}) reversed cancellation {cancellation_id}. "
                    f"Restored quantity: {self._to_float(cancelled_qty):.0f}"
                )
                
                return {
                    'success': True,
                    'restored_qty': self._to_float(cancelled_qty),
                    'reversed_by': user_info['username']
                }
                
        except AllocationError as e:
            logger.warning(f"Cancellation reversal error for user {user_id}: {e}")
            return {'success': False, 'error': str(e)}
        except Exception as e:
            if "foreign key constraint" in str(e).lower() and "reversed_by_user_id" in str(e).lower():
                logger.error(f"Foreign key violation for reversed_by_user_id {user_id}: {e}")
                return {
                    'success': False,
                    'error': "Session error. Your user account may have been deactivated. Please login again."
                }
            
            logger.error(f"Unexpected error reversing cancellation for user {user_id}: {e}", exc_info=True)
            return {
                'success': False,
                'error': "Unable to reverse cancellation. Please try again or contact support.",
                'technical_error': str(e)
            }
    
    # ==================== HELPER METHODS ====================
    def _generate_allocation_number(self, conn) -> str:
        """Generate unique allocation number"""
        try:
            now = datetime.now()
            year_month = now.strftime('%Y%m')
            
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
                last_number = result[0]
                sequence = int(last_number.split('-')[-1]) + 1
            else:
                sequence = 1
            
            return f"ALL-{year_month}-{sequence:04d}"
            
        except Exception as e:
            logger.error(f"Error generating allocation number: {e}")
            return f"ALL-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    def _get_oc_detail_info(self, conn, oc_detail_id: int) -> Optional[Dict]:
        """Get OC detail information for allocation"""
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
                    standard_quantity as effective_standard_quantity,
                    selling_quantity as effective_selling_quantity,
                    pending_standard_delivery_quantity,
                    pending_selling_delivery_quantity as pending_quantity,
                    selling_uom,
                    standard_uom,
                    uom_conversion,
                    total_delivered_standard_quantity,
                    total_delivered_selling_quantity,
                    total_effective_allocated_qty_standard
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
        """Get allocation detail with pending quantity calculated using Decimal"""
        detail_query = text("""
            SELECT 
                ad.*,
                CAST(ad.allocated_qty AS DECIMAL(15,2)) as allocated_qty,
                CAST(COALESCE(ac.cancelled_qty, 0) AS DECIMAL(15,2)) as cancelled_qty,
                CAST(COALESCE(adl.delivered_qty, 0) AS DECIMAL(15,2)) as delivered_qty,
                CAST((ad.allocated_qty - COALESCE(ac.cancelled_qty, 0)) AS DECIMAL(15,2)) as effective_qty,
                CAST((ad.allocated_qty - COALESCE(ac.cancelled_qty, 0) - COALESCE(adl.delivered_qty, 0)) AS DECIMAL(15,2)) as pending_qty,
                adl.delivery_count,
                adl.last_delivery_date
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
                    SUM(delivered_qty) as delivered_qty,
                    COUNT(*) as delivery_count,
                    MAX(created_at) as last_delivery_date
                FROM allocation_delivery_links
                GROUP BY allocation_detail_id
            ) adl ON ad.id = adl.allocation_detail_id
            WHERE ad.id = :detail_id
        """)
        
        result = conn.execute(detail_query, {'detail_id': allocation_detail_id}).fetchone()
        
        if result:
            detail_dict = dict(result._mapping)
            
            # Ensure numeric fields are Decimal
            for field in ['allocated_qty', 'cancelled_qty', 'delivered_qty', 'effective_qty', 'pending_qty']:
                if field in detail_dict:
                    detail_dict[field] = self._to_decimal(detail_dict[field])
            
            return detail_dict
        
        return None
    
    def _get_allocation_detail_for_update(self, conn, allocation_detail_id: int) -> Optional[Dict]:
        """Get allocation detail for ETD update"""
        check_query = text("""
            SELECT 
                ad.allocation_mode,
                ad.status,
                ad.etd_update_count,
                ad.allocated_etd,
                ad.allocated_qty,
                CAST(COALESCE(adl.delivered_qty, 0) AS DECIMAL(15,2)) as delivered_qty,
                CAST((ad.allocated_qty - COALESCE(ac.cancelled_qty, 0) - COALESCE(adl.delivered_qty, 0)) AS DECIMAL(15,2)) as pending_qty
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
    
    def _create_allocation_context(self, oc_info: Dict, allocations: List[Dict],
                                  mode: str, user_id: int, user_info: Dict = None) -> Dict:
        """Create allocation context with validated user info for audit trail"""
        # Use provided user_info or set defaults
        if not user_info:
            user_info = {
                'username': 'Unknown',
                'full_name': 'Unknown',
                'role': 'Unknown',
                'email': ''
            }
        
        return {
            'oc_detail': {
                'id': oc_info['ocd_id'],
                'oc_number': oc_info['oc_number'],
                'customer': oc_info['customer_name'],
                'product': oc_info['product_name'],
                'pending_qty_standard': self._to_float(oc_info.get('pending_standard_delivery_quantity', 0)),
                'pending_qty_selling': self._to_float(oc_info.get('pending_quantity', 0)),
                'standard_uom': oc_info['standard_uom'],
                'selling_uom': oc_info.get('selling_uom', oc_info['standard_uom']),
                'uom_conversion': oc_info.get('uom_conversion', '1')
            },
            'allocations': [
                {
                    'source_type': alloc.get('source_type'),
                    'source_id': alloc.get('source_id'),
                    'quantity': self._to_float(alloc['quantity']),
                    'source_info': {
                        k: v for k, v in alloc.get('supply_info', {}).items() 
                        if k in ['buying_uom', 'standard_uom', 'uom_conversion', 'reference',
                                'batch_number', 'arrival_note_number', 'po_number']
                    }
                } for alloc in allocations
            ],
            'mode': mode,
            'created_by': {
                'user_id': user_id,
                'username': user_info['username'],
                'full_name': user_info['full_name'],
                'role': user_info['role'],
                'email': user_info.get('email', '')
            },
            'created_at': datetime.now().isoformat()
        }
    
    def _create_allocation_detail(self, conn, allocation_plan_id: int, oc_info: Dict,
                                 alloc: Dict, mode: str, etd: datetime) -> Tuple[int, Decimal]:
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
        
        # Convert quantity to Decimal
        allocated_qty = self._to_decimal(alloc['quantity'])
        requested_qty_standard = self._to_decimal(oc_info.get('pending_standard_delivery_quantity', 0))
        
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
            'requested_qty': requested_qty_standard,
            'allocated_qty': allocated_qty,
            'etd': oc_info['etd'],
            'allocated_etd': etd,
            'notes': f"Source: {source_description}",
            'supply_source_type': supply_source_type,
            'supply_source_id': supply_source_id
        })
        
        return result.lastrowid, allocated_qty
    
    def _validate_allocation_request(self, conn, oc_info: Dict, 
                                    allocations: List[Dict], mode: str) -> Dict[str, Any]:
        """Validate allocation request with proper type conversion"""
        if not allocations:
            return {'valid': False, 'error': 'Please select at least one supply source'}
        
        # Convert all quantities to Decimal
        total_to_allocate = Decimal('0')
        for alloc in allocations:
            total_to_allocate += self._to_decimal(alloc.get('quantity', 0))
        
        if total_to_allocate <= 0:
            return {'valid': False, 'error': 'Total allocation quantity must be greater than zero'}
        
        # Get UOM info
        standard_uom = oc_info.get('standard_uom', '')
        product_id = oc_info['product_id']
        
        # Convert OC quantities to Decimal
        effective_qty_standard = self._to_decimal(oc_info.get('effective_standard_quantity', 0))
        pending_qty_standard = self._to_decimal(oc_info.get('pending_standard_delivery_quantity', 0))
        
        # Get existing allocations
        allocation_summary = self._get_enhanced_allocation_summary(conn, oc_info['ocd_id'])
        
        # Convert summary values to Decimal
        total_effective_allocated = self._to_decimal(allocation_summary['total_effective_allocated'])
        undelivered_allocated = self._to_decimal(allocation_summary['undelivered_allocated'])
        
        # Check 1: Total commitment vs effective OC quantity
        new_total_effective_allocated = total_effective_allocated + total_to_allocate
        
        if new_total_effective_allocated > effective_qty_standard:
            max_allowed = effective_qty_standard * Decimal(str(self.MAX_OVER_ALLOCATION_PERCENT / 100))
            
            if new_total_effective_allocated > max_allowed:
                return {
                    'valid': False,
                    'error': (
                        f"Over-commitment exceeds limit. "
                        f"OC effective quantity: {self._to_float(effective_qty_standard):.0f} {standard_uom}, "
                        f"Current effective allocation: {self._to_float(total_effective_allocated):.0f} {standard_uom}, "
                        f"Attempting to add: {self._to_float(total_to_allocate):.0f} {standard_uom}, "
                        f"Total would be: {self._to_float(new_total_effective_allocated):.0f} {standard_uom} "
                        f"(limit is {self._to_float(max_allowed):.0f} {standard_uom} = 100%)"
                    )
                }
        
        # Check 2: Pending over-allocation
        new_undelivered = undelivered_allocated + total_to_allocate
        
        if new_undelivered > pending_qty_standard:
            delivered_qty = self._to_decimal(oc_info.get('total_delivered_standard_quantity', 0))
            return {
                'valid': False,
                'error': (
                    f"Would create pending over-allocation. "
                    f"Pending delivery required: {self._to_float(pending_qty_standard):.0f} {standard_uom}, "
                    f"Already delivered: {self._to_float(delivered_qty):.0f} {standard_uom}, "
                    f"Current undelivered allocation: {self._to_float(undelivered_allocated):.0f} {standard_uom}, "
                    f"Adding: {self._to_float(total_to_allocate):.0f} {standard_uom} would exceed pending requirement"
                )
            }

        # Check 3: Supply capability check
        if mode == 'SOFT':
            supply_summary = self._get_product_supply_summary(conn, product_id)
            available_supply = self._to_decimal(supply_summary['available'])
            
            if total_to_allocate > available_supply:
                return {
                    'valid': False,
                    'error': (
                        f"Insufficient supply for SOFT allocation. "
                        f"Available: {self._to_float(available_supply):.0f} {standard_uom}, "
                        f"Requested: {self._to_float(total_to_allocate):.0f} {standard_uom}\n"
                        f"(Total supply: {self._to_float(supply_summary['total_supply']):.0f}, "
                        f"Already committed: {self._to_float(supply_summary['total_committed']):.0f})"
                    )
                }
        else:  # HARD allocation
            for idx, alloc in enumerate(allocations):
                if not alloc.get('source_type') or not alloc.get('source_id'):
                    return {
                        'valid': False,
                        'error': f"Item {idx + 1}: Source information required for HARD allocation"
                    }
                
                # Check supply availability
                available = self.supply_data.check_supply_availability(
                    alloc['source_type'],
                    alloc['source_id'],
                    product_id
                )
                
                if not available['available']:
                    return {
                        'valid': False,
                        'error': f"Item {idx + 1}: Supply source no longer available"
                    }
                
                # Get current commitments for this specific supply
                committed = self._get_supply_commitment(conn, alloc['source_type'], alloc['source_id'])
                committed_decimal = self._to_decimal(committed)
                available_qty_decimal = self._to_decimal(available['available_qty'])
                alloc_qty_decimal = self._to_decimal(alloc['quantity'])
                remaining = available_qty_decimal - committed_decimal
                
                if alloc_qty_decimal > remaining:
                    return {
                        'valid': False,
                        'error': (
                            f"Item {idx + 1}: Insufficient supply. "
                            f"Available: {self._to_float(available_qty_decimal):.0f} {standard_uom}, "
                            f"Already committed: {self._to_float(committed_decimal):.0f} {standard_uom}, "
                            f"Remaining: {self._to_float(remaining):.0f} {standard_uom}, "
                            f"Requested: {self._to_float(alloc_qty_decimal):.0f} {standard_uom}"
                        )
                    }
        
        return {'valid': True}
    
    def _get_enhanced_allocation_summary(self, conn, oc_detail_id: int) -> Dict:
        """Get comprehensive allocation summary with proper type conversion"""
        query = text("""
            SELECT 
                CAST(COALESCE(SUM(ad.allocated_qty), 0) AS DECIMAL(15,2)) as total_allocated,
                CAST(COALESCE(SUM(CASE WHEN ac.status = 'ACTIVE' THEN ac.cancelled_qty ELSE 0 END), 0) AS DECIMAL(15,2)) as total_cancelled,
                CAST(COALESCE(SUM(adl.delivered_qty), 0) AS DECIMAL(15,2)) as total_delivered,
                CAST(COALESCE(SUM(ad.allocated_qty - COALESCE(CASE WHEN ac.status = 'ACTIVE' THEN ac.cancelled_qty ELSE 0 END, 0)), 0) AS DECIMAL(15,2)) as total_effective_allocated,
                CAST(COALESCE(SUM(ad.allocated_qty - 
                            COALESCE(CASE WHEN ac.status = 'ACTIVE' THEN ac.cancelled_qty ELSE 0 END, 0) - 
                            COALESCE(adl.delivered_qty, 0)), 0) AS DECIMAL(15,2)) as undelivered_allocated
            FROM allocation_details ad
            LEFT JOIN (
                SELECT allocation_detail_id, SUM(cancelled_qty) as cancelled_qty, status
                FROM allocation_cancellations
                WHERE status = 'ACTIVE'
                GROUP BY allocation_detail_id, status
            ) ac ON ad.id = ac.allocation_detail_id
            LEFT JOIN (
                SELECT allocation_detail_id, SUM(delivered_qty) as delivered_qty
                FROM allocation_delivery_links
                GROUP BY allocation_detail_id
            ) adl ON ad.id = adl.allocation_detail_id
            WHERE ad.demand_reference_id = :oc_detail_id
            AND ad.demand_type = 'OC'
            AND ad.status = 'ALLOCATED'
        """)
        
        result = conn.execute(query, {'oc_detail_id': oc_detail_id}).fetchone()
        
        if result:
            return {
                'total_allocated': self._to_decimal(result._mapping['total_allocated']),
                'total_cancelled': self._to_decimal(result._mapping['total_cancelled']),
                'total_delivered': self._to_decimal(result._mapping['total_delivered']),
                'total_effective_allocated': self._to_decimal(result._mapping['total_effective_allocated']),
                'undelivered_allocated': self._to_decimal(result._mapping['undelivered_allocated'])
            }
        else:
            return {
                'total_allocated': Decimal('0'),
                'total_cancelled': Decimal('0'),
                'total_delivered': Decimal('0'),
                'total_effective_allocated': Decimal('0'),
                'undelivered_allocated': Decimal('0')
            }
    
    def _get_source_description(self, allocation: Dict) -> str:
        """Get human-readable description for supply source"""
        source_type = allocation.get('source_type', '')
        supply_info = allocation.get('supply_info', {})
        
        if source_type == 'INVENTORY':
            return f"Inventory Batch {supply_info.get('batch_number', 'N/A')}"
        elif source_type == 'PENDING_CAN':
            return f"CAN {supply_info.get('arrival_note_number', 'N/A')}"
        elif source_type == 'PENDING_PO':
            return f"PO {supply_info.get('po_number', 'N/A')}"
        elif source_type == 'PENDING_WHT':
            return f"Transfer {supply_info.get('from_warehouse', 'N/A')} → {supply_info.get('to_warehouse', 'N/A')}"
        else:
            return source_type or "Not specified"
    
    def _get_supply_commitment(self, conn, source_type: str, source_id: int) -> Decimal:
        """
        Get total committed quantity from a supply source
        IMPROVED: Uses MIN logic from outbound_oc_pending_delivery_view
        """
        query = text("""
            SELECT 
                COALESCE(
                    SUM(
                        GREATEST(0,
                            LEAST(
                                COALESCE(ocpd.pending_standard_delivery_quantity, 0),
                                COALESCE(ocpd.undelivered_allocated_qty_standard, 0)
                            )
                        )
                    ),
                0) as committed_qty
            FROM allocation_details ad
            INNER JOIN outbound_oc_pending_delivery_view ocpd
                ON ad.demand_reference_id = ocpd.ocd_id
                AND ad.demand_type = 'OC'
            WHERE ad.supply_source_type = :source_type
              AND ad.supply_source_id = :source_id
              AND ad.status = 'ALLOCATED'
              AND ocpd.pending_standard_delivery_quantity > 0
              AND ocpd.undelivered_allocated_qty_standard > 0
        """)
        
        result = conn.execute(query, {
            'source_type': source_type,
            'source_id': source_id
        }).fetchone()
        
        return self._to_decimal(result[0] if result else 0)
    
    def _get_product_supply_summary(self, conn, product_id: int) -> Dict[str, Decimal]:
        """Get supply summary for a product"""
        total_supply = self._get_total_product_supply(conn, product_id)
        total_committed = self._get_total_product_commitment(conn, product_id)
        available = total_supply - total_committed
        
        return {
            'total_supply': total_supply,
            'total_committed': total_committed,
            'available': available,
            'coverage_ratio': (available / total_supply * 100) if total_supply > 0 else Decimal('0')
        }
    
    def _get_total_product_supply(self, conn, product_id: int) -> Decimal:
        """Get total available supply for a product across all sources"""
        query = text("""
            SELECT 
                CAST(COALESCE(SUM(total_supply), 0) AS DECIMAL(15,2)) as total_supply
            FROM (
                SELECT SUM(remaining_quantity) as total_supply
                FROM inventory_detailed_view
                WHERE product_id = :product_id AND remaining_quantity > 0
                
                UNION ALL
                
                SELECT SUM(pending_quantity) as total_supply
                FROM can_pending_stockin_view
                WHERE product_id = :product_id AND pending_quantity > 0
                
                UNION ALL
                
                SELECT SUM(pending_standard_arrival_quantity) as total_supply
                FROM purchase_order_full_view
                WHERE product_id = :product_id AND pending_standard_arrival_quantity > 0
                
                UNION ALL
                
                SELECT SUM(transfer_quantity) as total_supply
                FROM warehouse_transfer_details_view
                WHERE product_id = :product_id AND is_completed = 0 AND transfer_quantity > 0
            ) supply_union
        """)
        
        result = conn.execute(query, {'product_id': product_id}).fetchone()
        return self._to_decimal(result[0] if result else 0)
    
    def _get_total_product_commitment(self, conn, product_id: int) -> Decimal:
        """
        Get total committed quantity for a product
        IMPROVED: Uses MIN logic from outbound_oc_pending_delivery_view
        Formula: Committed = Σ MIN(pending_delivery, undelivered_allocated)
        """
        query = text("""
            SELECT 
                COALESCE(
                    SUM(
                        GREATEST(0,
                            LEAST(
                                COALESCE(pending_standard_delivery_quantity, 0),
                                COALESCE(undelivered_allocated_qty_standard, 0)
                            )
                        )
                    ),
                0) as total_committed
            FROM outbound_oc_pending_delivery_view
            WHERE product_id = :product_id
              AND pending_standard_delivery_quantity > 0
              AND undelivered_allocated_qty_standard > 0
        """)
        
        result = conn.execute(query, {'product_id': product_id}).fetchone()
        return self._to_decimal(result[0] if result else 0)