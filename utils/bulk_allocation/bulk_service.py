"""
Bulk Allocation Service
=======================
Business logic for bulk allocation operations including:
- Transaction management
- Database operations
- Allocation context creation
- Commit and rollback
"""
import logging
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional, Union
from decimal import Decimal
import json
from contextlib import contextmanager
from threading import local
from sqlalchemy import text
import streamlit as st

from ..db import get_db_engine
from ..config import config
from .bulk_data import BulkAllocationData
from .bulk_validator import BulkAllocationValidator
from .strategy_engine import AllocationResult, StrategyConfig

logger = logging.getLogger(__name__)


# ==================== CUSTOM EXCEPTIONS ====================

class BulkAllocationError(Exception):
    """Base exception for bulk allocation errors"""
    pass


class ValidationError(BulkAllocationError):
    """Raised when validation fails"""
    pass


class InsufficientSupplyError(BulkAllocationError):
    """Raised when supply is insufficient"""
    pass


class UserValidationError(BulkAllocationError):
    """Raised when user validation fails"""
    pass


# ==================== SERVICE CLASS ====================

class BulkAllocationService:
    """Service for handling bulk allocation business logic"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.data = BulkAllocationData()
        self.validator = BulkAllocationValidator()
        
        # Configuration
        self.MAX_OVER_ALLOCATION_PERCENT = config.get_app_setting('MAX_OVER_ALLOCATION_PERCENT', 100)
        self.MIN_ALLOCATION_QTY = config.get_app_setting('MIN_ALLOCATION_QTY', 0.01)
        
        # Thread local storage for nested transactions
        self._local = local()
    
    # ==================== TYPE CONVERSION HELPERS ====================
    
    def _to_decimal(self, value: Union[None, int, float, str, Decimal]) -> Decimal:
        """Safely convert any value to Decimal"""
        if value is None:
            return Decimal('0')
        elif isinstance(value, Decimal):
            return value
        elif isinstance(value, (int, float)):
            return Decimal(str(value))
        elif isinstance(value, str):
            return Decimal(value.strip() if value.strip() else '0')
        else:
            try:
                if hasattr(value, 'item'):  # numpy scalar
                    return Decimal(str(value.item()))
                return Decimal(str(value))
            except:
                return Decimal('0')
    
    def _to_float(self, value: Union[None, int, float, str, Decimal]) -> float:
        """Safely convert any value to float"""
        if value is None:
            return 0.0
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, float):
            return value
        elif isinstance(value, (int, str)):
            return float(value)
        else:
            try:
                if hasattr(value, 'item'):
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
    
    # ==================== USER VALIDATION ====================
    
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
                return False, "User account not found. Please login again.", None
            
            user_data = dict(result._mapping)
            
            if not user_data['is_active']:
                logger.error(f"User ID {user_id} ({user_data['username']}) is inactive")
                return False, "Your account is inactive. Please contact an administrator.", None
            
            logger.debug(f"User validated: {user_data['username']} (ID: {user_id}, Role: {user_data['role']})")
            return True, None, user_data
            
        except Exception as e:
            logger.error(f"Error validating user {user_id}: {e}")
            return False, "Unable to validate user session. Please login again.", None
    
    # ==================== ALLOCATION NUMBER GENERATION ====================
    
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
    
    # ==================== MAIN COMMIT FUNCTION ====================
    
    def commit_bulk_allocation(self, 
                               allocation_results: List[Dict],
                               demands_dict: Dict[int, Dict],
                               scope: Dict,
                               strategy_config: Dict,
                               user_id: int,
                               notes: str = "") -> Dict[str, Any]:
        """
        Commit bulk allocation to database
        
        Args:
            allocation_results: List of allocation results with final_qty
            demands_dict: Dict mapping ocd_id -> OC info
            scope: Scope configuration
            strategy_config: Strategy configuration
            user_id: User performing the allocation
            notes: Optional notes
        
        Returns:
            Dict with success, allocation_number, details, etc.
        """
        try:
            # Early validation of user_id
            if not user_id:
                logger.error("commit_bulk_allocation called without user_id")
                return {
                    'success': False,
                    'error': "Session error. Please login again."
                }
            
            with self.db_transaction() as conn:
                # Validate user
                user_valid, user_error, user_info = self._validate_user_id(conn, user_id)
                if not user_valid:
                    return {
                        'success': False,
                        'error': user_error
                    }
                
                # Check permission
                if not self.validator.check_permission(user_info['role'], 'bulk_allocate'):
                    return {
                        'success': False,
                        'error': "You don't have permission to perform bulk allocation"
                    }
                
                # Filter allocations with qty > 0
                valid_allocations = [
                    a for a in allocation_results 
                    if float(a.get('final_qty', 0)) >= self.MIN_ALLOCATION_QTY
                ]
                
                if not valid_allocations:
                    return {
                        'success': False,
                        'error': "No valid allocations to commit"
                    }
                
                # Generate allocation number
                allocation_number = self._generate_allocation_number(conn)
                
                # Build allocation context
                allocation_context = self._build_allocation_context(
                    scope=scope,
                    strategy_config=strategy_config,
                    allocation_results=valid_allocations,
                    user_info=user_info
                )
                
                # Create allocation plan (1 row)
                plan_query = text("""
                    INSERT INTO allocation_plans 
                    (allocation_number, allocation_date, creator_id, notes, allocation_context)
                    VALUES (:allocation_number, NOW(), :creator_id, :notes, :allocation_context)
                """)
                
                result = conn.execute(plan_query, {
                    'allocation_number': allocation_number,
                    'creator_id': user_id,
                    'notes': notes or f"Bulk allocation: {len(valid_allocations)} OCs",
                    'allocation_context': json.dumps(allocation_context)
                })
                
                allocation_plan_id = result.lastrowid
                
                logger.info(
                    f"User {user_info['username']} (ID: {user_id}) created bulk allocation plan "
                    f"{allocation_number} (Plan ID: {allocation_plan_id})"
                )
                
                # Create allocation details (N rows)
                detail_ids = []
                total_allocated = Decimal('0')
                products_affected = set()
                customers_affected = set()
                
                for alloc in valid_allocations:
                    ocd_id = int(alloc['ocd_id'])
                    oc_info = demands_dict.get(ocd_id, {})
                    
                    if not oc_info:
                        logger.warning(f"OC info not found for ocd_id {ocd_id}")
                        continue
                    
                    allocated_qty = self._to_decimal(alloc['final_qty'])
                    
                    # Determine ETD - use OC's ETD as allocated_etd for bulk
                    etd = oc_info.get('etd')
                    allocated_etd = alloc.get('allocated_etd') or etd
                    
                    # Determine allocation mode and supply source
                    allocation_mode = strategy_config.get('allocation_mode', 'SOFT')
                    supply_source_type = None
                    supply_source_id = None
                    
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
                    
                    detail_result = conn.execute(detail_query, {
                        'allocation_plan_id': allocation_plan_id,
                        'allocation_mode': allocation_mode,
                        'demand_reference_id': ocd_id,
                        'demand_number': oc_info.get('oc_number', ''),
                        'product_id': int(oc_info.get('product_id', 0)),
                        'pt_code': oc_info.get('pt_code', ''),
                        'customer_code': oc_info.get('customer_code', ''),
                        'customer_name': oc_info.get('customer', ''),
                        'legal_entity_name': oc_info.get('legal_entity', ''),
                        'requested_qty': self._to_decimal(oc_info.get('pending_qty', 0)),
                        'allocated_qty': allocated_qty,
                        'etd': etd,
                        'allocated_etd': allocated_etd,
                        'notes': f"Bulk allocation via {strategy_config.get('strategy_type', 'HYBRID')} strategy",
                        'supply_source_type': supply_source_type,
                        'supply_source_id': supply_source_id
                    })
                    
                    detail_ids.append(detail_result.lastrowid)
                    total_allocated += allocated_qty
                    products_affected.add(int(oc_info.get('product_id', 0)))
                    customers_affected.add(oc_info.get('customer_code', ''))
                
                # Clear cache
                st.cache_data.clear()
                
                logger.info(
                    f"Successfully committed bulk allocation {allocation_number}: "
                    f"{len(detail_ids)} OCs, {len(products_affected)} products, "
                    f"total qty: {self._to_float(total_allocated):.0f}"
                )
                
                return {
                    'success': True,
                    'allocation_number': allocation_number,
                    'allocation_plan_id': allocation_plan_id,
                    'detail_count': len(detail_ids),
                    'total_allocated': self._to_float(total_allocated),
                    'products_affected': len(products_affected),
                    'customers_affected': len(customers_affected),
                    'creator_id': user_id,
                    'creator_username': user_info['username']
                }
                
        except BulkAllocationError as e:
            logger.warning(f"Bulk allocation error for user {user_id}: {e}")
            return {'success': False, 'error': str(e)}
        except Exception as e:
            if "foreign key constraint" in str(e).lower():
                logger.error(f"Foreign key violation for user {user_id}: {e}")
                return {
                    'success': False,
                    'error': "Session error. Your user account may have been deactivated. Please login again."
                }
            
            logger.error(f"Unexpected error in bulk allocation for user {user_id}: {e}", exc_info=True)
            return {
                'success': False,
                'error': "Unable to commit bulk allocation. Please try again or contact support.",
                'technical_error': str(e)
            }
    
    # ==================== CONTEXT BUILDERS ====================
    
    def _build_allocation_context(self, scope: Dict, strategy_config: Dict,
                                   allocation_results: List[Dict],
                                   user_info: Dict) -> Dict:
        """Build allocation context JSON for audit trail"""
        
        # Calculate summary statistics
        total_qty = sum(float(a.get('final_qty', 0)) for a in allocation_results)
        total_demand = sum(float(a.get('demand_qty', 0)) for a in allocation_results)
        avg_coverage = (total_qty / total_demand * 100) if total_demand > 0 else 0
        
        products = set(int(a.get('product_id', 0)) for a in allocation_results)
        customers = set(a.get('customer_code', '') for a in allocation_results)
        
        # Track adjustments (where final != suggested)
        adjustments = []
        for a in allocation_results:
            suggested = float(a.get('suggested_qty', 0))
            final = float(a.get('final_qty', 0))
            if abs(suggested - final) > 0.01:
                adjustments.append({
                    'ocd_id': a.get('ocd_id'),
                    'original_suggested': suggested,
                    'final_allocated': final,
                    'adjustment_reason': 'manual'
                })
        
        return {
            'allocation_type': 'BULK',
            'scope': {
                'brand_ids': scope.get('brand_ids', []),
                'customer_codes': scope.get('customer_codes', []),
                'legal_entities': scope.get('legal_entities', []),
                'etd_from': str(scope.get('etd_from', '')) if scope.get('etd_from') else None,
                'etd_to': str(scope.get('etd_to', '')) if scope.get('etd_to') else None,
                'include_partial_allocated': scope.get('include_partial_allocated', True)
            },
            'strategy': {
                'type': strategy_config.get('strategy_type', 'HYBRID'),
                'phases': strategy_config.get('phases', []),
                'allocation_mode': strategy_config.get('allocation_mode', 'SOFT'),
                'min_guarantee_percent': strategy_config.get('min_guarantee_percent', 30),
                'urgent_threshold_days': strategy_config.get('urgent_threshold_days', 7)
            },
            'summary': {
                'total_ocs_processed': len(allocation_results),
                'total_products_affected': len(products),
                'total_customers_affected': len(customers),
                'total_qty_allocated': total_qty,
                'total_demand': total_demand,
                'avg_coverage_percent': round(avg_coverage, 2)
            },
            'adjustments': adjustments,
            'created_by': {
                'user_id': user_info.get('id'),
                'username': user_info.get('username'),
                'full_name': user_info.get('full_name', ''),
                'role': user_info.get('role'),
                'email': user_info.get('email', '')
            },
            'created_at': datetime.now().isoformat()
        }
    
    # ==================== HELPER QUERIES ====================
    
    def get_last_bulk_allocations(self, limit: int = 10) -> List[Dict]:
        """Get recent bulk allocations for reference"""
        try:
            query = text("""
                SELECT 
                    ap.id,
                    ap.allocation_number,
                    ap.allocation_date,
                    ap.notes,
                    ap.allocation_context,
                    u.username as creator_username,
                    COUNT(ad.id) as detail_count,
                    SUM(ad.allocated_qty) as total_allocated
                FROM allocation_plans ap
                LEFT JOIN users u ON ap.creator_id = u.id
                LEFT JOIN allocation_details ad ON ap.id = ad.allocation_plan_id
                WHERE JSON_EXTRACT(ap.allocation_context, '$.allocation_type') = 'BULK'
                GROUP BY ap.id, ap.allocation_number, ap.allocation_date, ap.notes, 
                         ap.allocation_context, u.username
                ORDER BY ap.allocation_date DESC
                LIMIT :limit
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {'limit': limit})
                allocations = []
                for row in result:
                    alloc = dict(row._mapping)
                    if alloc.get('allocation_context'):
                        try:
                            alloc['allocation_context'] = json.loads(alloc['allocation_context'])
                        except:
                            pass
                    allocations.append(alloc)
                return allocations
                
        except Exception as e:
            logger.error(f"Error getting last bulk allocations: {e}")
            return []
