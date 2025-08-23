"""
Allocation Service for Business Logic
Handles CRUD operations for allocations
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import json
from sqlalchemy import text
import streamlit as st

from ..db import get_db_engine
from ..config import config
from .data_service import AllocationDataService

logger = logging.getLogger(__name__)


class AllocationService:
    """Service for handling allocation business logic"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.data_service = AllocationDataService()
    
    def create_allocation(self, oc_detail_id: int, allocations: List[Dict], 
                         mode: str, etd: datetime, notes: str, 
                         user_id: int) -> Dict[str, Any]:
        """
        Create new allocation for OC detail
        
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
        conn = self.engine.connect()
        trans = conn.begin()
        
        try:
            # Get OC detail information
            oc_info = self._get_oc_detail_info(conn, oc_detail_id)
            if not oc_info:
                raise ValueError(f"OC detail {oc_detail_id} not found")
            
            # Generate allocation number
            allocation_number = self._generate_allocation_number(conn)
            
            # Create allocation context
            allocation_context = {
                'oc_detail': oc_info,
                'allocations': allocations,
                'created_by': user_id,
                'created_at': datetime.now().isoformat()
            }
            
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
            
            # Check if there's available supply for the product
            total_supply = self.data_service.get_total_available_supply(oc_info['product_id'])
            if not total_supply['has_supply']:
                raise ValueError("No available supply for this product. Cannot create allocation.")
            
            # Create allocation details
            detail_ids = []
            total_allocated = 0
            
            for alloc in allocations:
                # For SOFT allocation, we don't need to validate specific supply source
                if mode == 'SOFT':
                    # SOFT allocation - no specific supply source
                    supply_source_type = None
                    supply_source_id = None
                    
                    # Just validate quantity is reasonable
                    if alloc['quantity'] <= 0:
                        raise ValueError(f"Allocation quantity must be positive")
                    
                    # Check total doesn't exceed available supply
                    if total_allocated + alloc['quantity'] > total_supply['total_available']:
                        raise ValueError(
                            f"Total allocation ({total_allocated + alloc['quantity']}) "
                            f"exceeds total available supply ({total_supply['total_available']})"
                        )
                    
                    source_description = "Not specified (SOFT allocation)"
                else:
                    # HARD allocation - must have specific supply source
                    if not alloc.get('source_type') or not alloc.get('source_id'):
                        raise ValueError("HARD allocation requires specific supply source")
                        
                    supply_source_type = alloc['source_type']
                    supply_source_id = alloc['source_id']
                    
                    # Validate supply availability for HARD allocation
                    availability = self.data_service.check_supply_availability(
                        supply_source_type,
                        supply_source_id,
                        oc_info['product_id']
                    )
                    
                    if not availability['available']:
                        raise ValueError(f"Supply source {supply_source_type} ID {supply_source_id} not available")
                    
                    if alloc['quantity'] > availability['available_qty']:
                        raise ValueError(
                            f"Requested quantity {alloc['quantity']} exceeds available {availability['available_qty']}"
                        )
                    
                    source_description = f"{supply_source_type}"
                
                # Insert allocation detail
                detail_query = text("""
                    INSERT INTO allocation_details (
                        allocation_plan_id, allocation_mode, demand_type, 
                        demand_reference_id, demand_number, product_id, pt_code,
                        customer_code, customer_name, legal_entity_name,
                        requested_qty, allocated_qty, delivered_qty,
                        etd, allocated_etd, status, notes,
                        supply_source_type, supply_source_id
                    ) VALUES (
                        :allocation_plan_id, :allocation_mode, 'OC',
                        :demand_reference_id, :demand_number, :product_id, :pt_code,
                        :customer_code, :customer_name, :legal_entity_name,
                        :requested_qty, :allocated_qty, 0,
                        :etd, :allocated_etd, 'ALLOCATED', :notes,
                        :supply_source_type, :supply_source_id
                    )
                """)
                
                result = conn.execute(detail_query, {
                    'allocation_plan_id': allocation_plan_id,
                    'allocation_mode': mode,
                    'demand_reference_id': oc_detail_id,
                    'demand_number': oc_info['oc_number'],
                    'product_id': oc_info['product_id'],
                    'pt_code': oc_info['pt_code'],
                    'customer_code': oc_info['customer_code'],
                    'customer_name': oc_info['customer_name'],
                    'legal_entity_name': oc_info['legal_entity'],
                    'requested_qty': oc_info['pending_quantity'],
                    'allocated_qty': alloc['quantity'],
                    'etd': oc_info['etd'],
                    'allocated_etd': etd,
                    'notes': f"Source: {source_description}",
                    'supply_source_type': supply_source_type,
                    'supply_source_id': supply_source_id
                })
                
                detail_ids.append(result.lastrowid)
                total_allocated += alloc['quantity']
            
            # Commit transaction
            trans.commit()
            
            # Clear cache to reflect new data
            st.cache_data.clear()
            
            logger.info(f"Created allocation {allocation_number} with {len(allocations)} items")
            
            return {
                'success': True,
                'allocation_number': allocation_number,
                'allocation_plan_id': allocation_plan_id,
                'detail_ids': detail_ids,
                'total_allocated': total_allocated
            }
            
        except Exception as e:
            trans.rollback()
            logger.error(f"Error creating allocation: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            conn.close()
    
    def cancel_allocation(self, allocation_detail_id: int, cancelled_qty: float,
                         reason: str, reason_category: str, user_id: int) -> Dict[str, Any]:
        """
        Cancel allocation or part of it
        
        Args:
            allocation_detail_id: Allocation detail ID to cancel
            cancelled_qty: Quantity to cancel
            reason: Cancellation reason
            reason_category: Category of reason
            user_id: User performing cancellation
            
        Returns:
            Dictionary with success status
        """
        conn = self.engine.connect()
        trans = conn.begin()
        
        try:
            # Get allocation detail info
            detail_query = text("""
                SELECT 
                    ad.*, 
                    (ad.allocated_qty - COALESCE(ac.cancelled_qty, 0)) as effective_qty
                FROM allocation_details ad
                LEFT JOIN (
                    SELECT 
                        allocation_detail_id,
                        SUM(CASE WHEN status = 'ACTIVE' THEN cancelled_qty ELSE 0 END) as cancelled_qty
                    FROM allocation_cancellations
                    GROUP BY allocation_detail_id
                ) ac ON ad.id = ac.allocation_detail_id
                WHERE ad.id = :detail_id
            """)
            
            result = conn.execute(detail_query, {'detail_id': allocation_detail_id}).fetchone()
            
            if not result:
                raise ValueError(f"Allocation detail {allocation_detail_id} not found")
            
            detail = dict(result._mapping)
            
            # Validate cancellation quantity
            if cancelled_qty > detail['effective_qty']:
                raise ValueError(
                    f"Cannot cancel {cancelled_qty}. Only {detail['effective_qty']} available to cancel"
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
            
            conn.execute(cancel_query, {
                'allocation_detail_id': allocation_detail_id,
                'allocation_plan_id': detail['allocation_plan_id'],
                'cancelled_qty': cancelled_qty,
                'reason': reason,
                'reason_category': reason_category,
                'cancelled_by_user_id': user_id
            })
            
            # Commit transaction
            trans.commit()
            
            # Clear cache
            st.cache_data.clear()
            
            logger.info(f"Cancelled {cancelled_qty} from allocation detail {allocation_detail_id}")
            
            return {
                'success': True,
                'cancelled_qty': cancelled_qty,
                'remaining_qty': detail['effective_qty'] - cancelled_qty
            }
            
        except Exception as e:
            trans.rollback()
            logger.error(f"Error cancelling allocation: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            conn.close()
    
    def update_allocation_etd(self, allocation_detail_id: int, new_etd: datetime,
                             user_id: int) -> Dict[str, Any]:
        """Update allocated ETD for an allocation detail"""
        conn = self.engine.connect()
        trans = conn.begin()
        
        try:
            # Check if allocation exists and is not HARD mode
            check_query = text("""
                SELECT allocation_mode, status 
                FROM allocation_details 
                WHERE id = :detail_id
            """)
            
            result = conn.execute(check_query, {'detail_id': allocation_detail_id}).fetchone()
            
            if not result:
                raise ValueError(f"Allocation detail {allocation_detail_id} not found")
            
            if result._mapping['allocation_mode'] == 'HARD':
                raise ValueError("Cannot update HARD allocation")
            
            if result._mapping['status'] != 'ALLOCATED':
                raise ValueError("Can only update ALLOCATED status allocations")
            
            # Update ETD
            update_query = text("""
                UPDATE allocation_details 
                SET allocated_etd = :new_etd
                WHERE id = :detail_id
            """)
            
            conn.execute(update_query, {
                'new_etd': new_etd,
                'detail_id': allocation_detail_id
            })
            
            trans.commit()
            
            # Clear cache
            st.cache_data.clear()
            
            logger.info(f"Updated ETD for allocation detail {allocation_detail_id}")
            
            return {'success': True}
            
        except Exception as e:
            trans.rollback()
            logger.error(f"Error updating allocation ETD: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            conn.close()
    
    def reverse_cancellation(self, cancellation_id: int, reversal_reason: str,
                           user_id: int) -> Dict[str, Any]:
        """Reverse a cancellation"""
        conn = self.engine.connect()
        trans = conn.begin()
        
        try:
            # Update cancellation status
            update_query = text("""
                UPDATE allocation_cancellations 
                SET 
                    status = 'REVERSED',
                    reversed_by_user_id = :user_id,
                    reversed_date = NOW(),
                    reversal_reason = :reason
                WHERE id = :cancellation_id
                AND status = 'ACTIVE'
            """)
            
            result = conn.execute(update_query, {
                'user_id': user_id,
                'reason': reversal_reason,
                'cancellation_id': cancellation_id
            })
            
            if result.rowcount == 0:
                raise ValueError("Cancellation not found or already reversed")
            
            trans.commit()
            
            # Clear cache
            st.cache_data.clear()
            
            logger.info(f"Reversed cancellation {cancellation_id}")
            
            return {'success': True}
            
        except Exception as e:
            trans.rollback()
            logger.error(f"Error reversing cancellation: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            conn.close()
    
    # ==================== Helper Methods ====================
    
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
                    min_allocated_etd,
                    max_allocated_etd,
                    pending_selling_delivery_quantity as pending_quantity,
                    selling_uom,
                    standard_uom
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
    
    def get_allocation_summary(self, allocation_plan_id: int) -> Dict[str, Any]:
        """Get summary of an allocation plan"""
        try:
            conn = self.engine.connect()
            
            # Get plan info
            plan_query = text("""
                SELECT 
                    ap.*,
                    u.username as creator_username,
                    COUNT(DISTINCT ad.id) as detail_count,
                    SUM(ad.allocated_qty) as total_allocated,
                    SUM(ad.delivered_qty) as total_delivered
                FROM allocation_plans ap
                LEFT JOIN users u ON ap.creator_id = u.id
                LEFT JOIN allocation_details ad ON ap.id = ad.allocation_plan_id
                WHERE ap.id = :plan_id
                GROUP BY ap.id
            """)
            
            result = conn.execute(plan_query, {'plan_id': allocation_plan_id}).fetchone()
            
            if result:
                summary = dict(result._mapping)
                
                # Get details
                details_query = text("""
                    SELECT 
                        ad.*,
                        p.name as product_name,
                        COALESCE(ac.cancelled_qty, 0) as cancelled_qty
                    FROM allocation_details ad
                    LEFT JOIN products p ON ad.product_id = p.id
                    LEFT JOIN (
                        SELECT 
                            allocation_detail_id,
                            SUM(CASE WHEN status = 'ACTIVE' THEN cancelled_qty ELSE 0 END) as cancelled_qty
                        FROM allocation_cancellations
                        GROUP BY allocation_detail_id
                    ) ac ON ad.id = ac.allocation_detail_id
                    WHERE ad.allocation_plan_id = :plan_id
                """)
                
                details_result = conn.execute(details_query, {'plan_id': allocation_plan_id})
                summary['details'] = [dict(row._mapping) for row in details_result]
                
                return summary
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting allocation summary: {e}")
            return {}
        finally:
            conn.close()
    
    def validate_allocation_mode_change(self, allocation_detail_id: int, 
                                      new_mode: str) -> Tuple[bool, str]:
        """Validate if allocation mode can be changed"""
        try:
            conn = self.engine.connect()
            
            query = text("""
                SELECT 
                    allocation_mode,
                    delivered_qty,
                    status
                FROM allocation_details
                WHERE id = :detail_id
            """)
            
            result = conn.execute(query, {'detail_id': allocation_detail_id}).fetchone()
            
            if not result:
                return False, "Allocation detail not found"
            
            detail = dict(result._mapping)
            
            # Cannot change if already delivered
            if detail['delivered_qty'] > 0:
                return False, "Cannot change mode for partially delivered allocation"
            
            # Cannot change if not in ALLOCATED status
            if detail['status'] != 'ALLOCATED':
                return False, f"Cannot change mode for {detail['status']} allocation"
            
            # Cannot change from HARD to SOFT
            if detail['allocation_mode'] == 'HARD' and new_mode == 'SOFT':
                return False, "Cannot change from HARD to SOFT mode"
            
            return True, "OK"
            
        except Exception as e:
            logger.error(f"Error validating mode change: {e}")
            return False, str(e)
        finally:
            conn.close()