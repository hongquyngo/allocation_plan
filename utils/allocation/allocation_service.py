"""
Allocation Service for Business Logic - Fixed UOM Conversion Version
Core business logic for allocation operations
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import json
from sqlalchemy import text
from contextlib import contextmanager
import streamlit as st

from ..db import get_db_engine
from ..config import config
from .data_service import AllocationDataService
from .uom_converter import UOMConverter

logger = logging.getLogger(__name__)


class AllocationService:
    """Service for handling allocation business logic"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.data_service = AllocationDataService()
        self.uom_converter = UOMConverter()  # Initialize UOM converter
        
        # Configuration
        self.MAX_OVER_ALLOCATION_PERCENT = 110  # Allow max 10% over-allocation
        self.MIN_ALLOCATION_QTY = 0.01
    
    @contextmanager
    def db_transaction(self):
        """Context manager for database transactions"""
        conn = self.engine.connect()
        trans = conn.begin()
        try:
            yield conn
            trans.commit()
        except Exception:
            trans.rollback()
            raise
        finally:
            conn.close()
    
    def create_allocation(self, oc_detail_id: int, allocations: List[Dict], 
                         mode: str, etd: datetime, notes: str, 
                         user_id: int) -> Dict[str, Any]:
        """
        Create new allocation with validation
        
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
                    return {
                        'success': False,
                        'error': 'Order confirmation not found'
                    }
                
                # Validate allocations before processing
                validation_result = self._validate_allocation_request(
                    conn, oc_info, allocations, mode
                )
                
                if not validation_result['valid']:
                    return {
                        'success': False,
                        'error': validation_result['error']
                    }
                
                # Generate allocation number
                allocation_number = self._generate_allocation_number(conn)
                
                # FIXED: Create allocation context with correct field names and UOM info
                allocation_context = {
                    'oc_detail': {
                        'id': oc_info['ocd_id'],
                        'oc_number': oc_info['oc_number'],
                        'customer': oc_info['customer_name'],
                        'product': oc_info['product_name'],
                        'pending_qty_standard': float(oc_info['pending_quantity']),  # This is standard qty
                        'pending_qty_selling': float(oc_info.get('pending_selling_quantity', 
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
                    # Determine supply source info
                    if mode == 'SOFT' or not alloc.get('source_type'):
                        # SOFT allocation - no specific source
                        supply_source_type = None
                        supply_source_id = None
                        source_description = "Not specified (SOFT allocation)"
                    else:
                        # HARD allocation with specific source
                        supply_source_type = alloc['source_type']
                        supply_source_id = alloc['source_id']
                        source_description = self._get_source_description(alloc)
                    
                    # Insert allocation detail (quantities are in standard UOM)
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
                        'requested_qty': oc_info['pending_quantity'],  # Standard UOM
                        'allocated_qty': alloc['quantity'],  # Standard UOM
                        'etd': oc_info['etd'],
                        'allocated_etd': etd,
                        'notes': f"Source: {source_description}",
                        'supply_source_type': supply_source_type,
                        'supply_source_id': supply_source_id
                    })
                    
                    detail_ids.append(result.lastrowid)
                    total_allocated += alloc['quantity']
                
                # Clear cache to reflect new data
                st.cache_data.clear()
                
                logger.info(f"Created allocation {allocation_number} with {len(allocations)} items, total qty: {total_allocated}")
                
                return {
                    'success': True,
                    'allocation_number': allocation_number,
                    'allocation_plan_id': allocation_plan_id,
                    'detail_ids': detail_ids,
                    'total_allocated': total_allocated
                }
                
        except Exception as e:
            logger.error(f"Error creating allocation: {e}")
            return {
                'success': False,
                'error': f"Failed to create allocation: {str(e)}"
            }
    
    def cancel_allocation(self, allocation_detail_id: int, cancelled_qty: float,
                         reason: str, reason_category: str, user_id: int) -> Dict[str, Any]:
        """Cancel allocation with validation"""
        try:
            with self.db_transaction() as conn:
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
                    return {
                        'success': False,
                        'error': 'Allocation not found'
                    }
                
                detail = dict(result._mapping)
                
                # Validate cancellation
                if cancelled_qty <= 0:
                    return {
                        'success': False,
                        'error': 'Cancel quantity must be positive'
                    }
                
                if cancelled_qty > detail['effective_qty']:
                    return {
                        'success': False,
                        'error': f'Cannot cancel {cancelled_qty:.0f}. Only {detail["effective_qty"]:.0f} available'
                    }
                
                # Check if already delivered
                if detail['delivered_qty'] > 0:
                    max_cancellable = detail['effective_qty'] - detail['delivered_qty']
                    if cancelled_qty > max_cancellable:
                        return {
                            'success': False,
                            'error': f'Cannot cancel delivered quantity. Maximum cancellable: {max_cancellable:.0f}'
                        }
                
                # Check allocation mode
                if detail['allocation_mode'] == 'HARD':
                    return {
                        'success': False,
                        'error': 'Cannot cancel HARD allocation. Please contact manager for approval'
                    }
                
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
                
                # Clear cache
                st.cache_data.clear()
                
                logger.info(f"Cancelled {cancelled_qty} from allocation detail {allocation_detail_id}")
                
                return {
                    'success': True,
                    'cancelled_qty': cancelled_qty,
                    'remaining_qty': detail['effective_qty'] - cancelled_qty
                }
                
        except Exception as e:
            logger.error(f"Error cancelling allocation: {e}")
            return {
                'success': False,
                'error': f"Failed to cancel allocation: {str(e)}"
            }
    
    def update_allocation_etd(self, allocation_detail_id: int, new_etd: datetime,
                             user_id: int) -> Dict[str, Any]:
        """Update allocated ETD with validation"""
        try:
            with self.db_transaction() as conn:
                # Check if allocation exists and is SOFT mode
                check_query = text("""
                    SELECT 
                        allocation_mode, 
                        status,
                        delivered_qty
                    FROM allocation_details 
                    WHERE id = :detail_id
                """)
                
                result = conn.execute(check_query, {'detail_id': allocation_detail_id}).fetchone()
                
                if not result:
                    return {
                        'success': False,
                        'error': 'Allocation not found'
                    }
                
                detail = dict(result._mapping)
                
                # Validation
                if detail['allocation_mode'] == 'HARD':
                    return {
                        'success': False,
                        'error': 'Cannot update ETD for HARD allocation'
                    }
                
                if detail['status'] != 'ALLOCATED':
                    return {
                        'success': False,
                        'error': 'Can only update ETD for ALLOCATED status'
                    }
                
                if detail['delivered_qty'] > 0:
                    return {
                        'success': False,
                        'error': 'Cannot update ETD for partially delivered allocation'
                    }
                
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
                
                # Clear cache
                st.cache_data.clear()
                
                logger.info(f"Updated ETD for allocation detail {allocation_detail_id} to {new_etd}")
                
                return {'success': True}
                
        except Exception as e:
            logger.error(f"Error updating allocation ETD: {e}")
            return {
                'success': False,
                'error': f"Failed to update ETD: {str(e)}"
            }
    
    def reverse_cancellation(self, cancellation_id: int, reversal_reason: str,
                           user_id: int) -> Dict[str, Any]:
        """Reverse a cancellation with validation"""
        try:
            with self.db_transaction() as conn:
                # Check if cancellation exists and is active
                check_query = text("""
                    SELECT 
                        ac.status,
                        ad.delivered_qty
                    FROM allocation_cancellations ac
                    INNER JOIN allocation_details ad ON ac.allocation_detail_id = ad.id
                    WHERE ac.id = :cancellation_id
                """)
                
                result = conn.execute(check_query, {'cancellation_id': cancellation_id}).fetchone()
                
                if not result:
                    return {
                        'success': False,
                        'error': 'Cancellation not found'
                    }
                
                if result._mapping['status'] != 'ACTIVE':
                    return {
                        'success': False,
                        'error': 'Cancellation has already been reversed'
                    }
                
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
                
                # Clear cache
                st.cache_data.clear()
                
                logger.info(f"Reversed cancellation {cancellation_id}")
                
                return {'success': True}
                
        except Exception as e:
            logger.error(f"Error reversing cancellation: {e}")
            return {
                'success': False,
                'error': f"Failed to reverse cancellation: {str(e)}"
            }
    
    # ==================== Helper Methods ====================
    
    def _validate_allocation_request(self, conn, oc_info: Dict, 
                                   allocations: List[Dict], mode: str) -> Dict[str, Any]:
        """
        Comprehensive validation for allocation request
        
        Returns:
            Dict with 'valid' boolean and 'error' message if invalid
        """
        # Check if allocations list is empty
        if not allocations:
            return {
                'valid': False,
                'error': 'No allocation items provided'
            }
        
        # Calculate total to be allocated (in standard UOM)
        total_to_allocate = sum(alloc['quantity'] for alloc in allocations)
        
        # Basic quantity validation
        if total_to_allocate <= 0:
            return {
                'valid': False,
                'error': 'Total allocation quantity must be positive'
            }
        
        # FIXED: Ensure we're comparing standard quantities
        # oc_info['pending_quantity'] should already be standard quantity from SQL
        pending_qty_standard = float(oc_info['pending_quantity'])
        max_allowed = pending_qty_standard * (self.MAX_OVER_ALLOCATION_PERCENT / 100)
        
        if total_to_allocate > max_allowed:
            # FIXED: Include UOM in error message
            standard_uom = oc_info.get('standard_uom', '')
            return {
                'valid': False,
                'error': f'Cannot allocate {total_to_allocate:.0f} {standard_uom}. Maximum allowed is {max_allowed:.0f} {standard_uom} ({self.MAX_OVER_ALLOCATION_PERCENT}% of {pending_qty_standard:.0f} {standard_uom})'
            }
        
        # For SOFT allocation, just check total supply availability
        if mode == 'SOFT':
            total_supply = self.data_service.get_total_available_supply(oc_info['product_id'])
            if not total_supply['has_supply']:
                return {
                    'valid': False,
                    'error': 'No available supply for this product'
                }
            
            if total_to_allocate > total_supply['total_available']:
                standard_uom = oc_info.get('standard_uom', '')
                return {
                    'valid': False,
                    'error': f'Insufficient supply. Available: {total_supply["total_available"]:.0f} {standard_uom}, Requested: {total_to_allocate:.0f} {standard_uom}'
                }
        else:
            # For HARD allocation, validate each specific source
            for alloc in allocations:
                if not alloc.get('source_type') or not alloc.get('source_id'):
                    return {
                        'valid': False,
                        'error': 'HARD allocation requires specific supply source'
                    }
                
                # Check availability
                availability = self.data_service.check_supply_availability(
                    alloc['source_type'],
                    alloc['source_id'],
                    oc_info['product_id']
                )
                
                if not availability['available']:
                    return {
                        'valid': False,
                        'error': f'{alloc["source_type"]} source is no longer available'
                    }
                
                if alloc['quantity'] > availability['available_qty']:
                    standard_uom = oc_info.get('standard_uom', '')
                    return {
                        'valid': False,
                        'error': f'Insufficient {alloc["source_type"]}. Available: {availability["available_qty"]:.0f} {standard_uom}, Requested: {alloc["quantity"]:.0f} {standard_uom}'
                    }
        
        # Check for duplicate allocations in the same request
        if mode == 'HARD':
            source_keys = []
            for alloc in allocations:
                key = f"{alloc['source_type']}_{alloc['source_id']}"
                if key in source_keys:
                    return {
                        'valid': False,
                        'error': 'Cannot allocate from the same source multiple times'
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
            # FIXED: Get all necessary UOM fields from view
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
                    pending_standard_delivery_quantity as pending_quantity,
                    pending_selling_delivery_quantity as pending_selling_quantity,
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
    
    def _get_source_description(self, allocation: Dict) -> str:
        """Get human-readable description for supply source"""
        source_type = allocation.get('source_type', '')
        supply_info = allocation.get('supply_info', {})
        
        if source_type == 'INVENTORY':
            return f"Inventory Batch {supply_info.get('batch_number', 'N/A')}"
        elif source_type == 'PENDING_CAN':
            desc = f"CAN {supply_info.get('arrival_note_number', 'N/A')}"
            # FIXED: Check if conversion is needed instead of string comparison
            if supply_info.get('buying_uom') and self.uom_converter.needs_conversion(supply_info.get('uom_conversion', '1')):
                desc += f" (Buying: {supply_info['buying_uom']})"
            return desc
        elif source_type == 'PENDING_PO':
            desc = f"PO {supply_info.get('po_number', 'N/A')}"
            # FIXED: Check if conversion is needed instead of string comparison
            if supply_info.get('buying_uom') and self.uom_converter.needs_conversion(supply_info.get('uom_conversion', '1')):
                desc += f" (Buying: {supply_info['buying_uom']})"
            return desc
        elif source_type == 'PENDING_WHT':
            return f"Transfer {supply_info.get('from_warehouse', 'N/A')} → {supply_info.get('to_warehouse', 'N/A')}"
        else:
            return source_type