"""
Allocation Management Service
==============================
Business logic for allocation management operations.

This module is INDEPENDENT - no imports from allocation/ or bulk_allocation/

Operations:
- Update quantity
- Update ETD
- Cancel allocation (full/partial)
- Reverse delivery
- Bulk operations
"""

import logging
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
from dataclasses import dataclass

from sqlalchemy import text

from utils.db import get_db_engine
from .mgmt_data import AllocationManagementData
from .mgmt_supply import AllocationSupplyData
from .mgmt_validators import AllocationManagementValidator, ValidationResult

logger = logging.getLogger(__name__)


@dataclass
class OperationResult:
    """Result of a management operation"""
    success: bool
    message: str
    data: Dict = None
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.data is None:
            self.data = {}


class AllocationManagementService:
    """Business logic for allocation management operations"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.data = AllocationManagementData()
        self.supply = AllocationSupplyData()
        self.validator = AllocationManagementValidator()
    
    # ================================================================
    # UPDATE QUANTITY
    # ================================================================
    
    def update_quantity(
        self,
        allocation_detail_id: int,
        new_qty: float,
        reason: str,
        user_id: int
    ) -> OperationResult:
        """
        Update allocated quantity.
        
        Args:
            allocation_detail_id: ID of allocation to update
            new_qty: New quantity
            reason: Reason for change
            user_id: User performing the action
        
        Returns:
            OperationResult with success status and details
        """
        try:
            # Get current allocation
            allocation = self.data.get_allocation_detail(allocation_detail_id)
            if not allocation:
                return OperationResult(
                    success=False,
                    message="Allocation not found",
                    errors=["Allocation not found"]
                )
            
            # Get supply info for validation
            product_id = allocation.get('product_id')
            supply_summary = self.supply.get_product_supply_summary(product_id)
            supply_available = supply_summary.get('available', 0)
            
            # Validate
            validation = self.validator.validate_quantity_update(
                allocation, new_qty, supply_available
            )
            if not validation.is_valid:
                return OperationResult(
                    success=False,
                    message="Validation failed",
                    errors=validation.errors
                )
            
            # Calculate old values for audit
            old_qty = float(allocation.get('allocated_qty', 0))
            old_values = {'allocated_qty': old_qty}
            new_values = {'allocated_qty': new_qty}
            
            # Execute update
            with self.engine.begin() as conn:
                # Update allocation_details
                update_query = text("""
                    UPDATE allocation_details
                    SET allocated_qty = :new_qty
                    WHERE id = :id
                """)
                conn.execute(update_query, {
                    'new_qty': new_qty,
                    'id': allocation_detail_id
                })
                
                # Insert audit log
                audit_query = text("""
                    INSERT INTO allocation_audit_log 
                    (allocation_detail_id, plan_id, action_type, old_values, new_values, 
                     change_reason, performed_by, performed_at)
                    VALUES 
                    (:detail_id, :plan_id, 'UPDATE_QTY', :old_values, :new_values,
                     :reason, :user_id, NOW())
                """)
                conn.execute(audit_query, {
                    'detail_id': allocation_detail_id,
                    'plan_id': allocation.get('plan_id'),
                    'old_values': json.dumps(old_values),
                    'new_values': json.dumps(new_values),
                    'reason': reason,
                    'user_id': user_id
                })
            
            # Clear cache
            self.data.search_allocations.clear()
            self.data.get_allocation_detail.clear()
            
            logger.info(
                f"Updated allocation {allocation_detail_id} quantity: "
                f"{old_qty} -> {new_qty} by user {user_id}"
            )
            
            return OperationResult(
                success=True,
                message=f"Quantity updated successfully: {old_qty:,.2f} → {new_qty:,.2f}",
                data={
                    'allocation_detail_id': allocation_detail_id,
                    'old_qty': old_qty,
                    'new_qty': new_qty,
                    'change': new_qty - old_qty,
                    'warnings': validation.warnings
                }
            )
            
        except Exception as e:
            logger.error(f"Error updating quantity: {e}")
            return OperationResult(
                success=False,
                message="Failed to update quantity",
                errors=[str(e)]
            )
    
    # ================================================================
    # UPDATE ETD
    # ================================================================
    
    def update_etd(
        self,
        allocation_detail_id: int,
        new_etd: date,
        reason: str,
        user_id: int
    ) -> OperationResult:
        """
        Update allocated ETD.
        
        Args:
            allocation_detail_id: ID of allocation to update
            new_etd: New ETD date
            reason: Reason for change
            user_id: User performing the action
        """
        try:
            # Get current allocation
            allocation = self.data.get_allocation_detail(allocation_detail_id)
            if not allocation:
                return OperationResult(
                    success=False,
                    message="Allocation not found",
                    errors=["Allocation not found"]
                )
            
            # Validate
            validation = self.validator.validate_etd_update(allocation, new_etd)
            if not validation.is_valid:
                return OperationResult(
                    success=False,
                    message="Validation failed",
                    errors=validation.errors
                )
            
            # Calculate old values for audit
            old_etd = allocation.get('allocated_etd')
            old_etd_str = str(old_etd) if old_etd else None
            new_etd_str = str(new_etd)
            
            old_values = {'allocated_etd': old_etd_str}
            new_values = {'allocated_etd': new_etd_str}
            
            # Current update count
            etd_update_count = int(allocation.get('etd_update_count', 0))
            
            # Execute update
            with self.engine.begin() as conn:
                # Update allocation_details
                update_query = text("""
                    UPDATE allocation_details
                    SET allocated_etd = :new_etd,
                        last_updated_etd_date = NOW(),
                        etd_update_count = :new_count
                    WHERE id = :id
                """)
                conn.execute(update_query, {
                    'new_etd': new_etd,
                    'new_count': etd_update_count + 1,
                    'id': allocation_detail_id
                })
                
                # Insert audit log
                audit_query = text("""
                    INSERT INTO allocation_audit_log 
                    (allocation_detail_id, plan_id, action_type, old_values, new_values, 
                     change_reason, performed_by, performed_at)
                    VALUES 
                    (:detail_id, :plan_id, 'UPDATE_ETD', :old_values, :new_values,
                     :reason, :user_id, NOW())
                """)
                conn.execute(audit_query, {
                    'detail_id': allocation_detail_id,
                    'plan_id': allocation.get('plan_id'),
                    'old_values': json.dumps(old_values),
                    'new_values': json.dumps(new_values),
                    'reason': reason,
                    'user_id': user_id
                })
            
            # Clear cache
            self.data.search_allocations.clear()
            self.data.get_allocation_detail.clear()
            
            logger.info(
                f"Updated allocation {allocation_detail_id} ETD: "
                f"{old_etd_str} -> {new_etd_str} by user {user_id}"
            )
            
            return OperationResult(
                success=True,
                message=f"ETD updated successfully: {old_etd_str} → {new_etd_str}",
                data={
                    'allocation_detail_id': allocation_detail_id,
                    'old_etd': old_etd_str,
                    'new_etd': new_etd_str,
                    'update_count': etd_update_count + 1,
                    'warnings': validation.warnings
                }
            )
            
        except Exception as e:
            logger.error(f"Error updating ETD: {e}")
            return OperationResult(
                success=False,
                message="Failed to update ETD",
                errors=[str(e)]
            )
    
    # ================================================================
    # CANCEL ALLOCATION
    # ================================================================
    
    def cancel_allocation(
        self,
        allocation_detail_id: int,
        cancel_qty: float,
        reason: str,
        reason_category: str,
        user_id: int
    ) -> OperationResult:
        """
        Cancel allocation (full or partial).
        
        Args:
            allocation_detail_id: ID of allocation to cancel
            cancel_qty: Quantity to cancel
            reason: Detailed reason
            reason_category: Category (CUSTOMER_REQUEST, SUPPLY_ISSUE, etc.)
            user_id: User performing the action
        """
        try:
            # Get current allocation
            allocation = self.data.get_allocation_detail(allocation_detail_id)
            if not allocation:
                return OperationResult(
                    success=False,
                    message="Allocation not found",
                    errors=["Allocation not found"]
                )
            
            # Validate
            validation = self.validator.validate_cancel(allocation, cancel_qty, reason)
            if not validation.is_valid:
                return OperationResult(
                    success=False,
                    message="Validation failed",
                    errors=validation.errors
                )
            
            # Get plan_id
            plan_id = allocation.get('plan_id')
            
            # Execute cancellation
            with self.engine.begin() as conn:
                # Insert into allocation_cancellations
                cancel_query = text("""
                    INSERT INTO allocation_cancellations 
                    (allocation_detail_id, allocation_plan_id, cancelled_qty, reason, 
                     reason_category, cancelled_by_user_id, cancelled_date, status)
                    VALUES 
                    (:detail_id, :plan_id, :qty, :reason, :category, :user_id, NOW(), 'ACTIVE')
                """)
                conn.execute(cancel_query, {
                    'detail_id': allocation_detail_id,
                    'plan_id': plan_id,
                    'qty': cancel_qty,
                    'reason': reason,
                    'category': reason_category,
                    'user_id': user_id
                })
                
                # Insert audit log
                old_values = {
                    'allocated_qty': float(allocation.get('allocated_qty', 0)),
                    'cancelled_qty': float(allocation.get('cancelled_qty', 0))
                }
                new_values = {
                    'cancelled_qty': float(allocation.get('cancelled_qty', 0)) + cancel_qty
                }
                
                audit_query = text("""
                    INSERT INTO allocation_audit_log 
                    (allocation_detail_id, plan_id, action_type, old_values, new_values, 
                     change_reason, cancelled_qty, performed_by, performed_at)
                    VALUES 
                    (:detail_id, :plan_id, 'CANCEL', :old_values, :new_values,
                     :reason, :cancelled_qty, :user_id, NOW())
                """)
                conn.execute(audit_query, {
                    'detail_id': allocation_detail_id,
                    'plan_id': plan_id,
                    'old_values': json.dumps(old_values),
                    'new_values': json.dumps(new_values),
                    'reason': reason,
                    'cancelled_qty': cancel_qty,
                    'user_id': user_id
                })
            
            # Clear cache
            self.data.search_allocations.clear()
            self.data.get_allocation_detail.clear()
            self.data.get_cancellation_history.clear()
            
            logger.info(
                f"Cancelled allocation {allocation_detail_id}: "
                f"{cancel_qty} units by user {user_id}, reason: {reason_category}"
            )
            
            return OperationResult(
                success=True,
                message=f"Cancelled {cancel_qty:,.2f} units successfully",
                data={
                    'allocation_detail_id': allocation_detail_id,
                    'cancelled_qty': cancel_qty,
                    'reason_category': reason_category,
                    'warnings': validation.warnings
                }
            )
            
        except Exception as e:
            logger.error(f"Error cancelling allocation: {e}")
            return OperationResult(
                success=False,
                message="Failed to cancel allocation",
                errors=[str(e)]
            )
    
    # ================================================================
    # REVERSE DELIVERY
    # ================================================================
    
    def reverse_delivery(
        self,
        allocation_detail_id: int,
        delivery_link_id: int,
        reverse_qty: float,
        reason: str,
        user_id: int
    ) -> OperationResult:
        """
        Reverse a delivery (post-delivery adjustment).
        
        Args:
            allocation_detail_id: ID of allocation
            delivery_link_id: ID of delivery link to reverse
            reverse_qty: Quantity to reverse
            reason: Reason for reversal
            user_id: User performing the action
        """
        try:
            # Get current allocation
            allocation = self.data.get_allocation_detail(allocation_detail_id)
            if not allocation:
                return OperationResult(
                    success=False,
                    message="Allocation not found",
                    errors=["Allocation not found"]
                )
            
            # Get delivery link
            delivery_link = self.data.get_delivery_link_detail(delivery_link_id)
            if not delivery_link:
                return OperationResult(
                    success=False,
                    message="Delivery link not found",
                    errors=["Delivery link not found"]
                )
            
            # Validate
            validation = self.validator.validate_reverse(
                allocation, delivery_link, reverse_qty, reason
            )
            if not validation.is_valid:
                return OperationResult(
                    success=False,
                    message="Validation failed",
                    errors=validation.errors
                )
            
            # Get current values
            link_delivered_qty = float(delivery_link.get('delivered_qty', 0))
            new_link_qty = link_delivered_qty - reverse_qty
            
            allocation_delivered_qty = float(allocation.get('delivered_qty', 0))
            new_allocation_delivered = allocation_delivered_qty - reverse_qty
            
            plan_id = allocation.get('plan_id')
            
            # Execute reversal
            with self.engine.begin() as conn:
                if new_link_qty <= 0:
                    # Delete the delivery link if fully reversed
                    delete_query = text("""
                        DELETE FROM allocation_delivery_links
                        WHERE id = :link_id
                    """)
                    conn.execute(delete_query, {'link_id': delivery_link_id})
                else:
                    # Update delivery link quantity
                    update_link_query = text("""
                        UPDATE allocation_delivery_links
                        SET delivered_qty = :new_qty
                        WHERE id = :link_id
                    """)
                    conn.execute(update_link_query, {
                        'new_qty': new_link_qty,
                        'link_id': delivery_link_id
                    })
                
                # Update allocation_details delivered_qty
                update_alloc_query = text("""
                    UPDATE allocation_details
                    SET delivered_qty = :new_delivered
                    WHERE id = :id
                """)
                conn.execute(update_alloc_query, {
                    'new_delivered': max(0, new_allocation_delivered),
                    'id': allocation_detail_id
                })
                
                # Insert audit log
                old_values = {
                    'delivered_qty': allocation_delivered_qty,
                    'link_delivered_qty': link_delivered_qty
                }
                new_values = {
                    'delivered_qty': new_allocation_delivered,
                    'link_delivered_qty': new_link_qty
                }
                
                audit_query = text("""
                    INSERT INTO allocation_audit_log 
                    (allocation_detail_id, plan_id, action_type, old_values, new_values, 
                     change_reason, reversed_qty, delivery_link_id, performed_by, performed_at)
                    VALUES 
                    (:detail_id, :plan_id, 'REVERSE', :old_values, :new_values,
                     :reason, :reversed_qty, :link_id, :user_id, NOW())
                """)
                conn.execute(audit_query, {
                    'detail_id': allocation_detail_id,
                    'plan_id': plan_id,
                    'old_values': json.dumps(old_values),
                    'new_values': json.dumps(new_values),
                    'reason': reason,
                    'reversed_qty': reverse_qty,
                    'link_id': delivery_link_id,
                    'user_id': user_id
                })
            
            # Clear cache
            self.data.search_allocations.clear()
            self.data.get_allocation_detail.clear()
            self.data.get_delivery_links.clear()
            
            logger.info(
                f"Reversed delivery for allocation {allocation_detail_id}: "
                f"{reverse_qty} units from link {delivery_link_id} by user {user_id}"
            )
            
            return OperationResult(
                success=True,
                message=f"Reversed {reverse_qty:,.2f} units successfully",
                data={
                    'allocation_detail_id': allocation_detail_id,
                    'delivery_link_id': delivery_link_id,
                    'reversed_qty': reverse_qty,
                    'new_delivered_qty': new_allocation_delivered,
                    'warnings': validation.warnings
                }
            )
            
        except Exception as e:
            logger.error(f"Error reversing delivery: {e}")
            return OperationResult(
                success=False,
                message="Failed to reverse delivery",
                errors=[str(e)]
            )
    
    # ================================================================
    # BULK OPERATIONS
    # ================================================================
    
    def bulk_update_etd(
        self,
        allocation_detail_ids: List[int],
        new_etd: date,
        reason: str,
        user_id: int
    ) -> OperationResult:
        """
        Update ETD for multiple allocations.
        
        Args:
            allocation_detail_ids: List of allocation IDs
            new_etd: New ETD for all
            reason: Reason for change
            user_id: User performing the action
        """
        try:
            if not allocation_detail_ids:
                return OperationResult(
                    success=False,
                    message="No allocations selected",
                    errors=["No allocations selected"]
                )
            
            # Get all allocations
            allocations_df = self.data.get_allocations_by_ids(allocation_detail_ids)
            if allocations_df.empty:
                return OperationResult(
                    success=False,
                    message="No allocations found",
                    errors=["No allocations found"]
                )
            
            allocations = allocations_df.to_dict('records')
            
            # Validate bulk operation
            validation = self.validator.validate_bulk_etd_update(allocations, new_etd)
            if not validation.is_valid:
                return OperationResult(
                    success=False,
                    message="Validation failed",
                    errors=validation.errors
                )
            
            # Process each allocation
            success_count = 0
            failed_count = 0
            results = []
            
            for alloc in allocations:
                alloc_id = alloc.get('allocation_detail_id')
                result = self.update_etd(alloc_id, new_etd, reason, user_id)
                
                if result.success:
                    success_count += 1
                else:
                    failed_count += 1
                
                results.append({
                    'allocation_detail_id': alloc_id,
                    'success': result.success,
                    'message': result.message
                })
            
            return OperationResult(
                success=failed_count == 0,
                message=f"Updated {success_count} of {len(allocations)} allocations",
                data={
                    'success_count': success_count,
                    'failed_count': failed_count,
                    'new_etd': str(new_etd),
                    'results': results,
                    'warnings': validation.warnings
                }
            )
            
        except Exception as e:
            logger.error(f"Error in bulk ETD update: {e}")
            return OperationResult(
                success=False,
                message="Failed to perform bulk ETD update",
                errors=[str(e)]
            )
    
    def bulk_cancel(
        self,
        allocation_detail_ids: List[int],
        reason: str,
        reason_category: str,
        user_id: int
    ) -> OperationResult:
        """
        Cancel multiple allocations (full cancel for each).
        
        Args:
            allocation_detail_ids: List of allocation IDs
            reason: Reason for cancellation
            reason_category: Category
            user_id: User performing the action
        """
        try:
            if not allocation_detail_ids:
                return OperationResult(
                    success=False,
                    message="No allocations selected",
                    errors=["No allocations selected"]
                )
            
            # Get all allocations
            allocations_df = self.data.get_allocations_by_ids(allocation_detail_ids)
            if allocations_df.empty:
                return OperationResult(
                    success=False,
                    message="No allocations found",
                    errors=["No allocations found"]
                )
            
            allocations = allocations_df.to_dict('records')
            
            # Validate bulk cancel
            validation = self.validator.validate_bulk_cancel(allocations, reason)
            if not validation.is_valid:
                return OperationResult(
                    success=False,
                    message="Validation failed",
                    errors=validation.errors
                )
            
            # Process each allocation
            success_count = 0
            failed_count = 0
            total_cancelled = 0
            results = []
            
            for alloc in allocations:
                alloc_id = alloc.get('allocation_detail_id')
                
                # Calculate cancellable qty for this allocation
                cancel_qty = self.validator.get_cancellable_qty(alloc)
                
                if cancel_qty <= 0:
                    results.append({
                        'allocation_detail_id': alloc_id,
                        'success': False,
                        'message': 'Nothing to cancel'
                    })
                    continue
                
                result = self.cancel_allocation(
                    alloc_id, cancel_qty, reason, reason_category, user_id
                )
                
                if result.success:
                    success_count += 1
                    total_cancelled += cancel_qty
                else:
                    failed_count += 1
                
                results.append({
                    'allocation_detail_id': alloc_id,
                    'success': result.success,
                    'cancelled_qty': cancel_qty if result.success else 0,
                    'message': result.message
                })
            
            return OperationResult(
                success=failed_count == 0,
                message=f"Cancelled {success_count} allocations ({total_cancelled:,.2f} total units)",
                data={
                    'success_count': success_count,
                    'failed_count': failed_count,
                    'total_cancelled': total_cancelled,
                    'results': results,
                    'warnings': validation.warnings
                }
            )
            
        except Exception as e:
            logger.error(f"Error in bulk cancel: {e}")
            return OperationResult(
                success=False,
                message="Failed to perform bulk cancellation",
                errors=[str(e)]
            )
