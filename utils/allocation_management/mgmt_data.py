"""
Allocation Management Data Layer
=================================
Data access for allocation management dashboard.

Uses: allocation_delivery_status_view (main view)
"""

import pandas as pd
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, date
import streamlit as st
from sqlalchemy import text

from utils.db import get_db_engine

logger = logging.getLogger(__name__)


class AllocationManagementData:
    """Data access layer for allocation management dashboard"""
    
    def __init__(self):
        self.engine = get_db_engine()
    
    # ================================================================
    # DASHBOARD STATISTICS
    # ================================================================
    
    @st.cache_data(ttl=120)
    def get_dashboard_statistics(_self) -> Dict[str, Any]:
        """Get comprehensive dashboard statistics"""
        try:
            query = text("""
                SELECT 
                    -- By Status
                    COUNT(*) as total,
                    SUM(CASE WHEN effective_status = 'ALLOCATED' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN effective_status = 'PARTIAL_DELIVERED' THEN 1 ELSE 0 END) as partial,
                    SUM(CASE WHEN effective_status = 'DELIVERED' THEN 1 ELSE 0 END) as delivered,
                    SUM(CASE WHEN effective_status = 'CANCELLED' THEN 1 ELSE 0 END) as cancelled,
                    
                    -- By Supply Source
                    SUM(CASE WHEN supply_source_type = 'INVENTORY' THEN 1 ELSE 0 END) as from_inventory,
                    SUM(CASE WHEN supply_source_type = 'PENDING_CAN' THEN 1 ELSE 0 END) as from_can,
                    SUM(CASE WHEN supply_source_type = 'PENDING_PO' THEN 1 ELSE 0 END) as from_po,
                    SUM(CASE WHEN supply_source_type = 'PENDING_WHT' THEN 1 ELSE 0 END) as from_wht,
                    
                    -- Quantities
                    SUM(allocated_qty) as total_allocated_qty,
                    SUM(delivered_qty) as total_delivered_qty,
                    SUM(remaining_qty) as total_remaining_qty,
                    
                    -- Alerts
                    SUM(CASE WHEN days_past_etd > 0 AND effective_status IN ('ALLOCATED', 'PARTIAL_DELIVERED') THEN 1 ELSE 0 END) as overdue_count
                    
                FROM allocation_delivery_status_view
                WHERE detail_status = 'ALLOCATED'
            """)
            
            with _self.engine.connect() as conn:
                result = conn.execute(query).fetchone()
                
                if result:
                    return {
                        'total': int(result[0] or 0),
                        'pending': int(result[1] or 0),
                        'partial': int(result[2] or 0),
                        'delivered': int(result[3] or 0),
                        'cancelled': int(result[4] or 0),
                        'from_inventory': int(result[5] or 0),
                        'from_can': int(result[6] or 0),
                        'from_po': int(result[7] or 0),
                        'from_wht': int(result[8] or 0),
                        'total_allocated_qty': float(result[9] or 0),
                        'total_delivered_qty': float(result[10] or 0),
                        'total_remaining_qty': float(result[11] or 0),
                        'overdue_count': int(result[12] or 0)
                    }
            
            return {k: 0 for k in ['total', 'pending', 'partial', 'delivered', 'cancelled',
                                   'from_inventory', 'from_can', 'from_po', 'from_wht',
                                   'total_allocated_qty', 'total_delivered_qty', 'total_remaining_qty',
                                   'overdue_count']}
            
        except Exception as e:
            logger.error(f"Error getting dashboard statistics: {e}")
            return {k: 0 for k in ['total', 'pending', 'partial', 'delivered', 'cancelled',
                                   'from_inventory', 'from_can', 'from_po', 'from_wht',
                                   'total_allocated_qty', 'total_delivered_qty', 'total_remaining_qty',
                                   'overdue_count']}
    
    # ================================================================
    # SEARCH ALLOCATIONS
    # ================================================================
    
    @st.cache_data(ttl=60)
    def search_allocations(
        _self,
        allocation_number: str = None,
        product_id: int = None,
        customer_code: str = None,
        effective_status: str = None,
        supply_source_type: str = None,
        date_from: date = None,
        date_to: date = None,
        created_by: int = None,
        show_overdue_only: bool = False,
        limit: int = 500
    ) -> pd.DataFrame:
        """
        Search allocations with flexible filters.
        Uses allocation_delivery_status_view.
        """
        try:
            conditions = ["detail_status = 'ALLOCATED'"]
            params = {'limit': limit}
            
            if allocation_number:
                conditions.append("allocation_number LIKE :allocation_number")
                params['allocation_number'] = f"%{allocation_number}%"
            
            if product_id:
                conditions.append("product_id = :product_id")
                params['product_id'] = product_id
            
            if customer_code:
                conditions.append("customer_code = :customer_code")
                params['customer_code'] = customer_code
            
            if effective_status:
                conditions.append("effective_status = :effective_status")
                params['effective_status'] = effective_status
            
            if supply_source_type:
                conditions.append("supply_source_type = :supply_source_type")
                params['supply_source_type'] = supply_source_type
            
            if date_from:
                conditions.append("DATE(allocation_date) >= :date_from")
                params['date_from'] = date_from
            
            if date_to:
                conditions.append("DATE(allocation_date) <= :date_to")
                params['date_to'] = date_to
            
            if created_by:
                conditions.append("creator_id = :created_by")
                params['created_by'] = created_by
            
            if show_overdue_only:
                conditions.append("days_past_etd > 0")
                conditions.append("effective_status IN ('ALLOCATED', 'PARTIAL_DELIVERED')")
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
                SELECT 
                    id,
                    allocation_plan_id,
                    allocation_number,
                    allocation_date,
                    allocation_mode,
                    demand_type,
                    demand_reference_id,
                    demand_number,
                    product_id,
                    pt_code,
                    product_name,
                    brand_name,
                    customer_code,
                    customer_name,
                    legal_entity_name,
                    requested_qty,
                    original_allocated_qty,
                    allocated_qty,
                    delivered_qty,
                    cancelled_qty,
                    remaining_qty,
                    original_etd,
                    allocated_etd,
                    last_updated_etd_date,
                    etd_update_count,
                    supply_source_type,
                    supply_source_id,
                    notes,
                    creator_id,
                    creator_name,
                    creator_email,
                    created_date,
                    cancel_count,
                    latest_cancel_reason,
                    linked_delivery_lines,
                    delivery_numbers,
                    last_delivery_date,
                    fulfillment_rate,
                    days_past_etd,
                    effective_status
                FROM allocation_delivery_status_view
                WHERE {where_clause}
                ORDER BY allocation_date DESC, id DESC
                LIMIT :limit
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            
            return df
            
        except Exception as e:
            logger.error(f"Error searching allocations: {e}")
            return pd.DataFrame()
    
    # ================================================================
    # GET SINGLE ALLOCATION
    # ================================================================
    
    def get_allocation_detail(self, allocation_id: int) -> Optional[Dict]:
        """Get single allocation detail by ID"""
        try:
            query = text("""
                SELECT *
                FROM allocation_delivery_status_view
                WHERE id = :id
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {'id': allocation_id}).fetchone()
                if result:
                    return dict(result._mapping)
            return None
            
        except Exception as e:
            logger.error(f"Error getting allocation detail: {e}")
            return None
    
    def get_allocations_by_ids(self, allocation_ids: List[int]) -> pd.DataFrame:
        """Get multiple allocations by IDs for bulk operations"""
        if not allocation_ids:
            return pd.DataFrame()
        
        try:
            ids_str = ','.join(map(str, allocation_ids))
            
            query = f"""
                SELECT *
                FROM allocation_delivery_status_view
                WHERE id IN ({ids_str})
                ORDER BY allocation_date DESC
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            return df
            
        except Exception as e:
            logger.error(f"Error getting allocations by IDs: {e}")
            return pd.DataFrame()
    
    # ================================================================
    # DELIVERY LINKS
    # ================================================================
    
    def get_delivery_links(self, allocation_detail_id: int) -> pd.DataFrame:
        """Get delivery history for an allocation"""
        try:
            query = text("""
                SELECT 
                    adl.id as delivery_link_id,
                    adl.allocation_detail_id,
                    adl.delivery_detail_id,
                    adl.delivered_qty,
                    adl.created_at as link_created_at,
                    
                    -- Delivery request details
                    sod.id as delivery_id,
                    sod.dn_number,
                    sod.created_date as delivery_date,
                    sod.status as delivery_status
                    
                FROM allocation_delivery_links adl
                LEFT JOIN stock_out_delivery_request_details sodrd 
                    ON adl.delivery_detail_id = sodrd.id
                LEFT JOIN stock_out_delivery sod 
                    ON sodrd.delivery_id = sod.id
                WHERE adl.allocation_detail_id = :allocation_detail_id
                ORDER BY adl.created_at DESC
            """)
            
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={'allocation_detail_id': allocation_detail_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting delivery links: {e}")
            return pd.DataFrame()
    
    def get_delivery_link_detail(self, delivery_link_id: int) -> Optional[Dict]:
        """Get single delivery link detail"""
        try:
            query = text("""
                SELECT 
                    adl.*,
                    ad.product_id,
                    ad.allocated_qty,
                    ad.delivered_qty as total_delivered
                FROM allocation_delivery_links adl
                JOIN allocation_details ad ON adl.allocation_detail_id = ad.id
                WHERE adl.id = :id
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {'id': delivery_link_id}).fetchone()
                if result:
                    return dict(result._mapping)
            return None
            
        except Exception as e:
            logger.error(f"Error getting delivery link detail: {e}")
            return None
    
    # ================================================================
    # CANCELLATION HISTORY
    # ================================================================
    
    def get_cancellation_history(self, allocation_detail_id: int) -> pd.DataFrame:
        """Get cancellation history for an allocation"""
        try:
            query = text("""
                SELECT 
                    ac.id as cancellation_id,
                    ac.allocation_detail_id,
                    ac.allocation_plan_id,
                    ac.cancelled_qty,
                    ac.reason,
                    ac.reason_category,
                    ac.cancelled_by_user_id,
                    u_cancel.username as cancelled_by_username,
                    ac.cancelled_date,
                    ac.status,
                    ac.reversed_by_user_id,
                    u_reverse.username as reversed_by_username,
                    ac.reversed_date,
                    ac.reversal_reason
                FROM allocation_cancellations ac
                LEFT JOIN users u_cancel ON ac.cancelled_by_user_id = u_cancel.id
                LEFT JOIN users u_reverse ON ac.reversed_by_user_id = u_reverse.id
                WHERE ac.allocation_detail_id = :allocation_detail_id
                ORDER BY ac.cancelled_date DESC
            """)
            
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={'allocation_detail_id': allocation_detail_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting cancellation history: {e}")
            return pd.DataFrame()
    
    # ================================================================
    # AUDIT LOG
    # ================================================================
    
    def get_audit_history(self, allocation_detail_id: int) -> pd.DataFrame:
        """Get audit log for an allocation"""
        try:
            query = text("""
                SELECT 
                    aal.id as audit_id,
                    aal.allocation_detail_id,
                    aal.plan_id,
                    aal.action_type,
                    aal.old_values,
                    aal.new_values,
                    aal.change_reason,
                    aal.cancelled_qty,
                    aal.reversed_qty,
                    aal.delivery_link_id,
                    aal.performed_by,
                    u.username as performed_by_username,
                    aal.performed_at,
                    aal.ip_address
                FROM allocation_audit_log aal
                LEFT JOIN users u ON aal.performed_by = u.id
                WHERE aal.allocation_detail_id = :allocation_detail_id
                ORDER BY aal.performed_at DESC
            """)
            
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={'allocation_detail_id': allocation_detail_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting audit history: {e}")
            return pd.DataFrame()
    
    # ================================================================
    # FILTER OPTIONS
    # ================================================================
    
    @st.cache_data(ttl=300)
    def get_filter_options(_self) -> Dict[str, List]:
        """Get options for filter dropdowns"""
        try:
            options = {
                'products': [],
                'customers': [],
                'creators': [],
                'statuses': ['ALLOCATED', 'PARTIAL_DELIVERED', 'DELIVERED', 'CANCELLED'],
                'supply_sources': ['INVENTORY', 'PENDING_CAN', 'PENDING_PO', 'PENDING_WHT']
            }
            
            with _self.engine.connect() as conn:
                # Products with allocations
                products_query = text("""
                    SELECT DISTINCT 
                        product_id, 
                        CONCAT(pt_code, ' | ', SUBSTRING(product_name, 1, 40)) as display
                    FROM allocation_delivery_status_view
                    WHERE detail_status = 'ALLOCATED'
                    ORDER BY pt_code
                    LIMIT 500
                """)
                products = conn.execute(products_query).fetchall()
                options['products'] = [(r[0], r[1]) for r in products]
                
                # Customers with allocations
                customers_query = text("""
                    SELECT DISTINCT 
                        customer_code,
                        CONCAT(customer_code, ' - ', SUBSTRING(customer_name, 1, 30)) as display
                    FROM allocation_delivery_status_view
                    WHERE customer_code IS NOT NULL AND detail_status = 'ALLOCATED'
                    ORDER BY customer_code
                    LIMIT 200
                """)
                customers = conn.execute(customers_query).fetchall()
                options['customers'] = [(r[0], r[1]) for r in customers]
                
                # Creators
                creators_query = text("""
                    SELECT DISTINCT 
                        creator_id,
                        creator_name
                    FROM allocation_delivery_status_view
                    WHERE creator_id IS NOT NULL
                    ORDER BY creator_name
                """)
                creators = conn.execute(creators_query).fetchall()
                options['creators'] = [(r[0], r[1]) for r in creators]
            
            return options
            
        except Exception as e:
            logger.error(f"Error getting filter options: {e}")
            return {
                'products': [],
                'customers': [],
                'creators': [],
                'statuses': [],
                'supply_sources': []
            }
    
    # ================================================================
    # RAW DATA FOR SERVICE LAYER
    # ================================================================
    
    def get_allocation_details_raw(self, allocation_detail_id: int) -> Optional[Dict]:
        """Get raw allocation_details record for updates"""
        try:
            query = text("""
                SELECT 
                    ad.*,
                    ap.allocation_number,
                    ap.creator_id
                FROM allocation_details ad
                JOIN allocation_plans ap ON ad.allocation_plan_id = ap.id
                WHERE ad.id = :id
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {'id': allocation_detail_id}).fetchone()
                if result:
                    return dict(result._mapping)
            return None
            
        except Exception as e:
            logger.error(f"Error getting raw allocation details: {e}")
            return None
    
    # ================================================================
    # OC INFO FOR EMAIL
    # ================================================================
    
    def get_oc_info_for_allocation(self, allocation_detail_id: int) -> Optional[Dict]:
        """Get OC information including creator email for notifications"""
        try:
            query = text("""
                SELECT 
                    v.demand_reference_id as ocd_id,
                    v.demand_number as oc_number,
                    v.customer_name as customer,
                    v.customer_code,
                    v.product_name,
                    v.pt_code,
                    v.allocated_etd as etd,
                    oc.created_by as oc_created_by,
                    e.email as oc_creator_email,
                    TRIM(CONCAT(COALESCE(e.first_name, ''), ' ', COALESCE(e.last_name, ''))) as oc_creator_name
                FROM allocation_delivery_status_view v
                LEFT JOIN order_comfirmation_details ocd ON v.demand_reference_id = ocd.id
                LEFT JOIN order_confirmations oc ON ocd.order_confirmation_id = oc.id
                LEFT JOIN employees e ON oc.created_by = e.keycloak_id
                WHERE v.id = :allocation_id
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {'allocation_id': allocation_detail_id}).fetchone()
                if result:
                    return dict(result._mapping)
            return None
            
        except Exception as e:
            logger.error(f"Error getting OC info: {e}")
            return None
    
    # ================================================================
    # USER INFO
    # ================================================================
    
    def get_user_info(self, user_id: int) -> Optional[Dict]:
        """Get user information"""
        try:
            query = text("""
                SELECT 
                    u.id,
                    u.username,
                    u.email,
                    TRIM(CONCAT(COALESCE(e.first_name, u.username), ' ', COALESCE(e.last_name, ''))) AS full_name
                FROM users u
                LEFT JOIN employees e ON u.employee_id = e.id
                WHERE u.id = :user_id
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {'user_id': user_id}).fetchone()
                if result:
                    return dict(result._mapping)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user info: {e}")
            return None
