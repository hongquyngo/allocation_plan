"""
Data Service for Allocation Module
Handles all database queries and data fetching
"""
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Any
import streamlit as st
from sqlalchemy import text

from ..db import get_db_engine
from ..config import config

logger = logging.getLogger(__name__)


class AllocationDataService:
    """Service for fetching allocation-related data from database"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.cache_ttl = config.get_app_setting('CACHE_TTL_SECONDS', 300)
    
    # ==================== OC Pending Data ====================
    
    @st.cache_data(ttl=300)
    def get_oc_pending(_self, filters: Dict = None) -> pd.DataFrame:
        """
        Get OC pending delivery data with filters
        
        Args:
            filters: Dictionary with filter criteria
                - date_from: Start date for ETD
                - date_to: End date for ETD
                - customers: List of customer codes
                - allocation_status: Status filter
                - products: List of product IDs
        
        Returns:
            DataFrame with OC pending data
        """
        try:
            # Build WHERE clause
            where_conditions = []
            params = {}
            
            if filters:
                if filters.get('date_from'):
                    where_conditions.append("etd >= :date_from")
                    params['date_from'] = filters['date_from']
                
                if filters.get('date_to'):
                    where_conditions.append("etd <= :date_to")
                    params['date_to'] = filters['date_to']
                
                if filters.get('customers'):
                    where_conditions.append("customer_code IN :customers")
                    params['customers'] = tuple(filters['customers'])
                
                if filters.get('allocation_status'):
                    status_map = {
                        'Not Allocated': "is_allocated = 'No'",
                        'Partially Allocated': "allocation_coverage_percent BETWEEN 1 AND 99",
                        'Fully Allocated': "allocation_coverage_percent = 100",
                        'Over Allocated': "is_over_allocated = 'Yes'"
                    }
                    if filters['allocation_status'] in status_map:
                        where_conditions.append(status_map[filters['allocation_status']])
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            
            query = f"""
                SELECT 
                    ocd_id,
                    oc_number,
                    customer_po_number,
                    customer,
                    customer_code,
                    legal_entity,
                    product_name,
                    product_id,
                    pt_code,
                    brand,
                    etd,
                    min_allocated_etd,
                    max_allocated_etd,
                    allocated_etds_list,
                    etd_comparison,
                    selling_quantity as pending_quantity,
                    selling_uom,
                    standard_quantity,
                    standard_uom,
                    effective_allocated_qty as allocated_quantity,
                    unallocated_qty,
                    allocation_coverage_percent,
                    is_over_allocated,
                    over_allocated_qty,
                    allocation_warning,
                    is_allocated,
                    allocation_count,
                    allocation_numbers,
                    outstanding_amount_usd,
                    CASE 
                        WHEN is_allocated = 'No' THEN 'Not Allocated'
                        WHEN allocation_coverage_percent >= 100 THEN 'Fully Allocated'
                        WHEN allocation_coverage_percent > 0 THEN 'Partially Allocated'
                        ELSE 'Not Allocated'
                    END as allocation_status
                FROM outbound_oc_pending_delivery_view
                {where_clause}
                ORDER BY etd ASC, outstanding_amount_usd DESC
                LIMIT 500
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            logger.info(f"Loaded {len(df)} OC pending records")
            return df
            
        except Exception as e:
            logger.error(f"Error loading OC pending data: {e}")
            return pd.DataFrame()
    
    # ==================== Supply Source Data ====================
    
    @st.cache_data(ttl=300)
    def get_inventory_summary(_self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get inventory summary grouped by product"""
        try:
            params = {}
            where_clause = "WHERE remaining_quantity > 0"
            
            if product_id:
                where_clause += " AND product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    product_id,
                    product_name,
                    SUM(remaining_quantity) as total_quantity,
                    COUNT(DISTINCT warehouse_id) as locations,
                    MIN(expiry_date) as earliest_expiry
                FROM inventory_detailed_view
                {where_clause}
                GROUP BY product_id, product_name
                ORDER BY product_name
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading inventory summary: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_can_summary(_self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get CAN pending summary"""
        try:
            params = {}
            where_clause = ""
            
            if product_id:
                where_clause = "WHERE product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    product_id,
                    product_name,
                    SUM(pending_quantity) as pending_quantity,
                    MIN(arrival_date) as arrival_date,
                    COUNT(DISTINCT arrival_note_number) as can_count
                FROM can_pending_stockin_view
                {where_clause}
                GROUP BY product_id, product_name
                ORDER BY arrival_date
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading CAN summary: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_po_summary(_self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get PO pending summary"""
        try:
            params = {}
            where_clause = "WHERE pending_standard_arrival_quantity > 0"
            
            if product_id:
                where_clause += " AND product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    product_id,
                    product_name,
                    SUM(pending_standard_arrival_quantity) as pending_quantity,
                    MIN(etd) as etd,
                    COUNT(DISTINCT po_number) as po_count
                FROM purchase_order_full_view
                {where_clause}
                GROUP BY product_id, product_name
                ORDER BY etd
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading PO summary: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_wht_summary(_self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get warehouse transfer summary"""
        try:
            params = {}
            where_clause = "WHERE is_completed = 0"
            
            if product_id:
                where_clause += " AND product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    product_id,
                    product_name,
                    SUM(transfer_quantity) as transfer_quantity,
                    COUNT(DISTINCT warehouse_transfer_line_id) as transfer_count,
                    CASE 
                        WHEN MAX(is_completed) = 1 THEN 'Completed'
                        ELSE 'In Progress'
                    END as status
                FROM warehouse_transfer_details_view
                {where_clause}
                GROUP BY product_id, product_name
                ORDER BY product_name
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading WHT summary: {e}")
            return pd.DataFrame()
    
    # ==================== Detailed Supply Data ====================
    
    def get_supply_details(self, product_id: int, source_type: str) -> pd.DataFrame:
        """Get detailed supply data for allocation"""
        try:
            if source_type == "Inventory":
                return self.get_inventory_details(product_id)
            elif source_type == "Pending CAN":
                return self.get_can_details(product_id)
            elif source_type == "Pending PO":
                return self.get_po_details(product_id)
            elif source_type == "Warehouse Transfer":
                return self.get_wht_details(product_id)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error loading supply details: {e}")
            return pd.DataFrame()
    
    def get_inventory_details(self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get detailed inventory for allocation"""
        try:
            params = {}
            where_clause = "WHERE remaining_quantity > 0"
            
            if product_id:
                where_clause += " AND product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    inventory_history_id as source_id,
                    product_id,
                    product_name,
                    batch_number,
                    expiry_date,
                    warehouse_name,
                    location,
                    remaining_quantity as available_quantity,
                    standard_uom as uom,
                    owning_company_name,
                    days_in_warehouse,
                    'INVENTORY' as source_type
                FROM inventory_detailed_view
                {where_clause}
                ORDER BY expiry_date ASC, batch_number
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading inventory details: {e}")
            return pd.DataFrame()
    
    def get_can_details(self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get detailed CAN pending for allocation"""
        try:
            params = {}
            where_clause = ""
            
            if product_id:
                where_clause = "WHERE product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    can_line_id as source_id,
                    product_id,
                    product_name,
                    arrival_note_number,
                    arrival_date,
                    pending_quantity as available_quantity,
                    standard_uom as uom,
                    days_since_arrival,
                    vendor,
                    po_number,
                    landed_cost_usd,
                    'PENDING_CAN' as source_type
                FROM can_pending_stockin_view
                {where_clause}
                ORDER BY arrival_date, arrival_note_number
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading CAN details: {e}")
            return pd.DataFrame()
    
    def get_po_details(self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get detailed PO pending for allocation"""
        try:
            params = {}
            where_clause = "WHERE pending_standard_arrival_quantity > 0"
            
            if product_id:
                where_clause += " AND product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    po_line_id as source_id,
                    product_id,
                    product_name,
                    po_number,
                    vendor_name,
                    etd,
                    eta,
                    pending_standard_arrival_quantity as available_quantity,
                    standard_uom as uom,
                    status as po_status,
                    'PENDING_PO' as source_type
                FROM purchase_order_full_view
                {where_clause}
                ORDER BY etd, po_number
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading PO details: {e}")
            return pd.DataFrame()
    
    def get_wht_details(self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get detailed warehouse transfer for allocation"""
        try:
            params = {}
            where_clause = "WHERE is_completed = 0"
            
            if product_id:
                where_clause += " AND product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    warehouse_transfer_line_id as source_id,
                    product_id,
                    product_name,
                    batch_number,
                    expiry_date,
                    from_warehouse,
                    to_warehouse,
                    transfer_quantity as available_quantity,
                    standard_uom as uom,
                    transfer_date,
                    is_completed,
                    'PENDING_WHT' as source_type
                FROM warehouse_transfer_details_view
                {where_clause}
                ORDER BY transfer_date DESC
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading WHT details: {e}")
            return pd.DataFrame()
    
    # ==================== Dashboard Metrics ====================
    
    @st.cache_data(ttl=60)
    def get_dashboard_metrics(_self) -> Dict[str, Any]:
        """Get dashboard metrics for display"""
        try:
            query = """
                SELECT 
                    COUNT(DISTINCT ocd_id) as total_pending_ocs,
                    SUM(outstanding_amount_usd) as total_pending_value_usd,
                    COUNT(CASE WHEN is_allocated = 'No' THEN 1 END) as unallocated_count,
                    COUNT(CASE WHEN is_over_allocated = 'Yes' THEN 1 END) as over_allocated_count,
                    SUM(CASE WHEN is_allocated = 'No' THEN selling_quantity ELSE 0 END) as total_unallocated_qty
                FROM outbound_oc_pending_delivery_view
                WHERE etd BETWEEN CURRENT_DATE AND DATE_ADD(CURRENT_DATE, INTERVAL 30 DAY)
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query)).fetchone()
                
                if result:
                    metrics = dict(result._mapping)
                    
                    # Calculate week-over-week change
                    last_week_query = """
                        SELECT COUNT(CASE WHEN is_allocated = 'No' THEN 1 END) as unallocated_count
                        FROM outbound_oc_pending_delivery_view
                        WHERE etd BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY) AND CURRENT_DATE
                    """
                    
                    last_week_result = conn.execute(text(last_week_query)).fetchone()
                    if last_week_result:
                        last_week_count = last_week_result._mapping['unallocated_count']
                        current_count = metrics['unallocated_count']
                        metrics['unallocated_change'] = last_week_count - current_count
                    else:
                        metrics['unallocated_change'] = 0
                    
                    return metrics
            
            return {
                'total_pending_ocs': 0,
                'total_pending_value_usd': 0,
                'unallocated_count': 0,
                'over_allocated_count': 0,
                'total_unallocated_qty': 0,
                'unallocated_change': 0
            }
            
        except Exception as e:
            logger.error(f"Error loading dashboard metrics: {e}")
            return {
                'total_pending_ocs': 0,
                'total_pending_value_usd': 0,
                'unallocated_count': 0,
                'over_allocated_count': 0,
                'total_unallocated_qty': 0,
                'unallocated_change': 0
            }
    
    # ==================== Reference Data ====================
    
    @st.cache_data(ttl=3600)
    def get_customer_list(_self) -> pd.DataFrame:
        """Get list of customers for filter"""
        try:
            query = """
                SELECT DISTINCT 
                    c.company_code as customer_code,
                    c.english_name as customer_name
                FROM companies c
                INNER JOIN (
                    SELECT DISTINCT customer_code 
                    FROM outbound_oc_pending_delivery_view
                ) oc ON c.company_code = oc.customer_code
                WHERE c.delete_flag = 0
                ORDER BY c.english_name
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading customer list: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=3600)
    def get_product_list(_self) -> pd.DataFrame:
        """Get list of products for filter"""
        try:
            query = """
                SELECT DISTINCT 
                    p.id as product_id,
                    p.name as product_name,
                    p.pt_code,
                    b.brand_name
                FROM products p
                LEFT JOIN brands b ON p.brand_id = b.id
                INNER JOIN (
                    SELECT DISTINCT pt_code 
                    FROM outbound_oc_pending_delivery_view
                ) oc ON p.pt_code = oc.pt_code
                WHERE p.delete_flag = 0
                ORDER BY p.name
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading product list: {e}")
            return pd.DataFrame()
    
    # ==================== Allocation Data ====================
    
    def get_existing_allocations(self, oc_detail_id: int) -> pd.DataFrame:
        """Get existing allocations for an OC detail"""
        try:
            query = """
                SELECT 
                    ad.id as allocation_detail_id,
                    ap.allocation_number,
                    ap.allocation_date,
                    ad.allocation_mode,
                    ad.allocated_qty,
                    ad.delivered_qty,
                    ad.allocated_etd,
                    ad.status,
                    COALESCE(ad.supply_source_type, 'No specific source') as supply_source_type,
                    ad.notes,
                    COALESCE(ac.cancelled_qty, 0) as cancelled_qty,
                    (ad.allocated_qty - COALESCE(ac.cancelled_qty, 0)) as effective_qty
                FROM allocation_details ad
                INNER JOIN allocation_plans ap ON ad.allocation_plan_id = ap.id
                LEFT JOIN (
                    SELECT 
                        allocation_detail_id,
                        SUM(CASE WHEN status = 'ACTIVE' THEN cancelled_qty ELSE 0 END) as cancelled_qty
                    FROM allocation_cancellations
                    GROUP BY allocation_detail_id
                ) ac ON ad.id = ac.allocation_detail_id
                WHERE ad.demand_reference_id = :oc_detail_id
                AND ad.demand_type = 'OC'
                AND ad.status = 'ALLOCATED'
                ORDER BY ap.allocation_date DESC
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'oc_detail_id': oc_detail_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading existing allocations: {e}")
            return pd.DataFrame()
    
    def check_supply_availability(self, source_type: str, source_id: int, 
                                 product_id: int) -> Dict[str, Any]:
        """Check current availability of a supply source"""
        try:
            if source_type == "INVENTORY":
                query = """
                    SELECT 
                        remaining_quantity as available_qty,
                        batch_number,
                        expiry_date
                    FROM inventory_detailed_view
                    WHERE inventory_history_id = :source_id
                    AND product_id = :product_id
                """
            elif source_type == "PENDING_CAN":
                query = """
                    SELECT 
                        pending_quantity as available_qty,
                        arrival_note_number,
                        arrival_date
                    FROM can_pending_stockin_view
                    WHERE can_line_id = :source_id
                    AND product_id = :product_id
                """
            elif source_type == "PENDING_PO":
                query = """
                    SELECT 
                        pending_standard_arrival_quantity as available_qty,
                        po_number,
                        etd
                    FROM purchase_order_full_view
                    WHERE po_line_id = :source_id
                    AND product_id = :product_id
                """
            elif source_type == "PENDING_WHT":
                query = """
                    SELECT 
                        transfer_quantity as available_qty,
                        from_warehouse,
                        to_warehouse
                    FROM warehouse_transfer_details_view
                    WHERE warehouse_transfer_line_id = :source_id
                    AND product_id = :product_id
                    AND is_completed = 0
                """
            else:
                return {'available': False, 'available_qty': 0}
            
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(query), 
                    params={'source_id': source_id, 'product_id': product_id}
                ).fetchone()
            
            if result:
                return {
                    'available': True,
                    'available_qty': float(result._mapping['available_qty'] or 0),
                    'details': dict(result._mapping)
                }
            
            return {'available': False, 'available_qty': 0}
            
        except Exception as e:
            logger.error(f"Error checking supply availability: {e}")
            return {'available': False, 'available_qty': 0}

    def get_total_available_supply(self, product_id: int) -> Dict[str, Any]:
        """Get total available supply from all sources for a product"""
        try:
            total_available = 0
            sources_summary = []
            
            # Check inventory
            inventory_df = self.get_inventory_details(product_id)
            if not inventory_df.empty:
                inventory_total = inventory_df['available_quantity'].sum()
                total_available += inventory_total
                sources_summary.append({
                    'source': 'Inventory',
                    'quantity': inventory_total,
                    'count': len(inventory_df)
                })
            
            # Check pending CAN
            can_df = self.get_can_details(product_id)
            if not can_df.empty:
                can_total = can_df['available_quantity'].sum()
                total_available += can_total
                sources_summary.append({
                    'source': 'Pending CAN',
                    'quantity': can_total,
                    'count': len(can_df)
                })
            
            # Check pending PO
            po_df = self.get_po_details(product_id)
            if not po_df.empty:
                po_total = po_df['available_quantity'].sum()
                total_available += po_total
                sources_summary.append({
                    'source': 'Pending PO',
                    'quantity': po_total,
                    'count': len(po_df)
                })
            
            # Check warehouse transfer
            wht_df = self.get_wht_details(product_id)
            if not wht_df.empty:
                wht_total = wht_df['available_quantity'].sum()
                total_available += wht_total
                sources_summary.append({
                    'source': 'Warehouse Transfer',
                    'quantity': wht_total,
                    'count': len(wht_df)
                })
            
            return {
                'total_available': total_available,
                'sources': sources_summary,
                'has_supply': total_available > 0
            }
            
        except Exception as e:
            logger.error(f"Error getting total available supply: {e}")
            return {
                'total_available': 0,
                'sources': [],
                'has_supply': False
            }