"""
Data Service for Allocation Module - Product Centric View
Optimized queries for product-first approach
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
    """Service for fetching allocation-related data with product-centric approach"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.cache_ttl = config.get_app_setting('CACHE_TTL_SECONDS', 300)
        
        # Log database connection info for debugging
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT DATABASE()")).fetchone()
                logger.info(f"Connected to database: {result[0] if result else 'Unknown'}")
        except Exception as e:
            logger.error(f"Error checking database connection: {e}")
    
    # ==================== Product-Centric Queries ====================
    
    @st.cache_data(ttl=300)
    def get_products_with_demand_supply(_self, filters: Dict = None, 
                                       page: int = 1, page_size: int = 50) -> pd.DataFrame:
        """
        Get products with aggregated demand and supply information
        
        Args:
            filters: Filter criteria (search, supply_status, etc.)
            page: Page number for pagination
            page_size: Items per page
            
        Returns:
            DataFrame with product-level aggregated data
        """
        try:
            # Build WHERE conditions
            where_conditions = ["p.delete_flag = 0"]
            having_conditions = []
            params = {
                'offset': (page - 1) * page_size,
                'limit': page_size
            }
            
            # Debug log
            logger.info(f"Building query with filters: {filters}")
            
            # Apply filters
            if filters:
                # Search filter
                if filters.get('search'):
                    search_term = f"%{filters['search']}%"
                    where_conditions.append("""
                        (p.name LIKE :search OR 
                         p.pt_code LIKE :search OR 
                         EXISTS (
                            SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                            WHERE ocpd.product_id = p.id 
                            AND ocpd.customer LIKE :search
                         ))
                    """)
                    params['search'] = search_term
                
                # Customer filter
                if filters.get('customer'):
                    where_conditions.append("""
                        EXISTS (
                            SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                            WHERE ocpd.product_id = p.id 
                            AND ocpd.customer_code = :customer_code
                        )
                    """)
                    params['customer_code'] = filters['customer']
                
                # Brand filter
                if filters.get('brand'):
                    where_conditions.append("p.brand_id = :brand_id")
                    params['brand_id'] = filters['brand']
                
                # ETD days filter
                if filters.get('etd_days'):
                    where_conditions.append("""
                        EXISTS (
                            SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                            WHERE ocpd.product_id = p.id 
                            AND ocpd.etd <= DATE_ADD(CURRENT_DATE, INTERVAL :etd_days DAY)
                        )
                    """)
                    params['etd_days'] = filters['etd_days']
                
                # Date range filter
                if filters.get('date_from') and filters.get('date_to'):
                    where_conditions.append("""
                        EXISTS (
                            SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                            WHERE ocpd.product_id = p.id 
                            AND ocpd.etd BETWEEN :date_from AND :date_to
                        )
                    """)
                    params['date_from'] = filters['date_from']
                    params['date_to'] = filters['date_to']
                
                # Supply status filter
                if filters.get('supply_status') == 'low':
                    having_conditions.append("(total_supply < total_demand * 0.5 AND total_demand > 0)")
                
                # ETD urgency filter
                if filters.get('etd_urgency') == 'urgent':
                    where_conditions.append("""
                        EXISTS (
                            SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                            WHERE ocpd.product_id = p.id 
                            AND ocpd.etd <= DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY)
                        )
                    """)
                
                # Allocation status filter
                if filters.get('allocation_status') == 'none':
                    where_conditions.append("""
                        EXISTS (
                            SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                            WHERE ocpd.product_id = p.id 
                            AND ocpd.is_allocated = 'No'
                        )
                    """)
                
                # Has inventory filter
                if filters.get('has_inventory'):
                    having_conditions.append("inventory_qty > 0")
                
                # Coverage filter
                if filters.get('coverage'):
                    if filters['coverage'] == 'Critical (<20%)':
                        having_conditions.append("(total_supply < total_demand * 0.2 AND total_demand > 0)")
                    elif filters['coverage'] == 'Low (<50%)':
                        having_conditions.append("(total_supply < total_demand * 0.5 AND total_demand > 0)")
                    elif filters['coverage'] == 'Partial (50-99%)':
                        having_conditions.append("(total_supply >= total_demand * 0.5 AND total_supply < total_demand)")
                    elif filters['coverage'] == 'Full (≥100%)':
                        having_conditions.append("(total_supply >= total_demand)")
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            having_clause = f"HAVING {' AND '.join(having_conditions)}" if having_conditions else ""
            
            # Main query using actual view columns
            query = f"""
                WITH product_demand AS (
                    -- Aggregate demand by product from OC view using standard UOM
                    SELECT 
                        product_id,
                        COUNT(DISTINCT ocd_id) as oc_count,
                        SUM(pending_standard_delivery_quantity) as total_demand,
                        SUM(outstanding_amount_usd) as total_value,
                        MIN(etd) as earliest_etd,
                        COUNT(CASE WHEN etd <= DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY) THEN 1 END) as urgent_ocs
                    FROM outbound_oc_pending_delivery_view
                    WHERE pending_standard_delivery_quantity > 0
                    GROUP BY product_id
                ),
                product_supply AS (
                    -- Aggregate supply from all sources using actual view columns
                    SELECT 
                        product_id,
                        SUM(inventory_qty) as inventory_qty,
                        SUM(can_qty) as can_qty,
                        SUM(po_qty) as po_qty,
                        SUM(wht_qty) as wht_qty,
                        SUM(total_qty) as total_supply
                    FROM (
                        -- Inventory
                        SELECT 
                            product_id,
                            SUM(remaining_quantity) as inventory_qty,
                            0 as can_qty,
                            0 as po_qty,
                            0 as wht_qty,
                            SUM(remaining_quantity) as total_qty
                        FROM inventory_detailed_view
                        WHERE remaining_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        -- Pending CAN
                        SELECT 
                            product_id,
                            0 as inventory_qty,
                            SUM(pending_quantity) as can_qty,
                            0 as po_qty,
                            0 as wht_qty,
                            SUM(pending_quantity) as total_qty
                        FROM can_pending_stockin_view
                        WHERE pending_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        -- Pending PO
                        SELECT 
                            product_id,
                            0 as inventory_qty,
                            0 as can_qty,
                            SUM(pending_standard_arrival_quantity) as po_qty,
                            0 as wht_qty,
                            SUM(pending_standard_arrival_quantity) as total_qty
                        FROM purchase_order_full_view
                        WHERE pending_standard_arrival_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        -- Warehouse Transfer
                        SELECT 
                            product_id,
                            0 as inventory_qty,
                            0 as can_qty,
                            0 as po_qty,
                            SUM(transfer_quantity) as wht_qty,
                            SUM(transfer_quantity) as total_qty
                        FROM warehouse_transfer_details_view
                        WHERE is_completed = 0
                        GROUP BY product_id
                    ) supply_union
                    GROUP BY product_id
                )
                SELECT 
                    p.id as product_id,
                    p.name as product_name,
                    p.pt_code,
                    COALESCE(p.package_size, 1) as package_size,
                    p.uom as standard_uom,
                    b.brand_name,
                    COALESCE(pd.oc_count, 0) as oc_count,
                    COALESCE(pd.total_demand, 0) as total_demand,
                    COALESCE(pd.total_value, 0) as total_value,
                    pd.earliest_etd,
                    COALESCE(pd.urgent_ocs, 0) as urgent_ocs,
                    COALESCE(ps.inventory_qty, 0) as inventory_qty,
                    COALESCE(ps.can_qty, 0) as can_qty,
                    COALESCE(ps.po_qty, 0) as po_qty,
                    COALESCE(ps.wht_qty, 0) as wht_qty,
                    COALESCE(ps.total_supply, 0) as total_supply,
                    CASE 
                        WHEN COALESCE(ps.total_supply, 0) >= COALESCE(pd.total_demand, 0) THEN 'Sufficient'
                        WHEN COALESCE(ps.total_supply, 0) >= COALESCE(pd.total_demand, 0) * 0.5 THEN 'Partial'
                        WHEN COALESCE(ps.total_supply, 0) > 0 THEN 'Low'
                        ELSE 'No Supply'
                    END as supply_status,
                    CASE 
                        WHEN pd.earliest_etd <= DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY) THEN 1
                        ELSE 0
                    END as is_urgent
                FROM products p
                LEFT JOIN brands b ON p.brand_id = b.id
                INNER JOIN product_demand pd ON p.id = pd.product_id
                LEFT JOIN product_supply ps ON p.id = ps.product_id
                {where_clause}
                {having_clause}
                ORDER BY 
                    pd.urgent_ocs DESC,  -- Urgent items first
                    (COALESCE(ps.total_supply, 0) / NULLIF(pd.total_demand, 0)) ASC,  -- Low supply ratio
                    pd.total_value DESC  -- High value items
                LIMIT :limit OFFSET :offset
            """
            
            # Debug log query
            logger.debug(f"Query: {query}")
            logger.debug(f"Params: {params}")
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            
            logger.info(f"Query returned {len(df)} products (page {page})")
            return df
            
        except Exception as e:
            logger.error(f"Error loading products with demand/supply: {e}")
            logger.exception(e)  # Full stack trace
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_ocs_by_product(_self, product_id: int) -> pd.DataFrame:
        """Get all pending OCs for a specific product"""
        try:
            query = """
                SELECT 
                    ocd_id,
                    oc_number,
                    customer_po_number,
                    customer,
                    customer_code,
                    product_id,
                    product_name,
                    etd,
                    selling_quantity as order_quantity,
                    pending_selling_delivery_quantity as pending_quantity,
                    selling_uom,
                    standard_uom,
                    effective_allocated_qty as allocated_quantity,
                    outstanding_amount_usd,
                    allocation_coverage_percent,
                    is_allocated,
                    allocation_numbers
                FROM outbound_oc_pending_delivery_view
                WHERE product_id = :product_id
                AND pending_selling_delivery_quantity > 0
                ORDER BY etd ASC, outstanding_amount_usd DESC
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(
                    text(query), 
                    conn, 
                    params={'product_id': product_id}
                )
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading OCs for product {product_id}: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_all_supply_for_product(_self, product_id: int) -> pd.DataFrame:
        """Get all available supply sources for a product in one query"""
        try:
            query = """
                -- Union all supply sources with actual view columns
                SELECT 
                    'INVENTORY' as source_type,
                    inventory_history_id as source_id,
                    CONCAT('Batch ', batch_number) as reference,
                    remaining_quantity as available_quantity,
                    standard_uom as uom,
                    expiry_date,
                    NULL as arrival_date,
                    NULL as etd,
                    batch_number,
                    location,
                    warehouse_name,
                    NULL as from_warehouse,
                    NULL as to_warehouse,
                    NULL as vendor_name,
                    NULL as po_number,
                    NULL as arrival_note_number
                FROM inventory_detailed_view
                WHERE product_id = :product_id
                AND remaining_quantity > 0
                
                UNION ALL
                
                SELECT 
                    'PENDING_CAN' as source_type,
                    can_line_id as source_id,
                    arrival_note_number as reference,
                    pending_quantity as available_quantity,
                    standard_uom as uom,
                    NULL as expiry_date,
                    arrival_date,
                    NULL as etd,
                    NULL as batch_number,
                    NULL as location,
                    NULL as warehouse_name,
                    NULL as from_warehouse,
                    NULL as to_warehouse,
                    vendor as vendor_name,
                    po_number,
                    arrival_note_number
                FROM can_pending_stockin_view
                WHERE product_id = :product_id
                AND pending_quantity > 0
                
                UNION ALL
                
                SELECT 
                    'PENDING_PO' as source_type,
                    po_line_id as source_id,
                    po_number as reference,
                    pending_standard_arrival_quantity as available_quantity,
                    standard_uom as uom,
                    NULL as expiry_date,
                    NULL as arrival_date,
                    etd,
                    NULL as batch_number,
                    NULL as location,
                    NULL as warehouse_name,
                    NULL as from_warehouse,
                    NULL as to_warehouse,
                    vendor_name,
                    po_number,
                    NULL as arrival_note_number
                FROM purchase_order_full_view
                WHERE product_id = :product_id
                AND pending_standard_arrival_quantity > 0
                
                UNION ALL
                
                SELECT 
                    'PENDING_WHT' as source_type,
                    warehouse_transfer_line_id as source_id,
                    CONCAT(from_warehouse, ' → ', to_warehouse) as reference,
                    transfer_quantity as available_quantity,
                    standard_uom as uom,
                    expiry_date,
                    NULL as arrival_date,
                    transfer_date as etd,
                    batch_number,
                    NULL as location,
                    NULL as warehouse_name,
                    from_warehouse,
                    to_warehouse,
                    NULL as vendor_name,
                    NULL as po_number,
                    NULL as arrival_note_number
                FROM warehouse_transfer_details_view
                WHERE product_id = :product_id
                AND is_completed = 0
                AND transfer_quantity > 0
                
                ORDER BY 
                    CASE source_type 
                        WHEN 'INVENTORY' THEN 1 
                        WHEN 'PENDING_CAN' THEN 2
                        WHEN 'PENDING_PO' THEN 3
                        WHEN 'PENDING_WHT' THEN 4
                    END,
                    COALESCE(expiry_date, arrival_date, etd)
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(
                    text(query), 
                    conn, 
                    params={'product_id': product_id}
                )
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading supply for product {product_id}: {e}")
            return pd.DataFrame()
    
    # ==================== Dashboard Metrics - Product View ====================
    
    @st.cache_data(ttl=60)
    def get_dashboard_metrics_product_view(_self) -> Dict[str, Any]:
        """Get dashboard metrics for product-centric view"""
        try:
            query = """
                WITH product_summary AS (
                    SELECT 
                        p.id as product_id,
                        COALESCE(SUM(ocpd.pending_standard_delivery_quantity), 0) as demand_qty,
                        COALESCE(inv.inventory_qty, 0) + 
                        COALESCE(can.can_qty, 0) + 
                        COALESCE(po.po_qty, 0) + 
                        COALESCE(wht.wht_qty, 0) as supply_qty,
                        MIN(ocpd.etd) as earliest_etd
                    FROM products p
                    INNER JOIN outbound_oc_pending_delivery_view ocpd ON p.id = ocpd.product_id
                    LEFT JOIN (
                        SELECT product_id, SUM(remaining_quantity) as inventory_qty
                        FROM inventory_detailed_view
                        WHERE remaining_quantity > 0
                        GROUP BY product_id
                    ) inv ON p.id = inv.product_id
                    LEFT JOIN (
                        SELECT product_id, SUM(pending_quantity) as can_qty
                        FROM can_pending_stockin_view
                        WHERE pending_quantity > 0
                        GROUP BY product_id
                    ) can ON p.id = can.product_id
                    LEFT JOIN (
                        SELECT product_id, SUM(pending_standard_arrival_quantity) as po_qty
                        FROM purchase_order_full_view
                        WHERE pending_standard_arrival_quantity > 0
                        GROUP BY product_id
                    ) po ON p.id = po.product_id
                    LEFT JOIN (
                        SELECT product_id, SUM(transfer_quantity) as wht_qty
                        FROM warehouse_transfer_details_view
                        WHERE is_completed = 0
                        GROUP BY product_id
                    ) wht ON p.id = wht.product_id
                    WHERE ocpd.pending_standard_delivery_quantity > 0
                    GROUP BY p.id, inv.inventory_qty, can.can_qty, po.po_qty, wht.wht_qty
                )
                SELECT 
                    COUNT(DISTINCT product_id) as total_products,
                    SUM(demand_qty) as total_demand_qty,
                    SUM(supply_qty) as total_supply_qty,
                    COUNT(CASE WHEN supply_qty < demand_qty * 0.2 THEN 1 END) as critical_products,
                    COUNT(CASE WHEN earliest_etd <= DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY) THEN 1 END) as urgent_etd_count
                FROM product_summary
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query)).fetchone()
                
                if result:
                    return dict(result._mapping)
            
            return {
                'total_products': 0,
                'total_demand_qty': 0,
                'total_supply_qty': 0,
                'critical_products': 0,
                'urgent_etd_count': 0
            }
            
        except Exception as e:
            logger.error(f"Error loading dashboard metrics: {e}")
            return {
                'total_products': 0,
                'total_demand_qty': 0,
                'total_supply_qty': 0,
                'critical_products': 0,
                'urgent_etd_count': 0
            }
    
    # ==================== Existing Methods (Kept for compatibility) ====================
    
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
                    AND remaining_quantity > 0
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
                    AND pending_quantity > 0
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
                    AND pending_standard_arrival_quantity > 0
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
                    AND transfer_quantity > 0
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
            # Use the optimized single query
            supply_df = self.get_all_supply_for_product(product_id)
            
            if supply_df.empty:
                return {
                    'total_available': 0,
                    'sources': [],
                    'has_supply': False
                }
            
            # Group by source type
            sources_summary = []
            for source_type in ['INVENTORY', 'PENDING_CAN', 'PENDING_PO', 'PENDING_WHT']:
                type_supplies = supply_df[supply_df['source_type'] == source_type]
                if not type_supplies.empty:
                    source_label = {
                        'INVENTORY': 'Inventory',
                        'PENDING_CAN': 'Pending CAN',
                        'PENDING_PO': 'Pending PO',
                        'PENDING_WHT': 'Warehouse Transfer'
                    }.get(source_type, source_type)
                    
                    sources_summary.append({
                        'source': source_label,
                        'quantity': type_supplies['available_quantity'].sum(),
                        'count': len(type_supplies)
                    })
            
            total_available = supply_df['available_quantity'].sum()
            
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
    
    # ==================== Optimized Summary Methods ====================
    
    @st.cache_data(ttl=300)
    def get_inventory_summary(_self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get inventory summary optimized for product view"""
        try:
            params = {}
            where_clause = "WHERE remaining_quantity > 0"
            
            if product_id:
                where_clause += " AND product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    inventory_history_id,
                    product_id,
                    product_name,
                    batch_number,
                    remaining_quantity as available_quantity,
                    standard_uom,
                    expiry_date,
                    warehouse_name,
                    location
                FROM inventory_detailed_view
                {where_clause}
                ORDER BY expiry_date ASC
                LIMIT 10
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading inventory summary: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_can_summary(_self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get CAN summary optimized for product view"""
        try:
            params = {}
            where_clause = "WHERE pending_quantity > 0"
            
            if product_id:
                where_clause += " AND product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    can_line_id,
                    product_id,
                    product_name,
                    arrival_note_number,
                    pending_quantity,
                    standard_uom,
                    arrival_date,
                    vendor,
                    po_number
                FROM can_pending_stockin_view
                {where_clause}
                ORDER BY arrival_date ASC
                LIMIT 10
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading CAN summary: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_po_summary(_self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get PO summary optimized for product view"""
        try:
            params = {}
            where_clause = "WHERE pending_standard_arrival_quantity > 0"
            
            if product_id:
                where_clause += " AND product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    po_line_id,
                    product_id,
                    product_name,
                    po_number,
                    pending_standard_arrival_quantity as pending_quantity,
                    standard_uom,
                    etd,
                    vendor_name
                FROM purchase_order_full_view
                {where_clause}
                ORDER BY etd ASC
                LIMIT 10
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading PO summary: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_wht_summary(_self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get warehouse transfer summary optimized for product view"""
        try:
            params = {}
            where_clause = "WHERE is_completed = 0 AND transfer_quantity > 0"
            
            if product_id:
                where_clause += " AND product_id = :product_id"
                params['product_id'] = product_id
            
            query = f"""
                SELECT 
                    warehouse_transfer_line_id,
                    product_id,
                    product_name,
                    from_warehouse,
                    to_warehouse,
                    transfer_quantity,
                    standard_uom,
                    transfer_date as etd,
                    CASE 
                        WHEN is_completed = 1 THEN 'Completed'
                        ELSE 'In Progress'
                    END as status
                FROM warehouse_transfer_details_view
                {where_clause}
                ORDER BY transfer_date DESC
                LIMIT 10
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params if params else None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading WHT summary: {e}")
            return pd.DataFrame()
    
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
                    WHERE pending_selling_delivery_quantity > 0
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
    def get_brand_list(_self) -> pd.DataFrame:
        """Get list of brands for filter"""
        try:
            query = """
                SELECT DISTINCT 
                    b.id as brand_id,
                    b.brand_name
                FROM brands b
                INNER JOIN products p ON b.id = p.brand_id
                WHERE b.delete_flag = 0
                AND EXISTS (
                    SELECT 1 
                    FROM outbound_oc_pending_delivery_view ocpd
                    WHERE ocpd.product_id = p.id
                    AND ocpd.pending_selling_delivery_quantity > 0
                )
                ORDER BY b.brand_name
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading brand list: {e}")
            return pd.DataFrame()