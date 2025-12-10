"""
Product Data Repository - REFACTORED with Dropdown Filter Support
Added methods for filter options: Products, Brands, Customers, Legal Entities
Updated query logic for AND-based multiselect filtering
"""
import pandas as pd
import logging
from typing import Dict, List, Optional, Any, Tuple
import streamlit as st
from sqlalchemy import text

from ..db import get_db_engine
from ..config import config

logger = logging.getLogger(__name__)


class ProductData:
    """Repository for product and OC-related data access"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.cache_ttl = config.get_app_setting('CACHE_TTL_SECONDS', 300)
        self.max_results = config.get_app_setting('MAX_QUERY_RESULTS', 10000)
    
    # ==================== FILTER OPTIONS QUERIES ====================
    
    @st.cache_data(ttl=300)
    def get_product_filter_options(_self) -> List[Dict]:
        """
        Get products that have pending OCs for filter dropdown
        Returns: List of {id, display_text, pt_code, name, package_size, brand}
        """
        try:
            query = """
                SELECT DISTINCT
                    p.id,
                    p.pt_code,
                    p.name,
                    p.package_size,
                    b.brand_name,
                    CONCAT(p.pt_code, ' | ', p.name, ' | ', COALESCE(p.package_size, ''), ' (', COALESCE(b.brand_name, ''), ')') as display_text
                FROM products p
                LEFT JOIN brands b ON p.brand_id = b.id
                INNER JOIN outbound_oc_pending_delivery_view ocpd ON p.id = ocpd.product_id
                WHERE p.delete_flag = 0
                AND ocpd.pending_standard_delivery_quantity > 0
                ORDER BY p.pt_code ASC
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query))
                options = [dict(row._mapping) for row in result]
            
            return options
            
        except Exception as e:
            logger.error(f"Error loading product filter options: {e}")
            return []
    
    @st.cache_data(ttl=300)
    def get_brand_filter_options(_self) -> List[Dict]:
        """
        Get brands that have products with pending OCs
        Returns: List of {id, brand_name}
        """
        try:
            query = """
                SELECT DISTINCT
                    b.id,
                    b.brand_name
                FROM brands b
                INNER JOIN products p ON p.brand_id = b.id
                INNER JOIN outbound_oc_pending_delivery_view ocpd ON p.id = ocpd.product_id
                WHERE b.delete_flag = 0
                AND p.delete_flag = 0
                AND ocpd.pending_standard_delivery_quantity > 0
                ORDER BY b.brand_name ASC
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query))
                options = [dict(row._mapping) for row in result]
            
            return options
            
        except Exception as e:
            logger.error(f"Error loading brand filter options: {e}")
            return []
    
    @st.cache_data(ttl=300)
    def get_customer_filter_options(_self) -> List[Dict]:
        """
        Get customers that have pending OCs
        Returns: List of {customer_code, customer}
        """
        try:
            query = """
                SELECT DISTINCT
                    customer_code,
                    customer
                FROM outbound_oc_pending_delivery_view
                WHERE pending_standard_delivery_quantity > 0
                ORDER BY customer ASC
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query))
                options = [dict(row._mapping) for row in result]
            
            return options
            
        except Exception as e:
            logger.error(f"Error loading customer filter options: {e}")
            return []
    
    @st.cache_data(ttl=300)
    def get_legal_entity_filter_options(_self) -> List[Dict]:
        """
        Get legal entities (sellers) that have pending OCs
        Returns: List of {legal_entity}
        """
        try:
            query = """
                SELECT DISTINCT
                    legal_entity
                FROM outbound_oc_pending_delivery_view
                WHERE pending_standard_delivery_quantity > 0
                AND legal_entity IS NOT NULL
                ORDER BY legal_entity ASC
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query))
                options = [dict(row._mapping) for row in result]
            
            return options
            
        except Exception as e:
            logger.error(f"Error loading legal entity filter options: {e}")
            return []
    
    # ==================== Query Builders ====================
    
    def _escape_like_pattern(self, pattern: str) -> str:
        """Escape special LIKE characters"""
        return pattern.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    
    def _build_safe_where_conditions(self, filters: Dict) -> Tuple[List[str], Dict]:
        """
        Build WHERE conditions safely with proper parameterization
        REFACTORED: Support for multiselect filters with AND logic
        """
        where_conditions = ["p.delete_flag = 0"]
        params = {}
        
        if not filters:
            return where_conditions, params
        
        # ==================== MULTISELECT FILTERS (AND logic) ====================
        
        # Product IDs filter (multiselect)
        if filters.get('product_ids') and len(filters['product_ids']) > 0:
            product_ids = filters['product_ids']
            placeholders = ', '.join([f':product_id_{i}' for i in range(len(product_ids))])
            where_conditions.append(f"p.id IN ({placeholders})")
            for i, pid in enumerate(product_ids):
                params[f'product_id_{i}'] = pid
        
        # Brand IDs filter (multiselect)
        if filters.get('brand_ids') and len(filters['brand_ids']) > 0:
            brand_ids = filters['brand_ids']
            placeholders = ', '.join([f':brand_id_{i}' for i in range(len(brand_ids))])
            where_conditions.append(f"p.brand_id IN ({placeholders})")
            for i, bid in enumerate(brand_ids):
                params[f'brand_id_{i}'] = bid
        
        # Customer codes filter (multiselect)
        if filters.get('customer_codes') and len(filters['customer_codes']) > 0:
            customer_codes = filters['customer_codes']
            placeholders = ', '.join([f':customer_code_{i}' for i in range(len(customer_codes))])
            where_conditions.append(f"""
                EXISTS (
                    SELECT 1 FROM outbound_oc_pending_delivery_view ocpd_cust
                    WHERE ocpd_cust.product_id = p.id 
                    AND ocpd_cust.customer_code IN ({placeholders})
                    AND ocpd_cust.pending_standard_delivery_quantity > 0
                )
            """)
            for i, code in enumerate(customer_codes):
                params[f'customer_code_{i}'] = code
        
        # Legal entity filter (multiselect)
        if filters.get('legal_entities') and len(filters['legal_entities']) > 0:
            legal_entities = filters['legal_entities']
            placeholders = ', '.join([f':legal_entity_{i}' for i in range(len(legal_entities))])
            where_conditions.append(f"""
                EXISTS (
                    SELECT 1 FROM outbound_oc_pending_delivery_view ocpd_le
                    WHERE ocpd_le.product_id = p.id 
                    AND ocpd_le.legal_entity IN ({placeholders})
                    AND ocpd_le.pending_standard_delivery_quantity > 0
                )
            """)
            for i, le in enumerate(legal_entities):
                params[f'legal_entity_{i}'] = le
        
        # ==================== SINGLE SELECT FILTERS ====================
        
        # Supply status filter
        if filters.get('supply_status'):
            status = filters['supply_status']
            if status == 'sufficient':
                # Will be handled in HAVING clause
                pass
            elif status == 'partial':
                pass
            elif status == 'low':
                pass
            elif status == 'no_supply':
                pass
        
        # ETD urgency filter
        if filters.get('etd_urgency'):
            urgency = filters['etd_urgency']
            if urgency == 'urgent':  # ≤7 days
                where_conditions.append("""
                    EXISTS (
                        SELECT 1 FROM outbound_oc_pending_delivery_view ocpd_etd
                        WHERE ocpd_etd.product_id = p.id 
                        AND ocpd_etd.etd <= DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY)
                        AND ocpd_etd.pending_standard_delivery_quantity > 0
                    )
                """)
            elif urgency == 'soon':  # 8-14 days
                where_conditions.append("""
                    EXISTS (
                        SELECT 1 FROM outbound_oc_pending_delivery_view ocpd_etd
                        WHERE ocpd_etd.product_id = p.id 
                        AND ocpd_etd.etd > DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY)
                        AND ocpd_etd.etd <= DATE_ADD(CURRENT_DATE, INTERVAL 14 DAY)
                        AND ocpd_etd.pending_standard_delivery_quantity > 0
                    )
                """)
            elif urgency == 'normal':  # >14 days
                where_conditions.append("""
                    EXISTS (
                        SELECT 1 FROM outbound_oc_pending_delivery_view ocpd_etd
                        WHERE ocpd_etd.product_id = p.id 
                        AND ocpd_etd.etd > DATE_ADD(CURRENT_DATE, INTERVAL 14 DAY)
                        AND ocpd_etd.pending_standard_delivery_quantity > 0
                    )
                """)
        
        # Allocation status filter
        if filters.get('allocation_status'):
            status = filters['allocation_status']
            if status == 'not_allocated':
                where_conditions.append("""
                    EXISTS (
                        SELECT 1 FROM outbound_oc_pending_delivery_view ocpd_alloc
                        WHERE ocpd_alloc.product_id = p.id 
                        AND ocpd_alloc.is_allocated = 'No'
                        AND ocpd_alloc.pending_standard_delivery_quantity > 0
                    )
                """)
            elif status == 'partial':
                where_conditions.append("""
                    EXISTS (
                        SELECT 1 FROM outbound_oc_pending_delivery_view ocpd_alloc
                        WHERE ocpd_alloc.product_id = p.id 
                        AND ocpd_alloc.is_allocated = 'Yes'
                        AND ocpd_alloc.undelivered_allocated_qty_standard < ocpd_alloc.pending_standard_delivery_quantity
                        AND ocpd_alloc.undelivered_allocated_qty_standard > 0
                        AND ocpd_alloc.pending_standard_delivery_quantity > 0
                    )
                """)
            elif status == 'fully_allocated':
                where_conditions.append("""
                    EXISTS (
                        SELECT 1 FROM outbound_oc_pending_delivery_view ocpd_alloc
                        WHERE ocpd_alloc.product_id = p.id 
                        AND ocpd_alloc.undelivered_allocated_qty_standard >= ocpd_alloc.pending_standard_delivery_quantity
                        AND ocpd_alloc.over_allocation_type = 'Normal'
                        AND ocpd_alloc.pending_standard_delivery_quantity > 0
                    )
                """)
            elif status == 'over_allocated':
                where_conditions.append("""
                    EXISTS (
                        SELECT 1 FROM outbound_oc_pending_delivery_view ocpd_alloc
                        WHERE ocpd_alloc.product_id = p.id 
                        AND (ocpd_alloc.is_over_committed = 'Yes' OR ocpd_alloc.is_pending_over_allocated = 'Yes')
                        AND ocpd_alloc.pending_standard_delivery_quantity > 0
                    )
                """)
        
        # ==================== TEXT SEARCH ====================
        # Now only searches OC Number and Customer PO Number
        if filters.get('search'):
            search_term = self._escape_like_pattern(filters['search'].strip()[:50])
            search_pattern = f"%{search_term}%"
            params['search_pattern'] = search_pattern
            
            where_conditions.append("""
                EXISTS (
                    SELECT 1 FROM outbound_oc_pending_delivery_view ocpd_search
                    WHERE ocpd_search.product_id = p.id 
                    AND (
                        ocpd_search.oc_number LIKE :search_pattern OR 
                        ocpd_search.customer_po_number LIKE :search_pattern
                    )
                    AND ocpd_search.pending_standard_delivery_quantity > 0
                )
            """)
        
        return where_conditions, params

    def _build_safe_having_conditions(self, filters: Dict) -> List[str]:
        """Build HAVING conditions for aggregate filters"""
        having_conditions = []
        
        if not filters:
            return having_conditions
        
        # Supply status filter (requires aggregate data)
        if filters.get('supply_status'):
            status = filters['supply_status']
            if status == 'sufficient':
                having_conditions.append("(total_supply >= total_demand AND total_demand > 0)")
            elif status == 'partial':
                having_conditions.append("(total_supply >= total_demand * 0.5 AND total_supply < total_demand AND total_demand > 0)")
            elif status == 'low':
                having_conditions.append("(total_supply > 0 AND total_supply < total_demand * 0.5 AND total_demand > 0)")
            elif status == 'no_supply':
                having_conditions.append("(total_supply = 0 OR total_supply IS NULL)")
        
        return having_conditions
    
    # ==================== Main Product List ====================
        
    @st.cache_data(ttl=300)
    def get_products_with_demand_supply(_self, filters: Dict = None, 
                                      page: int = 1, page_size: int = 50) -> pd.DataFrame:
        """Get products with aggregated demand and supply information"""
        try:
            where_conditions, params = _self._build_safe_where_conditions(filters or {})
            having_conditions = _self._build_safe_having_conditions(filters or {})
            
            params['offset'] = (page - 1) * page_size
            params['limit'] = page_size
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            having_clause = f"HAVING {' AND '.join(having_conditions)}" if having_conditions else ""
            
            query = f"""
                WITH product_demand AS (
                    SELECT 
                        product_id,
                        COUNT(DISTINCT ocd_id) as oc_count,
                        SUM(pending_standard_delivery_quantity) as total_demand,
                        SUM(outstanding_amount_usd) as total_value,
                        MIN(etd) as earliest_etd,
                        COUNT(CASE WHEN etd <= DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY) THEN 1 END) as urgent_ocs,
                        GROUP_CONCAT(DISTINCT oc_number ORDER BY oc_number SEPARATOR ', ') as oc_numbers,
                        GROUP_CONCAT(DISTINCT customer ORDER BY customer SEPARATOR ', ') as customers,
                        SUM(CASE 
                            WHEN is_over_committed = 'Yes' OR is_pending_over_allocated = 'Yes' 
                            THEN 1 ELSE 0 
                        END) as over_allocated_count,
                        MAX(CASE 
                            WHEN is_over_committed = 'Yes' OR is_pending_over_allocated = 'Yes' 
                            THEN 1 ELSE 0 
                        END) as has_over_allocation
                    FROM outbound_oc_pending_delivery_view
                    WHERE pending_standard_delivery_quantity > 0
                    GROUP BY product_id
                ),
                product_supply AS (
                    SELECT 
                        product_id,
                        SUM(CASE WHEN source_type = 'INVENTORY' THEN quantity ELSE 0 END) as inventory_qty,
                        SUM(CASE WHEN source_type = 'CAN' THEN quantity ELSE 0 END) as can_qty,
                        SUM(CASE WHEN source_type = 'PO' THEN quantity ELSE 0 END) as po_qty,
                        SUM(CASE WHEN source_type = 'WHT' THEN quantity ELSE 0 END) as wht_qty,
                        SUM(quantity) as total_supply
                    FROM (
                        SELECT product_id, 'INVENTORY' as source_type, SUM(remaining_quantity) as quantity
                        FROM inventory_detailed_view
                        WHERE remaining_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT product_id, 'CAN' as source_type, SUM(pending_quantity) as quantity
                        FROM can_pending_stockin_view
                        WHERE pending_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT product_id, 'PO' as source_type, SUM(pending_standard_arrival_quantity) as quantity
                        FROM purchase_order_full_view
                        WHERE pending_standard_arrival_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT product_id, 'WHT' as source_type, SUM(transfer_quantity) as quantity
                        FROM warehouse_transfer_details_view
                        WHERE is_completed = 0 AND transfer_quantity > 0
                        GROUP BY product_id
                    ) supply_union
                    GROUP BY product_id
                )
                SELECT 
                    p.id as product_id,
                    p.name as product_name,
                    p.pt_code,
                    p.uom as standard_uom,
                    p.package_size,
                    b.brand_name,
                    pd.oc_count,
                    pd.oc_numbers,
                    pd.customers,
                    pd.total_demand,
                    pd.total_value,
                    pd.earliest_etd,
                    pd.urgent_ocs,
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
                    END as is_urgent,
                    COALESCE(pd.over_allocated_count, 0) as over_allocated_count,
                    COALESCE(pd.has_over_allocation, 0) as has_over_allocation
                FROM products p
                LEFT JOIN brands b ON p.brand_id = b.id
                INNER JOIN product_demand pd ON p.id = pd.product_id
                LEFT JOIN product_supply ps ON p.id = ps.product_id
                {where_clause}
                {having_clause}
                ORDER BY 
                    pd.over_allocated_count DESC,
                    pd.urgent_ocs DESC,
                    (COALESCE(ps.total_supply, 0) / NULLIF(pd.total_demand, 0)) ASC,
                    pd.total_value DESC
                LIMIT :limit OFFSET :offset
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            
            # Limit GROUP_CONCAT results
            if not df.empty:
                df['oc_numbers'] = df['oc_numbers'].apply(
                    lambda x: ', '.join(x.split(', ')[:10]) + '...' if x and len(x.split(', ')) > 10 else x
                )
                df['customers'] = df['customers'].apply(
                    lambda x: ', '.join(x.split(', ')[:5]) + '...' if x and len(x.split(', ')) > 5 else x
                )
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading products with demand/supply: {e}", exc_info=True)
            return pd.DataFrame()
    
    # ==================== OC Details ====================

    @st.cache_data(ttl=300)
    def get_ocs_by_product(_self, product_id: int) -> pd.DataFrame:
        """Get all pending OCs for a product with allocation summary"""
        try:
            query = """
                SELECT 
                    ocpd.*,
                    ocpd.pending_selling_delivery_quantity as pending_quantity
                FROM outbound_oc_pending_delivery_view ocpd
                WHERE ocpd.product_id = :product_id
                AND ocpd.pending_selling_delivery_quantity > 0
                ORDER BY 
                    CASE ocpd.over_allocation_type 
                        WHEN 'Over-Committed' THEN 1 
                        WHEN 'Pending-Over-Allocated' THEN 2 
                        ELSE 3 
                    END,
                    ocpd.etd ASC
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'product_id': product_id})

            # Fix floating point precision issues
            quantity_columns = [
                'original_selling_quantity', 'original_standard_quantity',
                'selling_quantity', 'standard_quantity',
                'total_delivered_selling_quantity', 'total_delivered_standard_quantity',
                'pending_selling_delivery_quantity', 'pending_standard_delivery_quantity',
                'total_allocated_qty_standard', 'total_allocation_cancelled_qty_standard',
                'total_effective_allocated_qty_standard', 'undelivered_allocated_qty_standard'
            ]
            
            for col in quantity_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: round(round(x, 10), 2) if pd.notna(x) else x)

            return df
            
        except Exception as e:
            logger.error(f"Error loading OCs for product {product_id}: {e}")
            return pd.DataFrame()
    
    # ==================== Filter Count Methods ====================
    
    @st.cache_data(ttl=60)
    def get_filtered_product_count(_self, filters: Dict = None) -> int:
        """
        Get total count of products matching current filters
        Used to display filter results count
        """
        try:
            where_conditions, params = _self._build_safe_where_conditions(filters or {})
            having_conditions = _self._build_safe_having_conditions(filters or {})
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            having_clause = f"HAVING {' AND '.join(having_conditions)}" if having_conditions else ""
            
            query = f"""
                WITH product_demand AS (
                    SELECT 
                        product_id,
                        SUM(pending_standard_delivery_quantity) as total_demand
                    FROM outbound_oc_pending_delivery_view
                    WHERE pending_standard_delivery_quantity > 0
                    GROUP BY product_id
                ),
                product_supply AS (
                    SELECT 
                        product_id,
                        SUM(quantity) as total_supply
                    FROM (
                        SELECT product_id, SUM(remaining_quantity) as quantity
                        FROM inventory_detailed_view
                        WHERE remaining_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT product_id, SUM(pending_quantity) as quantity
                        FROM can_pending_stockin_view
                        WHERE pending_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT product_id, SUM(pending_standard_arrival_quantity) as quantity
                        FROM purchase_order_full_view
                        WHERE pending_standard_arrival_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT product_id, SUM(transfer_quantity) as quantity
                        FROM warehouse_transfer_details_view
                        WHERE is_completed = 0 AND transfer_quantity > 0
                        GROUP BY product_id
                    ) supply_union
                    GROUP BY product_id
                )
                SELECT COUNT(*) as total_count
                FROM (
                    SELECT 
                        p.id,
                        pd.total_demand,
                        COALESCE(ps.total_supply, 0) as total_supply
                    FROM products p
                    LEFT JOIN brands b ON p.brand_id = b.id
                    INNER JOIN product_demand pd ON p.id = pd.product_id
                    LEFT JOIN product_supply ps ON p.id = ps.product_id
                    {where_clause}
                    {having_clause}
                ) filtered_products
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query), params).fetchone()
                return result[0] if result else 0
                
        except Exception as e:
            logger.error(f"Error getting filtered product count: {e}")
            return 0
    
    @st.cache_data(ttl=60)
    def get_filter_counts(_self, current_filters: Dict = None) -> Dict[str, int]:
        """
        Get counts for filter options based on current filters
        Used to show "X available" in filter dropdowns
        """
        try:
            # Build base query with current filters
            where_conditions, params = _self._build_safe_where_conditions(current_filters or {})
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            
            query = f"""
                SELECT 
                    COUNT(DISTINCT p.id) as product_count,
                    COUNT(DISTINCT p.brand_id) as brand_count,
                    COUNT(DISTINCT ocpd.customer_code) as customer_count,
                    COUNT(DISTINCT ocpd.legal_entity) as legal_entity_count
                FROM products p
                LEFT JOIN brands b ON p.brand_id = b.id
                INNER JOIN outbound_oc_pending_delivery_view ocpd ON p.id = ocpd.product_id
                {where_clause}
                AND ocpd.pending_standard_delivery_quantity > 0
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query), params).fetchone()
                
                if result:
                    return {
                        'products': result[0] or 0,
                        'brands': result[1] or 0,
                        'customers': result[2] or 0,
                        'legal_entities': result[3] or 0
                    }
            
            return {'products': 0, 'brands': 0, 'customers': 0, 'legal_entities': 0}
            
        except Exception as e:
            logger.error(f"Error getting filter counts: {e}")
            return {'products': 0, 'brands': 0, 'customers': 0, 'legal_entities': 0}