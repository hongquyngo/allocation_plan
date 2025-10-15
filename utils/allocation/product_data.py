"""
Product Data Repository - Handles product and OC queries
Extracted from data_service.py for better organization
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
    
    # ==================== Query Builders ====================
    
    def _escape_like_pattern(self, pattern: str) -> str:
        """Escape special LIKE characters"""
        return pattern.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    
    def _build_safe_where_conditions(self, filters: Dict) -> Tuple[List[str], Dict]:
        """Build WHERE conditions safely with proper parameterization"""
        where_conditions = ["p.delete_flag = 0"]
        params = {}
        
        if not filters:
            return where_conditions, params
        
        # Product ID filter
        if filters.get('product_id'):
            where_conditions.append("p.id = :product_id")
            params['product_id'] = filters['product_id']
        
        # Search filter
        if filters.get('search'):
            search_term = self._escape_like_pattern(filters['search'].strip()[:50])
            search_pattern = f"%{search_term}%"
            params['search_pattern'] = search_pattern
            
            where_conditions.append("""
                (
                    p.name LIKE :search_pattern OR 
                    p.pt_code LIKE :search_pattern OR
                    p.package_size LIKE :search_pattern OR
                    b.brand_name LIKE :search_pattern OR
                    EXISTS (
                        SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                        WHERE ocpd.product_id = p.id 
                        AND (
                            ocpd.customer LIKE :search_pattern OR 
                            ocpd.oc_number LIKE :search_pattern
                        )
                    )
                )
            """)
        
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
            where_conditions.append("""
                EXISTS (
                    SELECT 1 FROM inventory_detailed_view inv
                    WHERE inv.product_id = p.id 
                    AND inv.remaining_quantity > 0
                )
            """)
        
        # Over-allocated filter
        if filters.get('over_allocated'):
            where_conditions.append("""
                EXISTS (
                    SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                    WHERE ocpd.product_id = p.id 
                    AND (ocpd.is_over_committed = 'Yes' OR ocpd.is_pending_over_allocated = 'Yes')
                )
            """)
        
        return where_conditions, params

    def _build_safe_having_conditions(self, filters: Dict) -> List[str]:
        """Build HAVING conditions for aggregate filters"""
        having_conditions = []
        
        if not filters:
            return having_conditions
        
        # Supply status filter
        if filters.get('supply_status') == 'low':
            having_conditions.append("(total_supply < total_demand * 0.5 AND total_demand > 0)")
        
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
                        WHERE is_completed = 0
                        GROUP BY product_id
                    ) supply_union
                    GROUP BY product_id
                )
                SELECT 
                    p.id as product_id,
                    p.name as product_name,
                    p.pt_code,
                    COALESCE(p.package_size, '') as package_size,
                    p.uom as standard_uom,
                    b.brand_name,
                    pd.oc_numbers,
                    pd.customers,
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
    
    # ==================== Search Suggestions ====================
    
    @st.cache_data(ttl=60)
    def get_search_suggestions(_self, search_term: str, limit: int = 5) -> Dict[str, List[str]]:
        """Get search suggestions for autocomplete"""
        try:
            if not search_term or len(search_term) < 2:
                return {'products': [], 'customers': [], 'brands': [], 'oc_numbers': []}
            
            search_term = _self._escape_like_pattern(search_term.strip()[:50])
            search_pattern = f"%{search_term}%"
            exact_pattern = f"{search_term}%"
            
            suggestions = {}
            
            with _self.engine.connect() as conn:
                # Product suggestions
                product_query = text("""
                    SELECT DISTINCT p.name, p.pt_code, p.package_size
                    FROM products p
                    WHERE p.delete_flag = 0
                    AND (p.name LIKE :exact_pattern OR p.pt_code LIKE :exact_pattern 
                         OR p.package_size LIKE :search_pattern)
                    ORDER BY 
                        CASE 
                            WHEN p.name LIKE :exact_pattern THEN 1
                            WHEN p.pt_code LIKE :exact_pattern THEN 2
                            ELSE 3
                        END,
                        p.name
                    LIMIT :limit
                """)
                
                result = conn.execute(product_query, {
                    'search_pattern': search_pattern,
                    'exact_pattern': exact_pattern,
                    'limit': limit
                })
                suggestions['products'] = [
                    f"{row['name']} | {row['pt_code']} | {row['package_size']}" 
                    for row in result
                ]
                
                # Brand suggestions
                brand_query = text("""
                    SELECT DISTINCT b.brand_name
                    FROM brands b
                    WHERE b.delete_flag = 0 AND b.brand_name LIKE :exact_pattern
                    ORDER BY b.brand_name
                    LIMIT :limit
                """)
                
                result = conn.execute(brand_query, {'exact_pattern': exact_pattern, 'limit': limit})
                suggestions['brands'] = [row['brand_name'] for row in result]
                
                # Customer suggestions
                customer_query = text("""
                    SELECT DISTINCT customer
                    FROM outbound_oc_pending_delivery_view
                    WHERE customer LIKE :exact_pattern
                    AND pending_standard_delivery_quantity > 0
                    ORDER BY customer
                    LIMIT :limit
                """)
                
                result = conn.execute(customer_query, {'exact_pattern': exact_pattern, 'limit': limit})
                suggestions['customers'] = [row['customer'] for row in result]
                
                # OC Number suggestions
                oc_query = text("""
                    SELECT DISTINCT oc_number
                    FROM outbound_oc_pending_delivery_view
                    WHERE oc_number LIKE :search_pattern
                    AND pending_standard_delivery_quantity > 0
                    ORDER BY oc_number DESC
                    LIMIT :limit
                """)
                
                result = conn.execute(oc_query, {'search_pattern': search_pattern, 'limit': limit})
                suggestions['oc_numbers'] = [row['oc_number'] for row in result]
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Error getting search suggestions: {e}")
            return {'products': [], 'customers': [], 'brands': [], 'oc_numbers': []}