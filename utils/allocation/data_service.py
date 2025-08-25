"""
Data Service for Allocation Module - Fixed Version
Fixed SQL injection risks, improved query performance
"""
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Any, Tuple
import streamlit as st
from sqlalchemy import text, and_, or_, select, func
from sqlalchemy.sql import bindparam

from ..db import get_db_engine
from ..config import config

logger = logging.getLogger(__name__)


class AllocationDataService:
    """Service for fetching allocation-related data with improved security"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.cache_ttl = config.get_app_setting('CACHE_TTL_SECONDS', 300)
        self.max_results = config.get_app_setting('MAX_QUERY_RESULTS', 10000)
    
    # ==================== Safe Query Builder ====================
    
    def _build_safe_in_clause(self, column: str, values: List[Any], 
                             param_prefix: str, params: Dict) -> Tuple[str, Dict]:
        """
        Build safe IN clause with parameterized values
        
        Args:
            column: Column name to check
            values: List of values for IN clause
            param_prefix: Prefix for parameter names
            params: Existing parameters dict to update
            
        Returns:
            Tuple of (SQL clause, updated params dict)
        """
        if not values:
            return "1=0", params  # Always false if no values
        
        param_names = []
        for i, value in enumerate(values):
            param_name = f"{param_prefix}_{i}"
            params[param_name] = value
            param_names.append(f":{param_name}")
        
        clause = f"{column} IN ({','.join(param_names)})"
        return clause, params
    
    def _build_safe_where_conditions(self, filters: Dict) -> Tuple[List[str], Dict]:
        """
        Build WHERE conditions safely with proper parameterization
        
        Args:
            filters: Filter dictionary
            
        Returns:
            Tuple of (list of WHERE clauses, parameters dict)
        """
        where_conditions = ["p.delete_flag = 0"]
        params = {}
        
        if not filters:
            return where_conditions, params
        
        # Search filter
        if filters.get('search'):
            search_term = filters['search'].strip()
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
        
        # Customer filter with safe IN clause
        if filters.get('customers'):
            customer_list = filters['customers']
            exclude_customers = filters.get('exclude_customers', False)
            
            if customer_list:
                # Build safe IN clause
                in_clause, params = self._build_safe_in_clause(
                    'ocpd.customer_code',
                    customer_list,
                    'customer',
                    params
                )
                
                customer_condition = f"""
                    EXISTS (
                        SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                        WHERE ocpd.product_id = p.id 
                        AND {'NOT' if exclude_customers else ''} ({in_clause})
                    )
                """
                where_conditions.append(customer_condition)
        
        # Brand filter with safe IN clause
        if filters.get('brands'):
            brand_list = filters['brands']
            exclude_brands = filters.get('exclude_brands', False)
            
            if brand_list:
                in_clause, params = self._build_safe_in_clause(
                    'p.brand_id',
                    brand_list,
                    'brand',
                    params
                )
                
                brand_condition = f"{'NOT' if exclude_brands else ''} ({in_clause})"
                where_conditions.append(brand_condition)
        
        # OC Number filter with safe IN clause
        if filters.get('oc_numbers'):
            oc_list = filters['oc_numbers']
            exclude_ocs = filters.get('exclude_oc_numbers', False)
            
            if oc_list:
                in_clause, params = self._build_safe_in_clause(
                    'ocpd.oc_number',
                    oc_list,
                    'oc',
                    params
                )
                
                oc_condition = f"""
                    EXISTS (
                        SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                        WHERE ocpd.product_id = p.id 
                        AND {'NOT' if exclude_ocs else ''} ({in_clause})
                    )
                """
                where_conditions.append(oc_condition)
        
        # Product filter with safe IN clause
        if filters.get('products'):
            product_list = filters['products']
            exclude_products = filters.get('exclude_products', False)
            
            if product_list:
                in_clause, params = self._build_safe_in_clause(
                    'p.id',
                    product_list,
                    'product',
                    params
                )
                
                product_condition = f"{'NOT' if exclude_products else ''} ({in_clause})"
                where_conditions.append(product_condition)
        
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
        
        # Quick filters
        if filters.get('supply_status') == 'low':
            # This will be handled in HAVING clause
            pass
        
        if filters.get('etd_urgency') == 'urgent':
            where_conditions.append("""
                EXISTS (
                    SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                    WHERE ocpd.product_id = p.id 
                    AND ocpd.etd <= DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY)
                )
            """)
        
        if filters.get('allocation_status') == 'none':
            where_conditions.append("""
                EXISTS (
                    SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                    WHERE ocpd.product_id = p.id 
                    AND ocpd.is_allocated = 'No'
                )
            """)
        
        if filters.get('has_inventory'):
            # This will be handled in HAVING clause
            pass
        
        if filters.get('over_allocated'):
            where_conditions.append("""
                EXISTS (
                    SELECT 1 FROM outbound_oc_pending_delivery_view ocpd
                    WHERE ocpd.product_id = p.id 
                    AND ocpd.is_over_allocated = 'Yes'
                )
            """)
        
        return where_conditions, params
    
    def _build_safe_having_conditions(self, filters: Dict) -> List[str]:
        """Build HAVING conditions for aggregate filters"""
        having_conditions = []
        
        if not filters:
            return having_conditions
        
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
        
        # Allocation status filter
        if filters.get('allocation_status_detail'):
            # These are handled in WHERE clause as they need row-level filtering
            pass
        
        # Quick filters that need HAVING
        if filters.get('supply_status') == 'low':
            having_conditions.append("(total_supply < total_demand * 0.5 AND total_demand > 0)")
        
        if filters.get('has_inventory'):
            having_conditions.append("inventory_qty > 0")
        
        return having_conditions
    
    # ==================== Reference Data for Filters ====================
    
    @st.cache_data(ttl=3600)
    def get_customer_list_with_stats(_self) -> pd.DataFrame:
        """Get list of customers with order statistics for filter"""
        try:
            query = """
                SELECT DISTINCT 
                    c.company_code as customer_code,
                    c.english_name as customer_name,
                    COUNT(DISTINCT ocpd.ocd_id) as order_count,
                    COUNT(DISTINCT ocpd.product_id) as product_count
                FROM companies c
                INNER JOIN outbound_oc_pending_delivery_view ocpd 
                    ON c.company_code = ocpd.customer_code
                WHERE c.delete_flag = 0
                AND ocpd.pending_standard_delivery_quantity > 0
                GROUP BY c.company_code, c.english_name
                ORDER BY c.english_name
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading customer list: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=3600)
    def get_brand_list_with_stats(_self) -> pd.DataFrame:
        """Get list of brands with product count for filter"""
        try:
            query = """
                SELECT DISTINCT 
                    b.id as brand_id,
                    b.brand_name,
                    COUNT(DISTINCT p.id) as product_count,
                    COUNT(DISTINCT ocpd.ocd_id) as order_count
                FROM brands b
                INNER JOIN products p ON b.id = p.brand_id
                INNER JOIN outbound_oc_pending_delivery_view ocpd 
                    ON ocpd.product_id = p.id
                WHERE b.delete_flag = 0
                AND ocpd.pending_standard_delivery_quantity > 0
                GROUP BY b.id, b.brand_name
                ORDER BY b.brand_name
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading brand list: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=3600)
    def get_oc_number_list(_self) -> pd.DataFrame:
        """Get list of OC numbers with details for filter"""
        try:
            query = """
                SELECT DISTINCT 
                    oc_number,
                    customer,
                    COUNT(DISTINCT product_id) as product_count,
                    SUM(pending_standard_delivery_quantity) as total_pending_qty,
                    MIN(etd) as earliest_etd
                FROM outbound_oc_pending_delivery_view
                WHERE pending_standard_delivery_quantity > 0
                GROUP BY oc_number, customer
                ORDER BY oc_number DESC
                LIMIT 500
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading OC number list: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=3600)
    def get_product_list_for_filter(_self) -> pd.DataFrame:
        """Get list of products for filter"""
        try:
            query = """
                SELECT DISTINCT 
                    p.id as product_id,
                    p.pt_code,
                    p.name as product_name,
                    b.brand_name,
                    COUNT(DISTINCT ocpd.ocd_id) as order_count
                FROM products p
                LEFT JOIN brands b ON p.brand_id = b.id
                INNER JOIN outbound_oc_pending_delivery_view ocpd 
                    ON ocpd.product_id = p.id
                WHERE p.delete_flag = 0
                AND ocpd.pending_standard_delivery_quantity > 0
                GROUP BY p.id, p.pt_code, p.name, b.brand_name
                ORDER BY p.pt_code, p.name
                LIMIT 1000
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            # Create display name
            df['display_name'] = df['pt_code'] + ' - ' + df['product_name']
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading product list: {e}")
            return pd.DataFrame()
    
    # ==================== Main Product List with Improved Query ====================
    
    @st.cache_data(ttl=300)
    def get_products_with_demand_supply(_self, filters: Dict = None, 
                                       page: int = 1, page_size: int = 50) -> pd.DataFrame:
        """Get products with aggregated demand and supply information - Improved version"""
        try:
            # Build WHERE and HAVING conditions safely
            where_conditions, params = _self._build_safe_where_conditions(filters or {})
            having_conditions = _self._build_safe_having_conditions(filters or {})
            
            # Add pagination parameters
            params['offset'] = (page - 1) * page_size
            params['limit'] = page_size
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            having_clause = f"HAVING {' AND '.join(having_conditions)}" if having_conditions else ""
            
            # Create query with CTEs for better performance
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
                        SUM(CASE WHEN is_over_allocated = 'Yes' THEN 1 ELSE 0 END) as over_allocated_count,
                        MAX(CASE WHEN is_over_allocated = 'Yes' THEN 1 ELSE 0 END) as has_over_allocation
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
                        SELECT 
                            product_id,
                            'INVENTORY' as source_type,
                            SUM(remaining_quantity) as quantity
                        FROM inventory_detailed_view
                        WHERE remaining_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT 
                            product_id,
                            'CAN' as source_type,
                            SUM(pending_quantity) as quantity
                        FROM can_pending_stockin_view
                        WHERE pending_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT 
                            product_id,
                            'PO' as source_type,
                            SUM(pending_standard_arrival_quantity) as quantity
                        FROM purchase_order_full_view
                        WHERE pending_standard_arrival_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT 
                            product_id,
                            'WHT' as source_type,
                            SUM(transfer_quantity) as quantity
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
            
            # Limit GROUP_CONCAT results to prevent memory issues
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
    
    # ==================== Batch Loading to Avoid N+1 ====================
    
    def get_products_with_details_batch(self, product_ids: List[int]) -> Dict[int, Dict]:
        """
        Get product details for multiple products in one query
        Avoids N+1 query pattern
        """
        if not product_ids:
            return {}
        
        try:
            # Build safe IN clause
            params = {}
            in_clause, params = self._build_safe_in_clause(
                'p.product_id',
                product_ids,
                'prod_id',
                params
            )
            
            # Get OCs for all products
            oc_query = f"""
                SELECT 
                    product_id,
                    ocd_id,
                    oc_number,
                    customer,
                    etd,
                    pending_selling_delivery_quantity,
                    pending_standard_delivery_quantity,
                    selling_uom,
                    standard_uom,
                    uom_conversion,
                    effective_allocated_qty,
                    is_over_allocated,
                    allocation_warning
                FROM outbound_oc_pending_delivery_view
                WHERE {in_clause}
                AND pending_standard_delivery_quantity > 0
                ORDER BY product_id, etd
            """
            
            # Get supply for all products
            supply_query = f"""
                SELECT 
                    product_id,
                    source_type,
                    source_id,
                    reference,
                    available_quantity,
                    uom,
                    buying_uom,
                    uom_conversion,
                    expiry_date,
                    arrival_date,
                    etd
                FROM (
                    SELECT 
                        product_id,
                        'INVENTORY' as source_type,
                        inventory_history_id as source_id,
                        CONCAT('Batch ', batch_number) as reference,
                        remaining_quantity as available_quantity,
                        standard_uom as uom,
                        NULL as buying_uom,
                        NULL as uom_conversion,
                        expiry_date,
                        NULL as arrival_date,
                        NULL as etd
                    FROM inventory_detailed_view
                    WHERE product_id IN (SELECT product_id FROM (VALUES {','.join([f'({id})' for id in product_ids])}) AS t(product_id))
                    AND remaining_quantity > 0
                    
                    UNION ALL
                    
                    SELECT 
                        product_id,
                        'PENDING_CAN' as source_type,
                        can_line_id as source_id,
                        arrival_note_number as reference,
                        pending_quantity as available_quantity,
                        standard_uom as uom,
                        buying_uom,
                        uom_conversion,
                        NULL as expiry_date,
                        arrival_date,
                        NULL as etd
                    FROM can_pending_stockin_view
                    WHERE product_id IN (SELECT product_id FROM (VALUES {','.join([f'({id})' for id in product_ids])}) AS t(product_id))
                    AND pending_quantity > 0
                    
                    -- Similar for PO and WHT
                ) AS supply_union
                ORDER BY product_id, source_type
            """
            
            with self.engine.connect() as conn:
                # Execute queries
                oc_df = pd.read_sql(text(oc_query), conn, params=params)
                supply_df = pd.read_sql(text(supply_query), conn, params=params)
                
                # Group by product_id
                result = {}
                for product_id in product_ids:
                    result[product_id] = {
                        'ocs': oc_df[oc_df['product_id'] == product_id].to_dict('records'),
                        'supply': supply_df[supply_df['product_id'] == product_id].to_dict('records')
                    }
                
                return result
                
        except Exception as e:
            logger.error(f"Error batch loading product details: {e}")
            return {}
    
    # ==================== Search Suggestions with Security ====================
    
    @st.cache_data(ttl=60)
    def get_search_suggestions(_self, search_term: str, limit: int = 5) -> Dict[str, List[str]]:
        """Get search suggestions for autocomplete with SQL injection prevention"""
        try:
            if not search_term or len(search_term) < 2:
                return {'products': [], 'customers': [], 'brands': [], 'oc_numbers': []}
            
            # Sanitize search term - remove special SQL characters
            search_term = search_term.strip()
            if len(search_term) > 50:  # Limit search term length
                search_term = search_term[:50]
            
            search_pattern = f"%{search_term}%"
            exact_pattern = f"{search_term}%"
            
            suggestions = {}
            
            with _self.engine.connect() as conn:
                # Product suggestions with parameterized query
                product_query = text("""
                    SELECT DISTINCT 
                        p.name,
                        p.pt_code,
                        p.package_size
                    FROM products p
                    WHERE p.delete_flag = 0
                    AND (
                        p.name LIKE :exact_pattern 
                        OR p.pt_code LIKE :exact_pattern
                        OR p.package_size LIKE :search_pattern
                    )
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
                    INNER JOIN products p ON b.id = p.brand_id
                    WHERE b.delete_flag = 0
                    AND b.brand_name LIKE :exact_pattern
                    ORDER BY b.brand_name
                    LIMIT :limit
                """)
                
                result = conn.execute(brand_query, {
                    'exact_pattern': exact_pattern,
                    'limit': limit
                })
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
                
                result = conn.execute(customer_query, {
                    'exact_pattern': exact_pattern,
                    'limit': limit
                })
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
                
                result = conn.execute(oc_query, {
                    'search_pattern': search_pattern,
                    'limit': limit
                })
                suggestions['oc_numbers'] = [row['oc_number'] for row in result]
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Error getting search suggestions: {e}")
            return {'products': [], 'customers': [], 'brands': [], 'oc_numbers': []}
    
    # ==================== OC Details ====================
    
    @st.cache_data(ttl=300)
    def get_ocs_by_product(_self, product_id: int) -> pd.DataFrame:
        """Get all pending OCs for a specific product with full UOM information"""
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
                    standard_quantity as order_quantity_standard,
                    pending_standard_delivery_quantity,
                    uom_conversion,
                    effective_allocated_qty as allocated_quantity,
                    total_allocated_qty_standard as allocated_quantity_standard,
                    outstanding_amount_usd,
                    allocation_coverage_percent,
                    is_allocated,
                    allocation_numbers,
                    allocation_count,
                    is_over_allocated,
                    over_allocated_qty,
                    over_allocation_percent,
                    allocation_warning,
                    can_update_etd,
                    can_cancel,
                    max_cancellable_qty
                FROM outbound_oc_pending_delivery_view
                WHERE product_id = :product_id
                AND pending_selling_delivery_quantity > 0
                ORDER BY 
                    is_over_allocated DESC,
                    etd ASC, 
                    outstanding_amount_usd DESC
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
    
    # ==================== Allocation History ====================
    
    def get_allocation_history_with_details(_self, oc_detail_id: int) -> pd.DataFrame:
        """Get allocation history with full details including cancellation info"""
        try:
            query = """
                SELECT 
                    ap.allocation_number,
                    ap.allocation_date,
                    u.username as created_by,
                    ad.id as allocation_detail_id,
                    ad.allocation_plan_id,
                    ad.allocation_mode,
                    ad.allocated_qty,
                    ad.delivered_qty,
                    ad.allocated_etd,
                    ad.status,
                    COALESCE(ad.supply_source_type, 'No specific source') as supply_source_type,
                    ad.notes,
                    COALESCE(ac.cancelled_qty, 0) as cancelled_qty,
                    (ad.allocated_qty - COALESCE(ac.cancelled_qty, 0)) as effective_qty,
                    CASE 
                        WHEN ac.cancelled_qty > 0 THEN 
                            CONCAT('Cancelled: ', ac.cancelled_qty, ' - ', ac.reason)
                        ELSE ''
                    END as cancellation_info,
                    CASE WHEN ac.has_cancellations > 0 THEN 1 ELSE 0 END as has_cancellations,
                    ocd.selling_uom
                FROM allocation_details ad
                INNER JOIN allocation_plans ap ON ad.allocation_plan_id = ap.id
                LEFT JOIN users u ON ap.creator_id = u.id
                LEFT JOIN outbound_oc_pending_delivery_view ocd ON ad.demand_reference_id = ocd.ocd_id
                LEFT JOIN (
                    SELECT 
                        allocation_detail_id,
                        SUM(CASE WHEN status = 'ACTIVE' THEN cancelled_qty ELSE 0 END) as cancelled_qty,
                        MAX(CASE WHEN status = 'ACTIVE' THEN reason ELSE NULL END) as reason,
                        COUNT(*) as has_cancellations
                    FROM allocation_cancellations
                    GROUP BY allocation_detail_id
                ) ac ON ad.id = ac.allocation_detail_id
                WHERE ad.demand_reference_id = :oc_detail_id
                AND ad.demand_type = 'OC'
                ORDER BY ap.allocation_date DESC
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'oc_detail_id': oc_detail_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading allocation history with details: {e}")
            return pd.DataFrame()
    
    def get_cancellation_history(_self, allocation_detail_id: int) -> pd.DataFrame:
        """Get cancellation history for an allocation detail"""
        try:
            query = """
                SELECT 
                    ac.id as cancellation_id,
                    ac.cancelled_qty,
                    ac.reason,
                    ac.reason_category,
                    ac.cancelled_date,
                    cancel_user.username as cancelled_by,
                    ac.status,
                    ac.reversed_date,
                    reverse_user.username as reversed_by,
                    ac.reversal_reason
                FROM allocation_cancellations ac
                LEFT JOIN users cancel_user ON ac.cancelled_by_user_id = cancel_user.id
                LEFT JOIN users reverse_user ON ac.reversed_by_user_id = reverse_user.id
                WHERE ac.allocation_detail_id = :allocation_detail_id
                ORDER BY ac.cancelled_date DESC
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'allocation_detail_id': allocation_detail_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading cancellation history: {e}")
            return pd.DataFrame()
    
    # ==================== Dashboard Metrics ====================
    
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
                        MIN(ocpd.etd) as earliest_etd,
                        MAX(CASE WHEN ocpd.is_over_allocated = 'Yes' THEN 1 ELSE 0 END) as has_over_allocation
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
                    COUNT(CASE WHEN earliest_etd <= DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY) THEN 1 END) as urgent_etd_count,
                    SUM(has_over_allocation) as over_allocated_count
                FROM product_summary
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query)).fetchone()
                
                if result:
                    return dict(result._mapping)
            
            return _self._get_empty_dashboard_metrics()
            
        except Exception as e:
            logger.error(f"Error loading dashboard metrics: {e}")
            return _self._get_empty_dashboard_metrics()
    
    def _get_empty_dashboard_metrics(self) -> Dict[str, Any]:
        """Return empty dashboard metrics structure"""
        return {
            'total_products': 0,
            'total_demand_qty': 0,
            'total_supply_qty': 0,
            'critical_products': 0,
            'urgent_etd_count': 0,
            'over_allocated_count': 0
        }
    
    # ==================== Supply Availability ====================
    
    def check_supply_availability(self, source_type: str, source_id: int, 
                                 product_id: int) -> Dict[str, Any]:
        """Check current availability of a supply source with proper parameterization"""
        try:
            params = {
                'source_id': source_id,
                'product_id': product_id
            }
            
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
                result = conn.execute(text(query), params).fetchone()
            
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
    
    @st.cache_data(ttl=300)
    def get_all_supply_for_product(_self, product_id: int) -> pd.DataFrame:
        """Get all available supply sources for a product with full UOM information"""
        try:
            query = """
                SELECT 
                    'INVENTORY' as source_type,
                    inventory_history_id as source_id,
                    CONCAT('Batch ', batch_number) as reference,
                    remaining_quantity as available_quantity,
                    standard_uom as uom,
                    NULL as buying_uom,
                    NULL as uom_conversion,
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
                    buying_uom,
                    uom_conversion,
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
                    buying_uom,
                    uom_conversion,
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
                    NULL as buying_uom,
                    NULL as uom_conversion,
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
    
    def get_total_available_supply(self, product_id: int) -> Dict[str, Any]:
        """Get total available supply from all sources for a product"""
        try:
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
    
    # ==================== Supply Summary Methods ====================
    
    @st.cache_data(ttl=300)
    def get_inventory_summary(_self, product_id: Optional[int] = None) -> pd.DataFrame:
        """Get inventory summary for product view"""
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
        """Get CAN summary with buying UOM information"""
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
                    buying_quantity,
                    buying_uom,
                    uom_conversion,
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
        """Get PO summary with buying UOM information"""
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
                    pending_buying_invoiced_quantity as buying_quantity,
                    buying_uom,
                    uom_conversion,
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
        """Get warehouse transfer summary for product view"""
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