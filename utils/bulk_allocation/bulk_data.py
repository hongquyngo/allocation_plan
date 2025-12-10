"""
Bulk Allocation Data Repository
===============================
Handles all data queries for bulk allocation including:
- Scope selection (brands, customers, products, ETD range)
- Demand data (OCs pending delivery)
- Supply data (Inventory, CAN, PO, WHT)
- Committed calculation with MIN logic
"""
import pandas as pd
import logging
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal
import streamlit as st
from sqlalchemy import text

from utils.db import get_db_engine
from utils.config import config

logger = logging.getLogger(__name__)


class BulkAllocationData:
    """Repository for bulk allocation data access"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.cache_ttl = config.get_app_setting('CACHE_TTL_SECONDS', 300)
    
    # ==================== FILTER OPTIONS ====================
    
    @st.cache_data(ttl=300)
    def get_brand_options(_self) -> List[Dict]:
        """Get brands that have products with pending OCs"""
        try:
            query = """
                SELECT DISTINCT
                    b.id,
                    b.brand_name,
                    COUNT(DISTINCT ocpd.ocd_id) as oc_count,
                    COUNT(DISTINCT ocpd.product_id) as product_count,
                    SUM(ocpd.pending_standard_delivery_quantity) as total_pending_qty
                FROM brands b
                INNER JOIN products p ON p.brand_id = b.id
                INNER JOIN outbound_oc_pending_delivery_view ocpd ON p.id = ocpd.product_id
                WHERE b.delete_flag = 0
                AND p.delete_flag = 0
                AND ocpd.pending_standard_delivery_quantity > 0
                GROUP BY b.id, b.brand_name
                ORDER BY b.brand_name ASC
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row._mapping) for row in result]
                
        except Exception as e:
            logger.error(f"Error loading brand options: {e}")
            return []
    
    @st.cache_data(ttl=300)
    def get_customer_options(_self) -> List[Dict]:
        """Get customers that have pending OCs"""
        try:
            query = """
                SELECT DISTINCT
                    customer_code,
                    customer,
                    COUNT(DISTINCT ocd_id) as oc_count,
                    COUNT(DISTINCT product_id) as product_count,
                    SUM(pending_standard_delivery_quantity) as total_pending_qty
                FROM outbound_oc_pending_delivery_view
                WHERE pending_standard_delivery_quantity > 0
                GROUP BY customer_code, customer
                ORDER BY customer ASC
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row._mapping) for row in result]
                
        except Exception as e:
            logger.error(f"Error loading customer options: {e}")
            return []
    
    @st.cache_data(ttl=300)
    def get_legal_entity_options(_self) -> List[Dict]:
        """Get legal entities that have pending OCs"""
        try:
            query = """
                SELECT DISTINCT
                    legal_entity,
                    COUNT(DISTINCT ocd_id) as oc_count,
                    SUM(pending_standard_delivery_quantity) as total_pending_qty
                FROM outbound_oc_pending_delivery_view
                WHERE pending_standard_delivery_quantity > 0
                AND legal_entity IS NOT NULL
                GROUP BY legal_entity
                ORDER BY legal_entity ASC
            """
            
            with _self.engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row._mapping) for row in result]
                
        except Exception as e:
            logger.error(f"Error loading legal entity options: {e}")
            return []
    
    # ==================== SCOPE PREVIEW ====================
    
    def get_scope_summary(self, scope: Dict) -> Dict[str, Any]:
        """
        Get summary statistics for selected scope
        
        Args:
            scope: Dict with keys: brand_ids, customer_codes, legal_entities, 
                   etd_from, etd_to, include_partial_allocated
        
        Returns:
            Dict with: total_products, total_ocs, total_demand, 
                       total_supply, avg_coverage
        """
        try:
            where_conditions, params = self._build_scope_conditions(scope)
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            
            query = f"""
                WITH scope_ocs AS (
                    SELECT 
                        ocpd.ocd_id,
                        ocpd.product_id,
                        ocpd.pending_standard_delivery_quantity as pending_qty,
                        ocpd.total_effective_allocated_qty_standard as allocated_qty,
                        ocpd.undelivered_allocated_qty_standard as undelivered_allocated,
                        ocpd.standard_quantity as effective_qty
                    FROM outbound_oc_pending_delivery_view ocpd
                    INNER JOIN products p ON p.id = ocpd.product_id
                    LEFT JOIN brands b ON p.brand_id = b.id
                    {where_clause}
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
                ),
                -- Committed using MIN logic
                product_committed AS (
                    SELECT 
                        product_id,
                        SUM(
                            GREATEST(0,
                                LEAST(
                                    COALESCE(pending_standard_delivery_quantity, 0),
                                    COALESCE(undelivered_allocated_qty_standard, 0)
                                )
                            )
                        ) as total_committed
                    FROM outbound_oc_pending_delivery_view
                    WHERE pending_standard_delivery_quantity > 0
                    AND undelivered_allocated_qty_standard > 0
                    GROUP BY product_id
                )
                SELECT 
                    COUNT(DISTINCT so.product_id) as total_products,
                    COUNT(DISTINCT so.ocd_id) as total_ocs,
                    COALESCE(SUM(so.pending_qty), 0) as total_demand,
                    COALESCE(SUM(DISTINCT ps.total_supply), 0) as total_supply_raw,
                    COALESCE(SUM(DISTINCT pc.total_committed), 0) as total_committed
                FROM scope_ocs so
                LEFT JOIN product_supply ps ON so.product_id = ps.product_id
                LEFT JOIN product_committed pc ON so.product_id = pc.product_id
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params).fetchone()
                
                if result:
                    total_demand = float(result._mapping['total_demand'] or 0)
                    total_supply = float(result._mapping['total_supply_raw'] or 0)
                    total_committed = float(result._mapping['total_committed'] or 0)
                    available_supply = total_supply - total_committed
                    
                    return {
                        'total_products': result._mapping['total_products'] or 0,
                        'total_ocs': result._mapping['total_ocs'] or 0,
                        'total_demand': total_demand,
                        'total_supply': total_supply,
                        'total_committed': total_committed,
                        'available_supply': available_supply,
                        'coverage_percent': (available_supply / total_demand * 100) if total_demand > 0 else 0
                    }
            
            return {
                'total_products': 0,
                'total_ocs': 0,
                'total_demand': 0,
                'total_supply': 0,
                'total_committed': 0,
                'available_supply': 0,
                'coverage_percent': 0
            }
            
        except Exception as e:
            logger.error(f"Error getting scope summary: {e}")
            return {
                'total_products': 0,
                'total_ocs': 0,
                'total_demand': 0,
                'total_supply': 0,
                'total_committed': 0,
                'available_supply': 0,
                'coverage_percent': 0
            }
    
    # ==================== DEMAND DATA ====================
    
    def get_demands_in_scope(self, scope: Dict) -> pd.DataFrame:
        """
        Get all OCs matching the scope filters
        
        Returns DataFrame with columns needed for allocation:
        - ocd_id, oc_number, oc_date, customer_code, customer, legal_entity
        - product_id, pt_code, product_name, brand_id, brand_name
        - etd, pending_qty, effective_qty, allocated_qty, undelivered_allocated
        - standard_uom, selling_uom, uom_conversion
        - outstanding_amount_usd (for revenue priority)
        """
        try:
            where_conditions, params = self._build_scope_conditions(scope)
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            
            query = f"""
                SELECT 
                    ocpd.ocd_id,
                    ocpd.oc_number,
                    ocpd.oc_date,
                    ocpd.customer_code,
                    ocpd.customer,
                    ocpd.legal_entity,
                    ocpd.product_id,
                    ocpd.pt_code,
                    ocpd.product_name,
                    p.brand_id,
                    b.brand_name,
                    ocpd.etd,
                    ocpd.pending_standard_delivery_quantity as pending_qty,
                    ocpd.standard_quantity as effective_qty,
                    COALESCE(ocpd.total_effective_allocated_qty_standard, 0) as allocated_qty,
                    COALESCE(ocpd.undelivered_allocated_qty_standard, 0) as undelivered_allocated,
                    ocpd.standard_uom,
                    ocpd.selling_uom,
                    COALESCE(ocpd.uom_conversion, 1) as uom_conversion,
                    COALESCE(ocpd.outstanding_amount_usd, 0) as outstanding_amount_usd,
                    ocpd.over_allocation_type,
                    -- Calculate remaining allocatable
                    GREATEST(0, 
                        ocpd.standard_quantity - COALESCE(ocpd.total_effective_allocated_qty_standard, 0)
                    ) as remaining_allocatable,
                    -- Calculate top-up needed (for partially allocated)
                    GREATEST(0,
                        ocpd.pending_standard_delivery_quantity - COALESCE(ocpd.undelivered_allocated_qty_standard, 0)
                    ) as topup_needed
                FROM outbound_oc_pending_delivery_view ocpd
                INNER JOIN products p ON p.id = ocpd.product_id
                LEFT JOIN brands b ON p.brand_id = b.id
                {where_clause}
                ORDER BY 
                    ocpd.product_id,
                    ocpd.etd ASC,
                    ocpd.oc_date ASC
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting demands in scope: {e}")
            return pd.DataFrame()
    
    # ==================== SUPPLY DATA ====================
    
    def get_supply_by_products(self, product_ids: List[int]) -> pd.DataFrame:
        """
        Get supply data for multiple products
        
        Returns DataFrame with:
        - product_id, total_supply, total_committed, available
        """
        if not product_ids:
            return pd.DataFrame()
        
        try:
            placeholders = ', '.join([f':pid_{i}' for i in range(len(product_ids))])
            params = {f'pid_{i}': pid for i, pid in enumerate(product_ids)}
            
            query = f"""
                WITH product_supply AS (
                    SELECT 
                        product_id,
                        SUM(quantity) as total_supply
                    FROM (
                        SELECT product_id, SUM(remaining_quantity) as quantity
                        FROM inventory_detailed_view
                        WHERE product_id IN ({placeholders}) AND remaining_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT product_id, SUM(pending_quantity) as quantity
                        FROM can_pending_stockin_view
                        WHERE product_id IN ({placeholders}) AND pending_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT product_id, SUM(pending_standard_arrival_quantity) as quantity
                        FROM purchase_order_full_view
                        WHERE product_id IN ({placeholders}) AND pending_standard_arrival_quantity > 0
                        GROUP BY product_id
                        
                        UNION ALL
                        
                        SELECT product_id, SUM(transfer_quantity) as quantity
                        FROM warehouse_transfer_details_view
                        WHERE product_id IN ({placeholders}) AND is_completed = 0 AND transfer_quantity > 0
                        GROUP BY product_id
                    ) supply_union
                    GROUP BY product_id
                ),
                -- Committed using MIN logic
                product_committed AS (
                    SELECT 
                        product_id,
                        SUM(
                            GREATEST(0,
                                LEAST(
                                    COALESCE(pending_standard_delivery_quantity, 0),
                                    COALESCE(undelivered_allocated_qty_standard, 0)
                                )
                            )
                        ) as total_committed
                    FROM outbound_oc_pending_delivery_view
                    WHERE product_id IN ({placeholders})
                    AND pending_standard_delivery_quantity > 0
                    AND undelivered_allocated_qty_standard > 0
                    GROUP BY product_id
                )
                SELECT 
                    ps.product_id,
                    ps.total_supply,
                    COALESCE(pc.total_committed, 0) as total_committed,
                    ps.total_supply - COALESCE(pc.total_committed, 0) as available
                FROM product_supply ps
                LEFT JOIN product_committed pc ON ps.product_id = pc.product_id
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting supply by products: {e}")
            return pd.DataFrame()
    
    def get_product_supply_detail(self, product_id: int) -> Dict[str, Any]:
        """
        Get detailed supply information for a single product
        
        Returns:
            Dict with total_supply, total_committed, available, coverage_ratio
        """
        try:
            query = text("""
                WITH supply_summary AS (
                    SELECT 
                        'total_supply' as metric,
                        COALESCE(SUM(total_supply), 0) as value
                    FROM (
                        SELECT SUM(remaining_quantity) as total_supply
                        FROM inventory_detailed_view
                        WHERE product_id = :product_id AND remaining_quantity > 0
                        
                        UNION ALL
                        
                        SELECT SUM(pending_quantity) as total_supply
                        FROM can_pending_stockin_view
                        WHERE product_id = :product_id AND pending_quantity > 0
                        
                        UNION ALL
                        
                        SELECT SUM(pending_standard_arrival_quantity) as total_supply
                        FROM purchase_order_full_view
                        WHERE product_id = :product_id AND pending_standard_arrival_quantity > 0
                        
                        UNION ALL
                        
                        SELECT SUM(transfer_quantity) as total_supply
                        FROM warehouse_transfer_details_view
                        WHERE product_id = :product_id AND is_completed = 0 AND transfer_quantity > 0
                    ) supply_union
                    
                    UNION ALL
                    
                    SELECT 
                        'total_committed' as metric,
                        COALESCE(
                            SUM(
                                GREATEST(0,
                                    LEAST(
                                        COALESCE(pending_standard_delivery_quantity, 0),
                                        COALESCE(undelivered_allocated_qty_standard, 0)
                                    )
                                )
                            ), 
                        0) as value
                    FROM outbound_oc_pending_delivery_view
                    WHERE product_id = :product_id
                    AND pending_standard_delivery_quantity > 0
                    AND undelivered_allocated_qty_standard > 0
                )
                SELECT 
                    MAX(CASE WHEN metric = 'total_supply' THEN value END) as total_supply,
                    MAX(CASE WHEN metric = 'total_committed' THEN value END) as total_committed
                FROM supply_summary
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {'product_id': product_id}).fetchone()
                
                if result:
                    total_supply = float(result[0] or 0)
                    total_committed = float(result[1] or 0)
                    available = total_supply - total_committed
                    
                    return {
                        'total_supply': total_supply,
                        'total_committed': total_committed,
                        'available': available,
                        'coverage_ratio': (available / total_supply * 100) if total_supply > 0 else 0
                    }
            
            return {
                'total_supply': 0,
                'total_committed': 0,
                'available': 0,
                'coverage_ratio': 0
            }
            
        except Exception as e:
            logger.error(f"Error getting product supply detail: {e}")
            return {
                'total_supply': 0,
                'total_committed': 0,
                'available': 0,
                'coverage_ratio': 0
            }
    
    # ==================== ALLOCATION SUMMARY ====================
    
    def get_oc_allocation_summary(self, ocd_id: int) -> Dict[str, Decimal]:
        """
        Get current allocation summary for an OC
        
        Returns:
            Dict with total_allocated, total_cancelled, total_delivered,
                   total_effective_allocated, undelivered_allocated
        """
        try:
            query = text("""
                SELECT 
                    CAST(COALESCE(SUM(ad.allocated_qty), 0) AS DECIMAL(15,2)) as total_allocated,
                    CAST(COALESCE(SUM(CASE WHEN ac.status = 'ACTIVE' THEN ac.cancelled_qty ELSE 0 END), 0) AS DECIMAL(15,2)) as total_cancelled,
                    CAST(COALESCE(SUM(adl.delivered_qty), 0) AS DECIMAL(15,2)) as total_delivered,
                    CAST(COALESCE(SUM(ad.allocated_qty - COALESCE(CASE WHEN ac.status = 'ACTIVE' THEN ac.cancelled_qty ELSE 0 END, 0)), 0) AS DECIMAL(15,2)) as total_effective_allocated,
                    CAST(COALESCE(SUM(ad.allocated_qty - 
                                COALESCE(CASE WHEN ac.status = 'ACTIVE' THEN ac.cancelled_qty ELSE 0 END, 0) - 
                                COALESCE(adl.delivered_qty, 0)), 0) AS DECIMAL(15,2)) as undelivered_allocated
                FROM allocation_details ad
                LEFT JOIN (
                    SELECT allocation_detail_id, SUM(cancelled_qty) as cancelled_qty, status
                    FROM allocation_cancellations
                    WHERE status = 'ACTIVE'
                    GROUP BY allocation_detail_id, status
                ) ac ON ad.id = ac.allocation_detail_id
                LEFT JOIN (
                    SELECT allocation_detail_id, SUM(delivered_qty) as delivered_qty
                    FROM allocation_delivery_links
                    GROUP BY allocation_detail_id
                ) adl ON ad.id = adl.allocation_detail_id
                WHERE ad.demand_reference_id = :ocd_id
                AND ad.demand_type = 'OC'
                AND ad.status = 'ALLOCATED'
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {'ocd_id': ocd_id}).fetchone()
                
                if result:
                    return {
                        'total_allocated': Decimal(str(result._mapping['total_allocated'])),
                        'total_cancelled': Decimal(str(result._mapping['total_cancelled'])),
                        'total_delivered': Decimal(str(result._mapping['total_delivered'])),
                        'total_effective_allocated': Decimal(str(result._mapping['total_effective_allocated'])),
                        'undelivered_allocated': Decimal(str(result._mapping['undelivered_allocated']))
                    }
            
            return {
                'total_allocated': Decimal('0'),
                'total_cancelled': Decimal('0'),
                'total_delivered': Decimal('0'),
                'total_effective_allocated': Decimal('0'),
                'undelivered_allocated': Decimal('0')
            }
            
        except Exception as e:
            logger.error(f"Error getting OC allocation summary: {e}")
            return {
                'total_allocated': Decimal('0'),
                'total_cancelled': Decimal('0'),
                'total_delivered': Decimal('0'),
                'total_effective_allocated': Decimal('0'),
                'undelivered_allocated': Decimal('0')
            }
    
    # ==================== HELPER METHODS ====================
    
    def _build_scope_conditions(self, scope: Dict) -> Tuple[List[str], Dict]:
        """Build WHERE conditions from scope filters"""
        conditions = [
            "p.delete_flag = 0",
            "ocpd.pending_standard_delivery_quantity > 0"
        ]
        params = {}
        
        # Brand filter
        if scope.get('brand_ids') and len(scope['brand_ids']) > 0:
            placeholders = ', '.join([f':brand_{i}' for i in range(len(scope['brand_ids']))])
            conditions.append(f"p.brand_id IN ({placeholders})")
            for i, bid in enumerate(scope['brand_ids']):
                params[f'brand_{i}'] = bid
        
        # Customer filter
        if scope.get('customer_codes') and len(scope['customer_codes']) > 0:
            placeholders = ', '.join([f':cust_{i}' for i in range(len(scope['customer_codes']))])
            conditions.append(f"ocpd.customer_code IN ({placeholders})")
            for i, code in enumerate(scope['customer_codes']):
                params[f'cust_{i}'] = code
        
        # Legal entity filter
        if scope.get('legal_entities') and len(scope['legal_entities']) > 0:
            placeholders = ', '.join([f':le_{i}' for i in range(len(scope['legal_entities']))])
            conditions.append(f"ocpd.legal_entity IN ({placeholders})")
            for i, le in enumerate(scope['legal_entities']):
                params[f'le_{i}'] = le
        
        # ETD range filter
        if scope.get('etd_from'):
            conditions.append("ocpd.etd >= :etd_from")
            params['etd_from'] = scope['etd_from']
        
        if scope.get('etd_to'):
            conditions.append("ocpd.etd <= :etd_to")
            params['etd_to'] = scope['etd_to']
        
        # Include/exclude partial allocated
        if not scope.get('include_partial_allocated', True):
            # Only include OCs that have not been allocated yet
            conditions.append("COALESCE(ocpd.total_effective_allocated_qty_standard, 0) = 0")
        
        # Exclude over-committed OCs
        if scope.get('exclude_over_committed', False):
            conditions.append("ocpd.over_allocation_type IS NULL")
        
        return conditions, params