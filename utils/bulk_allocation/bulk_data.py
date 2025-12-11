"""
Bulk Allocation Data Repository
===============================
Handles all data queries for bulk allocation including:
- Scope selection (brands, customers, products, ETD range)
- Demand data (OCs pending delivery)
- Supply data (Inventory, CAN, PO, WHT)
- Committed calculation with MIN logic
- Allocation status breakdown

REFACTORED: 2024-12 - Added OC creator info (oc_creator_email, oc_creator_name) 
                      from outbound_oc_pending_delivery_view for email notifications
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
        Get summary statistics for selected scope with allocation status breakdown
        """
        try:
            base_conditions, params = self._build_base_scope_conditions(scope)
            where_clause = f"WHERE {' AND '.join(base_conditions)}" if base_conditions else ""
            
            query = f"""
                WITH scope_ocs AS (
                    SELECT 
                        ocpd.ocd_id,
                        ocpd.product_id,
                        ocpd.pending_standard_delivery_quantity as pending_qty,
                        ocpd.standard_quantity as effective_qty,
                        COALESCE(ocpd.total_effective_allocated_qty_standard, 0) as current_allocated,
                        COALESCE(ocpd.undelivered_allocated_qty_standard, 0) as undelivered_allocated,
                        GREATEST(0, LEAST(
                            ocpd.standard_quantity - COALESCE(ocpd.total_effective_allocated_qty_standard, 0),
                            ocpd.pending_standard_delivery_quantity - COALESCE(ocpd.undelivered_allocated_qty_standard, 0)
                        )) as max_allocatable,
                        CASE 
                            WHEN COALESCE(ocpd.undelivered_allocated_qty_standard, 0) = 0 
                                AND COALESCE(ocpd.total_effective_allocated_qty_standard, 0) = 0
                            THEN 'NOT_ALLOCATED'
                            WHEN GREATEST(0, LEAST(
                                    ocpd.standard_quantity - COALESCE(ocpd.total_effective_allocated_qty_standard, 0),
                                    ocpd.pending_standard_delivery_quantity - COALESCE(ocpd.undelivered_allocated_qty_standard, 0)
                                 )) <= 0
                            THEN 'FULLY_ALLOCATED'
                            ELSE 'PARTIALLY_ALLOCATED'
                        END as allocation_status
                    FROM outbound_oc_pending_delivery_view ocpd
                    INNER JOIN products p ON p.id = ocpd.product_id
                    LEFT JOIN brands b ON p.brand_id = b.id
                    {where_clause}
                ),
                scope_products AS (
                    SELECT DISTINCT product_id FROM scope_ocs
                ),
                product_supply AS (
                    SELECT 
                        sp.product_id,
                        COALESCE(inv.qty, 0) + COALESCE(can.qty, 0) + 
                        COALESCE(po.qty, 0) + COALESCE(wht.qty, 0) as total_supply
                    FROM scope_products sp
                    LEFT JOIN (
                        SELECT product_id, SUM(remaining_quantity) as qty
                        FROM inventory_detailed_view WHERE remaining_quantity > 0 GROUP BY product_id
                    ) inv ON sp.product_id = inv.product_id
                    LEFT JOIN (
                        SELECT product_id, SUM(pending_quantity) as qty
                        FROM can_pending_stockin_view WHERE pending_quantity > 0 GROUP BY product_id
                    ) can ON sp.product_id = can.product_id
                    LEFT JOIN (
                        SELECT product_id, SUM(pending_standard_arrival_quantity) as qty
                        FROM purchase_order_full_view WHERE pending_standard_arrival_quantity > 0 GROUP BY product_id
                    ) po ON sp.product_id = po.product_id
                    LEFT JOIN (
                        SELECT product_id, SUM(transfer_quantity) as qty
                        FROM warehouse_transfer_details_view WHERE is_completed = 0 AND transfer_quantity > 0 GROUP BY product_id
                    ) wht ON sp.product_id = wht.product_id
                ),
                product_committed AS (
                    SELECT product_id,
                        SUM(GREATEST(0, LEAST(
                            COALESCE(pending_standard_delivery_quantity, 0),
                            COALESCE(undelivered_allocated_qty_standard, 0)
                        ))) as total_committed
                    FROM outbound_oc_pending_delivery_view
                    WHERE product_id IN (SELECT product_id FROM scope_products)
                    AND pending_standard_delivery_quantity > 0 AND undelivered_allocated_qty_standard > 0
                    GROUP BY product_id
                ),
                supply_totals AS (
                    SELECT COALESCE(SUM(ps.total_supply), 0) as total_supply,
                           COALESCE(SUM(pc.total_committed), 0) as total_committed
                    FROM product_supply ps
                    LEFT JOIN product_committed pc ON ps.product_id = pc.product_id
                ),
                oc_summary AS (
                    SELECT
                        COUNT(DISTINCT product_id) as total_products,
                        COUNT(*) as total_ocs,
                        SUM(CASE WHEN allocation_status = 'NOT_ALLOCATED' THEN 1 ELSE 0 END) as not_allocated_count,
                        SUM(CASE WHEN allocation_status = 'PARTIALLY_ALLOCATED' THEN 1 ELSE 0 END) as partially_allocated_count,
                        SUM(CASE WHEN allocation_status = 'FULLY_ALLOCATED' THEN 1 ELSE 0 END) as fully_allocated_count,
                        SUM(CASE WHEN max_allocatable > 0 THEN 1 ELSE 0 END) as need_allocation_count,
                        COALESCE(SUM(pending_qty), 0) as total_demand,
                        COALESCE(SUM(CASE WHEN max_allocatable > 0 THEN pending_qty ELSE 0 END), 0) as need_allocation_demand,
                        COALESCE(SUM(max_allocatable), 0) as total_allocatable,
                        COALESCE(SUM(undelivered_allocated), 0) as total_undelivered_allocated
                    FROM scope_ocs
                )
                SELECT os.*, st.total_supply, st.total_committed,
                       st.total_supply - st.total_committed as available_supply
                FROM oc_summary os CROSS JOIN supply_totals st
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params).fetchone()
                
                if result:
                    data = dict(result._mapping)
                    total_demand = float(data.get('total_demand', 0) or 0)
                    total_allocatable = float(data.get('total_allocatable', 0) or 0)
                    available_supply = float(data.get('available_supply', 0) or 0)
                    total_ocs = int(data.get('total_ocs', 0) or 0)
                    need_allocation_count = int(data.get('need_allocation_count', 0) or 0)
                    
                    return {
                        'total_products': int(data.get('total_products', 0) or 0),
                        'total_ocs': total_ocs,
                        'not_allocated_count': int(data.get('not_allocated_count', 0) or 0),
                        'partially_allocated_count': int(data.get('partially_allocated_count', 0) or 0),
                        'fully_allocated_count': int(data.get('fully_allocated_count', 0) or 0),
                        'need_allocation_count': need_allocation_count,
                        'need_allocation_percent': (need_allocation_count / total_ocs * 100) if total_ocs > 0 else 0,
                        'fully_allocated_percent': (int(data.get('fully_allocated_count', 0) or 0) / total_ocs * 100) if total_ocs > 0 else 0,
                        'total_demand': total_demand,
                        'need_allocation_demand': float(data.get('need_allocation_demand', 0) or 0),
                        'total_allocatable': total_allocatable,
                        'total_undelivered_allocated': float(data.get('total_undelivered_allocated', 0) or 0),
                        'total_supply': float(data.get('total_supply', 0) or 0),
                        'total_committed': float(data.get('total_committed', 0) or 0),
                        'available_supply': available_supply,
                        'coverage_percent': (available_supply / total_demand * 100) if total_demand > 0 else 0,
                        'allocatable_coverage_percent': (available_supply / total_allocatable * 100) if total_allocatable > 0 else 0
                    }
            
            return self._empty_scope_summary()
            
        except Exception as e:
            logger.error(f"Error getting scope summary: {e}")
            return self._empty_scope_summary()
    
    def _empty_scope_summary(self) -> Dict[str, Any]:
        return {
            'total_products': 0, 'total_ocs': 0, 'not_allocated_count': 0,
            'partially_allocated_count': 0, 'fully_allocated_count': 0, 'need_allocation_count': 0,
            'need_allocation_percent': 0, 'fully_allocated_percent': 0, 'total_demand': 0,
            'need_allocation_demand': 0, 'total_allocatable': 0, 'total_undelivered_allocated': 0,
            'total_supply': 0, 'total_committed': 0, 'available_supply': 0,
            'coverage_percent': 0, 'allocatable_coverage_percent': 0
        }
    
    # ==================== DEMAND DATA (UPDATED) ====================
    
    def get_demands_in_scope(self, scope: Dict) -> pd.DataFrame:
        """
        Get all OCs matching the scope filters
        
        Returns DataFrame with columns needed for allocation:
        - ocd_id, oc_number, oc_date, customer_code, customer, legal_entity
        - product_id, pt_code, product_name, brand_id, brand_name
        - etd, pending_qty, effective_qty, allocated_qty, undelivered_allocated
        - standard_uom, selling_uom, uom_conversion
        - outstanding_amount_usd (for revenue priority)
        - allocation_status, max_allocatable
        
        NEW: OC Creator info for email notifications:
        - oc_created_by (keycloak_id)
        - oc_creator_email
        - oc_creator_name
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
                    COALESCE(ocpd.package_size, '') as package_size,
                    CONCAT(
                        ocpd.pt_code, 
                        CASE WHEN ocpd.product_name IS NOT NULL AND ocpd.product_name != '' 
                             THEN CONCAT(' | ', ocpd.product_name) ELSE '' END,
                        CASE WHEN ocpd.package_size IS NOT NULL AND ocpd.package_size != '' 
                             THEN CONCAT(' | ', ocpd.package_size) ELSE '' END,
                        CASE WHEN ocpd.brand IS NOT NULL AND ocpd.brand != '' 
                             THEN CONCAT(' (', ocpd.brand, ')') ELSE '' END
                    ) as product_display,
                    p.brand_id,
                    ocpd.brand as brand_name,
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
                    
                    -- ===== NEW: OC Creator Info for Email Notifications =====
                    ocpd.oc_created_by,
                    ocpd.oc_creator_email,
                    ocpd.oc_creator_name,
                    
                    -- Calculate max allocatable
                    GREATEST(0, LEAST(
                        ocpd.standard_quantity - COALESCE(ocpd.total_effective_allocated_qty_standard, 0),
                        ocpd.pending_standard_delivery_quantity - COALESCE(ocpd.undelivered_allocated_qty_standard, 0)
                    )) as max_allocatable,
                    
                    -- Allocation status
                    CASE 
                        WHEN COALESCE(ocpd.undelivered_allocated_qty_standard, 0) = 0 
                            AND COALESCE(ocpd.total_effective_allocated_qty_standard, 0) = 0
                        THEN 'NOT_ALLOCATED'
                        WHEN GREATEST(0, LEAST(
                                ocpd.standard_quantity - COALESCE(ocpd.total_effective_allocated_qty_standard, 0),
                                ocpd.pending_standard_delivery_quantity - COALESCE(ocpd.undelivered_allocated_qty_standard, 0)
                             )) <= 0
                        THEN 'FULLY_ALLOCATED'
                        ELSE 'PARTIALLY_ALLOCATED'
                    END as allocation_status,
                    
                    -- Legacy columns for compatibility
                    GREATEST(0, 
                        ocpd.standard_quantity - COALESCE(ocpd.total_effective_allocated_qty_standard, 0)
                    ) as remaining_allocatable,
                    GREATEST(0,
                        ocpd.pending_standard_delivery_quantity - COALESCE(ocpd.undelivered_allocated_qty_standard, 0)
                    ) as topup_needed
                    
                FROM outbound_oc_pending_delivery_view ocpd
                INNER JOIN products p ON p.id = ocpd.product_id
                {where_clause}
                ORDER BY 
                    ocpd.product_id,
                    ocpd.etd ASC,
                    ocpd.oc_date ASC
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            
            logger.info(f"Loaded {len(df)} OCs in scope with creator info")
            return df
            
        except Exception as e:
            logger.error(f"Error getting demands in scope: {e}")
            return pd.DataFrame()
    
    # ==================== SUPPLY DATA ====================
    
    def get_supply_by_products(self, product_ids: List[int]) -> pd.DataFrame:
        """Get supply data for multiple products"""
        if not product_ids:
            return pd.DataFrame()
        
        try:
            placeholders = ', '.join([f':pid_{i}' for i in range(len(product_ids))])
            params = {f'pid_{i}': pid for i, pid in enumerate(product_ids)}
            
            query = f"""
                WITH product_supply AS (
                    SELECT product_id, SUM(quantity) as total_supply
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
                product_committed AS (
                    SELECT product_id,
                        SUM(GREATEST(0, LEAST(
                            COALESCE(pending_standard_delivery_quantity, 0),
                            COALESCE(undelivered_allocated_qty_standard, 0)
                        ))) as total_committed
                    FROM outbound_oc_pending_delivery_view
                    WHERE product_id IN ({placeholders})
                    AND pending_standard_delivery_quantity > 0 AND undelivered_allocated_qty_standard > 0
                    GROUP BY product_id
                )
                SELECT ps.product_id, ps.total_supply,
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
        """Get detailed supply information for a single product"""
        try:
            query = text("""
                WITH supply_summary AS (
                    SELECT 'total_supply' as metric, COALESCE(SUM(total_supply), 0) as value
                    FROM (
                        SELECT SUM(remaining_quantity) as total_supply FROM inventory_detailed_view
                        WHERE product_id = :product_id AND remaining_quantity > 0
                        UNION ALL
                        SELECT SUM(pending_quantity) FROM can_pending_stockin_view
                        WHERE product_id = :product_id AND pending_quantity > 0
                        UNION ALL
                        SELECT SUM(pending_standard_arrival_quantity) FROM purchase_order_full_view
                        WHERE product_id = :product_id AND pending_standard_arrival_quantity > 0
                        UNION ALL
                        SELECT SUM(transfer_quantity) FROM warehouse_transfer_details_view
                        WHERE product_id = :product_id AND is_completed = 0 AND transfer_quantity > 0
                    ) supply_union
                    UNION ALL
                    SELECT 'total_committed' as metric,
                        COALESCE(SUM(GREATEST(0, LEAST(
                            COALESCE(pending_standard_delivery_quantity, 0),
                            COALESCE(undelivered_allocated_qty_standard, 0)
                        ))), 0) as value
                    FROM outbound_oc_pending_delivery_view
                    WHERE product_id = :product_id
                    AND pending_standard_delivery_quantity > 0 AND undelivered_allocated_qty_standard > 0
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
            
            return {'total_supply': 0, 'total_committed': 0, 'available': 0, 'coverage_ratio': 0}
            
        except Exception as e:
            logger.error(f"Error getting product supply detail: {e}")
            return {'total_supply': 0, 'total_committed': 0, 'available': 0, 'coverage_ratio': 0}
    
    # ==================== ALLOCATION SUMMARY ====================
    
    def get_oc_allocation_summary(self, ocd_id: int) -> Dict[str, Decimal]:
        """Get current allocation summary for an OC"""
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
                    FROM allocation_cancellations WHERE status = 'ACTIVE'
                    GROUP BY allocation_detail_id, status
                ) ac ON ad.id = ac.allocation_detail_id
                LEFT JOIN (
                    SELECT allocation_detail_id, SUM(delivered_qty) as delivered_qty
                    FROM allocation_delivery_links GROUP BY allocation_detail_id
                ) adl ON ad.id = adl.allocation_detail_id
                WHERE ad.demand_reference_id = :ocd_id AND ad.demand_type = 'OC' AND ad.status = 'ALLOCATED'
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
                'total_allocated': Decimal('0'), 'total_cancelled': Decimal('0'),
                'total_delivered': Decimal('0'), 'total_effective_allocated': Decimal('0'),
                'undelivered_allocated': Decimal('0')
            }
            
        except Exception as e:
            logger.error(f"Error getting OC allocation summary: {e}")
            return {
                'total_allocated': Decimal('0'), 'total_cancelled': Decimal('0'),
                'total_delivered': Decimal('0'), 'total_effective_allocated': Decimal('0'),
                'undelivered_allocated': Decimal('0')
            }
    
    # ==================== HELPER METHODS ====================
    
    def _build_base_scope_conditions(self, scope: Dict) -> Tuple[List[str], Dict]:
        """Build WHERE conditions from scope filters WITHOUT allocation status filters."""
        conditions = ["p.delete_flag = 0", "ocpd.pending_standard_delivery_quantity > 0"]
        params = {}
        
        if scope.get('brand_ids') and len(scope['brand_ids']) > 0:
            placeholders = ', '.join([f':brand_{i}' for i in range(len(scope['brand_ids']))])
            conditions.append(f"p.brand_id IN ({placeholders})")
            for i, bid in enumerate(scope['brand_ids']):
                params[f'brand_{i}'] = bid
        
        if scope.get('customer_codes') and len(scope['customer_codes']) > 0:
            placeholders = ', '.join([f':cust_{i}' for i in range(len(scope['customer_codes']))])
            conditions.append(f"ocpd.customer_code IN ({placeholders})")
            for i, code in enumerate(scope['customer_codes']):
                params[f'cust_{i}'] = code
        
        if scope.get('legal_entities') and len(scope['legal_entities']) > 0:
            placeholders = ', '.join([f':le_{i}' for i in range(len(scope['legal_entities']))])
            conditions.append(f"ocpd.legal_entity IN ({placeholders})")
            for i, le in enumerate(scope['legal_entities']):
                params[f'le_{i}'] = le
        
        if scope.get('etd_from'):
            conditions.append("ocpd.etd >= :etd_from")
            params['etd_from'] = scope['etd_from']
        
        if scope.get('etd_to'):
            conditions.append("ocpd.etd <= :etd_to")
            params['etd_to'] = scope['etd_to']
        
        return conditions, params
    
    def _build_scope_conditions(self, scope: Dict) -> Tuple[List[str], Dict]:
        """Build WHERE conditions from scope filters INCLUDING allocation status filters."""
        conditions, params = self._build_base_scope_conditions(scope)
        
        if scope.get('exclude_fully_allocated', True):
            conditions.append("""
                GREATEST(0, LEAST(
                    ocpd.standard_quantity - COALESCE(ocpd.total_effective_allocated_qty_standard, 0),
                    ocpd.pending_standard_delivery_quantity - COALESCE(ocpd.undelivered_allocated_qty_standard, 0)
                )) > 0
            """)
        
        if scope.get('only_unallocated', False):
            conditions.append("COALESCE(ocpd.total_effective_allocated_qty_standard, 0) = 0")
            conditions.append("COALESCE(ocpd.undelivered_allocated_qty_standard, 0) = 0")
        elif not scope.get('include_partial_allocated', True):
            conditions.append("COALESCE(ocpd.total_effective_allocated_qty_standard, 0) = 0")
        
        if scope.get('exclude_over_committed', False):
            conditions.append("ocpd.over_allocation_type IS NULL")
        
        return conditions, params