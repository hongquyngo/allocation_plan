"""
Supply Data Repository - Handles supply source queries
REFACTORED: Implemented MIN logic for committed quantity calculation
to handle data inconsistency during transition period
"""
import pandas as pd
import logging
from typing import Dict, Any
import streamlit as st
from sqlalchemy import text

from ..db import get_db_engine
from ..config import config

logger = logging.getLogger(__name__)


class SupplyData:
    """Repository for supply source data access"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.cache_ttl = config.get_app_setting('CACHE_TTL_SECONDS', 300)
    
    # ==================== Supply Summary ====================
    
    @st.cache_data(ttl=60)
    def get_product_supply_summary(_self, product_id: int) -> Dict[str, Any]:
        """
        Get supply summary for a product including availability
        
        IMPROVED COMMITTED CALCULATION:
        Uses MIN logic to handle data inconsistency during transition period
        Formula: Committed = Σ MIN(pending_delivery, undelivered_allocated)
        
        This prevents over-blocking supply when allocation_delivery_links
        are not yet fully populated with historical delivery data.
        
        Returns:
            Dict with keys:
            - total_supply: Total available supply from all sources
            - total_committed: Total committed quantity (using MIN logic)
            - available: Available supply (total - committed)
            - coverage_ratio: Percentage of supply available
        """
        try:
            query = text("""
                WITH supply_summary AS (
                    -- ===== TOTAL SUPPLY =====
                    -- Sum from all supply sources: Inventory, CAN, PO, WHT
                    SELECT 
                        'total_supply' as metric,
                        COALESCE(SUM(total_supply), 0) as value
                    FROM (
                        -- Inventory
                        SELECT SUM(remaining_quantity) as total_supply
                        FROM inventory_detailed_view
                        WHERE product_id = :product_id 
                          AND remaining_quantity > 0
                        
                        UNION ALL
                        
                        -- Pending CAN (Confirmed Arrival Notice)
                        SELECT SUM(pending_quantity) as total_supply
                        FROM can_pending_stockin_view
                        WHERE product_id = :product_id 
                          AND pending_quantity > 0
                        
                        UNION ALL
                        
                        -- Pending PO (Purchase Order)
                        SELECT SUM(pending_standard_arrival_quantity) as total_supply
                        FROM purchase_order_full_view
                        WHERE product_id = :product_id 
                          AND pending_standard_arrival_quantity > 0
                        
                        UNION ALL
                        
                        -- Warehouse Transfer
                        SELECT SUM(transfer_quantity) as total_supply
                        FROM warehouse_transfer_details_view
                        WHERE product_id = :product_id 
                          AND is_completed = 0 
                          AND transfer_quantity > 0
                    ) supply_union
                    
                    UNION ALL
                    
                    -- ===== COMMITTED (IMPROVED MIN LOGIC) =====
                    -- Formula: Committed = Σ MIN(pending_delivery, undelivered_allocated)
                    --
                    -- Why MIN?
                    -- - pending_delivery: Actual demand from OC system (source of truth)
                    -- - undelivered_allocated: From allocation system
                    -- 
                    -- During transition, some deliveries may not be linked yet in
                    -- allocation_delivery_links, causing undelivered_allocated to be
                    -- higher than actual pending. Using MIN prevents over-blocking supply.
                    --
                    SELECT 
                        'total_committed' as metric,
                        COALESCE(
                            SUM(
                                GREATEST(0,  -- Ensure non-negative
                                    LEAST(
                                        COALESCE(pending_standard_delivery_quantity, 0),
                                        COALESCE(undelivered_allocated_qty_standard, 0)
                                    )
                                )
                            ), 
                        0) as value
                    FROM outbound_oc_pending_delivery_view
                    WHERE product_id = :product_id
                      -- Only count OCs with pending delivery
                      AND pending_standard_delivery_quantity > 0
                      -- Only count if there's actual allocation
                      AND undelivered_allocated_qty_standard > 0
                )
                SELECT 
                    MAX(CASE WHEN metric = 'total_supply' THEN value END) as total_supply,
                    MAX(CASE WHEN metric = 'total_committed' THEN value END) as total_committed
                FROM supply_summary
            """)
            
            with _self.engine.connect() as conn:
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
            logger.error(f"Error getting product supply summary: {e}")
            return {
                'total_supply': 0,
                'total_committed': 0,
                'available': 0,
                'coverage_ratio': 0
            }
    
    @st.cache_data(ttl=300)
    def get_supply_with_availability(_self, product_id: int) -> pd.DataFrame:
        """Get all supply sources with availability info after considering commitments"""
        try:
            query = """
            WITH supply_sources AS (
                SELECT 
                    'INVENTORY' as source_type,
                    inventory_history_id as source_id,
                    CONCAT('Batch ', batch_number) as reference,
                    remaining_quantity as total_quantity,
                    standard_uom as uom,
                    NULL as buying_uom,
                    NULL as uom_conversion,
                    expiry_date,
                    NULL as arrival_date,
                    NULL as etd,
                    NULL as eta,
                    batch_number,
                    location,
                    warehouse_name,
                    NULL as from_warehouse,
                    NULL as to_warehouse,
                    NULL as vendor_name,
                    NULL as po_number,
                    NULL as arrival_note_number
                FROM inventory_detailed_view
                WHERE product_id = :product_id AND remaining_quantity > 0
                
                UNION ALL
                
                SELECT 
                    'PENDING_CAN' as source_type,
                    can_line_id as source_id,
                    arrival_note_number as reference,
                    pending_quantity as total_quantity,
                    standard_uom as uom,
                    buying_uom,
                    uom_conversion,
                    NULL as expiry_date,
                    arrival_date,
                    NULL as etd,
                    NULL as eta,
                    NULL as batch_number,
                    NULL as location,
                    NULL as warehouse_name,
                    NULL as from_warehouse,
                    NULL as to_warehouse,
                    vendor as vendor_name,
                    po_number,
                    arrival_note_number
                FROM can_pending_stockin_view
                WHERE product_id = :product_id AND pending_quantity > 0
                
                UNION ALL
                
                SELECT 
                    'PENDING_PO' as source_type,
                    po_line_id as source_id,
                    po_number as reference,
                    pending_standard_arrival_quantity as total_quantity,
                    standard_uom as uom,
                    buying_uom,
                    uom_conversion,
                    NULL as expiry_date,
                    NULL as arrival_date,
                    etd,
                    eta,
                    NULL as batch_number,
                    NULL as location,
                    NULL as warehouse_name,
                    NULL as from_warehouse,
                    NULL as to_warehouse,
                    vendor_name,
                    po_number,
                    NULL as arrival_note_number
                FROM purchase_order_full_view
                WHERE product_id = :product_id AND pending_standard_arrival_quantity > 0
                
                UNION ALL
                
                SELECT 
                    'PENDING_WHT' as source_type,
                    warehouse_transfer_line_id as source_id,
                    CONCAT(from_warehouse, ' → ', to_warehouse) as reference,
                    transfer_quantity as total_quantity,
                    standard_uom as uom,
                    NULL as buying_uom,
                    NULL as uom_conversion,
                    expiry_date,
                    NULL as arrival_date,
                    transfer_date as etd,
                    NULL as eta,
                    batch_number,
                    NULL as location,
                    NULL as warehouse_name,
                    from_warehouse,
                    to_warehouse,
                    NULL as vendor_name,
                    NULL as po_number,
                    NULL as arrival_note_number
                FROM warehouse_transfer_details_view
                WHERE product_id = :product_id AND is_completed = 0 AND transfer_quantity > 0
            ),
            commitments AS (
                -- IMPROVED: Use MIN logic for commitment calculation
                SELECT 
                    ad.supply_source_type,
                    ad.supply_source_id,
                    SUM(
                        GREATEST(0,
                            LEAST(
                                COALESCE(ocpd.pending_standard_delivery_quantity, 0),
                                COALESCE(ocpd.undelivered_allocated_qty_standard, 0)
                            )
                        )
                    ) as committed_qty
                FROM allocation_details ad
                INNER JOIN outbound_oc_pending_delivery_view ocpd
                    ON ad.demand_reference_id = ocpd.ocd_id
                    AND ad.demand_type = 'OC'
                WHERE ad.product_id = :product_id
                  AND ad.status = 'ALLOCATED'
                  AND ocpd.pending_standard_delivery_quantity > 0
                  AND ocpd.undelivered_allocated_qty_standard > 0
                GROUP BY ad.supply_source_type, ad.supply_source_id
            )
            SELECT 
                s.*,
                COALESCE(c.committed_qty, 0) as committed_quantity,
                s.total_quantity - COALESCE(c.committed_qty, 0) as available_quantity
            FROM supply_sources s
            LEFT JOIN commitments c 
                ON s.source_type = c.supply_source_type 
                AND s.source_id = c.supply_source_id
            ORDER BY 
                CASE s.source_type 
                    WHEN 'INVENTORY' THEN 1 
                    WHEN 'PENDING_CAN' THEN 2
                    WHEN 'PENDING_PO' THEN 3
                    WHEN 'PENDING_WHT' THEN 4
                END,
                COALESCE(s.expiry_date, s.arrival_date, s.etd)
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'product_id': product_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading supply with availability: {e}")
            return pd.DataFrame()
    
    # ==================== Individual Supply Type Queries ====================
    
    @st.cache_data(ttl=300)
    def get_inventory_summary(_self, product_id: int) -> pd.DataFrame:
        """Get inventory summary for product view"""
        try:
            query = """
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
                WHERE product_id = :product_id AND remaining_quantity > 0
                ORDER BY expiry_date ASC
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'product_id': product_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading inventory summary: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_can_summary(_self, product_id: int) -> pd.DataFrame:
        """Get CAN summary with buying UOM information"""
        try:
            query = """
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
                WHERE product_id = :product_id AND pending_quantity > 0
                ORDER BY arrival_date ASC
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'product_id': product_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading CAN summary: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_po_summary(_self, product_id: int) -> pd.DataFrame:
        """Get PO summary with buying UOM information"""
        try:
            query = """
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
                    eta,
                    vendor_name
                FROM purchase_order_full_view
                WHERE product_id = :product_id AND pending_standard_arrival_quantity > 0
                ORDER BY etd ASC
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'product_id': product_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading PO summary: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def get_wht_summary(_self, product_id: int) -> pd.DataFrame:
        """Get warehouse transfer summary for product view"""
        try:
            query = """
                SELECT 
                    warehouse_transfer_line_id,
                    product_id,
                    product_name,
                    from_warehouse,
                    to_warehouse,
                    transfer_quantity,
                    standard_uom,
                    transfer_date as etd,
                    CASE WHEN is_completed = 1 THEN 'Completed' ELSE 'In Progress' END as status
                FROM warehouse_transfer_details_view
                WHERE product_id = :product_id AND is_completed = 0 AND transfer_quantity > 0
                ORDER BY transfer_date DESC
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'product_id': product_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading WHT summary: {e}")
            return pd.DataFrame()
    
    # ==================== Supply Availability Check ====================
    
    def check_supply_availability(self, source_type: str, source_id: int, 
                                product_id: int) -> Dict[str, Any]:
        """Check current availability of a supply source"""
        try:
            params = {'source_id': source_id, 'product_id': product_id}
            
            queries = {
                "INVENTORY": """
                    SELECT remaining_quantity as available_qty, batch_number, expiry_date
                    FROM inventory_detailed_view
                    WHERE inventory_history_id = :source_id AND product_id = :product_id
                    AND remaining_quantity > 0
                """,
                "PENDING_CAN": """
                    SELECT pending_quantity as available_qty, arrival_note_number, arrival_date
                    FROM can_pending_stockin_view
                    WHERE can_line_id = :source_id AND product_id = :product_id
                    AND pending_quantity > 0
                """,
                "PENDING_PO": """
                    SELECT pending_standard_arrival_quantity as available_qty, po_number, etd, eta
                    FROM purchase_order_full_view
                    WHERE po_line_id = :source_id AND product_id = :product_id
                    AND pending_standard_arrival_quantity > 0
                """,
                "PENDING_WHT": """
                    SELECT transfer_quantity as available_qty, from_warehouse, to_warehouse
                    FROM warehouse_transfer_details_view
                    WHERE warehouse_transfer_line_id = :source_id AND product_id = :product_id
                    AND is_completed = 0 AND transfer_quantity > 0
                """
            }
            
            query = queries.get(source_type)
            if not query:
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