"""
Allocation Management - Supply Data
====================================
Supply source queries for allocation management.

This module is INDEPENDENT - copied and adapted from supply_data.py
Uses same MIN logic for committed calculation.

Formula: Committed = Σ MIN(pending_delivery, undelivered_allocated)
"""

import pandas as pd
import logging
from typing import Dict, Any, List
import streamlit as st
from sqlalchemy import text

from utils.db import get_db_engine

logger = logging.getLogger(__name__)


class AllocationSupplyData:
    """Supply data queries for allocation management"""
    
    def __init__(self):
        self.engine = get_db_engine()
    
    # ================================================================
    # SUPPLY SUMMARY
    # ================================================================
    
    @st.cache_data(ttl=60)
    def get_product_supply_summary(_self, product_id: int) -> Dict[str, Any]:
        """
        Get supply summary for a product including availability.
        
        Uses MIN logic for committed calculation:
        Committed = Σ MIN(pending_delivery, undelivered_allocated)
        
        This prevents over-blocking supply when allocation_delivery_links
        are not yet fully populated with historical delivery data.
        """
        try:
            query = text("""
                WITH supply_summary AS (
                    -- ===== TOTAL SUPPLY =====
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
                        
                        -- Pending CAN
                        SELECT SUM(pending_quantity) as total_supply
                        FROM can_pending_stockin_view
                        WHERE product_id = :product_id 
                          AND pending_quantity > 0
                        
                        UNION ALL
                        
                        -- Pending PO
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
                    
                    -- ===== COMMITTED (MIN LOGIC) =====
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
    
    # ================================================================
    # DETAILED SUPPLY BY TYPE
    # ================================================================
    
    @st.cache_data(ttl=120)
    def get_inventory_details(_self, product_id: int) -> pd.DataFrame:
        """Get inventory batches for product (FEFO order)"""
        try:
            query = text("""
                SELECT 
                    inventory_history_id,
                    product_id,
                    product_name,
                    batch_number,
                    remaining_quantity,
                    standard_uom,
                    expiry_date,
                    warehouse_name,
                    location,
                    expiry_status,
                    days_in_warehouse
                FROM inventory_detailed_view
                WHERE product_id = :product_id 
                  AND remaining_quantity > 0
                ORDER BY expiry_date ASC, days_in_warehouse DESC
            """)
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={'product_id': product_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting inventory details: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=120)
    def get_pending_can_details(_self, product_id: int) -> pd.DataFrame:
        """Get pending CAN for product"""
        try:
            query = text("""
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
                    po_number,
                    days_since_arrival
                FROM can_pending_stockin_view
                WHERE product_id = :product_id 
                  AND pending_quantity > 0
                ORDER BY arrival_date ASC
            """)
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={'product_id': product_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting pending CAN details: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=120)
    def get_pending_po_details(_self, product_id: int) -> pd.DataFrame:
        """Get pending PO for product"""
        try:
            query = text("""
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
                    vendor_name,
                    status
                FROM purchase_order_full_view
                WHERE product_id = :product_id 
                  AND pending_standard_arrival_quantity > 0
                ORDER BY etd ASC
            """)
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={'product_id': product_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting pending PO details: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=120)
    def get_warehouse_transfer_details(_self, product_id: int) -> pd.DataFrame:
        """Get in-transit warehouse transfers for product"""
        try:
            query = text("""
                SELECT 
                    warehouse_transfer_line_id,
                    product_id,
                    product_name,
                    from_warehouse,
                    to_warehouse,
                    transfer_quantity,
                    standard_uom,
                    transfer_date,
                    batch_number,
                    expiry_date
                FROM warehouse_transfer_details_view
                WHERE product_id = :product_id 
                  AND is_completed = 0 
                  AND transfer_quantity > 0
                ORDER BY transfer_date DESC
            """)
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={'product_id': product_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting warehouse transfer details: {e}")
            return pd.DataFrame()
    
    # ================================================================
    # SUPPLY AVAILABILITY CHECK
    # ================================================================
    
    def check_supply_source_availability(
        self, 
        source_type: str, 
        source_id: int, 
        product_id: int
    ) -> Dict[str, Any]:
        """
        Check current availability of a specific supply source.
        Used to validate if supply is still available before update.
        """
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
                return {'available': False, 'available_qty': 0, 'error': 'Unknown source type'}
            
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
            return {'available': False, 'available_qty': 0, 'error': str(e)}
    
    # ================================================================
    # COMBINED SUPPLY DETAILS
    # ================================================================
    
    def get_all_supply_details(self, product_id: int) -> Dict[str, Any]:
        """
        Get comprehensive supply details for a product.
        Combines summary + detailed breakdown by source type.
        """
        try:
            summary = self.get_product_supply_summary(product_id)
            
            inventory_df = self.get_inventory_details(product_id)
            can_df = self.get_pending_can_details(product_id)
            po_df = self.get_pending_po_details(product_id)
            wht_df = self.get_warehouse_transfer_details(product_id)
            
            return {
                'summary': summary,
                'inventory': {
                    'count': len(inventory_df),
                    'total_qty': inventory_df['remaining_quantity'].sum() if len(inventory_df) > 0 else 0,
                    'data': inventory_df
                },
                'pending_can': {
                    'count': len(can_df),
                    'total_qty': can_df['pending_quantity'].sum() if len(can_df) > 0 else 0,
                    'data': can_df
                },
                'pending_po': {
                    'count': len(po_df),
                    'total_qty': po_df['pending_quantity'].sum() if len(po_df) > 0 else 0,
                    'data': po_df
                },
                'warehouse_transfer': {
                    'count': len(wht_df),
                    'total_qty': wht_df['transfer_quantity'].sum() if len(wht_df) > 0 else 0,
                    'data': wht_df
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting all supply details: {e}")
            return {
                'summary': {'total_supply': 0, 'total_committed': 0, 'available': 0, 'coverage_ratio': 0},
                'inventory': {'count': 0, 'total_qty': 0, 'data': pd.DataFrame()},
                'pending_can': {'count': 0, 'total_qty': 0, 'data': pd.DataFrame()},
                'pending_po': {'count': 0, 'total_qty': 0, 'data': pd.DataFrame()},
                'warehouse_transfer': {'count': 0, 'total_qty': 0, 'data': pd.DataFrame()}
            }
