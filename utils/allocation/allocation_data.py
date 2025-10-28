"""
Allocation Data Repository - REFACTORED
Added dual ETD date display (Original ETD + Latest ETD) in delivery history
"""
import pandas as pd
import logging
from typing import Dict, Any
import streamlit as st
from sqlalchemy import text

from ..db import get_db_engine
from ..config import config

logger = logging.getLogger(__name__)


class AllocationData:
    """Repository for allocation-related data access"""
    
    def __init__(self):
        self.engine = get_db_engine()
        self.cache_ttl = config.get_app_setting('CACHE_TTL_SECONDS', 300)
    
    # ==================== Allocation History ====================
    
    def get_allocation_history_with_details(_self, oc_detail_id: int) -> pd.DataFrame:
        """Get allocation history with delivery data from allocation_delivery_links"""
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
                    COALESCE(adl.delivered_qty, 0) as delivered_qty,
                    ad.allocated_etd,
                    ad.status,
                    COALESCE(ad.supply_source_type, 'No specific source') as supply_source_type,
                    ad.notes,
                    COALESCE(ac.cancelled_qty, 0) as cancelled_qty,
                    (ad.allocated_qty - COALESCE(ac.cancelled_qty, 0)) as effective_qty,
                    (ad.allocated_qty - COALESCE(ac.cancelled_qty, 0) - COALESCE(adl.delivered_qty, 0)) as pending_qty,
                    CASE 
                        WHEN ac.cancelled_qty > 0 THEN 
                            CONCAT('Cancelled: ', ac.cancelled_qty, ' - ', ac.reason)
                        ELSE ''
                    END as cancellation_info,
                    CASE WHEN ac.has_cancellations > 0 THEN 1 ELSE 0 END as has_cancellations,
                    adl.delivery_count,
                    adl.first_delivery_date,
                    adl.last_delivery_date,
                    adl.delivery_references
                FROM allocation_details ad
                INNER JOIN allocation_plans ap ON ad.allocation_plan_id = ap.id
                LEFT JOIN users u ON ap.creator_id = u.id
                
                LEFT JOIN (
                    SELECT 
                        adl.allocation_detail_id,
                        SUM(adl.delivered_qty) as delivered_qty,
                        COUNT(DISTINCT adl.delivery_detail_id) as delivery_count,
                        MIN(adl.created_at) as first_delivery_date,
                        MAX(adl.created_at) as last_delivery_date,
                        GROUP_CONCAT(DISTINCT sod.dn_number ORDER BY adl.created_at SEPARATOR ', ') as delivery_references
                    FROM allocation_delivery_links adl
                    LEFT JOIN stock_out_delivery_request_details sodrd ON adl.delivery_detail_id = sodrd.id
                    LEFT JOIN stock_out_delivery sod ON sodrd.delivery_id = sod.id
                    WHERE sodrd.delete_flag = 0 AND sod.delete_flag = 0
                    GROUP BY adl.allocation_detail_id
                ) adl ON ad.id = adl.allocation_detail_id
                
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
            logger.error(f"Error loading allocation history: {e}")
            return pd.DataFrame()

    def get_allocation_delivery_details(_self, allocation_detail_id: int) -> pd.DataFrame:
        """
        Get delivery details for a specific allocation
        REFACTORED: Now returns both Original ETD and Latest ETD
        """
        try:
            query = """
                SELECT 
                    adl.id as link_id,
                    adl.delivered_qty,
                    adl.created_at as delivery_linked_date,
                    sodrd.id as delivery_detail_id,
                    sod.dn_number as delivery_number,
                    
                    -- ===== DUAL ETD DATES =====
                    sod.etd_date as original_etd,
                    sod.adjust_etd_date as latest_etd,
                    
                    -- Current delivery_date (for backwards compatibility)
                    DATE(COALESCE(sod.adjust_etd_date, sod.etd_date, sodrd.etd, sodrd.adjust_etd)) as delivery_date,
                    
                    -- ETD update tracking
                    sod.etd_update_count,
                    
                    sodrd.stock_out_quantity as total_delivery_qty,
                    sodrd.selling_stock_out_quantity as total_delivery_qty_selling,
                    sod.shipment_status as delivery_status,
                    w.name as from_warehouse,
                    c.english_name as customer_name,
                    
                    -- Additional useful fields
                    sod.dispatch_date,
                    sod.date_delivered
                    
                FROM allocation_delivery_links adl
                INNER JOIN stock_out_delivery_request_details sodrd ON adl.delivery_detail_id = sodrd.id
                INNER JOIN stock_out_delivery sod ON sodrd.delivery_id = sod.id
                LEFT JOIN warehouses w ON sod.preference_warehouse_id = w.id
                LEFT JOIN order_comfirmation_details ocd ON sodrd.oc_detail_id = ocd.id
                LEFT JOIN order_confirmations oc ON ocd.order_confirmation_id = oc.id
                LEFT JOIN companies c ON oc.buyer_id = c.id
                WHERE adl.allocation_detail_id = :allocation_detail_id
                AND sodrd.delete_flag = 0
                AND sod.delete_flag = 0
                ORDER BY sod.etd_date DESC, adl.created_at DESC
            """
            
            with _self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'allocation_detail_id': allocation_detail_id})
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading allocation delivery details: {e}")
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
                        MAX(CASE 
                            WHEN ocpd.is_over_committed = 'Yes' OR ocpd.is_pending_over_allocated = 'Yes' 
                            THEN 1 ELSE 0 
                        END) as has_over_allocation
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
            
            return {
                'total_products': 0,
                'total_demand_qty': 0,
                'total_supply_qty': 0,
                'critical_products': 0,
                'urgent_etd_count': 0,
                'over_allocated_count': 0
            }
            
        except Exception as e:
            logger.error(f"Error loading dashboard metrics: {e}")
            return {
                'total_products': 0,
                'total_demand_qty': 0,
                'total_supply_qty': 0,
                'critical_products': 0,
                'urgent_etd_count': 0,
                'over_allocated_count': 0
            }