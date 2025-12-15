"""
Allocation Management Email Service
=====================================
Email notifications for allocation management operations.

This module is INDEPENDENT - no imports from allocation/ or bulk_allocation/

FIXED: 2024-12 - Now uses OUTBOUND_EMAIL_CONFIG from config.py for both local and cloud.
                 Previous version used os.getenv() directly which doesn't work on Streamlit Cloud.

Email Types:
- Quantity updated
- ETD updated
- Allocation cancelled
- Delivery reversed

Recipients (same as allocation creation):
- OC Creator (primary)
- CC: Allocation email group + Person who made the change
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Optional

from .mgmt_data import AllocationManagementData
# FIXED: Import email config from centralized config (works on both local and cloud)
from utils.config import OUTBOUND_EMAIL_CONFIG

logger = logging.getLogger(__name__)


class AllocationManagementEmail:
    """Email service for allocation management operations"""
    
    def __init__(self):
        # FIXED: Use centralized config instead of os.getenv() directly
        # This ensures it works on both local (.env) and Streamlit Cloud (st.secrets)
        self.smtp_host = OUTBOUND_EMAIL_CONFIG.get("host", "smtp.gmail.com")
        self.smtp_port = int(OUTBOUND_EMAIL_CONFIG.get("port", 587))
        self.sender_email = OUTBOUND_EMAIL_CONFIG.get("sender", "outbound@prostech.vn")
        self.sender_password = OUTBOUND_EMAIL_CONFIG.get("password", "")
        self.allocation_cc = "allocation@prostech.vn"
        self.data = AllocationManagementData()
    
    # ================================================================
    # MAIN EMAIL METHODS
    # ================================================================
    
    def send_quantity_updated_email(
        self,
        allocation_detail_id: int,
        old_qty: float,
        new_qty: float,
        reason: str,
        updater_user_id: int
    ) -> Tuple[bool, str]:
        """Send email notification for quantity update"""
        try:
            # Get allocation info
            allocation = self.data.get_allocation_detail(allocation_detail_id)
            if not allocation:
                return False, "Allocation not found"
            
            # Get OC info for recipient
            oc_info = self.data.get_oc_info_for_allocation(allocation_detail_id)
            if not oc_info or not oc_info.get('oc_creator_email'):
                return False, "OC creator email not found"
            
            # Get updater info
            updater = self.data.get_user_info(updater_user_id)
            updater_name = updater.get('full_name', 'System') if updater else 'System'
            updater_email = updater.get('email') if updater else None
            
            # Build email
            allocation_number = allocation.get('allocation_number', 'N/A')
            oc_number = oc_info.get('oc_number', 'N/A')
            product_name = allocation.get('product_name', 'N/A')
            customer_name = allocation.get('customer_name', 'N/A')
            
            change = new_qty - old_qty
            change_indicator = "📈 Increased" if change > 0 else "📉 Reduced"
            change_color = "#28a745" if change > 0 else "#dc3545"
            
            subject = f"📦 Allocation Updated - {oc_number} | Qty: {old_qty:,.0f} → {new_qty:,.0f}"
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>{self._build_style()}</head>
            <body>
                <div class="header header-blue">
                    <h1>📦 Allocation Quantity Updated</h1>
                    <p>Allocation: <strong>{allocation_number}</strong></p>
                </div>
                
                <div class="content">
                    <p>Hi <strong>{oc_info.get('oc_creator_name', 'Team')}</strong>,</p>
                    <p>The allocated quantity for your OC has been updated:</p>
                    
                    <table class="info-table">
                        <tr><td class="label">OC Number:</td><td><strong>{oc_number}</strong></td></tr>
                        <tr><td class="label">Customer:</td><td>{customer_name}</td></tr>
                        <tr><td class="label">Product:</td><td>{product_name}</td></tr>
                    </table>
                    
                    <div class="change-box" style="border-left: 4px solid {change_color};">
                        <h3>{change_indicator}</h3>
                        <div class="qty-change">
                            <span class="old-qty">{old_qty:,.2f}</span>
                            <span class="arrow">→</span>
                            <span class="new-qty" style="color: {change_color};">{new_qty:,.2f}</span>
                        </div>
                        <p class="change-amount">Change: {change:+,.2f} units</p>
                    </div>
                    
                    <div class="reason-box">
                        <h4>Reason:</h4>
                        <p>{reason}</p>
                    </div>
                    
                    <div class="footer">
                        <p>Updated by: <strong>{updater_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            # Build CC list
            cc_list = [self.allocation_cc]
            if updater_email:
                cc_list.append(updater_email)
            
            return self._send_email(
                to_email=oc_info.get('oc_creator_email'),
                cc_emails=cc_list,
                reply_to=updater_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending quantity update email: {e}")
            return False, str(e)
    
    def send_etd_updated_email(
        self,
        allocation_detail_id: int,
        old_etd: str,
        new_etd: str,
        reason: str,
        updater_user_id: int
    ) -> Tuple[bool, str]:
        """Send email notification for ETD update"""
        try:
            # Get allocation info
            allocation = self.data.get_allocation_detail(allocation_detail_id)
            if not allocation:
                return False, "Allocation not found"
            
            # Get OC info
            oc_info = self.data.get_oc_info_for_allocation(allocation_detail_id)
            if not oc_info or not oc_info.get('oc_creator_email'):
                return False, "OC creator email not found"
            
            # Get updater info
            updater = self.data.get_user_info(updater_user_id)
            updater_name = updater.get('full_name', 'System') if updater else 'System'
            updater_email = updater.get('email') if updater else None
            
            # Build email
            allocation_number = allocation.get('allocation_number', 'N/A')
            oc_number = oc_info.get('oc_number', 'N/A')
            product_name = allocation.get('product_name', 'N/A')
            customer_name = allocation.get('customer_name', 'N/A')
            allocated_qty = allocation.get('effective_allocated_qty', 0)
            
            subject = f"📅 ETD Updated - {oc_number} | {old_etd} → {new_etd}"
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>{self._build_style()}</head>
            <body>
                <div class="header header-orange">
                    <h1>📅 Allocation ETD Updated</h1>
                    <p>Allocation: <strong>{allocation_number}</strong></p>
                </div>
                
                <div class="content">
                    <p>Hi <strong>{oc_info.get('oc_creator_name', 'Team')}</strong>,</p>
                    <p>The Expected Delivery Date for your OC allocation has been updated:</p>
                    
                    <table class="info-table">
                        <tr><td class="label">OC Number:</td><td><strong>{oc_number}</strong></td></tr>
                        <tr><td class="label">Customer:</td><td>{customer_name}</td></tr>
                        <tr><td class="label">Product:</td><td>{product_name}</td></tr>
                        <tr><td class="label">Allocated Qty:</td><td>{allocated_qty:,.2f}</td></tr>
                    </table>
                    
                    <div class="change-box" style="border-left: 4px solid #fd7e14;">
                        <h3>📅 ETD Change</h3>
                        <div class="etd-change">
                            <span class="old-etd">{old_etd or 'Not set'}</span>
                            <span class="arrow">→</span>
                            <span class="new-etd">{new_etd}</span>
                        </div>
                    </div>
                    
                    <div class="reason-box">
                        <h4>Reason:</h4>
                        <p>{reason}</p>
                    </div>
                    
                    <div class="footer">
                        <p>Updated by: <strong>{updater_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            cc_list = [self.allocation_cc]
            if updater_email:
                cc_list.append(updater_email)
            
            return self._send_email(
                to_email=oc_info.get('oc_creator_email'),
                cc_emails=cc_list,
                reply_to=updater_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending ETD update email: {e}")
            return False, str(e)
    
    def send_cancelled_email(
        self,
        allocation_detail_id: int,
        cancelled_qty: float,
        reason: str,
        reason_category: str,
        canceller_user_id: int
    ) -> Tuple[bool, str]:
        """Send email notification for cancellation"""
        try:
            # Get allocation info
            allocation = self.data.get_allocation_detail(allocation_detail_id)
            if not allocation:
                return False, "Allocation not found"
            
            # Get OC info
            oc_info = self.data.get_oc_info_for_allocation(allocation_detail_id)
            if not oc_info or not oc_info.get('oc_creator_email'):
                return False, "OC creator email not found"
            
            # Get canceller info
            canceller = self.data.get_user_info(canceller_user_id)
            canceller_name = canceller.get('full_name', 'System') if canceller else 'System'
            canceller_email = canceller.get('email') if canceller else None
            
            # Build email
            allocation_number = allocation.get('allocation_number', 'N/A')
            oc_number = oc_info.get('oc_number', 'N/A')
            product_name = allocation.get('product_name', 'N/A')
            customer_name = allocation.get('customer_name', 'N/A')
            original_qty = allocation.get('allocated_qty', 0)
            remaining_qty = float(original_qty) - float(allocation.get('cancelled_qty', 0)) - cancelled_qty
            
            # Determine if full or partial cancel
            is_full_cancel = remaining_qty <= 0
            cancel_type = "Full Cancellation" if is_full_cancel else "Partial Cancellation"
            header_color = "#dc3545" if is_full_cancel else "#fd7e14"
            
            subject = f"❌ Allocation {cancel_type} - {oc_number} | {cancelled_qty:,.0f} units"
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>{self._build_style()}</head>
            <body>
                <div class="header" style="background: {header_color};">
                    <h1>❌ Allocation {cancel_type}</h1>
                    <p>Allocation: <strong>{allocation_number}</strong></p>
                </div>
                
                <div class="content">
                    <p>Hi <strong>{oc_info.get('oc_creator_name', 'Team')}</strong>,</p>
                    <p>An allocation for your OC has been {'fully' if is_full_cancel else 'partially'} cancelled:</p>
                    
                    <table class="info-table">
                        <tr><td class="label">OC Number:</td><td><strong>{oc_number}</strong></td></tr>
                        <tr><td class="label">Customer:</td><td>{customer_name}</td></tr>
                        <tr><td class="label">Product:</td><td>{product_name}</td></tr>
                    </table>
                    
                    <div class="cancel-box">
                        <h3>Cancellation Details</h3>
                        <table class="detail-table">
                            <tr>
                                <td>Original Allocated:</td>
                                <td><strong>{original_qty:,.2f}</strong></td>
                            </tr>
                            <tr>
                                <td>Cancelled:</td>
                                <td style="color: #dc3545;"><strong>-{cancelled_qty:,.2f}</strong></td>
                            </tr>
                            <tr>
                                <td>Remaining:</td>
                                <td><strong>{max(0, remaining_qty):,.2f}</strong></td>
                            </tr>
                        </table>
                    </div>
                    
                    <div class="reason-box">
                        <h4>Category: {reason_category.replace('_', ' ').title()}</h4>
                        <p>{reason}</p>
                    </div>
                    
                    <div class="footer">
                        <p>Cancelled by: <strong>{canceller_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            cc_list = [self.allocation_cc]
            if canceller_email:
                cc_list.append(canceller_email)
            
            return self._send_email(
                to_email=oc_info.get('oc_creator_email'),
                cc_emails=cc_list,
                reply_to=canceller_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending cancellation email: {e}")
            return False, str(e)
    
    def send_reversed_email(
        self,
        allocation_detail_id: int,
        reversed_qty: float,
        reason: str,
        reverser_user_id: int
    ) -> Tuple[bool, str]:
        """Send email notification for delivery reversal"""
        try:
            # Get allocation info
            allocation = self.data.get_allocation_detail(allocation_detail_id)
            if not allocation:
                return False, "Allocation not found"
            
            # Get OC info
            oc_info = self.data.get_oc_info_for_allocation(allocation_detail_id)
            if not oc_info or not oc_info.get('oc_creator_email'):
                return False, "OC creator email not found"
            
            # Get reverser info
            reverser = self.data.get_user_info(reverser_user_id)
            reverser_name = reverser.get('full_name', 'System') if reverser else 'System'
            reverser_email = reverser.get('email') if reverser else None
            
            # Build email
            allocation_number = allocation.get('allocation_number', 'N/A')
            oc_number = oc_info.get('oc_number', 'N/A')
            product_name = allocation.get('product_name', 'N/A')
            customer_name = allocation.get('customer_name', 'N/A')
            
            subject = f"↩️ Delivery Reversed - {oc_number} | {reversed_qty:,.0f} units"
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>{self._build_style()}</head>
            <body>
                <div class="header" style="background: #6c757d;">
                    <h1>↩️ Delivery Reversed</h1>
                    <p>Allocation: <strong>{allocation_number}</strong></p>
                </div>
                
                <div class="content">
                    <p>Hi <strong>{oc_info.get('oc_creator_name', 'Team')}</strong>,</p>
                    <p>A delivery has been reversed for your OC allocation:</p>
                    
                    <table class="info-table">
                        <tr><td class="label">OC Number:</td><td><strong>{oc_number}</strong></td></tr>
                        <tr><td class="label">Customer:</td><td>{customer_name}</td></tr>
                        <tr><td class="label">Product:</td><td>{product_name}</td></tr>
                    </table>
                    
                    <div class="change-box" style="border-left: 4px solid #6c757d;">
                        <h3>↩️ Reversed Quantity</h3>
                        <div class="reverse-qty">
                            <span style="font-size: 24px; font-weight: bold;">{reversed_qty:,.2f}</span>
                            <span> units returned to pending</span>
                        </div>
                    </div>
                    
                    <div class="reason-box">
                        <h4>Reason:</h4>
                        <p>{reason}</p>
                    </div>
                    
                    <p style="margin-top: 15px; padding: 10px; background: #fff3cd; border-radius: 4px;">
                        ⚠️ This reversal means the goods are now back in pending delivery status.
                    </p>
                    
                    <div class="footer">
                        <p>Reversed by: <strong>{reverser_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            cc_list = [self.allocation_cc]
            if reverser_email:
                cc_list.append(reverser_email)
            
            return self._send_email(
                to_email=oc_info.get('oc_creator_email'),
                cc_emails=cc_list,
                reply_to=reverser_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending reversal email: {e}")
            return False, str(e)
    
    # ================================================================
    # BULK EMAIL
    # ================================================================
    
    def send_bulk_update_summary_email(
        self,
        action_type: str,  # 'ETD_UPDATE' or 'CANCEL'
        results: List[Dict],
        reason: str,
        performer_user_id: int
    ) -> Tuple[bool, str]:
        """Send summary email for bulk operations"""
        try:
            # Get performer info
            performer = self.data.get_user_info(performer_user_id)
            performer_name = performer.get('full_name', 'System') if performer else 'System'
            performer_email = performer.get('email') if performer else None
            
            success_count = sum(1 for r in results if r.get('success'))
            failed_count = len(results) - success_count
            
            if action_type == 'ETD_UPDATE':
                subject = f"📅 Bulk ETD Update Complete - {success_count} allocations updated"
                action_desc = "ETD Update"
                header_color = "#fd7e14"
            else:
                subject = f"❌ Bulk Cancellation Complete - {success_count} allocations cancelled"
                action_desc = "Cancellation"
                header_color = "#dc3545"
            
            # Build results table
            rows_html = ""
            for r in results:
                status_icon = "✅" if r.get('success') else "❌"
                rows_html += f"""
                <tr>
                    <td>{status_icon}</td>
                    <td>{r.get('allocation_detail_id', 'N/A')}</td>
                    <td>{r.get('message', '')}</td>
                </tr>
                """
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>{self._build_style()}</head>
            <body>
                <div class="header" style="background: {header_color};">
                    <h1>Bulk {action_desc} Complete</h1>
                </div>
                
                <div class="content">
                    <div class="summary-grid">
                        <div class="summary-item">
                            <div class="summary-value">{success_count}</div>
                            <div class="summary-label">Success</div>
                        </div>
                        <div class="summary-item">
                            <div class="summary-value">{failed_count}</div>
                            <div class="summary-label">Failed</div>
                        </div>
                    </div>
                    
                    <div class="reason-box">
                        <h4>Reason:</h4>
                        <p>{reason}</p>
                    </div>
                    
                    <h3>Details</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>Status</th>
                                <th>Allocation ID</th>
                                <th>Message</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows_html}
                        </tbody>
                    </table>
                    
                    <div class="footer">
                        <p>Performed by: <strong>{performer_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            return self._send_email(
                to_email=performer_email or self.sender_email,
                cc_emails=[self.allocation_cc],
                reply_to=self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending bulk summary email: {e}")
            return False, str(e)
    
    # ================================================================
    # HELPER METHODS
    # ================================================================
    
    def _build_style(self) -> str:
        """Build common CSS styles"""
        return """
        <style>
            body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
            .header { padding: 20px; color: white; text-align: center; }
            .header-blue { background: #0066cc; }
            .header-orange { background: #fd7e14; }
            .header h1 { margin: 0 0 10px 0; font-size: 24px; }
            .header p { margin: 0; opacity: 0.9; }
            .content { padding: 20px; }
            .info-table { margin: 15px 0; border-collapse: collapse; }
            .info-table td { padding: 8px 15px 8px 0; }
            .info-table .label { color: #666; min-width: 120px; }
            .change-box { 
                background: #f8f9fa; 
                padding: 15px; 
                margin: 20px 0; 
                border-radius: 4px;
            }
            .change-box h3 { margin: 0 0 10px 0; }
            .qty-change, .etd-change { 
                font-size: 20px; 
                display: flex; 
                align-items: center; 
                gap: 10px;
            }
            .arrow { color: #666; }
            .old-qty, .old-etd { color: #999; text-decoration: line-through; }
            .change-amount { margin-top: 10px; color: #666; }
            .cancel-box { 
                background: #fff5f5; 
                padding: 15px; 
                margin: 20px 0; 
                border-radius: 4px;
                border-left: 4px solid #dc3545;
            }
            .detail-table { width: 100%; }
            .detail-table td { padding: 8px 0; }
            .reason-box { 
                background: #e9ecef; 
                padding: 15px; 
                margin: 20px 0; 
                border-radius: 4px;
            }
            .reason-box h4 { margin: 0 0 10px 0; }
            .reason-box p { margin: 0; }
            .summary-grid { 
                display: flex; 
                gap: 20px; 
                margin: 20px 0;
            }
            .summary-item { 
                background: #f8f9fa; 
                padding: 15px 25px; 
                border-radius: 4px; 
                text-align: center;
            }
            .summary-value { font-size: 28px; font-weight: bold; }
            .summary-label { color: #666; font-size: 12px; }
            table { width: 100%; border-collapse: collapse; margin: 15px 0; }
            th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
            th { background: #f8f9fa; }
            .footer { 
                margin-top: 30px; 
                padding-top: 15px; 
                border-top: 1px solid #eee; 
                font-size: 12px; 
                color: #666;
            }
        </style>
        """
    
    def _send_email(
        self,
        to_email: str,
        cc_emails: List[str],
        reply_to: str,
        subject: str,
        html_content: str
    ) -> Tuple[bool, str]:
        """Send email via SMTP"""
        try:
            if not self.sender_password:
                logger.warning("Email password not configured, skipping email")
                return False, "Email not configured"
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.sender_email
            msg['To'] = to_email
            if cc_emails:
                msg['Cc'] = ', '.join(cc_emails)
            msg['Reply-To'] = reply_to
            
            msg.attach(MIMEText(html_content, 'html'))
            
            all_recipients = [to_email] + (cc_emails or [])
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, all_recipients, msg.as_string())
            
            logger.info(f"Email sent successfully to {to_email}")
            return True, "Email sent successfully"
            
        except Exception as e:
            logger.error(f"Error sending email: {e}")
            return False, str(e)