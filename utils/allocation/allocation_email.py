"""
Allocation Email Service
Email notifications for allocation operations (Create, Cancel, Update ETD, Reverse)
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
import os
from typing import Dict, List, Tuple, Optional
from sqlalchemy import text

from ..db import get_db_engine

logger = logging.getLogger(__name__)


class AllocationEmailService:
    """Handle email notifications for allocation operations"""
    
    def __init__(self):
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.sender_email = os.getenv("OUTBOUND_EMAIL_SENDER", os.getenv("EMAIL_SENDER", "outbound@prostech.vn"))
        self.sender_password = os.getenv("OUTBOUND_EMAIL_PASSWORD", os.getenv("EMAIL_PASSWORD", ""))
        self.allocation_cc = "allocation@prostech.vn"
    
    # ============== DATA FETCHING METHODS ==============
    
    def get_oc_creator_info(self, ocd_id: int) -> Optional[Dict]:
        """Get OC creator information from order_confirmations"""
        try:
            if not ocd_id:
                logger.warning("get_oc_creator_info: ocd_id is None or empty")
                return None
                
            engine = get_db_engine()
            query = text("""
                SELECT 
                    oc.oc_number,
                    oc.customerponumber AS customer_po,
                    oc.created_by AS oc_created_by,
                    buyer.english_name AS customer_name,
                    seller.english_name AS legal_entity,
                    ocd.etd AS oc_etd,
                    p.pt_code,
                    p.name AS product_name,
                    p.package_size,
                    b.brand_name AS brand,
                    ocd.uom AS standard_uom,
                    ocd.selling_quantity,
                    ocd.sellinguom AS selling_uom,
                    e.email AS creator_email,
                    CONCAT(e.first_name, ' ', e.last_name) AS creator_name
                FROM order_comfirmation_details ocd
                JOIN order_confirmations oc ON ocd.order_confirmation_id = oc.id
                LEFT JOIN companies buyer ON oc.buyer_id = buyer.id
                LEFT JOIN companies seller ON oc.seller_id = seller.id
                LEFT JOIN products p ON ocd.product_id = p.id
                LEFT JOIN brands b ON p.brand_id = b.id
                LEFT JOIN employees e ON oc.created_by = e.keycloak_id
                WHERE ocd.id = :ocd_id
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {'ocd_id': ocd_id}).fetchone()
                if result:
                    row = dict(result._mapping)
                    if not row.get('creator_email'):
                        logger.warning(f"OC creator email not found for ocd_id={ocd_id}, created_by={row.get('oc_created_by')}")
                    return row
                else:
                    logger.warning(f"No OC found for ocd_id={ocd_id}")
        except Exception as e:
            logger.error(f"Error fetching OC creator info: {e}", exc_info=True)
        return None
    
    def get_user_info(self, user_id: int) -> Optional[Dict]:
        """Get user information"""
        try:
            engine = get_db_engine()
            query = text("""
                SELECT 
                    u.id,
                    e.email,
                    CONCAT(e.first_name, ' ', e.last_name) AS full_name
                FROM users u
                LEFT JOIN employees e ON u.employee_id = e.id
                WHERE u.id = :user_id
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {'user_id': user_id}).fetchone()
                if result:
                    return dict(result._mapping)
        except Exception as e:
            logger.error(f"Error fetching user info: {e}")
        return None
    
    def get_allocation_summary(self, ocd_id: int) -> Dict:
        """Get current allocation summary for an OC line"""
        try:
            engine = get_db_engine()
            query = text("""
                SELECT 
                    COALESCE(SUM(ad.allocated_qty), 0) AS total_allocated,
                    COALESCE(SUM(CASE WHEN ac.status = 'ACTIVE' THEN ac.cancelled_qty ELSE 0 END), 0) AS total_cancelled,
                    COALESCE(SUM(adl.delivered_qty), 0) AS total_delivered
                FROM allocation_details ad
                JOIN allocation_plans ap ON ad.allocation_plan_id = ap.id
                LEFT JOIN allocation_cancellations ac ON ad.id = ac.allocation_detail_id
                LEFT JOIN allocation_delivery_links adl ON ad.id = adl.allocation_detail_id
                WHERE ad.demand_reference_id = :ocd_id
                AND ad.demand_type = 'OC'
                AND ad.status = 'ALLOCATED'
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {'ocd_id': ocd_id}).fetchone()
                if result:
                    row = dict(result._mapping)
                    total = float(row.get('total_allocated', 0))
                    cancelled = float(row.get('total_cancelled', 0))
                    delivered = float(row.get('total_delivered', 0))
                    return {
                        'total_allocated': total,
                        'total_cancelled': cancelled,
                        'total_delivered': delivered,
                        'effective_allocated': total - cancelled,
                        'pending': total - cancelled - delivered
                    }
        except Exception as e:
            logger.error(f"Error fetching allocation summary: {e}")
        return {'total_allocated': 0, 'total_cancelled': 0, 'total_delivered': 0, 'effective_allocated': 0, 'pending': 0}
    
    # ============== EMAIL SENDING ==============
    
    def _send_email(self, to_email: str, cc_emails: List[str], reply_to: str, 
                    subject: str, html_content: str) -> Tuple[bool, str]:
        """Send email using SMTP"""
        try:
            if not self.sender_email or not self.sender_password:
                return False, "Email configuration missing"
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.sender_email
            msg['To'] = to_email
            msg['Reply-To'] = reply_to
            
            if cc_emails:
                msg['Cc'] = ', '.join(cc_emails)
            
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                
                recipients = [to_email] + cc_emails
                server.sendmail(self.sender_email, recipients, msg.as_string())
            
            logger.info(f"Allocation email sent to {to_email}")
            return True, "Email sent successfully"
            
        except smtplib.SMTPAuthenticationError:
            return False, "Email authentication failed"
        except Exception as e:
            logger.error(f"Error sending email: {e}")
            return False, str(e)
    
    # ============== EMAIL TEMPLATES ==============
    
    def _format_number(self, value) -> str:
        """Format number with thousand separators"""
        try:
            return "{:,.2f}".format(float(value))
        except:
            return str(value)
    
    def _format_date(self, date_value) -> str:
        """Format date as DD MMM YYYY"""
        try:
            if isinstance(date_value, str):
                from datetime import datetime
                date_value = datetime.strptime(date_value[:10], '%Y-%m-%d')
            return date_value.strftime('%d %b %Y')
        except:
            return str(date_value) if date_value else 'N/A'
    
    def _build_base_style(self) -> str:
        """Base CSS styles for emails"""
        return """
        <style>
            body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
            .header { padding: 20px; text-align: center; color: white; }
            .header-green { background-color: #2e7d32; }
            .header-blue { background-color: #1976d2; }
            .header-red { background-color: #c62828; }
            .header-purple { background-color: #7b1fa2; }
            .content { padding: 20px; }
            .info-box { background-color: #f5f5f5; border-radius: 5px; padding: 15px; margin: 15px 0; }
            .label { color: #666; font-size: 12px; margin-bottom: 3px; }
            .value { font-weight: bold; font-size: 14px; }
            table { width: 100%; border-collapse: collapse; margin: 15px 0; }
            th { background-color: #f5f5f5; padding: 10px; text-align: left; border: 1px solid #ddd; }
            td { padding: 10px; border: 1px solid #ddd; }
            .badge { display: inline-block; padding: 3px 8px; border-radius: 3px; font-size: 12px; }
            .badge-soft { background-color: #e3f2fd; color: #1976d2; }
            .badge-hard { background-color: #fce4ec; color: #c62828; }
            .progress-bar { height: 20px; background-color: #e0e0e0; border-radius: 10px; overflow: hidden; }
            .progress-fill { height: 100%; background-color: #4caf50; }
            .footer { margin-top: 30px; padding: 20px; background-color: #f5f5f5; text-align: center; font-size: 12px; color: #666; }
        </style>
        """
    
    # ============== PUBLIC METHODS ==============
    
    def send_allocation_created_email(self, ocd_id: int, allocations: List[Dict], 
                                       total_qty: float, mode: str, etd, 
                                       user_id: int, allocation_number: str) -> Tuple[bool, str]:
        """Send email when allocation is created"""
        try:
            # Get OC info
            oc_info = self.get_oc_creator_info(ocd_id)
            if not oc_info:
                return False, "Could not find OC information"
            
            # Get allocator info
            allocator = self.get_user_info(user_id)
            allocator_email = allocator.get('email', '') if allocator else ''
            allocator_name = allocator.get('full_name', 'Unknown') if allocator else 'Unknown'
            
            # Use creator email, fallback to allocator email, then to allocation CC
            to_email = oc_info.get('creator_email') or allocator_email or self.allocation_cc
            if not to_email:
                return False, "No recipient email available"
            
            # Get allocation summary
            summary = self.get_allocation_summary(ocd_id)
            
            # Build email
            subject = f"✅ [Allocation] {oc_info['oc_number']} - {self._format_number(total_qty)} {oc_info.get('standard_uom', '')} allocated for {oc_info.get('customer_name', 'Customer')}"
            
            # Build sources table
            sources_html = ""
            for alloc in allocations:
                source_type = alloc.get('source_type', 'SOFT')
                if source_type:
                    supply_info = alloc.get('supply_info', {})
                    sources_html += f"""
                    <tr>
                        <td>{source_type}</td>
                        <td>{supply_info.get('batch_number', supply_info.get('po_number', supply_info.get('arrival_note_number', 'N/A')))}</td>
                        <td>{self._format_number(alloc.get('quantity', 0))}</td>
                        <td>{supply_info.get('warehouse', supply_info.get('warehouse_name', 'N/A'))}</td>
                    </tr>
                    """
            
            if not sources_html:
                sources_html = f"<tr><td colspan='4'>SOFT Allocation - {self._format_number(total_qty)} {oc_info.get('standard_uom', '')}</td></tr>"
            
            # Calculate progress
            oc_qty = float(oc_info.get('selling_quantity', 1))
            effective = summary.get('effective_allocated', 0)
            progress_pct = min(100, (effective / oc_qty * 100)) if oc_qty > 0 else 0
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>{self._build_base_style()}</head>
            <body>
                <div class="header header-green">
                    <h1>✅ Allocation Created</h1>
                    <p>{allocation_number}</p>
                </div>
                
                <div class="content">
                    <div class="info-box">
                        <table style="border: none;">
                            <tr>
                                <td style="border: none; width: 50%;">
                                    <div class="label">OC Number</div>
                                    <div class="value">{oc_info.get('oc_number', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Customer</div>
                                    <div class="value">{oc_info.get('customer_name', 'N/A')}</div>
                                </td>
                            </tr>
                            <tr>
                                <td style="border: none;">
                                    <div class="label">Product</div>
                                    <div class="value">{oc_info.get('pt_code', '')} - {oc_info.get('product_name', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Mode</div>
                                    <div class="value"><span class="badge badge-{'soft' if mode == 'SOFT' else 'hard'}">{mode}</span></div>
                                </td>
                            </tr>
                        </table>
                    </div>
                    
                    <h3>📦 Allocation Details</h3>
                    <table>
                        <tr>
                            <th>Source Type</th>
                            <th>Reference</th>
                            <th>Quantity</th>
                            <th>Warehouse</th>
                        </tr>
                        {sources_html}
                    </table>
                    
                    <div class="info-box">
                        <div class="label">Allocated ETD</div>
                        <div class="value">{self._format_date(etd)}</div>
                    </div>
                    
                    <h3>📊 OC Allocation Status</h3>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {progress_pct}%;"></div>
                    </div>
                    <p style="text-align: center;">{progress_pct:.1f}% Allocated ({self._format_number(effective)} / {self._format_number(oc_qty)} {oc_info.get('standard_uom', '')})</p>
                    
                    <div class="footer">
                        <p>Created by: <strong>{allocator_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                        <p style="color: #999;">This is an automated notification from Prostech Allocation System</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            # Send email
            cc_emails = [self.allocation_cc]
            if allocator_email and allocator_email != to_email:
                cc_emails.append(allocator_email)
            
            return self._send_email(
                to_email=to_email,
                cc_emails=cc_emails,
                reply_to=allocator_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending allocation created email: {e}", exc_info=True)
            return False, str(e)
    
    def send_allocation_cancelled_email(self, ocd_id: int, allocation_number: str,
                                         cancelled_qty: float, reason: str, reason_category: str,
                                         user_id: int) -> Tuple[bool, str]:
        """Send email when allocation is cancelled"""
        try:
            oc_info = self.get_oc_creator_info(ocd_id)
            if not oc_info:
                return False, "Could not find OC information"
            
            canceller = self.get_user_info(user_id)
            canceller_email = canceller.get('email', '') if canceller else ''
            canceller_name = canceller.get('full_name', 'Unknown') if canceller else 'Unknown'
            
            # Use creator email, fallback to canceller email, then to allocation CC
            to_email = oc_info.get('creator_email') or canceller_email or self.allocation_cc
            if not to_email:
                return False, "No recipient email available"
            
            subject = f"❌ [Allocation Cancelled] {oc_info['oc_number']} - {self._format_number(cancelled_qty)} {oc_info.get('standard_uom', '')} released"
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>{self._build_base_style()}</head>
            <body>
                <div class="header header-red">
                    <h1>❌ Allocation Cancelled</h1>
                    <p>{allocation_number}</p>
                </div>
                
                <div class="content">
                    <div class="info-box">
                        <table style="border: none;">
                            <tr>
                                <td style="border: none; width: 50%;">
                                    <div class="label">OC Number</div>
                                    <div class="value">{oc_info.get('oc_number', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Customer</div>
                                    <div class="value">{oc_info.get('customer_name', 'N/A')}</div>
                                </td>
                            </tr>
                            <tr>
                                <td style="border: none;">
                                    <div class="label">Product</div>
                                    <div class="value">{oc_info.get('pt_code', '')} - {oc_info.get('product_name', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Cancelled Quantity</div>
                                    <div class="value" style="color: #c62828;">{self._format_number(cancelled_qty)} {oc_info.get('standard_uom', '')}</div>
                                </td>
                            </tr>
                        </table>
                    </div>
                    
                    <h3>📝 Cancellation Details</h3>
                    <div class="info-box">
                        <div class="label">Reason Category</div>
                        <div class="value">{reason_category.replace('_', ' ').title()}</div>
                        <div class="label" style="margin-top: 10px;">Detailed Reason</div>
                        <div class="value">{reason}</div>
                    </div>
                    
                    <div style="background-color: #fff3e0; border-left: 4px solid #ff9800; padding: 15px; margin: 15px 0;">
                        <strong>⚠️ Action Required:</strong> The released quantity is now available for re-allocation.
                    </div>
                    
                    <div class="footer">
                        <p>Cancelled by: <strong>{canceller_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            cc_emails = [self.allocation_cc]
            if canceller_email and canceller_email != to_email:
                cc_emails.append(canceller_email)
            
            return self._send_email(
                to_email=to_email,
                cc_emails=cc_emails,
                reply_to=canceller_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending cancellation email: {e}", exc_info=True)
            return False, str(e)
    
    def send_allocation_etd_updated_email(self, ocd_id: int, allocation_number: str,
                                           previous_etd, new_etd, pending_qty: float,
                                           update_count: int, user_id: int) -> Tuple[bool, str]:
        """Send email when allocation ETD is updated"""
        try:
            oc_info = self.get_oc_creator_info(ocd_id)
            if not oc_info:
                return False, "Could not find OC information"
            
            updater = self.get_user_info(user_id)
            updater_email = updater.get('email', '') if updater else ''
            updater_name = updater.get('full_name', 'Unknown') if updater else 'Unknown'
            
            # Use creator email, fallback to updater email, then to allocation CC
            to_email = oc_info.get('creator_email') or updater_email or self.allocation_cc
            if not to_email:
                return False, "No recipient email available"
            
            # Calculate days difference
            try:
                from datetime import datetime
                if isinstance(previous_etd, str):
                    prev = datetime.strptime(previous_etd[:10], '%Y-%m-%d')
                else:
                    prev = previous_etd
                if isinstance(new_etd, str):
                    new = datetime.strptime(str(new_etd)[:10], '%Y-%m-%d')
                else:
                    new = new_etd
                days_diff = (new - prev).days
            except:
                days_diff = 0
            
            direction = "delayed" if days_diff > 0 else "advanced"
            
            subject = f"📅 [Allocation Update] {oc_info['oc_number']} - ETD changed: {self._format_date(previous_etd)} → {self._format_date(new_etd)}"
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>{self._build_base_style()}</head>
            <body>
                <div class="header header-blue">
                    <h1>📅 ETD Updated</h1>
                    <p>{allocation_number}</p>
                </div>
                
                <div class="content">
                    <div class="info-box">
                        <table style="border: none;">
                            <tr>
                                <td style="border: none; width: 50%;">
                                    <div class="label">OC Number</div>
                                    <div class="value">{oc_info.get('oc_number', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Customer</div>
                                    <div class="value">{oc_info.get('customer_name', 'N/A')}</div>
                                </td>
                            </tr>
                            <tr>
                                <td style="border: none;">
                                    <div class="label">Product</div>
                                    <div class="value">{oc_info.get('pt_code', '')} - {oc_info.get('product_name', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Affected Quantity</div>
                                    <div class="value">{self._format_number(pending_qty)} {oc_info.get('standard_uom', '')}</div>
                                </td>
                            </tr>
                        </table>
                    </div>
                    
                    <h3>📅 ETD Change</h3>
                    <div style="text-align: center; padding: 20px; background-color: #f5f5f5; border-radius: 5px;">
                        <span style="font-size: 18px;">{self._format_date(previous_etd)}</span>
                        <span style="font-size: 24px; margin: 0 15px;">→</span>
                        <span style="font-size: 18px; font-weight: bold; color: #1976d2;">{self._format_date(new_etd)}</span>
                        <p style="color: {'#c62828' if days_diff > 0 else '#2e7d32'}; margin-top: 10px;">
                            {'⚠️' if days_diff > 0 else '✅'} {direction.title()} by {abs(days_diff)} day(s)
                        </p>
                    </div>
                    
                    <p style="text-align: center; color: #666;">This is update #{update_count} for this allocation</p>
                    
                    <div class="footer">
                        <p>Updated by: <strong>{updater_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            cc_emails = [self.allocation_cc]
            if updater_email and updater_email != to_email:
                cc_emails.append(updater_email)
            
            return self._send_email(
                to_email=to_email,
                cc_emails=cc_emails,
                reply_to=updater_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending ETD update email: {e}", exc_info=True)
            return False, str(e)
    
    def send_cancellation_reversed_email(self, ocd_id: int, allocation_number: str,
                                          restored_qty: float, reversal_reason: str,
                                          user_id: int) -> Tuple[bool, str]:
        """Send email when cancellation is reversed"""
        try:
            oc_info = self.get_oc_creator_info(ocd_id)
            if not oc_info:
                return False, "Could not find OC information"
            
            reverser = self.get_user_info(user_id)
            reverser_email = reverser.get('email', '') if reverser else ''
            reverser_name = reverser.get('full_name', 'Unknown') if reverser else 'Unknown'
            
            # Use creator email, fallback to reverser email, then to allocation CC
            to_email = oc_info.get('creator_email') or reverser_email or self.allocation_cc
            if not to_email:
                return False, "No recipient email available"
            
            subject = f"🔄 [Allocation Restored] {oc_info['oc_number']} - {self._format_number(restored_qty)} {oc_info.get('standard_uom', '')} re-allocated"
            
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>{self._build_base_style()}</head>
            <body>
                <div class="header header-purple">
                    <h1>🔄 Cancellation Reversed</h1>
                    <p>{allocation_number}</p>
                </div>
                
                <div class="content">
                    <div class="info-box">
                        <table style="border: none;">
                            <tr>
                                <td style="border: none; width: 50%;">
                                    <div class="label">OC Number</div>
                                    <div class="value">{oc_info.get('oc_number', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Customer</div>
                                    <div class="value">{oc_info.get('customer_name', 'N/A')}</div>
                                </td>
                            </tr>
                            <tr>
                                <td style="border: none;">
                                    <div class="label">Product</div>
                                    <div class="value">{oc_info.get('pt_code', '')} - {oc_info.get('product_name', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Restored Quantity</div>
                                    <div class="value" style="color: #7b1fa2;">{self._format_number(restored_qty)} {oc_info.get('standard_uom', '')}</div>
                                </td>
                            </tr>
                        </table>
                    </div>
                    
                    <h3>📝 Reversal Details</h3>
                    <div class="info-box">
                        <div class="label">Reason for Reversal</div>
                        <div class="value">{reversal_reason}</div>
                    </div>
                    
                    <div style="background-color: #e8f5e9; border-left: 4px solid #4caf50; padding: 15px; margin: 15px 0;">
                        <strong>✅ Status:</strong> The previously cancelled quantity has been restored to the allocation.
                    </div>
                    
                    <div class="footer">
                        <p>Reversed by: <strong>{reverser_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            cc_emails = [self.allocation_cc]
            if reverser_email and reverser_email != to_email:
                cc_emails.append(reverser_email)
            
            return self._send_email(
                to_email=to_email,
                cc_emails=cc_emails,
                reply_to=reverser_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending reversal email: {e}", exc_info=True)
            return False, str(e)