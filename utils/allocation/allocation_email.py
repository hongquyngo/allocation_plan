"""
Allocation Email Service - REFACTORED v2.0
============================================
Simplified email notifications for allocation operations.

CHANGES from v1.0:
- Removed get_oc_creator_info() - Now uses oc_info dict passed directly from UI
- Removed get_user_info() - Now uses actor_info dict passed directly from session
- All send methods now accept oc_info and actor_info dicts directly
- Cleaner error handling with consistent fallback to allocation_cc

USAGE:
    # In modals, call with oc_info from session and actor from session_state.user
    oc_info = st.session_state.selections.get('oc_info')
    actor_info = {
        'email': st.session_state.user.get('email'),
        'name': st.session_state.user.get('full_name')
    }
    email_service.send_allocation_created_email(
        oc_info=oc_info,
        actor_info=actor_info,
        ...
    )
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
import os
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class AllocationEmailService:
    """
    Simplified email notifications for allocation operations.
    
    All methods now accept oc_info and actor_info dicts directly,
    eliminating the need for database queries.
    """
    
    def __init__(self):
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.sender_email = os.getenv("OUTBOUND_EMAIL_SENDER", os.getenv("EMAIL_SENDER", "outbound@prostech.vn"))
        self.sender_password = os.getenv("OUTBOUND_EMAIL_PASSWORD", os.getenv("EMAIL_PASSWORD", ""))
        self.allocation_cc = "allocation@prostech.vn"
    
    # ============== HELPER METHODS ==============
    
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
                date_value = datetime.strptime(date_value[:10], '%Y-%m-%d')
            return date_value.strftime('%d %b %Y')
        except:
            return str(date_value) if date_value else 'N/A'
    
    def _get_recipient_email(self, oc_info: Dict, actor_info: Dict) -> Optional[str]:
        """
        Get recipient email with fallback chain:
        1. OC Creator email (from view)
        2. Actor email (person performing action)
        3. Allocation CC email
        """
        return (
            oc_info.get('oc_creator_email') or 
            actor_info.get('email') or 
            self.allocation_cc
        )
    
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
    
    # ============== PUBLIC METHODS ==============
    
    def send_allocation_created_email(
        self, 
        oc_info: Dict,
        actor_info: Dict,
        allocations: List[Dict], 
        total_qty: float, 
        mode: str, 
        etd,
        allocation_number: str
    ) -> Tuple[bool, str]:
        """
        Send email when allocation is created.
        
        Args:
            oc_info: Dict containing OC details from view (includes oc_creator_email, oc_creator_name)
            actor_info: Dict with 'email' and 'name' of the person creating allocation
            allocations: List of allocation items
            total_qty: Total allocated quantity
            mode: 'SOFT' or 'HARD'
            etd: Allocated ETD date
            allocation_number: Generated allocation number
        """
        try:
            if not oc_info:
                return False, "OC information not provided"
            
            to_email = self._get_recipient_email(oc_info, actor_info)
            if not to_email:
                return False, "No recipient email available"
            
            actor_email = actor_info.get('email', '')
            actor_name = actor_info.get('name', 'Unknown')
            standard_uom = oc_info.get('standard_uom', '')
            
            # Build sources table
            sources_html = ""
            for alloc in allocations:
                source_type = alloc.get('source_type', 'SOFT')
                if source_type:
                    supply_info = alloc.get('supply_info', {})
                    ref = supply_info.get('batch_number') or supply_info.get('po_number') or supply_info.get('arrival_note_number') or 'N/A'
                    warehouse = supply_info.get('warehouse') or supply_info.get('warehouse_name') or 'N/A'
                    sources_html += f"""
                    <tr>
                        <td>{source_type}</td>
                        <td>{ref}</td>
                        <td>{self._format_number(alloc.get('quantity', 0))}</td>
                        <td>{warehouse}</td>
                    </tr>
                    """
            
            if not sources_html:
                sources_html = f"<tr><td colspan='4'>SOFT Allocation - {self._format_number(total_qty)} {standard_uom}</td></tr>"
            
            # Calculate progress
            oc_qty = float(oc_info.get('standard_quantity', oc_info.get('selling_quantity', 1)))
            effective_allocated = float(oc_info.get('total_effective_allocated_qty_standard', 0)) + total_qty
            progress_pct = min(100, (effective_allocated / oc_qty * 100)) if oc_qty > 0 else 0
            
            subject = f"✅ [Allocation] {oc_info.get('oc_number', 'N/A')} - {self._format_number(total_qty)} {standard_uom} allocated for {oc_info.get('customer', 'Customer')}"
            
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
                                    <div class="value">{oc_info.get('customer', 'N/A')}</div>
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
                    <p style="text-align: center;">{progress_pct:.1f}% Allocated ({self._format_number(effective_allocated)} / {self._format_number(oc_qty)} {standard_uom})</p>
                    
                    <div class="footer">
                        <p>Created by: <strong>{actor_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                        <p style="color: #999;">This is an automated notification from Prostech Allocation System</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            cc_emails = [self.allocation_cc]
            if actor_email and actor_email != to_email:
                cc_emails.append(actor_email)
            
            return self._send_email(
                to_email=to_email,
                cc_emails=cc_emails,
                reply_to=actor_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending allocation created email: {e}", exc_info=True)
            return False, str(e)
    
    def send_allocation_cancelled_email(
        self, 
        oc_info: Dict,
        actor_info: Dict,
        allocation_number: str,
        cancelled_qty: float, 
        reason: str, 
        reason_category: str
    ) -> Tuple[bool, str]:
        """
        Send email when allocation is cancelled.
        
        Args:
            oc_info: Dict containing OC details from view
            actor_info: Dict with 'email' and 'name' of the person cancelling
            allocation_number: The allocation being cancelled
            cancelled_qty: Quantity being cancelled
            reason: Cancellation reason
            reason_category: Category of reason
        """
        try:
            if not oc_info:
                return False, "OC information not provided"
            
            to_email = self._get_recipient_email(oc_info, actor_info)
            if not to_email:
                return False, "No recipient email available"
            
            actor_email = actor_info.get('email', '')
            actor_name = actor_info.get('name', 'Unknown')
            standard_uom = oc_info.get('standard_uom', '')
            
            # Format reason category
            category_labels = {
                'CUSTOMER_REQUEST': '👤 Customer Request',
                'SUPPLY_ISSUE': '⚠️ Supply Issue',
                'QUALITY_ISSUE': '❌ Quality Issue',
                'BUSINESS_DECISION': '💼 Business Decision',
                'OTHER': '📝 Other'
            }
            category_label = category_labels.get(reason_category, reason_category)
            
            subject = f"❌ [Allocation Cancelled] {oc_info.get('oc_number', 'N/A')} - {self._format_number(cancelled_qty)} {standard_uom} released"
            
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
                                    <div class="value">{oc_info.get('customer', 'N/A')}</div>
                                </td>
                            </tr>
                            <tr>
                                <td style="border: none;">
                                    <div class="label">Product</div>
                                    <div class="value">{oc_info.get('pt_code', '')} - {oc_info.get('product_name', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Cancelled Quantity</div>
                                    <div class="value" style="color: #c62828;">{self._format_number(cancelled_qty)} {standard_uom}</div>
                                </td>
                            </tr>
                        </table>
                    </div>
                    
                    <h3>📝 Cancellation Details</h3>
                    <div class="info-box">
                        <div class="label">Category</div>
                        <div class="value">{category_label}</div>
                        <div class="label" style="margin-top: 10px;">Reason</div>
                        <div class="value">{reason}</div>
                    </div>
                    
                    <div style="background-color: #ffebee; border-left: 4px solid #c62828; padding: 15px; margin: 15px 0;">
                        <strong>⚠️ Impact:</strong> {self._format_number(cancelled_qty)} {standard_uom} has been released and is now available for other allocations.
                    </div>
                    
                    <div class="footer">
                        <p>Cancelled by: <strong>{actor_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            cc_emails = [self.allocation_cc]
            if actor_email and actor_email != to_email:
                cc_emails.append(actor_email)
            
            return self._send_email(
                to_email=to_email,
                cc_emails=cc_emails,
                reply_to=actor_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending cancellation email: {e}", exc_info=True)
            return False, str(e)
    
    def send_allocation_etd_updated_email(
        self, 
        oc_info: Dict,
        actor_info: Dict,
        allocation_number: str,
        previous_etd,
        new_etd,
        pending_qty: float,
        update_count: int = 1
    ) -> Tuple[bool, str]:
        """
        Send email when allocation ETD is updated.
        
        Args:
            oc_info: Dict containing OC details from view
            actor_info: Dict with 'email' and 'name' of the person updating
            allocation_number: The allocation being updated
            previous_etd: Previous ETD date
            new_etd: New ETD date
            pending_qty: Quantity affected by the change
            update_count: Number of times this allocation has been updated
        """
        try:
            if not oc_info:
                return False, "OC information not provided"
            
            to_email = self._get_recipient_email(oc_info, actor_info)
            if not to_email:
                return False, "No recipient email available"
            
            actor_email = actor_info.get('email', '')
            actor_name = actor_info.get('name', 'Unknown')
            standard_uom = oc_info.get('standard_uom', '')
            
            # Calculate days difference
            try:
                from datetime import date
                if isinstance(previous_etd, str):
                    prev = datetime.strptime(previous_etd[:10], '%Y-%m-%d').date()
                elif isinstance(previous_etd, datetime):
                    prev = previous_etd.date()
                else:
                    prev = previous_etd
                    
                if isinstance(new_etd, str):
                    new = datetime.strptime(new_etd[:10], '%Y-%m-%d').date()
                elif isinstance(new_etd, datetime):
                    new = new_etd.date()
                else:
                    new = new_etd
                    
                days_diff = (new - prev).days
            except:
                days_diff = 0
            
            direction = "delayed" if days_diff > 0 else "advanced"
            
            subject = f"📅 [Allocation Update] {oc_info.get('oc_number', 'N/A')} - ETD changed: {self._format_date(previous_etd)} → {self._format_date(new_etd)}"
            
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
                                    <div class="value">{oc_info.get('customer', 'N/A')}</div>
                                </td>
                            </tr>
                            <tr>
                                <td style="border: none;">
                                    <div class="label">Product</div>
                                    <div class="value">{oc_info.get('pt_code', '')} - {oc_info.get('product_name', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Affected Quantity</div>
                                    <div class="value">{self._format_number(pending_qty)} {standard_uom}</div>
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
                        <p>Updated by: <strong>{actor_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            cc_emails = [self.allocation_cc]
            if actor_email and actor_email != to_email:
                cc_emails.append(actor_email)
            
            return self._send_email(
                to_email=to_email,
                cc_emails=cc_emails,
                reply_to=actor_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending ETD update email: {e}", exc_info=True)
            return False, str(e)
    
    def send_cancellation_reversed_email(
        self, 
        oc_info: Dict,
        actor_info: Dict,
        allocation_number: str,
        restored_qty: float, 
        reversal_reason: str
    ) -> Tuple[bool, str]:
        """
        Send email when cancellation is reversed.
        
        Args:
            oc_info: Dict containing OC details from view
            actor_info: Dict with 'email' and 'name' of the person reversing
            allocation_number: The allocation being restored
            restored_qty: Quantity being restored
            reversal_reason: Reason for reversal
        """
        try:
            if not oc_info:
                return False, "OC information not provided"
            
            to_email = self._get_recipient_email(oc_info, actor_info)
            if not to_email:
                return False, "No recipient email available"
            
            actor_email = actor_info.get('email', '')
            actor_name = actor_info.get('name', 'Unknown')
            standard_uom = oc_info.get('standard_uom', '')
            
            subject = f"🔄 [Allocation Restored] {oc_info.get('oc_number', 'N/A')} - {self._format_number(restored_qty)} {standard_uom} re-allocated"
            
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
                                    <div class="value">{oc_info.get('customer', 'N/A')}</div>
                                </td>
                            </tr>
                            <tr>
                                <td style="border: none;">
                                    <div class="label">Product</div>
                                    <div class="value">{oc_info.get('pt_code', '')} - {oc_info.get('product_name', 'N/A')}</div>
                                </td>
                                <td style="border: none;">
                                    <div class="label">Restored Quantity</div>
                                    <div class="value" style="color: #7b1fa2;">{self._format_number(restored_qty)} {standard_uom}</div>
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
                        <p>Reversed by: <strong>{actor_name}</strong></p>
                        <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            cc_emails = [self.allocation_cc]
            if actor_email and actor_email != to_email:
                cc_emails.append(actor_email)
            
            return self._send_email(
                to_email=to_email,
                cc_emails=cc_emails,
                reply_to=actor_email or self.sender_email,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"Error sending reversal email: {e}", exc_info=True)
            return False, str(e)