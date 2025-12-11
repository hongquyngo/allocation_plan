"""
Bulk Allocation Email Service
Email notifications for bulk allocation operations.

Email flow:
1. Summary email to allocator (contains ALL OCs)
2. Individual emails to each OC creator (only their OCs)
   - CC: allocator + allocation@prostech.vn
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
import os
from typing import Dict, List, Tuple, Optional, Set
from sqlalchemy import text

from utils.db import get_db_engine

logger = logging.getLogger(__name__)


class BulkEmailService:
    """Handle email notifications for bulk allocation operations"""
    
    def __init__(self):
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.sender_email = os.getenv("OUTBOUND_EMAIL_SENDER", os.getenv("EMAIL_SENDER", "outbound@prostech.vn"))
        self.sender_password = os.getenv("OUTBOUND_EMAIL_PASSWORD", os.getenv("EMAIL_PASSWORD", ""))
        self.allocation_cc = "allocation@prostech.vn"
    
    # ============== DATA FETCHING METHODS ==============
    
    def get_user_info(self, user_id: int) -> Optional[Dict]:
        """Get user information"""
        try:
            engine = get_db_engine()
            query = text("""
                SELECT 
                    u.id,
                    u.username,
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
    
    def get_oc_creators_for_allocations(self, ocd_ids: List[int]) -> Dict[str, Dict]:
        """
        Get OC creator information grouped by email.
        
        Returns:
            Dict[email] = {
                'full_name': str,
                'ocd_ids': List[int]
            }
        """
        creators = {}
        
        if not ocd_ids:
            return creators
        
        try:
            engine = get_db_engine()
            query = text("""
                SELECT 
                    ocd.id AS ocd_id,
                    e.email,
                    CONCAT(e.first_name, ' ', e.last_name) AS full_name
                FROM order_comfirmation_details ocd
                INNER JOIN order_confirmations oc ON ocd.order_confirmation_id = oc.id
                INNER JOIN employees e ON oc.created_by = e.keycloak_id
                WHERE ocd.id IN :ocd_ids
                AND e.email IS NOT NULL
                AND e.email != ''
                AND e.delete_flag = 0
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {'ocd_ids': tuple(ocd_ids)})
                for row in result:
                    email = row.email
                    if email not in creators:
                        creators[email] = {
                            'full_name': row.full_name or 'Sales',
                            'ocd_ids': []
                        }
                    creators[email]['ocd_ids'].append(row.ocd_id)
            
            logger.info(f"Found {len(creators)} unique OC creators for {len(ocd_ids)} OCs")
            
        except Exception as e:
            logger.error(f"Error getting OC creators: {e}")
        
        return creators
    
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
            
            logger.info(f"Bulk allocation email sent to {to_email}")
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
            return "{:,.0f}".format(float(value))
        except:
            return str(value)
    
    def _format_date(self, date_value) -> str:
        """Format date as DD MMM YYYY"""
        try:
            if date_value is None:
                return 'N/A'
            if isinstance(date_value, str):
                date_value = datetime.strptime(date_value[:10], '%Y-%m-%d')
            return date_value.strftime('%d %b %Y')
        except:
            return str(date_value) if date_value else 'N/A'
    
    def _build_base_style(self) -> str:
        """Base CSS styles for emails"""
        return """
        <style>
            body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }
            .header { padding: 25px; text-align: center; color: white; }
            .header-green { background: linear-gradient(135deg, #2e7d32 0%, #1b5e20 100%); }
            .header-blue { background: linear-gradient(135deg, #1976d2 0%, #1565c0 100%); }
            .header h1 { margin: 0 0 5px 0; font-size: 24px; }
            .header p { margin: 0; opacity: 0.9; }
            .content { padding: 25px; background: #f9f9f9; }
            .info-box { background-color: #fff; border-radius: 8px; padding: 15px; margin: 15px 0; border-left: 4px solid #1976d2; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            .label { color: #666; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 3px; }
            .value { font-weight: bold; font-size: 14px; color: #333; }
            .summary-grid { display: flex; justify-content: space-around; margin: 20px 0; }
            .summary-item { text-align: center; padding: 15px; background: #fff; border-radius: 8px; min-width: 100px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            .summary-value { font-size: 28px; font-weight: bold; color: #1976d2; }
            .summary-label { font-size: 11px; color: #666; text-transform: uppercase; }
            table { width: 100%; border-collapse: collapse; margin: 15px 0; background: #fff; }
            th { background-color: #1976d2; color: white; padding: 10px 8px; text-align: left; font-size: 12px; }
            td { padding: 8px; border-bottom: 1px solid #eee; font-size: 13px; }
            tr:hover { background-color: #f5f5f5; }
            .coverage-high { color: #2e7d32; font-weight: bold; }
            .coverage-mid { color: #f57c00; font-weight: bold; }
            .coverage-low { color: #c62828; font-weight: bold; }
            .etd-delay { color: #c62828; }
            .badge { display: inline-block; padding: 3px 8px; border-radius: 12px; font-size: 11px; }
            .badge-strategy { background-color: #e3f2fd; color: #1976d2; }
            .badge-mode { background-color: #f3e5f5; color: #7b1fa2; }
            .warning-box { background: #fff3e0; border-left: 4px solid #ff9800; padding: 12px; margin: 15px 0; border-radius: 0 8px 8px 0; }
            .footer { margin-top: 20px; padding: 20px; background-color: #f0f0f0; text-align: center; font-size: 12px; color: #666; border-radius: 0 0 8px 8px; }
        </style>
        """
    
    # ============== MAIN PUBLIC METHODS ==============
    
    def send_bulk_allocation_emails(
        self,
        commit_result: Dict,
        allocation_results: List[Dict],
        scope: Dict,
        strategy_config: Dict,
        allocator_user_id: int,
        split_allocations: Dict = None
    ) -> Dict[str, any]:
        """
        Main method to send all bulk allocation emails.
        
        1. Summary email to allocator
        2. Individual emails to each OC creator
        
        Returns:
            Dict with summary_sent, individual_sent, errors
        """
        results = {
            'success': False,
            'summary_sent': False,
            'individual_sent': 0,
            'individual_total': 0,
            'errors': []
        }
        
        # Get allocator info
        allocator = self.get_user_info(allocator_user_id)
        allocator_email = allocator.get('email', '') if allocator else ''
        allocator_name = allocator.get('full_name', 'Unknown') if allocator else 'Unknown'
        
        if not allocator_email:
            logger.warning(f"Allocator email not found for user_id={allocator_user_id}")
            results['errors'].append(f"Allocator email not found for user_id={allocator_user_id}")
        
        # 1. Send summary email to allocator
        if allocator_email:
            try:
                success, msg = self.send_summary_email_to_allocator(
                    commit_result=commit_result,
                    allocation_results=allocation_results,
                    scope=scope,
                    strategy_config=strategy_config,
                    allocator_email=allocator_email,
                    allocator_name=allocator_name,
                    split_allocations=split_allocations or {}
                )
                results['summary_sent'] = success
                if not success:
                    results['errors'].append(f"Summary email: {msg}")
            except Exception as e:
                logger.error(f"Summary email error: {e}", exc_info=True)
                results['errors'].append(f"Summary email error: {str(e)}")
        
        # 2. Send individual emails to OC creators
        try:
            individual_result = self.send_individual_creator_emails(
                commit_result=commit_result,
                allocation_results=allocation_results,
                allocator_email=allocator_email,
                allocator_name=allocator_name
            )
            results['individual_sent'] = individual_result.get('sent_count', 0)
            results['individual_total'] = individual_result.get('total_creators', 0)
            if individual_result.get('errors'):
                results['errors'].extend(individual_result['errors'])
        except Exception as e:
            logger.error(f"Individual emails error: {e}", exc_info=True)
            results['errors'].append(f"Individual emails error: {str(e)}")
        
        results['success'] = results['summary_sent'] or results['individual_sent'] > 0
        return results
    
    def send_summary_email_to_allocator(
        self,
        commit_result: Dict,
        allocation_results: List[Dict],
        scope: Dict,
        strategy_config: Dict,
        allocator_email: str,
        allocator_name: str,
        split_allocations: Dict
    ) -> Tuple[bool, str]:
        """
        Send comprehensive summary email to the person who created the bulk allocation.
        Contains ALL allocated OCs.
        """
        if not allocator_email:
            return False, "No allocator email provided"
        
        allocation_number = commit_result.get('allocation_number', 'N/A')
        detail_count = commit_result.get('detail_count', 0)
        total_allocated = commit_result.get('total_allocated', 0)
        products_affected = commit_result.get('products_affected', 0)
        customers_affected = commit_result.get('customers_affected', 0)
        
        # Build scope summary
        scope_parts = []
        if scope.get('brand_ids'):
            scope_parts.append(f"Brands: {len(scope['brand_ids'])}")
        if scope.get('customer_codes'):
            scope_parts.append(f"Customers: {len(scope['customer_codes'])}")
        if scope.get('etd_from') or scope.get('etd_to'):
            scope_parts.append(f"ETD: {scope.get('etd_from', 'Any')} → {scope.get('etd_to', 'Any')}")
        scope_summary = ' | '.join(scope_parts) if scope_parts else 'All'
        
        # Strategy info
        strategy_type = strategy_config.get('strategy_type', 'HYBRID')
        allocation_mode = strategy_config.get('allocation_mode', 'SOFT')
        
        # Count warnings
        etd_delay_count = 0
        split_count = len([k for k, v in split_allocations.items() if len(v) > 1])
        
        for alloc in allocation_results:
            oc_etd = alloc.get('oc_etd')
            allocated_etd = alloc.get('allocated_etd')
            if oc_etd and allocated_etd:
                try:
                    if self._compare_dates(allocated_etd, oc_etd) > 0:
                        etd_delay_count += 1
                except:
                    pass
        
        # Build allocation table (top 30)
        rows_html = self._build_allocation_table_rows(allocation_results, split_allocations, max_rows=30)
        
        # Build warnings section
        warnings_html = ""
        if etd_delay_count > 0 or split_count > 0:
            warning_items = []
            if etd_delay_count > 0:
                warning_items.append(f"⚠️ {etd_delay_count} OCs have allocated ETD later than requested")
            if split_count > 0:
                warning_items.append(f"✂️ {split_count} OCs have split allocations")
            warnings_html = f"""
            <div class="warning-box">
                <strong>Attention:</strong><br>
                {'<br>'.join(warning_items)}
            </div>
            """
        
        subject = f"📦 [Bulk Allocation] {allocation_number} - {detail_count} OCs Allocated"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>{self._build_base_style()}</head>
        <body>
            <div class="header header-green">
                <h1>📦 Bulk Allocation Completed</h1>
                <p>{allocation_number}</p>
            </div>
            
            <div class="content">
                <div class="info-box">
                    <table style="border: none; box-shadow: none;">
                        <tr>
                            <td style="border: none; width: 50%;">
                                <div class="label">Created By</div>
                                <div class="value">{allocator_name}</div>
                            </td>
                            <td style="border: none;">
                                <div class="label">Date</div>
                                <div class="value">{datetime.now().strftime('%d %b %Y %H:%M')}</div>
                            </td>
                        </tr>
                        <tr>
                            <td style="border: none;">
                                <div class="label">Strategy</div>
                                <div class="value">
                                    <span class="badge badge-strategy">{strategy_type}</span>
                                    <span class="badge badge-mode">{allocation_mode}</span>
                                </div>
                            </td>
                            <td style="border: none;">
                                <div class="label">Scope</div>
                                <div class="value">{scope_summary}</div>
                            </td>
                        </tr>
                    </table>
                </div>
                
                <div class="summary-grid">
                    <div class="summary-item">
                        <div class="summary-value">{detail_count}</div>
                        <div class="summary-label">OCs Allocated</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-value">{self._format_number(total_allocated)}</div>
                        <div class="summary-label">Total Qty</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-value">{products_affected}</div>
                        <div class="summary-label">Products</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-value">{customers_affected}</div>
                        <div class="summary-label">Customers</div>
                    </div>
                </div>
                
                {warnings_html}
                
                <h3>📋 Allocation Details {f'(Top 30 of {len(allocation_results)})' if len(allocation_results) > 30 else ''}</h3>
                <table>
                    <thead>
                        <tr>
                            <th>OC Number</th>
                            <th>Customer</th>
                            <th>Product</th>
                            <th style="text-align: right;">Allocated</th>
                            <th style="text-align: center;">ETD</th>
                            <th style="text-align: right;">Coverage</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
                
                <div class="footer">
                    <p>This is an automated notification from the Allocation Planning System.</p>
                    <p>Please do not reply to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Send to allocator only, CC allocation group
        return self._send_email(
            to_email=allocator_email,
            cc_emails=[self.allocation_cc],
            reply_to=allocator_email,
            subject=subject,
            html_content=html_content
        )
    
    def send_individual_creator_emails(
        self,
        commit_result: Dict,
        allocation_results: List[Dict],
        allocator_email: str,
        allocator_name: str
    ) -> Dict[str, any]:
        """
        Send individual emails to each OC creator.
        Each creator receives email containing only their OCs.
        CC: allocator + allocation@prostech.vn
        """
        result = {
            'sent_count': 0,
            'total_creators': 0,
            'errors': []
        }
        
        if not allocation_results:
            return result
        
        allocation_number = commit_result.get('allocation_number', 'N/A')
        
        # Get OC creators grouped by email
        ocd_ids = [a.get('ocd_id') for a in allocation_results if a.get('ocd_id') and float(a.get('final_qty', 0)) > 0]
        creators = self.get_oc_creators_for_allocations(ocd_ids)
        
        result['total_creators'] = len(creators)
        
        # Build lookup: ocd_id -> allocation data
        alloc_by_ocd = {a.get('ocd_id'): a for a in allocation_results}
        
        # Send email to each creator
        for creator_email, creator_info in creators.items():
            # Skip if creator is the allocator (they already got summary email)
            if creator_email == allocator_email:
                continue
            
            try:
                # Get allocations for this creator
                creator_allocations = [
                    alloc_by_ocd[ocd_id] 
                    for ocd_id in creator_info['ocd_ids'] 
                    if ocd_id in alloc_by_ocd
                ]
                
                if not creator_allocations:
                    continue
                
                success, msg = self._send_creator_notification(
                    creator_email=creator_email,
                    creator_name=creator_info['full_name'],
                    allocations=creator_allocations,
                    allocation_number=allocation_number,
                    allocator_email=allocator_email,
                    allocator_name=allocator_name
                )
                
                if success:
                    result['sent_count'] += 1
                else:
                    result['errors'].append(f"{creator_email}: {msg}")
                    
            except Exception as e:
                result['errors'].append(f"{creator_email}: {str(e)}")
        
        logger.info(f"Sent {result['sent_count']}/{result['total_creators']} individual creator emails")
        return result
    
    def _send_creator_notification(
        self,
        creator_email: str,
        creator_name: str,
        allocations: List[Dict],
        allocation_number: str,
        allocator_email: str,
        allocator_name: str
    ) -> Tuple[bool, str]:
        """Send notification to individual OC creator"""
        
        total_qty = sum(float(a.get('final_qty', 0)) for a in allocations)
        oc_count = len(allocations)
        
        subject = f"✅ [Allocation] {oc_count} of your OCs allocated - {allocation_number}"
        
        # Build table rows
        rows_html = ""
        for alloc in sorted(allocations, key=lambda x: float(x.get('final_qty', 0)), reverse=True):
            coverage = float(alloc.get('coverage_percent', 0))
            coverage_class = 'coverage-high' if coverage >= 80 else 'coverage-mid' if coverage >= 50 else 'coverage-low'
            
            # Product display
            product = alloc.get('product_display') or alloc.get('pt_code', 'N/A')
            if len(product) > 40:
                product = product[:37] + '...'
            
            # ETD display
            allocated_etd = alloc.get('allocated_etd')
            oc_etd = alloc.get('oc_etd')
            etd_display = self._format_date(allocated_etd or oc_etd)
            
            if oc_etd and allocated_etd:
                try:
                    days_diff = self._compare_dates(allocated_etd, oc_etd)
                    if days_diff > 0:
                        etd_display = f"{self._format_date(allocated_etd)} <span class='etd-delay'>(+{days_diff}d)</span>"
                except:
                    pass
            
            rows_html += f"""
            <tr>
                <td>{alloc.get('oc_number', 'N/A')}</td>
                <td>{alloc.get('customer_code', 'N/A')}</td>
                <td title="{alloc.get('product_display', '')}">{product}</td>
                <td style="text-align: right;">{self._format_number(alloc.get('final_qty', 0))}</td>
                <td style="text-align: center;">{etd_display}</td>
                <td style="text-align: right;" class="{coverage_class}">{coverage:.0f}%</td>
            </tr>
            """
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>{self._build_base_style()}</head>
        <body>
            <div class="header header-blue">
                <h1>✅ Your OCs Have Been Allocated</h1>
                <p>{allocation_number}</p>
            </div>
            
            <div class="content">
                <p>Hi <strong>{creator_name}</strong>,</p>
                <p>Your Order Confirmations have been allocated in bulk allocation <strong>{allocation_number}</strong>.</p>
                
                <div class="summary-grid">
                    <div class="summary-item">
                        <div class="summary-value">{oc_count}</div>
                        <div class="summary-label">Your OCs</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-value">{self._format_number(total_qty)}</div>
                        <div class="summary-label">Total Allocated</div>
                    </div>
                </div>
                
                <h3>📋 Allocation Details</h3>
                <table>
                    <thead>
                        <tr>
                            <th>OC Number</th>
                            <th>Customer</th>
                            <th>Product</th>
                            <th style="text-align: right;">Allocated</th>
                            <th style="text-align: center;">ETD</th>
                            <th style="text-align: right;">Coverage</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
                
                <div class="footer">
                    <p>Allocated by: <strong>{allocator_name}</strong></p>
                    <p>Date: {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                    <p style="margin-top: 10px; font-size: 11px;">
                        This is an automated notification. Reply to this email to contact the allocator.
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # CC: allocator + allocation group
        cc_emails = [self.allocation_cc]
        if allocator_email:
            cc_emails.append(allocator_email)
        
        return self._send_email(
            to_email=creator_email,
            cc_emails=cc_emails,
            reply_to=allocator_email or self.sender_email,
            subject=subject,
            html_content=html_content
        )
    
    # ============== HELPER METHODS ==============
    
    def _build_allocation_table_rows(
        self, 
        allocation_results: List[Dict], 
        split_allocations: Dict,
        max_rows: int = 30
    ) -> str:
        """Build HTML table rows for allocation details"""
        rows_html = ""
        
        # Sort by final_qty descending
        sorted_results = sorted(
            [a for a in allocation_results if float(a.get('final_qty', 0)) > 0],
            key=lambda x: float(x.get('final_qty', 0)),
            reverse=True
        )[:max_rows]
        
        for alloc in sorted_results:
            coverage = float(alloc.get('coverage_percent', 0))
            coverage_class = 'coverage-high' if coverage >= 80 else 'coverage-mid' if coverage >= 50 else 'coverage-low'
            
            # Product display (truncate if long)
            product = alloc.get('product_display') or alloc.get('pt_code', 'N/A')
            if len(product) > 45:
                product = product[:42] + '...'
            
            # ETD display with delay indicator
            allocated_etd = alloc.get('allocated_etd')
            oc_etd = alloc.get('oc_etd')
            etd_display = self._format_date(allocated_etd or oc_etd)
            
            if oc_etd and allocated_etd:
                try:
                    days_diff = self._compare_dates(allocated_etd, oc_etd)
                    if days_diff > 0:
                        etd_display = f"<span class='etd-delay'>{self._format_date(allocated_etd)} (+{days_diff}d)</span>"
                except:
                    pass
            
            # Check for splits
            ocd_id = alloc.get('ocd_id')
            split_indicator = ""
            if ocd_id and split_allocations.get(ocd_id) and len(split_allocations[ocd_id]) > 1:
                split_indicator = f" <span style='color: #666; font-size: 10px;'>({len(split_allocations[ocd_id])} splits)</span>"
            
            rows_html += f"""
            <tr>
                <td>{alloc.get('oc_number', 'N/A')}{split_indicator}</td>
                <td>{alloc.get('customer_code', 'N/A')}</td>
                <td title="{alloc.get('product_display', '')}">{product}</td>
                <td style="text-align: right; font-weight: bold;">{self._format_number(alloc.get('final_qty', 0))}</td>
                <td style="text-align: center;">{etd_display}</td>
                <td style="text-align: right;" class="{coverage_class}">{coverage:.0f}%</td>
            </tr>
            """
        
        # Show "and X more" if truncated
        remaining = len([a for a in allocation_results if float(a.get('final_qty', 0)) > 0]) - max_rows
        if remaining > 0:
            rows_html += f"""
            <tr>
                <td colspan="6" style="text-align: center; font-style: italic; background: #f9f9f9;">
                    ... and {remaining} more allocations
                </td>
            </tr>
            """
        
        return rows_html
    
    def _compare_dates(self, date1, date2) -> int:
        """Compare two dates, return difference in days (date1 - date2)"""
        try:
            from datetime import date as date_type
            
            def to_date(d):
                if d is None:
                    return None
                if isinstance(d, str):
                    return datetime.strptime(d[:10], '%Y-%m-%d').date()
                if hasattr(d, 'date') and callable(d.date):
                    return d.date()
                if isinstance(d, date_type):
                    return d
                return d
            
            d1 = to_date(date1)
            d2 = to_date(date2)
            
            if d1 and d2:
                return (d1 - d2).days
        except:
            pass
        return 0
    
    # ============== BACKWARD COMPATIBILITY ==============
    
    def get_recipients_for_scope(
        self, 
        scope: Dict, 
        creator_email: str = None,
        allocation_results: List[Dict] = None
    ) -> List[str]:
        """
        Get email recipients (backward compatible method).
        For bulk emails, use send_bulk_allocation_emails() instead.
        """
        recipients = set()
        
        if creator_email:
            recipients.add(creator_email)
        
        if self.allocation_cc:
            recipients.add(self.allocation_cc)
        
        return [email for email in recipients if email]
    
    def send_bulk_allocation_email(
        self,
        commit_result: Dict,
        allocation_results: List[Dict],
        scope: Dict,
        strategy_config: Dict,
        recipients: List[str],
        split_allocations: Dict = None
    ) -> Dict[str, any]:
        """
        Backward compatible method.
        Use send_bulk_allocation_emails() for full functionality.
        """
        # Just send summary to first recipient
        if not recipients:
            return {'success': False, 'message': 'No recipients'}
        
        success, msg = self.send_summary_email_to_allocator(
            commit_result=commit_result,
            allocation_results=allocation_results,
            scope=scope,
            strategy_config=strategy_config,
            allocator_email=recipients[0],
            allocator_name='Allocator',
            split_allocations=split_allocations or {}
        )
        
        return {'success': success, 'message': msg}