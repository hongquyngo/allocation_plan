"""
Bulk Allocation Email Service
=============================
Email notification service for bulk allocation operations.
Sends summary email after bulk allocation commit.
"""
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Any, Optional
from datetime import datetime
import os

from ..config import config

logger = logging.getLogger(__name__)


class BulkEmailService:
    """Email service for bulk allocation notifications"""
    
    def __init__(self):
        # SMTP Configuration from environment
        self.smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_user = os.getenv('SMTP_USER', '')
        self.smtp_password = os.getenv('SMTP_PASSWORD', '')
        self.from_email = os.getenv('FROM_EMAIL', self.smtp_user)
        self.cc_email = os.getenv('ALLOCATION_CC_EMAIL', '')
        
        # Check if email is configured
        self.is_configured = bool(self.smtp_user and self.smtp_password)
    
    def send_bulk_allocation_email(self, 
                                   commit_result: Dict,
                                   allocation_results: List[Dict],
                                   scope: Dict,
                                   strategy_config: Dict,
                                   recipients: List[str]) -> Dict[str, Any]:
        """
        Send summary email after bulk allocation
        
        Args:
            commit_result: Result from BulkAllocationService.commit_bulk_allocation()
            allocation_results: List of allocation results
            scope: Scope configuration
            strategy_config: Strategy configuration
            recipients: List of email addresses
        
        Returns:
            Dict with success, message
        """
        if not self.is_configured:
            logger.warning("Email not configured, skipping notification")
            return {'success': False, 'message': 'Email not configured'}
        
        if not recipients:
            logger.warning("No recipients provided for bulk allocation email")
            return {'success': False, 'message': 'No recipients'}
        
        try:
            # Build email content
            subject = self._build_subject(commit_result)
            html_body = self._build_html_body(commit_result, allocation_results, scope, strategy_config)
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = ', '.join(recipients)
            
            if self.cc_email:
                msg['Cc'] = self.cc_email
                all_recipients = recipients + [self.cc_email]
            else:
                all_recipients = recipients
            
            # Attach HTML body
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.from_email, all_recipients, msg.as_string())
            
            logger.info(f"Bulk allocation email sent to {len(recipients)} recipients")
            return {'success': True, 'message': 'Email sent successfully'}
            
        except Exception as e:
            logger.error(f"Error sending bulk allocation email: {e}")
            return {'success': False, 'message': str(e)}
    
    def _build_subject(self, commit_result: Dict) -> str:
        """Build email subject line"""
        allocation_number = commit_result.get('allocation_number', 'N/A')
        detail_count = commit_result.get('detail_count', 0)
        return f"[Bulk Allocation] {allocation_number} - {detail_count} OCs Allocated"
    
    def _build_html_body(self, commit_result: Dict, allocation_results: List[Dict],
                         scope: Dict, strategy_config: Dict) -> str:
        """Build HTML email body"""
        
        allocation_number = commit_result.get('allocation_number', 'N/A')
        creator = commit_result.get('creator_username', 'Unknown')
        detail_count = commit_result.get('detail_count', 0)
        total_allocated = commit_result.get('total_allocated', 0)
        products_affected = commit_result.get('products_affected', 0)
        customers_affected = commit_result.get('customers_affected', 0)
        
        # Build scope summary
        scope_items = []
        if scope.get('brand_ids'):
            scope_items.append(f"Brands: {len(scope['brand_ids'])} selected")
        if scope.get('customer_codes'):
            scope_items.append(f"Customers: {len(scope['customer_codes'])} selected")
        if scope.get('legal_entities'):
            scope_items.append(f"Legal Entities: {len(scope['legal_entities'])} selected")
        if scope.get('etd_from') or scope.get('etd_to'):
            etd_range = f"{scope.get('etd_from', 'Any')} to {scope.get('etd_to', 'Any')}"
            scope_items.append(f"ETD Range: {etd_range}")
        
        scope_summary = '<br>'.join(scope_items) if scope_items else 'All'
        
        # Strategy summary
        strategy_type = strategy_config.get('strategy_type', 'HYBRID')
        allocation_mode = strategy_config.get('allocation_mode', 'SOFT')
        
        # Build allocation details table (top 20)
        rows_html = ""
        sorted_results = sorted(
            [a for a in allocation_results if float(a.get('final_qty', 0)) > 0],
            key=lambda x: float(x.get('final_qty', 0)),
            reverse=True
        )[:20]
        
        for alloc in sorted_results:
            coverage = float(alloc.get('coverage_percent', 0))
            coverage_color = '#28a745' if coverage >= 80 else '#ffc107' if coverage >= 50 else '#dc3545'
            
            rows_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #ddd;">{alloc.get('oc_number', alloc.get('ocd_id', 'N/A'))}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd;">{alloc.get('customer_code', 'N/A')}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd;">{alloc.get('pt_code', 'N/A')}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: right;">{float(alloc.get('demand_qty', 0)):,.0f}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: right;">{float(alloc.get('final_qty', 0)):,.0f}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: right; color: {coverage_color};">{coverage:.0f}%</td>
            </tr>
            """
        
        if len(allocation_results) > 20:
            rows_html += f"""
            <tr>
                <td colspan="6" style="padding: 8px; text-align: center; font-style: italic;">
                    ... and {len(allocation_results) - 20} more allocations
                </td>
            </tr>
            """
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: #2563eb; color: white; padding: 20px; border-radius: 8px 8px 0 0; }}
                .content {{ background-color: #f8f9fa; padding: 20px; border: 1px solid #ddd; }}
                .info-box {{ background-color: white; padding: 15px; margin: 10px 0; border-radius: 4px; border-left: 4px solid #2563eb; }}
                .summary-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin: 20px 0; }}
                .summary-item {{ background: white; padding: 15px; border-radius: 4px; text-align: center; }}
                .summary-value {{ font-size: 24px; font-weight: bold; color: #2563eb; }}
                .summary-label {{ font-size: 12px; color: #666; }}
                table {{ width: 100%; border-collapse: collapse; background: white; }}
                th {{ background-color: #2563eb; color: white; padding: 10px; text-align: left; }}
                .footer {{ text-align: center; padding: 15px; color: #666; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2 style="margin: 0;">📦 Bulk Allocation Completed</h2>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">{allocation_number}</p>
                </div>
                
                <div class="content">
                    <div class="info-box">
                        <strong>Created by:</strong> {creator}<br>
                        <strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                        <strong>Strategy:</strong> {strategy_type} ({allocation_mode} mode)
                    </div>
                    
                    <div class="summary-grid">
                        <div class="summary-item">
                            <div class="summary-value">{detail_count}</div>
                            <div class="summary-label">OCs Allocated</div>
                        </div>
                        <div class="summary-item">
                            <div class="summary-value">{total_allocated:,.0f}</div>
                            <div class="summary-label">Total Quantity</div>
                        </div>
                        <div class="summary-item">
                            <div class="summary-value">{products_affected}</div>
                            <div class="summary-label">Products</div>
                        </div>
                    </div>
                    
                    <div class="info-box">
                        <strong>Scope:</strong><br>
                        {scope_summary}
                    </div>
                    
                    <h3>Allocation Details (Top 20)</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>OC Number</th>
                                <th>Customer</th>
                                <th>Product</th>
                                <th style="text-align: right;">Demand</th>
                                <th style="text-align: right;">Allocated</th>
                                <th style="text-align: right;">Coverage</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows_html}
                        </tbody>
                    </table>
                </div>
                
                <div class="footer">
                    This is an automated notification from the Allocation System.<br>
                    Please do not reply to this email.
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def get_recipients_for_scope(self, scope: Dict, creator_email: str = None) -> List[str]:
        """
        Get email recipients based on scope
        
        For bulk allocation, typically sends to:
        - Creator
        - Allocation CC email
        - Optionally: Sales managers for affected customers
        """
        recipients = []
        
        # Add creator
        if creator_email:
            recipients.append(creator_email)
        
        # Add CC email
        if self.cc_email:
            recipients.append(self.cc_email)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_recipients = []
        for email in recipients:
            if email and email not in seen:
                seen.add(email)
                unique_recipients.append(email)
        
        return unique_recipients
