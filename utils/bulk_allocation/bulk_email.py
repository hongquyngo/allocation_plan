"""
Bulk Allocation Email Service
=============================
Email notifications for bulk allocation operations.

REFACTORED: 2024-12 - Simplified OC creator lookup using data from 
                      outbound_oc_pending_delivery_view (oc_creator_email, oc_creator_name)
                      instead of separate database queries.

BUGFIX: 2024-12 - Split allocations now expanded into multiple rows in emails.
                  Previously only showed "(X splits)" indicator with single row.
                  Now each split gets its own row with specific qty and ETD.
                  Fixed in: _build_allocation_table_rows(), send_individual_email_to_creator()

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
from typing import Dict, List, Tuple, Optional
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
    
    # ============================================================
    # NEW: Simplified OC Creator Grouping (No DB Query Needed)
    # ============================================================
    
    def group_allocations_by_creator(
        self, 
        allocation_results: List[Dict],
        demands_dict: Dict[int, Dict]
    ) -> Dict[str, Dict]:
        """
        Group allocations by OC creator email using data already in demands_dict.
        
        NO DATABASE QUERY NEEDED - uses oc_creator_email, oc_creator_name 
        from outbound_oc_pending_delivery_view via demands_dict.
        
        Args:
            allocation_results: List of allocation results with ocd_id
            demands_dict: Dict mapping ocd_id -> demand info (includes creator fields)
        
        Returns:
            Dict[email] = {
                'full_name': str,
                'allocations': List[Dict]  # List of allocation results for this creator
            }
        """
        creators = {}
        skipped_no_email = 0
        
        for alloc in allocation_results:
            # Skip zero allocations
            if float(alloc.get('final_qty', 0)) <= 0:
                continue
            
            ocd_id = int(alloc.get('ocd_id', 0))
            oc_info = demands_dict.get(ocd_id, {})
            
            # Get creator info directly from view data
            creator_email = oc_info.get('oc_creator_email')
            creator_name = oc_info.get('oc_creator_name') or 'Sales'
            
            # Skip if no email
            if not creator_email or not creator_email.strip():
                skipped_no_email += 1
                logger.debug(f"OC {oc_info.get('oc_number', ocd_id)} has no creator email")
                continue
            
            creator_email = creator_email.strip().lower()
            
            # Group by email
            if creator_email not in creators:
                creators[creator_email] = {
                    'full_name': creator_name.strip(),
                    'allocations': []
                }
            
            # Add allocation with enriched OC info
            creators[creator_email]['allocations'].append({
                **alloc,
                'oc_number': oc_info.get('oc_number', ''),
                'customer_code': oc_info.get('customer_code', ''),
                'customer': oc_info.get('customer', ''),
                'product_display': alloc.get('product_display') or oc_info.get('product_display', ''),
                'pt_code': oc_info.get('pt_code', ''),
                'oc_etd': oc_info.get('etd'),
            })
        
        if skipped_no_email > 0:
            logger.info(f"Skipped {skipped_no_email} allocations without creator email")
        
        logger.info(
            f"Grouped {sum(len(c['allocations']) for c in creators.values())} allocations "
            f"into {len(creators)} unique OC creators"
        )
        
        return creators
    
    # ============================================================
    # User Info (for allocator only)
    # ============================================================
    
    def get_user_info(self, user_id: int) -> Optional[Dict]:
        """Get allocator user information"""
        try:
            engine = get_db_engine()
            query = text("""
                SELECT 
                    u.id,
                    u.username,
                    e.email,
                    TRIM(CONCAT(COALESCE(e.first_name, u.username), ' ', COALESCE(e.last_name, ''))) AS full_name
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
    
    # ============================================================
    # Main Email Orchestration (UPDATED)
    # ============================================================
    
    def send_bulk_allocation_emails(
        self,
        commit_result: Dict,
        allocation_results: List[Dict],
        scope: Dict,
        strategy_config: Dict,
        allocator_user_id: int,
        demands_dict: Dict[int, Dict] = None,  # NEW: Required for creator emails
        split_allocations: Dict = None
    ) -> Dict:
        """
        Main method to send all bulk allocation emails.
        
        UPDATED: Uses OC creator info from demands_dict (via view) instead of 
                 separate database queries.
        
        Args:
            commit_result: Result from commit operation
            allocation_results: List of allocation results
            scope: Allocation scope
            strategy_config: Strategy configuration
            allocator_user_id: ID of user performing allocation
            demands_dict: Dict mapping ocd_id -> demand info (includes oc_creator_email, oc_creator_name)
            split_allocations: Split allocation data
        
        Returns:
            Dict with success status and details
        """
        split_allocations = split_allocations or {}
        demands_dict = demands_dict or {}
        
        result = {
            'success': False,
            'summary_sent': False,
            'individual_sent': 0,
            'individual_total': 0,
            'errors': []
        }
        
        # Get allocator info
        allocator_info = self.get_user_info(allocator_user_id)
        if not allocator_info:
            result['errors'].append(f"Could not find allocator user ID {allocator_user_id}")
            allocator_email = None
            allocator_name = "System"
        else:
            allocator_email = allocator_info.get('email')
            allocator_name = allocator_info.get('full_name', allocator_info.get('username', 'Allocator'))
        
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
                    split_allocations=split_allocations
                )
                result['summary_sent'] = success
                if not success:
                    result['errors'].append(f"Summary email failed: {msg}")
            except Exception as e:
                logger.error(f"Error sending summary email: {e}", exc_info=True)
                result['errors'].append(f"Summary email error: {str(e)}")
        else:
            result['errors'].append("No allocator email - skipping summary email")
        
        # 2. Send individual emails to OC creators (SIMPLIFIED)
        if demands_dict:
            try:
                # NEW: Use group_allocations_by_creator instead of DB query
                creators = self.group_allocations_by_creator(allocation_results, demands_dict)
                result['individual_total'] = len(creators)
                
                for creator_email, creator_data in creators.items():
                    # Skip if creator is the allocator (they already got summary email)
                    if allocator_email and creator_email.lower() == allocator_email.lower():
                        logger.debug(f"Skipping creator {creator_email} (same as allocator)")
                        result['individual_total'] -= 1
                        continue
                    
                    try:
                        success, msg = self.send_individual_email_to_creator(
                            creator_email=creator_email,
                            creator_name=creator_data['full_name'],
                            creator_allocations=creator_data['allocations'],
                            commit_result=commit_result,
                            allocator_email=allocator_email,
                            allocator_name=allocator_name,
                            split_allocations=split_allocations
                        )
                        if success:
                            result['individual_sent'] += 1
                        else:
                            result['errors'].append(f"Email to {creator_email} failed: {msg}")
                    except Exception as e:
                        logger.error(f"Error sending email to {creator_email}: {e}")
                        result['errors'].append(f"Email to {creator_email} error: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error processing individual emails: {e}", exc_info=True)
                result['errors'].append(f"Individual emails error: {str(e)}")
        else:
            logger.warning("No demands_dict provided - skipping individual creator emails")
            result['errors'].append("No demands data provided for individual emails")
        
        # Overall success
        result['success'] = result['summary_sent'] or result['individual_sent'] > 0
        
        logger.info(
            f"Email results: summary={'sent' if result['summary_sent'] else 'failed'}, "
            f"individual={result['individual_sent']}/{result['individual_total']}"
        )
        
        return result
    
    # ============================================================
    # Email Sending Core
    # ============================================================
    
    def _send_email(self, to_email: str, cc_emails: List[str], reply_to: str,
                    subject: str, html_content: str) -> Tuple[bool, str]:
        """Send email using SMTP"""
        try:
            if not self.sender_email or not self.sender_password:
                return False, "Email configuration missing (SMTP credentials)"
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.sender_email
            msg['To'] = to_email
            msg['Reply-To'] = reply_to or self.sender_email
            
            if cc_emails:
                valid_cc = [cc for cc in cc_emails if cc and cc.strip()]
                if valid_cc:
                    msg['Cc'] = ', '.join(valid_cc)
            
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(html_part)
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                
                recipients = [to_email]
                if cc_emails:
                    recipients.extend([cc for cc in cc_emails if cc and cc.strip()])
                
                server.sendmail(self.sender_email, recipients, msg.as_string())
            
            logger.info(f"Email sent successfully to {to_email}")
            return True, "Email sent successfully"
            
        except smtplib.SMTPAuthenticationError:
            logger.error("SMTP authentication failed")
            return False, "Email authentication failed"
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error: {e}")
            return False, f"SMTP error: {str(e)}"
        except Exception as e:
            logger.error(f"Error sending email to {to_email}: {e}")
            return False, str(e)
    
    # ============================================================
    # Email Templates & Helpers
    # ============================================================
    
    def _format_number(self, value) -> str:
        try:
            return "{:,.0f}".format(float(value))
        except:
            return str(value)
    
    def _format_date(self, date_value) -> str:
        try:
            if date_value is None:
                return 'N/A'
            if isinstance(date_value, str):
                date_value = datetime.strptime(date_value[:10], '%Y-%m-%d')
            return date_value.strftime('%d %b %Y')
        except:
            return str(date_value) if date_value else 'N/A'
    
    def _compare_dates(self, date1, date2) -> int:
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
    
    def _build_base_style(self) -> str:
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
            .summary-grid { display: flex; justify-content: space-around; margin: 20px 0; flex-wrap: wrap; }
            .summary-item { text-align: center; padding: 15px; background: #fff; border-radius: 8px; min-width: 100px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin: 5px; }
            .summary-value { font-size: 28px; font-weight: bold; color: #1976d2; }
            .summary-label { font-size: 11px; color: #666; text-transform: uppercase; }
            table { width: 100%; border-collapse: collapse; margin: 15px 0; background: #fff; }
            th { background-color: #1976d2; color: white; padding: 10px 8px; text-align: left; font-size: 12px; }
            td { padding: 8px; border-bottom: 1px solid #eee; font-size: 13px; }
            tr:hover { background-color: #f5f5f5; }
            .coverage-high { color: #2e7d32; font-weight: bold; }
            .coverage-mid { color: #f57c00; font-weight: bold; }
            .coverage-low { color: #c62828; font-weight: bold; }
            .etd-delay { color: #c62828; font-size: 11px; }
            .badge { display: inline-block; padding: 3px 8px; border-radius: 12px; font-size: 11px; }
            .badge-strategy { background-color: #e3f2fd; color: #1976d2; }
            .badge-mode { background-color: #f3e5f5; color: #7b1fa2; }
            .warning-box { background: #fff3e0; border-left: 4px solid #ff9800; padding: 12px; margin: 15px 0; border-radius: 0 8px 8px 0; }
            .footer { margin-top: 20px; padding: 20px; background-color: #f0f0f0; text-align: center; font-size: 12px; color: #666; border-radius: 0 0 8px 8px; }
        </style>
        """
    
    def _build_allocation_table_rows(self, allocation_results: List[Dict], split_allocations: Dict, max_rows: int = 30) -> str:
        """
        Build HTML table rows for allocation results.
        
        FIXED 2024-12: Expand split allocations into multiple rows instead of just showing indicator.
        Each split entry now gets its own row with specific qty and ETD.
        """
        rows_html = ""
        
        # Expand split allocations into separate rows
        expanded_results = []
        for alloc in allocation_results:
            if float(alloc.get('final_qty', 0)) <= 0:
                continue
            
            ocd_id = alloc.get('ocd_id')
            splits = split_allocations.get(ocd_id, []) if split_allocations else []
            
            if splits and len(splits) > 1:
                # Expand into multiple rows - one per split
                for idx, split in enumerate(splits):
                    split_qty = float(split.get('qty', 0))
                    if split_qty > 0:
                        demand_qty = float(alloc.get('demand_qty', 0))
                        expanded_results.append({
                            **alloc,
                            'final_qty': split_qty,
                            'allocated_etd': split.get('etd'),
                            'split_info': f" (split {idx+1}/{len(splits)})",
                            'coverage_percent': (split_qty / demand_qty * 100) if demand_qty > 0 else 0
                        })
            else:
                # Single allocation - no split
                expanded_results.append({**alloc, 'split_info': ''})
        
        # Sort by qty descending and limit
        sorted_results = sorted(expanded_results, key=lambda x: float(x.get('final_qty', 0)), reverse=True)[:max_rows]
        
        for alloc in sorted_results:
            coverage = float(alloc.get('coverage_percent', 0))
            coverage_class = 'coverage-high' if coverage >= 80 else 'coverage-mid' if coverage >= 50 else 'coverage-low'
            
            product = alloc.get('product_display') or alloc.get('pt_code', 'N/A')
            if len(product) > 45:
                product = product[:42] + '...'
            
            allocated_etd = alloc.get('allocated_etd')
            oc_etd = alloc.get('oc_etd')
            etd_display = self._format_date(allocated_etd or oc_etd)
            
            if oc_etd and allocated_etd:
                days_diff = self._compare_dates(allocated_etd, oc_etd)
                if days_diff > 0:
                    etd_display = f"{self._format_date(allocated_etd)} <span class='etd-delay'>(+{days_diff}d)</span>"
            
            # Split indicator from expanded data
            split_indicator = alloc.get('split_info', '')
            
            # Customer display with name (consistent with individual email)
            customer_display = alloc.get('customer_code', '')
            if alloc.get('customer'):
                cust_name = alloc['customer'][:15] + '...' if len(alloc.get('customer', '')) > 15 else alloc.get('customer', '')
                customer_display = f"{customer_display} - {cust_name}"
            
            rows_html += f"""
            <tr>
                <td>{alloc.get('oc_number', 'N/A')}{split_indicator}</td>
                <td>{customer_display}</td>
                <td title="{alloc.get('product_display', '')}">{product}</td>
                <td style="text-align: right; font-weight: bold;">{self._format_number(alloc.get('final_qty', 0))}</td>
                <td style="text-align: center;">{etd_display}</td>
                <td style="text-align: right;" class="{coverage_class}">{coverage:.0f}%</td>
            </tr>
            """
        
        remaining = len(expanded_results) - max_rows
        if remaining > 0:
            rows_html += f"""
            <tr>
                <td colspan="6" style="text-align: center; font-style: italic; background: #f9f9f9;">
                    ... and {remaining} more allocations
                </td>
            </tr>
            """
        
        return rows_html
    
    # ============================================================
    # Summary Email to Allocator
    # ============================================================
    
    def send_summary_email_to_allocator(
        self, commit_result: Dict, allocation_results: List[Dict], scope: Dict,
        strategy_config: Dict, allocator_email: str, allocator_name: str, split_allocations: Dict
    ) -> Tuple[bool, str]:
        
        if not allocator_email:
            return False, "No allocator email provided"
        
        allocation_number = commit_result.get('allocation_number', 'N/A')
        total_allocated = commit_result.get('total_allocated', 0)
        detail_count = commit_result.get('detail_count', 0)
        products_affected = commit_result.get('products_affected', 0)
        customers_affected = commit_result.get('customers_affected', 0)
        
        scope_parts = []
        if scope.get('brand_ids'):
            scope_parts.append(f"{len(scope['brand_ids'])} Brand(s)")
        if scope.get('customer_codes'):
            scope_parts.append(f"{len(scope['customer_codes'])} Customer(s)")
        if scope.get('legal_entities'):
            scope_parts.append(f"{len(scope['legal_entities'])} Legal Entity(s)")
        if scope.get('etd_from') or scope.get('etd_to'):
            etd_from = self._format_date(scope.get('etd_from')) if scope.get('etd_from') else 'Any'
            etd_to = self._format_date(scope.get('etd_to')) if scope.get('etd_to') else 'Any'
            scope_parts.append(f"ETD: {etd_from} → {etd_to}")
        scope_summary = ' | '.join(scope_parts) if scope_parts else 'All'
        
        strategy_type = strategy_config.get('strategy_type', 'HYBRID')
        allocation_mode = strategy_config.get('allocation_mode', 'SOFT')
        
        etd_delay_count = 0
        split_count = len([k for k, v in split_allocations.items() if len(v) > 1])
        
        for alloc in allocation_results:
            oc_etd = alloc.get('oc_etd')
            allocated_etd = alloc.get('allocated_etd')
            if oc_etd and allocated_etd:
                if self._compare_dates(allocated_etd, oc_etd) > 0:
                    etd_delay_count += 1
        
        rows_html = self._build_allocation_table_rows(allocation_results, split_allocations, max_rows=50)
        
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
        
        subject = f"✅ Bulk Allocation {allocation_number} - {detail_count} OCs Allocated"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>{self._build_base_style()}</head>
        <body>
            <div class="header header-green">
                <h1>✅ Bulk Allocation Complete</h1>
                <p>Allocation Number: <strong>{allocation_number}</strong></p>
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
                
                {warnings_html}
                
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
                </div>
            </div>
        </body>
        </html>
        """
        
        cc_emails = [self.allocation_cc] if self.allocation_cc else []
        
        return self._send_email(
            to_email=allocator_email,
            cc_emails=cc_emails,
            reply_to=allocator_email,
            subject=subject,
            html_content=html_content
        )
    
    # ============================================================
    # Individual Email to OC Creator
    # ============================================================
    
    def send_individual_email_to_creator(
        self, creator_email: str, creator_name: str, creator_allocations: List[Dict],
        commit_result: Dict, allocator_email: str, allocator_name: str, split_allocations: Dict = None
    ) -> Tuple[bool, str]:
        """
        Send individual email to OC creator with their allocated OCs.
        
        FIXED 2024-12: Expand split allocations into multiple rows.
        """
        if not creator_email:
            return False, "No creator email provided"
        
        split_allocations = split_allocations or {}
        
        allocation_number = commit_result.get('allocation_number', 'N/A')
        
        # Expand split allocations into separate entries
        expanded_allocations = []
        for alloc in creator_allocations:
            if float(alloc.get('final_qty', 0)) <= 0:
                continue
            
            ocd_id = alloc.get('ocd_id')
            splits = split_allocations.get(ocd_id, []) if split_allocations else []
            
            if splits and len(splits) > 1:
                # Expand into multiple entries
                for idx, split in enumerate(splits):
                    split_qty = float(split.get('qty', 0))
                    if split_qty > 0:
                        demand_qty = float(alloc.get('demand_qty', 0))
                        expanded_allocations.append({
                            **alloc,
                            'final_qty': split_qty,
                            'allocated_etd': split.get('etd'),
                            'split_info': f" (split {idx+1}/{len(splits)})",
                            'coverage_percent': (split_qty / demand_qty * 100) if demand_qty > 0 else 0
                        })
            else:
                expanded_allocations.append({**alloc, 'split_info': ''})
        
        oc_count = len(set(a.get('ocd_id') for a in expanded_allocations))  # Unique OCs
        total_qty = sum(float(a.get('final_qty', 0)) for a in expanded_allocations)
        
        rows_html = ""
        for alloc in sorted(expanded_allocations, key=lambda x: float(x.get('final_qty', 0)), reverse=True):
            coverage = float(alloc.get('coverage_percent', 0))
            coverage_class = 'coverage-high' if coverage >= 80 else 'coverage-mid' if coverage >= 50 else 'coverage-low'
            
            product = alloc.get('product_display') or alloc.get('pt_code', 'N/A')
            if len(product) > 40:
                product = product[:37] + '...'
            
            allocated_etd = alloc.get('allocated_etd') or alloc.get('oc_etd')
            etd_display = self._format_date(allocated_etd)
            
            oc_etd = alloc.get('oc_etd')
            if oc_etd and allocated_etd:
                days_diff = self._compare_dates(allocated_etd, oc_etd)
                if days_diff > 0:
                    etd_display = f"{self._format_date(allocated_etd)} <span class='etd-delay'>(+{days_diff}d)</span>"
            
            # Split indicator from expanded data
            split_indicator = alloc.get('split_info', '')
            
            customer_display = alloc.get('customer_code', '')
            if alloc.get('customer'):
                cust_name = alloc['customer'][:15] + '...' if len(alloc.get('customer', '')) > 15 else alloc.get('customer', '')
                customer_display = f"{customer_display} - {cust_name}"
            
            rows_html += f"""
            <tr>
                <td>{alloc.get('oc_number', 'N/A')}{split_indicator}</td>
                <td>{customer_display}</td>
                <td title="{alloc.get('product_display', '')}">{product}</td>
                <td style="text-align: right; font-weight: bold;">{self._format_number(alloc.get('final_qty', 0))}</td>
                <td style="text-align: center;">{etd_display}</td>
                <td style="text-align: right;" class="{coverage_class}">{coverage:.0f}%</td>
            </tr>
            """
        
        subject = f"📦 Allocation {allocation_number} - {oc_count} of Your OCs Allocated"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>{self._build_base_style()}</head>
        <body>
            <div class="header header-blue">
                <h1>📦 Your OCs Have Been Allocated</h1>
                <p>Allocation Number: <strong>{allocation_number}</strong></p>
            </div>
            
            <div class="content">
                <p>Hi <strong>{creator_name}</strong>,</p>
                <p>The following OCs that you created have been allocated inventory:</p>
                
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
        
        cc_emails = [self.allocation_cc] if self.allocation_cc else []
        if allocator_email:
            cc_emails.append(allocator_email)
        
        return self._send_email(
            to_email=creator_email,
            cc_emails=cc_emails,
            reply_to=allocator_email or self.sender_email,
            subject=subject,
            html_content=html_content
        )
    
    # ============================================================
    # DEPRECATED Methods (kept for backward compatibility)
    # ============================================================
    
    def get_oc_creators_for_allocations(self, ocd_ids: List[int]) -> Dict[str, Dict]:
        """DEPRECATED: Use group_allocations_by_creator() instead."""
        logger.warning("get_oc_creators_for_allocations() is DEPRECATED.")
        creators = {}
        if not ocd_ids:
            return creators
        try:
            engine = get_db_engine()
            query = text("""
                SELECT ocd.id AS ocd_id, e.email,
                       TRIM(CONCAT(COALESCE(e.first_name, ''), ' ', COALESCE(e.last_name, ''))) AS full_name
                FROM order_comfirmation_details ocd
                INNER JOIN order_confirmations oc ON ocd.order_confirmation_id = oc.id
                INNER JOIN employees e ON oc.created_by = e.keycloak_id
                WHERE ocd.id IN :ocd_ids AND e.email IS NOT NULL AND e.email != '' AND e.delete_flag = 0
            """)
            with engine.connect() as conn:
                result = conn.execute(query, {'ocd_ids': tuple(ocd_ids)})
                for row in result:
                    email = row.email.strip().lower() if row.email else None
                    if not email:
                        continue
                    if email not in creators:
                        creators[email] = {'full_name': row.full_name or 'Sales', 'ocd_ids': []}
                    creators[email]['ocd_ids'].append(row.ocd_id)
        except Exception as e:
            logger.error(f"Error getting OC creators: {e}")
        return creators
    
    def send_individual_creator_emails(self, commit_result: Dict, allocation_results: List[Dict],
                                       allocator_email: str, allocator_name: str) -> Dict:
        """DEPRECATED: Individual emails are now handled in send_bulk_allocation_emails()."""
        logger.warning("send_individual_creator_emails() is DEPRECATED.")
        return {'sent_count': 0, 'total_creators': 0, 'errors': ['Method deprecated']}