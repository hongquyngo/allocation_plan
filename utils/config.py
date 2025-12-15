# utils/config.py

import os
import json
import logging
from dotenv import load_dotenv
from typing import Dict, Any, Optional

# Initialize logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def is_running_on_streamlit_cloud() -> bool:
    """Detect if running on Streamlit Cloud"""
    try:
        import streamlit as st
        return hasattr(st, 'secrets') and "DB_CONFIG" in st.secrets
    except Exception:
        return False


class Config:
    """Centralized configuration management for iSCM Dashboard"""
    
    def __init__(self):
        self.is_cloud = is_running_on_streamlit_cloud()
        self._load_config()
        
    def _load_config(self):
        """Load configuration based on environment"""
        if self.is_cloud:
            self._load_cloud_config()
        else:
            self._load_local_config()
            
        # Common configuration
        self._load_app_config()
        
    def _load_cloud_config(self):
        """Load configuration from Streamlit Cloud secrets"""
        import streamlit as st
        
        # Database configuration
        self.db_config = dict(st.secrets["DB_CONFIG"])
        
        # API Keys
        self.api_keys = {
            "exchange_rate": st.secrets["API"]["EXCHANGE_RATE_API_KEY"]
        }
        
        # Google Cloud Service Account
        self.google_service_account = dict(st.secrets.get("gcp_service_account", {}))
        
        # Email configuration - Support multiple accounts
        email_config = st.secrets.get("EMAIL", {})
        self.email_config = {
            "inbound": {
                "sender": email_config.get("INBOUND_EMAIL_SENDER"),
                "password": email_config.get("INBOUND_EMAIL_PASSWORD")
            },
            "outbound": {
                "sender": email_config.get("OUTBOUND_EMAIL_SENDER"),
                "password": email_config.get("OUTBOUND_EMAIL_PASSWORD")
            },
            "smtp": {
                "host": email_config.get("SMTP_HOST", "smtp.gmail.com"),
                "port": int(email_config.get("SMTP_PORT", 587))
            }
        }
        
        # AWS S3 Configuration
        aws_config = st.secrets.get("AWS", {})
        self.aws_config = {
            "access_key_id": aws_config.get("ACCESS_KEY_ID"),
            "secret_access_key": aws_config.get("SECRET_ACCESS_KEY"),
            "region": aws_config.get("REGION", "ap-southeast-1"),
            "bucket_name": aws_config.get("BUCKET_NAME", "prostech-erp-dev"),
            "app_prefix": aws_config.get("APP_PREFIX", "streamlit-app")
        }
        
        logger.info("☁️  Running in STREAMLIT CLOUD")
        self._log_config_status()
        
    def _load_local_config(self):
        """Load configuration from local environment"""
        # Load .env file
        load_dotenv()
        
        # Database configuration - No hardcoding!
        self.db_config = {
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT", "3306")),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "database": os.getenv("DB_NAME", os.getenv("DB_DATABASE", "prostechvn"))
        }
        
        # Validate required DB config
        if not all([self.db_config["host"], self.db_config["user"], self.db_config["password"]]):
            raise ValueError("Missing required database configuration. Please check .env file.")
        
        # API Keys
        self.api_keys = {
            "exchange_rate": os.getenv("EXCHANGE_RATE_API_KEY")
        }
        
        # Google Cloud Service Account
        self.google_service_account = {}
        credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
        if os.path.exists(credentials_path):
            try:
                with open(credentials_path, "r") as f:
                    self.google_service_account = json.load(f)
            except Exception as e:
                logger.warning(f"Could not load Google credentials from {credentials_path}: {e}")
        
        # Email configuration - Support multiple accounts
        self.email_config = {
            "inbound": {
                "sender": os.getenv("INBOUND_EMAIL_SENDER"),
                "password": os.getenv("INBOUND_EMAIL_PASSWORD")
            },
            "outbound": {
                "sender": os.getenv("OUTBOUND_EMAIL_SENDER"),
                "password": os.getenv("OUTBOUND_EMAIL_PASSWORD")
            },
            "smtp": {
                "host": os.getenv("SMTP_HOST", "smtp.gmail.com"),
                "port": int(os.getenv("SMTP_PORT", "587"))
            }
        }
        
        # AWS S3 Configuration
        self.aws_config = {
            "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region": os.getenv("AWS_REGION", "ap-southeast-1"),
            "bucket_name": os.getenv("S3_BUCKET_NAME", "prostech-erp-dev"),
            "app_prefix": os.getenv("S3_APP_PREFIX", "streamlit-app")
        }
        
        logger.info("💻 Running in LOCAL environment")
        self._log_config_status()
        
    def _load_app_config(self):
        """Load application-specific configuration"""
        self.app_config = {
            # Session management
            "SESSION_TIMEOUT_HOURS": int(os.getenv("SESSION_TIMEOUT_HOURS", "8")),
            
            # Email settings
            "MAX_EMAIL_RECIPIENTS": int(os.getenv("MAX_EMAIL_RECIPIENTS", "50")),
            
            # Business logic
            "DELIVERY_WEEKS_AHEAD": int(os.getenv("DELIVERY_WEEKS_AHEAD", "4")),
            "PO_WEEKS_AHEAD": int(os.getenv("PO_WEEKS_AHEAD", "8")),
            
            # Performance
            "CACHE_TTL_SECONDS": int(os.getenv("CACHE_TTL_SECONDS", "300")),  # 5 minutes
            "DB_POOL_SIZE": int(os.getenv("DB_POOL_SIZE", "5")),
            "DB_POOL_RECYCLE": int(os.getenv("DB_POOL_RECYCLE", "3600")),
            
            # Localization
            "TIMEZONE": os.getenv("TIMEZONE", "Asia/Ho_Chi_Minh"),
            
            # Features
            "ENABLE_ANALYTICS": os.getenv("ENABLE_ANALYTICS", "true").lower() == "true",
            "ENABLE_EMAIL_NOTIFICATIONS": os.getenv("ENABLE_EMAIL_NOTIFICATIONS", "true").lower() == "true",
            "ENABLE_CALENDAR_INTEGRATION": os.getenv("ENABLE_CALENDAR_INTEGRATION", "true").lower() == "true",
        }
        
    def _log_config_status(self):
        """Log configuration status for debugging with detailed validation"""
        
        issues = []  # Track issues for summary
        
        # ═══════════════════════════════════════════════════════════════
        # DATABASE
        # ═══════════════════════════════════════════════════════════════
        logger.info("─" * 55)
        logger.info("📊 DATABASE CONFIGURATION")
        
        db_host = self.db_config.get('host')
        db_user = self.db_config.get('user')
        db_pass = self.db_config.get('password')
        db_name = self.db_config.get('database')
        db_port = self.db_config.get('port', 3306)
        
        if all([db_host, db_user, db_pass, db_name]):
            logger.info(f"   ✅ Host: {db_host}:{db_port}")
            logger.info(f"   ✅ Database: {db_name}")
            logger.info(f"   ✅ User: {db_user}")
            logger.info(f"   ✅ Password: {'*' * 8} (configured)")
        else:
            missing = []
            if not db_host: missing.append('host')
            if not db_user: missing.append('user')
            if not db_pass: missing.append('password')
            if not db_name: missing.append('database')
            logger.error(f"   ❌ Missing: {', '.join(missing)}")
            issues.append(f"Database: missing {', '.join(missing)}")
        
        # ═══════════════════════════════════════════════════════════════
        # EMAIL - OUTBOUND
        # ═══════════════════════════════════════════════════════════════
        logger.info("─" * 55)
        logger.info("📧 EMAIL CONFIGURATION")
        
        # Outbound (CRITICAL - used for sending)
        out_sender = self.email_config.get('outbound', {}).get('sender')
        out_pass = self.email_config.get('outbound', {}).get('password')
        
        if out_sender and out_pass:
            logger.info(f"   ✅ Outbound Sender: {out_sender}")
            logger.info(f"   ✅ Outbound Password: {'*' * 8} (configured)")
        elif out_sender and not out_pass:
            logger.warning(f"   ⚠️  Outbound Sender: {out_sender}")
            logger.error(f"   ❌ Outbound Password: MISSING - emails will FAIL!")
            issues.append("Outbound email: password missing")
        else:
            logger.error(f"   ❌ Outbound Email: Not configured")
            issues.append("Outbound email: not configured")
        
        # Inbound (optional)
        in_sender = self.email_config.get('inbound', {}).get('sender')
        in_pass = self.email_config.get('inbound', {}).get('password')
        
        if in_sender and in_pass:
            logger.info(f"   ✅ Inbound Sender: {in_sender}")
            logger.info(f"   ✅ Inbound Password: {'*' * 8} (configured)")
        elif in_sender and not in_pass:
            logger.warning(f"   ⚠️  Inbound Sender: {in_sender}")
            logger.warning(f"   ⚠️  Inbound Password: MISSING")
        else:
            logger.info(f"   ℹ️  Inbound Email: Not configured (optional)")
        
        # SMTP
        smtp_host = self.email_config.get('smtp', {}).get('host', 'smtp.gmail.com')
        smtp_port = self.email_config.get('smtp', {}).get('port', 587)
        logger.info(f"   ✅ SMTP Server: {smtp_host}:{smtp_port}")
        
        # ═══════════════════════════════════════════════════════════════
        # AWS S3
        # ═══════════════════════════════════════════════════════════════
        logger.info("─" * 55)
        logger.info("☁️  AWS S3 CONFIGURATION")
        
        aws_key = self.aws_config.get('access_key_id')
        aws_secret = self.aws_config.get('secret_access_key')
        aws_bucket = self.aws_config.get('bucket_name')
        aws_region = self.aws_config.get('region', 'ap-southeast-1')
        
        if all([aws_key, aws_secret, aws_bucket]):
            logger.info(f"   ✅ Bucket: {aws_bucket}")
            logger.info(f"   ✅ Region: {aws_region}")
            # Show partial key for verification
            key_preview = f"{aws_key[:8]}...{aws_key[-4:]}" if len(str(aws_key)) > 12 else "configured"
            logger.info(f"   ✅ Access Key: {key_preview}")
            logger.info(f"   ✅ Secret Key: {'*' * 8} (configured)")
        elif aws_bucket:
            logger.warning(f"   ⚠️  Bucket: {aws_bucket}")
            if not aws_key:
                logger.error(f"   ❌ Access Key: MISSING")
                issues.append("AWS S3: access key missing")
            if not aws_secret:
                logger.error(f"   ❌ Secret Key: MISSING")
                issues.append("AWS S3: secret key missing")
        else:
            logger.info(f"   ℹ️  AWS S3: Not configured (optional)")
        
        # ═══════════════════════════════════════════════════════════════
        # API KEYS
        # ═══════════════════════════════════════════════════════════════
        logger.info("─" * 55)
        logger.info("🔑 API KEYS")
        
        exchange_key = self.api_keys.get('exchange_rate')
        if exchange_key:
            key_preview = f"{exchange_key[:6]}...{exchange_key[-4:]}" if len(str(exchange_key)) > 10 else "configured"
            logger.info(f"   ✅ Exchange Rate API: {key_preview}")
        else:
            logger.info(f"   ℹ️  Exchange Rate API: Not configured (optional)")
        
        # ═══════════════════════════════════════════════════════════════
        # GOOGLE CLOUD
        # ═══════════════════════════════════════════════════════════════
        logger.info("─" * 55)
        logger.info("🔐 GOOGLE CLOUD")
        
        if self.google_service_account:
            gcp_project = self.google_service_account.get('project_id', 'Unknown')
            gcp_email = self.google_service_account.get('client_email', 'Unknown')
            logger.info(f"   ✅ Project: {gcp_project}")
            logger.info(f"   ✅ Service Account: {gcp_email}")
        else:
            logger.info(f"   ℹ️  Google Service Account: Not configured (optional)")
        
        # ═══════════════════════════════════════════════════════════════
        # SUMMARY
        # ═══════════════════════════════════════════════════════════════
        logger.info("─" * 55)
        if issues:
            logger.warning(f"⚠️  CONFIGURATION ISSUES FOUND ({len(issues)}):")
            for issue in issues:
                logger.warning(f"   • {issue}")
            logger.info("─" * 55)
        else:
            logger.info("✅ ALL REQUIRED CONFIGURATIONS LOADED SUCCESSFULLY")
            logger.info("─" * 55)
        
    def get_db_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return self.db_config.copy()
        
    def get_email_config(self, module: str = "outbound") -> Dict[str, Any]:
        """Get email configuration for specific module"""
        email = self.email_config.get(module, self.email_config["outbound"])
        return {
            **email,
            **self.email_config["smtp"]
        }
        
    def get_api_key(self, service: str) -> Optional[str]:
        """Get API key for specific service"""
        return self.api_keys.get(service)
        
    def get_google_service_account(self) -> Dict[str, Any]:
        """Get Google service account configuration"""
        return self.google_service_account.copy()
        
    def get_aws_config(self) -> Dict[str, Any]:
        """Get AWS configuration"""
        return self.aws_config.copy()
        
    def get_app_setting(self, key: str, default: Any = None) -> Any:
        """Get application setting"""
        return self.app_config.get(key, default)
        
    def is_feature_enabled(self, feature: str) -> bool:
        """Check if a feature is enabled"""
        return self.app_config.get(f"ENABLE_{feature.upper()}", True)


# Create singleton instance
config = Config()

# Export commonly used values for backward compatibility
IS_RUNNING_ON_CLOUD = config.is_cloud
DB_CONFIG = config.db_config
AWS_CONFIG = config.aws_config
EXCHANGE_RATE_API_KEY = config.api_keys.get("exchange_rate")
GOOGLE_SERVICE_ACCOUNT_JSON = config.google_service_account
APP_CONFIG = config.app_config

# Module-specific email configs
INBOUND_EMAIL_CONFIG = config.get_email_config("inbound")
OUTBOUND_EMAIL_CONFIG = config.get_email_config("outbound")

# For backward compatibility - single email config
EMAIL_SENDER = config.email_config.get("outbound", {}).get("sender")
EMAIL_PASSWORD = config.email_config.get("outbound", {}).get("password")


# Export all
__all__ = [
    'config',
    'Config',
    'IS_RUNNING_ON_CLOUD',
    'DB_CONFIG',
    'AWS_CONFIG',
    'EXCHANGE_RATE_API_KEY',
    'GOOGLE_SERVICE_ACCOUNT_JSON',
    'APP_CONFIG',
    'EMAIL_SENDER',
    'EMAIL_PASSWORD',
    'INBOUND_EMAIL_CONFIG',
    'OUTBOUND_EMAIL_CONFIG'
]