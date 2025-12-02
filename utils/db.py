# utils/db.py
"""
Database connection management with singleton pattern

Version: 1.1.0
Changes:
- Added singleton pattern to reuse engine instead of creating new one each time
- Added connection pool configuration from APP_CONFIG
- Added connection health check
- Added auto-reconnect with pool_pre_ping
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError, DatabaseError
from urllib.parse import quote_plus
import logging
import threading
from typing import Tuple, Optional

from .config import DB_CONFIG, APP_CONFIG

logger = logging.getLogger(__name__)

# Singleton engine instance
_engine = None
_engine_lock = threading.Lock()


def get_db_engine():
    """
    Create and return SQLAlchemy database engine (singleton pattern)
    
    Returns the same engine instance across all calls to avoid
    creating multiple connections and exhausting the connection pool.
    """
    global _engine
    
    # Double-checked locking pattern for thread safety
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                logger.info("🔌 Creating database engine (singleton)...")
                
                user = DB_CONFIG["user"]
                password = quote_plus(str(DB_CONFIG["password"]))
                host = DB_CONFIG["host"]
                port = DB_CONFIG["port"]
                database = DB_CONFIG["database"]
                
                url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
                logger.info(f"🔐 SQLAlchemy URL: mysql+pymysql://{user}:***@{host}:{port}/{database}")
                
                # Get pool settings from APP_CONFIG
                pool_size = APP_CONFIG.get("DB_POOL_SIZE", 5)
                pool_recycle = APP_CONFIG.get("DB_POOL_RECYCLE", 3600)
                
                _engine = create_engine(
                    url,
                    poolclass=QueuePool,
                    pool_size=pool_size,        # Number of connections to keep open
                    max_overflow=10,            # Additional connections when pool is full
                    pool_timeout=30,            # Seconds to wait for available connection
                    pool_recycle=pool_recycle,  # Recycle connections after N seconds
                    pool_pre_ping=True,         # Test connection before using (auto-reconnect)
                    echo=False                  # Set to True for SQL debugging
                )
                
                logger.info(f"✅ Database engine created (pool_size={pool_size}, recycle={pool_recycle}s)")
    
    return _engine


def check_db_connection() -> Tuple[bool, Optional[str]]:
    """
    Check if database connection is healthy
    
    Returns:
        Tuple of (is_connected: bool, error_message: str or None)
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except OperationalError as e:
        error_msg = "Cannot connect to database. Please check your network/VPN connection."
        logger.error(f"❌ Database connection failed: {e}")
        return False, error_msg
    except Exception as e:
        error_msg = f"Database error: {str(e)}"
        logger.error(f"❌ Database error: {e}")
        return False, error_msg


def reset_db_engine():
    """
    Reset the database engine (useful for reconnection after errors)
    
    Call this if you need to force a new connection after persistent errors.
    """
    global _engine
    
    with _engine_lock:
        if _engine is not None:
            try:
                _engine.dispose()
                logger.info("🔄 Database engine disposed")
            except Exception as e:
                logger.error(f"Error disposing engine: {e}")
            _engine = None
    
    logger.info("🔄 Database engine reset - will reconnect on next query")


def get_connection_pool_status() -> dict:
    """
    Get current connection pool status for debugging
    
    Returns:
        Dictionary with pool statistics
    """
    if _engine is None:
        return {"status": "not_initialized"}
    
    try:
        pool = _engine.pool
        return {
            "status": "active",
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalidatedcount() if hasattr(pool, 'invalidatedcount') else 'N/A'
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}