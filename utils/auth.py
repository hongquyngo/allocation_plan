# utils/auth.py

import streamlit as st
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import logging
from sqlalchemy import text
from .db import get_db_engine

logger = logging.getLogger(__name__)

class AuthManager:
    """Authentication manager for SCM app with consistent session state management"""
    
    def __init__(self):
        self.session_timeout = timedelta(hours=8)
    
    def hash_password(self, password: str, salt: str = None) -> Tuple[str, str]:
        """Hash password with salt - same as user management app"""
        if not salt:
            salt = secrets.token_hex(32)
        
        pwd_hash = hashlib.sha256((password + salt).encode()).hexdigest()
        return pwd_hash, salt
    
    def verify_password(self, password: str, stored_hash: str, salt: str) -> bool:
        """Verify password against stored hash"""
        pwd_hash, _ = self.hash_password(password, salt)
        return pwd_hash == stored_hash
    
    def authenticate(self, username: str, password: str) -> Tuple[bool, Optional[Dict]]:
        """Authenticate user and return user info"""
        try:
            engine = get_db_engine()
            
            query = text("""
            SELECT 
                u.id,
                u.username,
                u.password_hash,
                u.password_salt,
                u.email,
                u.role,
                u.is_active,
                u.last_login,
                u.employee_id,
                e.id as emp_id,
                CONCAT(e.first_name, ' ', e.last_name) as full_name
            FROM users u
            LEFT JOIN employees e ON u.employee_id = e.id
            WHERE u.username = :username
            AND u.delete_flag = 0
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {'username': username}).fetchone()
            
            if not result:
                return False, {"error": "Invalid username or password"}
            
            user = dict(result._mapping)
            
            # Check if user is active
            if not user['is_active']:
                return False, {"error": "Account is inactive. Please contact administrator."}
            
            # Verify password
            if not self.verify_password(password, user['password_hash'], user['password_salt']):
                return False, {"error": "Invalid username or password"}
            
            # Update last login
            try:
                update_query = text("""
                UPDATE users 
                SET last_login = NOW() 
                WHERE id = :user_id
                """)
                
                with engine.connect() as conn:
                    conn.execute(update_query, {'user_id': user['id']})
                    conn.commit()
            except Exception as e:
                logger.warning(f"Could not update last_login: {e}")
            
            # Return user info
            return True, {
                'id': user['id'],
                'username': user['username'],
                'email': user['email'],
                'role': user['role'],
                'employee_id': user['employee_id'],
                'full_name': user['full_name'] or user['username'],
                'login_time': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False, {"error": "Authentication failed. Please try again."}
    
    def check_session(self) -> bool:
        """Check if user session is valid"""
        if 'authenticated' not in st.session_state:
            return False
        
        if not st.session_state.authenticated:
            return False
        
        # Check if user_id exists and is valid
        if not st.session_state.get('user_id'):
            logger.warning("No user_id in session state")
            return False
        
        # Check session timeout
        login_time = st.session_state.get('login_time')
        if login_time:
            elapsed = datetime.now() - login_time
            if elapsed > self.session_timeout:
                logger.info(f"Session timeout for user {st.session_state.get('username', 'unknown')}")
                self.logout()
                return False
        
        return True
    
    def login(self, user_info: Dict):
        """Set up user session with consistent naming"""
        # Main authentication flags
        st.session_state.authenticated = True
        st.session_state.login_time = user_info['login_time']
        
        # User information - using consistent keys
        st.session_state.user_id = user_info['id']
        st.session_state.username = user_info['username']
        st.session_state.user_email = user_info['email']
        st.session_state.user_role = user_info['role']
        st.session_state.user_fullname = user_info['full_name']
        st.session_state.employee_id = user_info['employee_id']
        
        # IMPORTANT: Also set authenticated_user_id for backward compatibility
        st.session_state.authenticated_user_id = user_info['id']
        
        # Initialize user dict for modules that use it
        st.session_state.user = {
            'id': user_info['id'],
            'username': user_info['username'],
            'email': user_info['email'],
            'role': user_info['role'],
            'full_name': user_info['full_name'],
            'employee_id': user_info['employee_id']
        }
        
        # Initialize other session state variables
        st.session_state.debug_mode = False
        
        logger.info(f"User {user_info['username']} (ID: {user_info['id']}) logged in successfully")
    
    def logout(self):
        """Clear user session"""
        # Get username before clearing
        username = st.session_state.get('username', 'Unknown')
        user_id = st.session_state.get('user_id', 'Unknown')
        
        # Clear authentication-related session state
        auth_keys = [
            'authenticated', 'user_id', 'username', 'user_email', 
            'user_role', 'user_fullname', 'employee_id', 'login_time',
            'authenticated_user_id', 'user'
        ]
        
        for key in auth_keys:
            if key in st.session_state:
                del st.session_state[key]
        
        # Clear cache
        st.cache_data.clear()
        
        logger.info(f"User {username} (ID: {user_id}) logged out")
    
    def require_auth(self):
        """Decorator to require authentication for a page"""
        if not self.check_session():
            st.warning("⚠️ Please login to access this page")
            st.stop()
            return False
        return True
    
    def get_current_user_id(self) -> Optional[int]:
        """Get current authenticated user ID with validation"""
        user_id = st.session_state.get('user_id')
        
        if not user_id:
            logger.error("No user_id found in session state")
            return None
        
        try:
            # Ensure it's an integer
            return int(user_id)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid user_id format: {user_id}, error: {e}")
            return None
    
    def get_user_display_name(self) -> str:
        """Get user display name"""
        if 'user_fullname' in st.session_state and st.session_state.user_fullname:
            return st.session_state.user_fullname
        return st.session_state.get('username', 'User')
    
    def validate_user_exists(self, user_id: int) -> bool:
        """Validate that a user exists in the database"""
        try:
            engine = get_db_engine()
            query = text("""
                SELECT id 
                FROM users 
                WHERE id = :user_id 
                AND delete_flag = 0 
                AND is_active = 1
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {'user_id': user_id}).fetchone()
                return result is not None
                
        except Exception as e:
            logger.error(f"Error validating user {user_id}: {e}")
            return False
    
    def update_session_activity(self):
        """Update session activity to prevent timeout"""
        if 'login_time' in st.session_state:
            # Reset timeout by updating activity timestamp
            st.session_state.last_activity = datetime.now()