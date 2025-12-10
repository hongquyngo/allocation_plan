"""
Allocation Planning System - Login Page
Entry point for the application
"""
import streamlit as st
from utils.auth import AuthManager
from utils.config import config
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Allocation Planning - Login",
    page_icon="🔐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize auth manager
auth = AuthManager()

def show_login_form():
    """Display the login form"""
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("# 📦 Allocation Planning System")
        st.markdown("---")
        
        # Login form
        with st.form("login_form"):
            st.subheader("🔐 Login")
            
            username = st.text_input(
                "Username",
                placeholder="Enter your username",
                help="Use your company username"
            )
            
            password = st.text_input(
                "Password",
                type="password",
                placeholder="Enter your password"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                submit = st.form_submit_button(
                    "Login",
                    type="primary",
                    use_container_width=True
                )
            with col2:
                st.form_submit_button(
                    "Forgot Password?",
                    use_container_width=True,
                    disabled=True,
                    help="Contact IT support for password reset"
                )
            
            if submit:
                if username and password:
                    # Attempt authentication
                    success, result = auth.authenticate(username, password)
                    
                    if success:
                        # Set up session
                        auth.login(result)
                        st.success("✅ Login successful!")
                        st.balloons()
                        
                        # Redirect to main page
                        st.switch_page("pages/1_🎯_Allocation_Plan.py")
                    else:
                        # Show error
                        error_msg = result.get("error", "Authentication failed")
                        st.error(f"❌ {error_msg}")
                else:
                    st.warning("⚠️ Please enter both username and password")
        
        # Footer
        st.markdown("---")
        st.caption(
            f"Version 1.0.0 | "
            f"{'☁️ Cloud' if config.is_cloud else '💻 Local'} Environment | "
            f"© 2024 Your Company"
        )

def main():
    """Main function"""
    # Check if already logged in
    if auth.check_session():
        # Already logged in, redirect to main page
        st.switch_page("pages/1_🎯_Allocation_Plan.py")
    else:
        # Show login form
        show_login_form()

if __name__ == "__main__":
    main()