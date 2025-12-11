"""
Allocation Planning System - Main Entry Point
Simple login and navigation hub
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
    page_title="Allocation Planning System",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize auth manager
auth = AuthManager()

# ==================== CUSTOM STYLES ====================

st.markdown("""
<style>
    /* Card styling */
    .module-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 12px;
        padding: 25px;
        color: white;
        text-align: center;
        transition: transform 0.2s, box-shadow 0.2s;
        cursor: pointer;
        height: 180px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .module-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 10px 30px rgba(0,0,0,0.2);
    }
    .module-card.blue {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    .module-card.green {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }
    .module-card h3 {
        margin: 0 0 10px 0;
        font-size: 1.4rem;
    }
    .module-card p {
        margin: 0;
        font-size: 0.9rem;
        opacity: 0.9;
    }
    .module-icon {
        font-size: 2.5rem;
        margin-bottom: 10px;
    }
    
    /* Welcome section */
    .welcome-header {
        text-align: center;
        padding: 20px 0;
    }
    .welcome-header h1 {
        color: #1f2937;
        margin-bottom: 5px;
    }
    .welcome-header p {
        color: #6b7280;
        font-size: 1.1rem;
    }
    
    /* Stats card */
    .stat-card {
        background: #f8fafc;
        border-radius: 8px;
        padding: 15px;
        text-align: center;
        border: 1px solid #e2e8f0;
    }
    .stat-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1e40af;
    }
    .stat-label {
        font-size: 0.85rem;
        color: #64748b;
    }
</style>
""", unsafe_allow_html=True)


# ==================== LOGIN PAGE ====================

def show_login_page():
    """Display simple login form"""
    
    # Center the form
    col1, col2, col3 = st.columns([1, 1.5, 1])
    
    with col2:
        st.markdown("")
        st.markdown("")
        
        # Logo/Title
        st.markdown("""
        <div style="text-align: center; margin-bottom: 30px;">
            <div style="font-size: 4rem;">📦</div>
            <h1 style="margin: 10px 0 5px 0; color: #1f2937;">Allocation Planning</h1>
            <p style="color: #6b7280; margin: 0;">Supply Chain Management System</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Login form
        with st.form("login_form", clear_on_submit=False):
            username = st.text_input(
                "Username",
                placeholder="Enter your username",
                key="login_username"
            )
            
            password = st.text_input(
                "Password",
                type="password",
                placeholder="Enter your password",
                key="login_password"
            )
            
            st.markdown("")
            submit = st.form_submit_button("Login", type="primary", use_container_width=True)
            
            if submit:
                if username and password:
                    success, result = auth.authenticate(username, password)
                    
                    if success:
                        auth.login(result)
                        st.success("✅ Login successful!")
                        st.rerun()
                    else:
                        error_msg = result.get("error", "Invalid username or password")
                        st.error(f"❌ {error_msg}")
                else:
                    st.warning("⚠️ Please enter username and password")
        
        # Footer
        st.markdown("---")
        st.caption(
            f"v1.0.0 | "
            f"{'☁️ Cloud' if config.is_cloud else '💻 Local'} | "
            f"© 2024 Prostech"
        )


# ==================== GREETING PAGE ====================

def show_greeting_page():
    """Display welcome page with module navigation"""
    
    # Get user info
    user = st.session_state.get('user', {})
    username = user.get('username', 'User')
    role = user.get('role', 'user')
    
    # Sidebar - User info & Logout
    with st.sidebar:
        st.markdown(f"### 👤 {username}")
        st.caption(f"Role: {role}")
        st.markdown("---")
        
        if st.button("🚪 Logout", use_container_width=True):
            auth.logout()
            st.rerun()
    
    # Welcome header
    st.markdown(f"""
    <div class="welcome-header">
        <h1>👋 Welcome, {username}!</h1>
        <p>Select a module to get started</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("")
    
    # Module cards
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        module_col1, module_col2 = st.columns(2)
        
        with module_col1:
            st.markdown("""
            <div class="module-card blue">
                <div class="module-icon">🎯</div>
                <h3>Allocation Plan</h3>
                <p>Single OC allocation with detailed control</p>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("Open Allocation Plan", key="btn_allocation", use_container_width=True):
                st.switch_page("pages/1_🎯_Allocation_Plan.py")
        
        with module_col2:
            st.markdown("""
            <div class="module-card green">
                <div class="module-icon">📦</div>
                <h3>Bulk Allocation</h3>
                <p>Mass allocation with smart strategies</p>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("Open Bulk Allocation", key="btn_bulk", use_container_width=True):
                st.switch_page("pages/2_📦_Bulk_Allocation.py")
    
    # Quick info section
    st.markdown("")
    st.markdown("---")
    st.markdown("##### 📊 Quick Overview")
    
    # Placeholder stats (can be connected to real data)
    stat1, stat2, stat3, stat4 = st.columns(4)
    
    with stat1:
        st.markdown("""
        <div class="stat-card">
            <div class="stat-value">-</div>
            <div class="stat-label">Pending OCs</div>
        </div>
        """, unsafe_allow_html=True)
    
    with stat2:
        st.markdown("""
        <div class="stat-card">
            <div class="stat-value">-</div>
            <div class="stat-label">Need Allocation</div>
        </div>
        """, unsafe_allow_html=True)
    
    with stat3:
        st.markdown("""
        <div class="stat-card">
            <div class="stat-value">-</div>
            <div class="stat-label">Available Supply</div>
        </div>
        """, unsafe_allow_html=True)
    
    with stat4:
        st.markdown("""
        <div class="stat-card">
            <div class="stat-value">-</div>
            <div class="stat-label">Coverage %</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.caption("💡 Tip: Use sidebar navigation or click module cards above to switch between modules")


# ==================== MAIN ====================

def main():
    """Main entry point"""
    if auth.check_session():
        show_greeting_page()
    else:
        show_login_page()


if __name__ == "__main__":
    main()