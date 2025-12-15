import streamlit as st
import os
import pandas as pd
from typing import List, Dict, Any

# Shared Utilities
from shared_tools.llm_utils import get_llm_model
from shared_tools.ai_utils import load_project_context
from shared_tools.dp_utils import scan_data_products_for_catalog
from shared_tools.deploy import starburst_health_check
from shared_tools.starburst_client import StarburstClient

# Local Handlers
from .streamlit_handlers import get_starburst_config_details, intSarburst_config

# --- Helper: View Details Dialog ---
@st.dialog("🔍 View Details", width="large")
def show_view_details(view_data: Dict[str, Any]):
    """
    Displays detailed information about a specific view in a modal popup.
    """
    st.markdown(f"### `{view_data['name']}`")
    st.caption(f"**Type:** {view_data['type']}")
    st.markdown(f"**Description:** {view_data['description']}")
    
    st.divider()
    st.markdown("#### 📋 Schema Definition")
    
    columns = view_data.get('columns', [])
    if columns:
        # Format for display
        df = pd.DataFrame(columns)
        # Rename for cleaner UI if keys exist
        if not df.empty and 'name' in df.columns:
            df = df.rename(columns={"name": "Column Name", "type": "Data Type", "description": "Description"})
            st.dataframe(
                df, 
                column_config={
                    "Column Name": st.column_config.TextColumn(width="medium"),
                    "Data Type": st.column_config.TextColumn(width="small"),
                    "Description": st.column_config.TextColumn(width="large"),
                },
                hide_index=True, 
                use_container_width=True
            )
    else:
        st.info("No explicit column definitions found in YAML.")


# --- 1. Sidebar Renderer (Dashboard Style) ---
def render_sidebar():
    """Renders the dashboard-style sidebar with nested expanders."""
    intSarburst_config()  # Initialize Starburst configuration if needed
    
    # Initialize API Client
    client = StarburstClient()
    
    # Fetch config early to use SB_URL in the status indicator
    config = get_starburst_config_details()
    
    with st.sidebar:
        st.header("🏭 Factory Control")
        
        # --- 1. Compact Health Check ---
        llm_status = "model" in st.session_state
        sb_status, sb_msg = starburst_health_check()
        
        col_h1, col_h2 = st.columns(2)
        with col_h1:
            st.markdown("🟢 **AI Model**" if llm_status else "🔴 **AI Model**")
        with col_h2:
            if sb_status:
                sb_url = config.get('SB_URL', 'N/A')
                if sb_url and sb_url != 'N/A':
                    st.markdown(f"[🟢 **Starburst**]({sb_url})")
                else:
                    st.markdown("🟢 **Starburst**")
            else:
                st.markdown("🔴 **Starburst**")

        st.divider()

        # --- 2. Data Product Catalog with Search ---
        st.subheader("📚 Data Domains")
        
        # Scan the file system for domains
        catalog_list = scan_data_products_for_catalog(root_dir="data_products")
        
        # Search Filter
        search_query = st.text_input("Filter domains...", placeholder="e.g. automotive").lower()
        
        if catalog_list:
            filtered_list = [
                d for d in catalog_list 
                if search_query in d['domain_name'].lower() 
                or search_query in d['domain_description'].lower()
            ]
            
            st.caption(f"Showing {len(filtered_list)} of {len(catalog_list)} domains")
            
            # --- DOMAIN LOOP ---
            for domain_obj in sorted(filtered_list, key=lambda x: x['domain_name']):
                
                domain_name = domain_obj['domain_name']
                data_script = domain_obj['data_script_path']
                products = domain_obj['data_products']
                
                # LEVEL 1: DOMAIN EXPANDER
                with st.expander(f"📂 {domain_name}", expanded=False):
                    
                    # Metrics Row
                    m1, m2 = st.columns(2)
                    m1.metric("Products", len(products))
                    total_views = sum(p['total_views'] for p in products)
                    m2.metric("Views", total_views)
                    
                    # --- ACTION BUTTONS (Split into 2 Columns) ---
                    if data_script != 'N/A':
                        c1, c2 = st.columns(2)
                        with c1:
                            if st.button("🚀 Run All", key=f"run_{domain_obj['folder_name']}", help="Generate Data & Deploy", use_container_width=True):
                                st.session_state['command_to_execute'] = f"python {data_script}"
                                st.rerun()
                        with c2:
                            if st.button("🔄 Deploy", key=f"deploy_{domain_obj['folder_name']}", help="Deploy Metadata Only", use_container_width=True):
                                st.session_state['command_to_execute'] = f"python {data_script} --deploy-only"
                                st.rerun()
                    else:
                        st.warning("No script found.")

                    st.markdown("---")
                    st.markdown("#### Data Products")

                    # LEVEL 2: DATA PRODUCT EXPANDER
                    for dp in products:
                        with st.expander(f"📊 {dp['name']}", expanded=False):
                            st.info(f"_{dp['description']}_") # Data Product Description
                            
                            # --- WEB LINK LOGIC ---
                            # Search for the product ID to build the deep link
                            try:
                                if sb_status: # Only attempt if Starburst is online
                                    # Search by name
                                    results = client.search_products(dp['name'])
                                    # Find exact match
                                    remote_product = next((p for p in results if p['name'] == dp['name']), None)
                                    
                                    if remote_product:
                                        pid = remote_product['id']
                                        # Construct URL: https://<HOST>/ui/insights/dataproduct/product/display/<UUID>
                                        base_url = client.base_url.rstrip('/')
                                        product_url = f"{base_url}/ui/insights/dataproduct/product/display/{pid}"
                                        
                                        st.markdown(f"[[🌐 Open in Starburst]]({product_url})")
                            except Exception:
                                # Silently fail if ID lookup doesn't work (e.g., product not deployed yet)
                                pass
                            
                            st.markdown("**Views & Objects:**")
                            
                            # List Views as Items with Popup Button
                            for view in dp['views']:
                                btn_key = f"view_btn_{domain_name}_{dp['name']}_{view['name']}"
                                
                                c1, c2 = st.columns([0.85, 0.15])
                                with c1:
                                    st.text(f"• {view['name']}")
                                with c2:
                                    if st.button("👁️", key=btn_key, help="See Schema"):
                                        show_view_details(view)

        else:
            st.info("No domains found. Start chatting to create one!")
            
        st.divider()
        
        # --- 3. Cluster Info ---
        with st.expander("⚙️ Cluster Config", expanded=True):
            st.markdown(f"**Host:** `{config['SB_HOST']}`")
            st.markdown(f"**User:** `{config['SB_USER']}`")
            
            if config.get('SB_URL') and config['SB_URL'] != 'N/A':
                st.markdown("---")
                st.link_button("🚀 Open Starburst Web UI", config['SB_URL'], use_container_width=True)


# --- 2. Main Content Renderer (Interactive Hero) ---
def render_main_content():
    """Renders the main content area with a modern header and interactive quick starts."""
    
    # Hero Section
    st.title("🤖 Data Product Factory")
    st.markdown("#### *From Idea to Iceberg in Seconds*")
    
    st.info("""
    **AI-Powered Data Engineering:** Describe your business domain, and I will generate:
    1. 🐍 **Python Scripts** for synthetic data generation (Faker/Pandas).
    2. 📄 **Data Product YAMLs** for Starburst semantic views.
    """)

    # --- Interactive Quick Starts ---
    st.markdown("### 🚀 Quick Start Templates")
    
    col1, col2, col3 = st.columns(3)
    
    selected_prompt = None

    with col1:
        st.markdown("**✈️ Aviation**")
        st.caption("Flight ops & passenger traffic")
        if st.button("Load Aviation Demo", use_container_width=True):
            selected_prompt = "Create a dataset for airline flight operations with data products for delays and passenger traffic."

    with col2:
        st.markdown("**📦 Logistics**")
        st.caption("Supply chain & delivery tracking")
        if st.button("Load Supply Chain Demo", use_container_width=True):
            selected_prompt = """Create a Data Product for Supply Chain Logistics. 
Entities: Shipments, Warehouses, Carriers. 
Goal: Track On-Time Delivery Rate."""

    with col3:
        st.markdown("**⚡ Energy**")
        st.caption("Smart grid & consumption")
        if st.button("Load Energy Demo", use_container_width=True):
            selected_prompt = """Generate a dataset for Smart Grid Energy Monitoring.
Entities: Smart Meters, Substations, Readings.
Data Products: Peak Usage Analysis & Consumption Forecasting.
Constraints: Ensure the domain is set to 'Energy & Utilities'."""

    return selected_prompt