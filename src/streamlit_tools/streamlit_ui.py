import streamlit as st
import os
from typing import List, Dict, Any

# Assume shared utilities are available in the path
from shared_tools.llm_utils import get_llm_model
from shared_tools.ai_utils import load_project_context
from shared_tools.dp_utils import scan_data_products_for_catalog
from shared_tools.deploy import starburst_health_check 
# Import handlers for the execution buttons and config

from .streamlit_handlers import get_starburst_config_details, intSarburst_config


# --- 1. Sidebar Renderer ---
def render_sidebar():
    """Renders the entire application sidebar."""
    intSarburst_config()  # Initialize Starburst configuration if needed
    with st.sidebar:
        st.title("🏭 Factory Settings")
        st.markdown("---") 
        
        # 1. System Status and LLM Context
        st.subheader("🟢 System Status")
        
        # --- LLM Status Check ---
        llm_status = False
        try:
            if "model" not in st.session_state:
                # Assuming get_llm_model() handles initialization
                st.session_state.model = get_llm_model() 
            llm_status = True
        except ValueError as e:
            st.error(f"LLM Error: {e}")
            st.stop() # Stop execution if the critical LLM configuration is missing
            
        # --- Starburst Status Check (NEW) ---
        sb_status, sb_message = starburst_health_check()

        if llm_status and sb_status:
            st.success("All systems operational.")
        else:
            st.error(f"Starburst connection failed.")
            
        # 2. Cluster Configuration
        st.subheader("☁️ Starburst Cluster ")
        config_details = get_starburst_config_details()
        with st.expander("Starburst ", expanded=True):
            st.markdown(f"Host: `{config_details['SB_HOST']}`")
            st.markdown(f"User: `{config_details['SB_USER']}`")
            
        st.markdown("---") 
        
        # 3. Data Product Catalog
        st.subheader("📚 Data Product Catalog")
        catalog_list = scan_data_products_for_catalog(root_dir="data_products")
        
        if catalog_list:
            st.info(f"Found {len(catalog_list)} active Data Domains.")
            
            # Sort by Domain name
            sorted_domains = sorted(catalog_list, key=lambda x: x['domain_name'])
            
            for domain_obj in sorted_domains:
                
                domain_name = domain_obj['domain_name']
                domain_description = domain_obj['domain_description']
                data_script = domain_obj['data_script_path']
                domain_products = domain_obj['data_products']
                
                # Display Domain name as title
                with st.expander(f"**{domain_name}** ({len(domain_products)} DPs)", expanded=False):
                    
                    # --- Actions and Data Script ---
                    if data_script != 'N/A':
                        st.markdown(f"**Pipeline Actions:**")
                        col_run, col_deploy = st.columns(2)

                        # Button 1: Run Full Pipeline (Generate and Deploy)
                        if st.button("🚀 Run Data + Deploy", key=f"run_full_{domain_name}", type="primary"):
                            st.session_state['command_to_execute'] = f"python {data_script}"
                            st.rerun() 

                        # Button 2: Deploy DP Only
                        if st.button("🔄 Deploy Only", key=f"deploy_only_{domain_name}"):
                            st.session_state['command_to_execute'] = f"python {data_script} --deploy-only"
                            st.rerun()
                    else:
                        st.warning("No data generation script found in this domain folder.")

                    st.markdown("---")
                    
                    # --- List Data Products in Domain ---
                    for dp in domain_products:
                        st.markdown(f"**{dp['name']}**")
                        # Show description of the Data Product
                        st.caption(f"{dp.get('description', 'No description available.')} | Views: {dp['total_views']}") 
                        
                        # Optional: Expand to see view names
                        with st.expander(f"View Details ({dp['total_views']})"):
                            for view in dp['views']:
                                st.write(f"- `{view['name']}` ({view['type']})")
        else:
            st.warning("No Data Product YAMLs found in data_products/")


# --- 2. Main Content Renderer ---
def render_main_content():
    """Renders the main content area including title and examples."""
    st.title("🤖 Starburst Data Product Architect")
    st.subheader("Generate Data Pipelines and Semantic Layers with AI")

    st.markdown("""
    This tool uses Gemini to generate complete, runnable Python scripts and Starburst Data Product YAML definitions based on your natural language requests.
    """)

    # Example Prompts Section
    with st.expander("🚀 Click here for Prompt Examples & Instructions", expanded=False):
        st.markdown("### 💡 Example 1: Flight operations ")
        st.code(
            """
            Create a dataset for airline flight operations with data products for delays and passenger traffic.
            """
        )
        st.markdown("### 💡 Example 2: Supply Chain Logistics")
        st.code(
            """
            Create a Data Product for Supply Chain Logistics. Entities: Shipments, Warehouses, Carriers, Routes. 
            Goal: Track On-Time Delivery Rate and Average Cost per Mile. 
            Note: Use the existing project structure.
            """
        )
        st.markdown("### 💡 Example 3: Energy Consumption Monitoring")
        st.code(
            """
            Please generate a complete dataset and two distinct Data Products for Energy Consumption Monitoring in a Smart Grid context.
            1. The Dataset: Smart Meters, Substations, Meter Readings (high volume), Weather Data, Tariff Plans.
            2. Data Products:
               - A: Peak Usage Analysis (v_regional_peak_load, v_high_usage_customers).
               - B: Consumption Forecasting (mv_daily_usage_trends - with 24h refresh, v_weather_impact_correlation).
            Constraints: Ensure the domain is set to 'Energy & Utilities'.
            """
        )
    st.markdown("---")