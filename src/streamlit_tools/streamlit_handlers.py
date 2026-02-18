import streamlit as st
import os
import subprocess
from dotenv import load_dotenv
import sys

# Assume absolute path imports for shared utilities
from shared_tools.env_utils import load_project_env

# --- DIALOG (MODAL) WRAPPER ---
@st.dialog("⚙️ Data Product Pipeline", width="large")
def _run_pipeline_dialog(command):
    """
    Internal dialog function that runs the subprocess and streams output 
    inside a modal window.
    """
    st.caption(f"Executing: `{command}`")
    
    # Placeholder for streaming logs
    log_container = st.empty()
    # Container for final results and buttons
    result_container = st.container()
    
    logs = []
    
    try:
        # Start the process
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            cwd="." 
        )
        
        # Stream output in real-time to the modal
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            if line:
                logs.append(line.strip())
                # Update the code block in the modal
                log_container.code('\n'.join(logs), language='bash')
        
        return_code = process.wait()
        
        # Update Session State for the main page history
        st.session_state['execution_output'] = logs
        st.session_state['execution_command'] = command
        st.session_state['execution_complete'] = True

        if return_code == 0:
            result_container.success("✅ Pipeline executed successfully!")
            st.session_state['execution_status_label'] = "✅ Data Pipeline Complete"
        else:
            result_container.error(f"❌ Execution failed (Exit Code: {return_code})")
            st.session_state['execution_status_label'] = f"❌ Execution Failed ({return_code})"

    except Exception as e:
        result_container.error(f"System Error: {e}")
    
    # Close Button to dismiss the modal and refresh the app state
    if result_container.button("Close & Refresh", type="primary"):
        st.session_state['command_to_execute'] = None
        st.session_state['is_running'] = False
        st.rerun()


# --- PUBLIC ENTRY POINT ---
def execute_and_stream(command):
    """
    Called by app.py. Triggers the modal dialog for execution.
    """
    # Mark as running to prevent app.py form re-triggering
    st.session_state['is_running'] = True
    
    # Open the modal
    _run_pipeline_dialog(command)


# --- CONFIGURATION HELPERS (Preserved) ---

def intSarburst_config():
    """Initializes Starburst configuration from .env files."""
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    local_env_path = os.path.abspath(os.path.join(root_dir, 'data_products/.env'))
    if os.path.exists(local_env_path):
        load_dotenv(dotenv_path=local_env_path, override=True, verbose=False)   
    pass

def get_starburst_config_details():
    """Reads critical Starburst config from the environment."""
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    local_env_path = os.path.abspath(os.path.join(root_dir, 'data_products/.env'))
    if os.path.exists(local_env_path):
        load_dotenv(dotenv_path=local_env_path, override=True, verbose=False)   
    
    config = {
        "SB_HOST": os.environ.get("SB_HOST", "N/A"),
        "SB_USER": os.environ.get("SB_USER", "N/A"),
        "SB_DOMAIN_LOCATION_BASE": os.environ.get("SB_DOMAIN_LOCATION_BASE", "N/A"),
        "SB_URL": os.environ.get("SB_URL", "N/A")
    }
    
    if config['SB_URL'] == 'N/A' and config['SB_HOST'] != 'N/A':
        config['SB_URL'] = f"https://{config['SB_HOST']}"
        
    return config