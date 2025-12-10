import streamlit as st
import os
import subprocess
from dotenv import load_dotenv
import sys

# Assume absolute path imports for shared utilities
from shared_tools.env_utils import load_project_env


# --- Function to execute the script and stream output ---
def execute_and_stream(command):
    st.session_state['is_running'] = True
    st.session_state['execution_output'] = []
    
    # Use st.status for a clean, expandable view of the process during execution
    with st.status(f"Running command: `{command}`", expanded=True) as status_box:
        output_container = st.empty()
        
        try:
            # Use Popen to run the script and capture stdout/stderr in real-time
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                cwd="." # Ensure the script runs from the project root
            )
            
            # Read output line by line and update the Streamlit container
            while True:
                line = process.stdout.readline()
                if not line and process.poll() is not None:
                    break
                if line:
                    st.session_state['execution_output'].append(line.strip())
                    # Update the displayed code block
                    output_container.code('\n'.join(st.session_state['execution_output']), language='bash')
                    
            # Wait for the process to finish
            return_code = process.wait()
            
            # Save final status and code to session state for the persistent log
            st.session_state['execution_command'] = command
            if return_code == 0:
                status_box.update(label="✅ Data Pipeline Complete (Finalizing View...)", state="complete", expanded=True)
                st.session_state['execution_status_label'] = "✅ Data Pipeline Complete"
            else:
                status_box.update(label=f"❌ Execution Failed (Code {return_code}) (Finalizing View...)", state="error", expanded=True)
                st.session_state['execution_status_label'] = f"❌ Execution Failed (Code {return_code})"
                
        except Exception as e:
            st.session_state['execution_output'].append(f"Execution Error: {e}")
            st.session_state['execution_status_label'] = f"❌ Execution Failed (System Error)"
            status_box.update(label=st.session_state['execution_status_label'], state="error", expanded=True)
        
        finally:
            # Transition state for the next run cycle
            st.session_state['is_running'] = False
            st.session_state['command_to_execute'] = None
            st.session_state['execution_complete'] = True # Flag to display permanent log

    # Trigger rerun to jump out of this function and render the persistent log
    st.rerun() 

def intSarburst_config():
    """Initializes Starburst configuration from .env files."""

    # 1. Ensure environment is loaded 
    # Path relative to src/streamlit_tools/
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    local_env_path = os.path.abspath(os.path.join(root_dir, 'data_products/.env'))
    if os.path.exists(local_env_path):
        load_dotenv(dotenv_path=local_env_path, override=True, verbose=False)   
    pass

# --- Helper to display Starburst Config ---
def get_starburst_config_details():
    """Reads critical Starburst config from the environment."""

    # 1. Ensure environment is loaded 
    # Path relative to src/streamlit_tools/
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    local_env_path = os.path.abspath(os.path.join(root_dir, 'data_products/.env'))
    if os.path.exists(local_env_path):
        load_dotenv(dotenv_path=local_env_path, override=True, verbose=False)   
    
    # 2. Retrieve config values
    config = {
        "SB_HOST": os.environ.get("SB_HOST", "N/A"),
        "SB_USER": os.environ.get("SB_USER", "N/A"),
        "SB_DOMAIN_LOCATION_BASE": os.environ.get("SB_DOMAIN_LOCATION_BASE", "N/A"),
        "SB_URL": os.environ.get("SB_URL", "N/A")
    }
    
    if config['SB_URL'] == 'N/A' and config['SB_HOST'] != 'N/A':
        config['SB_URL'] = f"https://{config['SB_HOST']}"
        
    return config