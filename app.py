import streamlit as st
import os
import sys

# --- 1. PAGE CONFIGURATION (MUST BE FIRST) ---
st.set_page_config(
    page_title="Starburst Data Product Factory",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Setup Paths and Imports ---
# Ensure we can import from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# Import core AI utilities
from shared_tools.ai_utils import (
    load_project_context, 
    generate_response, 
    parse_generated_files,
    save_files
)
from shared_tools.llm_utils import get_llm_model

# Import handlers and UI components from the new streamlit_tools package
from streamlit_tools.streamlit_handlers import execute_and_stream
from streamlit_tools.streamlit_ui import render_sidebar, render_main_content


# --- Application Setup and Initialization ---

# Initialize Session State
if "messages" not in st.session_state: st.session_state.messages = []
if "chat_session" not in st.session_state: st.session_state["chat_session"] = None
if 'files_to_save' not in st.session_state: st.session_state['files_to_save'] = None
if 'files_saved' not in st.session_state: st.session_state['files_saved'] = False
if 'data_script_path' not in st.session_state: st.session_state['data_script_path'] = None
if 'command_to_execute' not in st.session_state: st.session_state['command_to_execute'] = None
if 'is_running' not in st.session_state: st.session_state['is_running'] = False
if 'execution_output' not in st.session_state: st.session_state['execution_output'] = []
if 'execution_complete' not in st.session_state: st.session_state['execution_complete'] = False
if 'execution_command' not in st.session_state: st.session_state['execution_command'] = None
if 'execution_status_label' not in st.session_state: st.session_state['execution_status_label'] = None

# Initialize Model (One-time)
if "model" not in st.session_state:
    try:
        st.session_state.model = get_llm_model()
    except ValueError as e:
        st.error(f"LLM Configuration Error: {e}")
        st.stop()


# --- Execution Runner ---
# This block executes commands triggered from the Sidebar
if st.session_state.get('command_to_execute') and not st.session_state.get('is_running'):
    execute_and_stream(st.session_state['command_to_execute'])


# --- UI Rendering ---
render_sidebar()
# Capture the suggested prompt from the Quick Start buttons
suggested_prompt = render_main_content()


# --- Execution Log Display ---
if st.session_state.get('execution_complete') == True:
    st.subheader(st.session_state.get('execution_status_label', "Execution Completed"))
    with st.expander(f"Full Log for: `{st.session_state.get('execution_command', 'N/A')}`", expanded=True):
        st.code('\n'.join(st.session_state.get('execution_output', [])), language='bash')
    st.session_state['execution_complete'] = False


# --- Display Chat History ---
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if "files" in msg:
            for fname, content in msg["files"]:
                with st.expander(f"📄 {fname}"):
                    st.code(content)


# --- Chat Input Handler ---
user_input = st.chat_input("Describe your Data Product...")

if prompt := (user_input or suggested_prompt):
    
    # 1. User Message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 2. Assistant Logic
    with st.spinner("Thinking..."):
        # Load context inside the loop to ensure it's fresh if files were saved
        context, _ = load_project_context()

        response_text, st.session_state.chat_session = generate_response(
            st.session_state.model, 
            prompt, 
            context, 
            st.session_state.chat_session
        )
        
        parsed_files = parse_generated_files(response_text)
        
        # Scenario A: Files Generated
        if parsed_files:
            st.success(f"Generated {len(parsed_files)} files")
            
            # Store files in session state for later saving
            st.session_state['files_to_save'] = parsed_files
            st.session_state['files_saved'] = False # Reset flag

            # Find the main script path (used for logic, but UI buttons removed)
            data_script_path = next((path for path, content in parsed_files if path.endswith('_data.py')), None)
            st.session_state['data_script_path'] = data_script_path

            # Show files in UI
            files_for_history = []
            for fname, content in parsed_files:
                files_for_history.append((fname, content))
                with st.expander(f"📄 {fname}", expanded=True):
                    st.code(content)
            
            # Add to history
            st.session_state.messages.append({
                "role": "assistant", 
                "content": response_text.split("### FILE:")[0], 
                "files": files_for_history
            })
            
            if suggested_prompt:
                st.rerun()
            
        # Scenario B: Conversational Response (No files)
        else:
            st.markdown(response_text)
            st.session_state.messages.append({
                "role": "assistant", 
                "content": response_text
            })
            if suggested_prompt:
                st.rerun()


# --- File Action Handlers ---

# 1. Standalone File Saving Handler 
if st.session_state.get('files_to_save'):
    st.divider()
    st.markdown("### 💾 Save & Review")
    
    # Simple save button. After saving, the sidebar will refresh.
    if st.button("Save Generated Files to Project", type="primary"):
        save_files(st.session_state['files_to_save'])
        st.toast("Files saved successfully! Check the sidebar.", icon="✅")
        
        # Clear the pending files and set saved flag
        st.session_state['files_to_save'] = None
        st.session_state['files_saved'] = True
        
        # Rerun to update the Sidebar Catalog immediately
        st.rerun()