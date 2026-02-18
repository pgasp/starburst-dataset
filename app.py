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
if 'suggested_improve_prompt' not in st.session_state: st.session_state['suggested_improve_prompt'] = None
if 'suggested_improve_files' not in st.session_state: st.session_state['suggested_improve_files'] = []

# Initialize Model (One-time)
if "model" not in st.session_state:
    try:
        st.session_state.model = get_llm_model()
    except ValueError as e:
        st.error(f"LLM Configuration Error: {e}")
        st.stop()


# --- Execution Runner ---
if st.session_state.get('command_to_execute') and not st.session_state.get('is_running'):
    execute_and_stream(st.session_state['command_to_execute'])


# --- UI Rendering ---
render_sidebar()
# Capture the suggested prompt from the Quick Start buttons
suggested_prompt = render_main_content()

# --- Logic: Handle Improvement Prompt from Sidebar ---
# Initialize local variables for this run
files_to_attach = []

if st.session_state.get('suggested_improve_prompt'):
    suggested_prompt = st.session_state['suggested_improve_prompt']
    files_to_attach = st.session_state.get('suggested_improve_files', [])
    
    # Cleanup state to prevent re-triggering
    st.session_state['suggested_improve_prompt'] = None
    st.session_state['suggested_improve_files'] = []


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
        if "files" in msg and msg["files"]:
            for fname, content in msg["files"]:
                with st.expander(f"📄 {fname}", expanded=False): 
                    st.code(content)


# --- Chat Input Handler ---
user_input = st.chat_input("Describe your Data Product...")

if prompt := (user_input or suggested_prompt):
    
    # 1. User Message Object
    user_msg_obj = {"role": "user", "content": prompt}
    
    # If this prompt came from the "Improve" button, attach the files
    if files_to_attach:
        user_msg_obj["files"] = files_to_attach
        
    st.session_state.messages.append(user_msg_obj)
    
    # Render immediately
    with st.chat_message("user"):
        st.markdown(prompt)
        if files_to_attach:
            for fname, content in files_to_attach:
                with st.expander(f"📄 {fname}", expanded=False):
                    st.code(content)

    # 2. Assistant Logic
    with st.spinner("Thinking..."):
        context, _ = load_project_context()

        # Construct the payload for the LLM
        # If files are attached, we must explicitly add them to the prompt text sent to the model
        llm_prompt = prompt
        if files_to_attach:
            llm_prompt += "\n\nHere are the current definition files for context:\n"
            for fname, content in files_to_attach:
                llm_prompt += f"\n### FILE: {fname}\n{content}\n"
            llm_prompt += "\n\nPlease analyze these files and suggest improvements or help me modify them."

        response_text, st.session_state.chat_session = generate_response(
            st.session_state.model, 
            llm_prompt, 
            context, 
            st.session_state.chat_session
        )
        
        parsed_files = parse_generated_files(response_text)
        
        # Scenario A: Files Generated
        if parsed_files:
            st.success(f"Generated {len(parsed_files)} files")
            
            st.session_state['files_to_save'] = parsed_files
            st.session_state['files_saved'] = False 

            data_script_path = next((path for path, content in parsed_files if path.endswith('_data.py')), None)
            st.session_state['data_script_path'] = data_script_path

            files_for_history = []
            for fname, content in parsed_files:
                files_for_history.append((fname, content))
                # UPDATED: expanded=False to hide content by default
                with st.expander(f"📄 {fname}", expanded=False):
                    st.code(content)
            
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
    
    if st.button("Save Generated Files to Project", type="primary"):
        save_files(st.session_state['files_to_save'])
        st.toast("Files saved successfully! Check the sidebar.", icon="✅")
        
        st.session_state['files_to_save'] = None
        st.session_state['files_saved'] = True
        
        st.rerun()