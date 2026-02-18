import os
import logging
import re

# --- System Prompt Definition ---
def get_system_prompt_template():
    """
    Defines the system prompt template. Moved into a function to prevent 
    any module-level execution issues that contribute to circular imports.
    """
    # Note: We escape the {CATALOG} vars so Python format doesn't break
    return """
You are an expert Data Engineer and Starburst Data Product Developer. 
Your goal is to generate complete, runnable code for a Data Product Factory pipeline based on user requests.

You have access to the current project's codebase (context provided below). 
You must strictly follow the existing patterns, directory structure, and coding standards.

**Rules for Generation:**
1.  **Directory Structure:** Create a new directory under `data_products/<domain_name>/`.
2.  **Files to Generate:**
    * `__init__.py`: Empty file.
    * `<domain>_data.py`: Python script to generate synthetic data (using Faker/pandas) and upload to Starburst. You must import the `_data.py` files from the context. MUST use `shared_tools` imports.
    * `<domain>_data_product.yaml`: The Data Product definition file. MUST use explicit 3-part naming (`${{CATALOG}}.${{SCHEMA}}.table`) in SQL. Each Data Product must have at least 2 views. Data Product must include at least one view 50 words and beetween 20 to 50 words for views. Thoses description will help to provide context.For each view provide columns name, type and description
    * `.env`: Local environment variables for this specific data product.
3.  **Code Style:**
    * Use `shared_tools.lakehouse_utils` for schema setup and upload.
    * Use `shared_tools.deploy` conventions if applicable.
    * Use `load_project_env(__file__)` in the Python script.
4.  **Output Format:**
    * Provide the content for each file clearly separated.
    * Use special markers like `### FILE: path/to/file` to indicate the start of a file.
    * **DO NOT** use markdown code blocks (like ```python) inside the file content sections.

**Context (Existing Codebase):**
{context}
"""

# --- Context Loading ---
def load_project_context(base_path="."):
    """
    Reads key files to provide context to the LLM.
    Args:
        base_path: The root directory of the project.
    """
    context = ""
    # Define files relative to base_path that provide the best examples
    files_to_read = [
        "src/shared_tools/deploy.py",
        "src/shared_tools/lakehouse_utils.py",
        "src/shared_tools/env_utils.py",
        "data_products/.env",
        "data_products/integrated_manufacturing/integrated_manufacturing_data.py",
        "data_products/integrated_manufacturing/operational_process_dp.yaml",
        "data_products/integrated_manufacturing/product_quality_dp.yaml",
        "data_products/integrated_manufacturing/value_chain_federated_dp.yaml",
        "data_products/integrated_manufacturing/.env"
    ]

    context_files = []
    for rel_path in files_to_read:
        file_path = os.path.join(base_path, rel_path)
        if os.path.exists(file_path):
            try:
                with open(file_path, "r") as f:
                    content = f.read()
                    context += f"\n--- START OF FILE: {rel_path} ---\n"
                    context += content
                    context += f"\n--- END OF FILE: {rel_path} ---\n"
                    context_files.append(rel_path)
            except Exception as e:
                logging.error(f"Failed to read context file {file_path}: {e}")
    
    return context, context_files

# --- Prompt Engineering ---
def generate_response(model, user_input, context, chat_session=None):
    """
    Sends the prompt to Gemini and gets the response, maintaining chat history.
    """
    if chat_session is None:
        SYSTEM_PROMPT_TEMPLATE = get_system_prompt_template()
        initial_prompt = SYSTEM_PROMPT_TEMPLATE.format(context=context)
        chat_session = model.start_chat(history=[
            {"role": "user", "parts": [initial_prompt]},
            {"role": "model", "parts": ["Understood. I am ready to generate Data Product code based on your requests."]}
        ])

    try:
        logging.info("Sending request to Gemini...")
        response = chat_session.send_message(user_input)
        logging.info("Received response from Gemini.")
        return response.text, chat_session
    except Exception as e:
        logging.error(f"Gemini API call failed: {e}", exc_info=True)
        return f"Error communicating with Gemini: {e}", chat_session

def parse_generated_files(response_text):
    """
    Parses the LLM response into a list of (filepath, content) tuples.
    """
    file_blocks = re.split(r'### FILE:\s*', response_text)
    
    # Simple logic to ignore preamble text
    if len(file_blocks) < 2:
        return []
    
    # Skip the first split if it's empty or clearly preamble
    if not file_blocks[0].strip():
        file_blocks = file_blocks[1:]
    elif len(file_blocks) > 1 and "### FILE:" not in response_text[:20]:
         file_blocks = file_blocks[1:]
        
    parsed_files = []
    for block in file_blocks:
        if not block.strip(): continue
        
        lines = block.strip().split('\n')
        file_path = lines[0].strip()
        
        # Everything after the first line is content
        code_content = '\n'.join(lines[1:])
        
        # --- CLEANING: Robust Markdown Stripping ---
        # 1. Remove starting ```python, ```yaml, ```bash, etc.
        code_content = re.sub(r'^\s*```[a-zA-Z0-9]*\n', '', code_content)
        # 2. Remove ending ```
        code_content = re.sub(r'\n\s*```\s*$', '', code_content)
        # 3. Remove stray ``` if they are the only thing on a line
        code_content = re.sub(r'^```$', '', code_content, flags=re.MULTILINE)
        
        parsed_files.append((file_path, code_content))
        
    return parsed_files

def save_files(parsed_files, base_path="."):
    """
    Saves parsed files to disk.
    """
    saved_paths = []
    for file_path, content in parsed_files:
        # Security: Prevent writing outside of data_products
        if ".." in file_path or file_path.startswith("/"):
             logging.warning(f"Skipping unsafe path: {file_path}")
             continue
             
        full_path = os.path.join(base_path, file_path)
        directory = os.path.dirname(full_path)
        
        if directory and not os.path.exists(directory):
            try:
                os.makedirs(directory)
                logging.info(f"Created directory: {directory}")
            except OSError as e:
                logging.error(f"Error creating directory {directory}: {e}")
                continue
            
        try:
            with open(full_path, "w") as f:
                f.write(content.strip())
                f.write('\n')
            logging.info(f"Saved file: {full_path}")
            saved_paths.append(full_path)
        except IOError as e:
             logging.error(f"Error writing file {full_path}: {e}")
    return saved_paths