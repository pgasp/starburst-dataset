import os
import sys
import logging
import google.generativeai as genai
from dotenv import load_dotenv
import re

# --- Logging Configuration ---
# Logs will appear in the console. 
# To log to a file, add: filename='app.log', filemode='a' to basicConfig
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Load environment variables (for Gemini API Key)
load_dotenv()

# --- Configuration ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") 
if not GEMINI_API_KEY:
    logging.critical("GEMINI_API_KEY not found in .env file.")
    sys.exit(1)

genai.configure(api_key=GEMINI_API_KEY)

# Select the model
# Using 'gemini-1.5-pro' as the primary model.
try:
    model = genai.GenerativeModel('gemini-2.5-pro')
    logging.info("Successfully initialized model: gemini-2.5-pro")
except Exception as e:
    logging.warning(f"Primary model not found ({e}). Falling back to 'gemini-2.5-pro'.")
    model = genai.GenerativeModel('gemini-1.5-flash')

# --- Context Loading ---
def load_project_context():
    """
    Reads key files to provide context to the LLM about the project structure and coding standards.
    """
    context = ""
    
    # Update these paths if your project structure changes
    files_to_read = [
        "src/shared_tools/deploy.py",
        "src/shared_tools/lakehouse_utils.py",
        "src/shared_tools/env_utils.py",
        "data_products/.env",
        # "data_products/cib_esg/cib_esg_data.py",      
        # "data_products/cib_esg/cib_esg_data_product.yaml", 
        # "data_products/cib_esg/.env",
        # "data_products/telecom/telecom_data.py",
        # "data_products/telecom/bss_data_product.yaml",
        # "data_products/telecom/federated_data_product.yaml",
        # "data_products/telecom/oss_data_product.yaml",
        # "data_products/telecom/.env",
        "data_products/integrated_manufacturing/integrated_manufacturing_data.py",
        "data_products/integrated_manufacturing/operational_process_dp.yaml",
        "data_products/integrated_manufacturing/product_quality_dp.yaml",
        "data_products/integrated_manufacturing/value_chain_federated_dp.yaml",
        "data_products/integrated_manufacturing/.env"
    ]

    for file_path in files_to_read:
        if os.path.exists(file_path):
            try:
                with open(file_path, "r") as f:
                    content = f.read()
                    context += f"\n--- START OF FILE: {file_path} ---\n"
                    context += content
                    context += f"\n--- END OF FILE: {file_path} ---\n"
            except Exception as e:
                logging.error(f"Failed to read context file {file_path}: {e}")
        else:
            # logging.debug(f"Context file {file_path} not found (skipping).")
            pass

    return context

# --- Prompt Engineering ---
# NOTE: Double curly braces {{ }} are used around CATALOG and SCHEMA 
# so they are treated as literal text and not Python format variables.
SYSTEM_PROMPT = """
You are an expert Data Engineer and Starburst Data Product Developer. 
Your goal is to generate complete, runnable code for a Data Product Factory pipeline based on user requests.

You have access to the current project's codebase (context provided below). 
You must strictly follow the existing patterns, directory structure, and coding standards.

**Rules for Generation:**
1.  **Directory Structure:** * Create a new directory under `data_products/<domain_name>/`.
    * All files for the new domain go into this directory.
2.  **Files to Generate:**
    * `__init__.py`: Empty file.
    * `<domain>_data.py`: Python script to generate synthetic data (using Faker/pandas) and upload to Starburst.you must import the _data.py files from the context. MUST use `shared_tools` imports.
    * `<domain>_data_product.yaml`: The Data Product definition file. MUST use explicit 3-part naming (`${{CATALOG}}.${{SCHEMA}}.table`) in SQL.
    * `.env`: Local environment variables for this specific data product.
3.  **Code Style:**
    * Use `shared_tools.lakehouse_utils` for schema setup and upload.
    * Use `shared_tools.deploy` conventions if applicable.
    * Use `load_project_env(__file__)` in the Python script.
4.  **Output Format:**
    * Provide the content for each file clearly separated.
    * Use special markers like `### FILE: path/to/file` to indicate the start of a file.

**Context (Existing Codebase):**
{context}
"""

def generate_response(user_input, context):
    """
    Sends the prompt to Gemini and gets the response.
    """
    # .format() will now correctly ignore {{CATALOG}} and only replace {context}
    full_prompt = SYSTEM_PROMPT.format(context=context) + f"\n\n**User Request:** {user_input}\n"
    
    try:
        logging.info("Sending request to Gemini...")
        response = model.generate_content(full_prompt)
        logging.info("Received response from Gemini.")
        return response.text
    except Exception as e:
        # Logs the full stack trace for debugging API errors
        logging.error(f"Gemini API call failed: {e}", exc_info=True)
        return f"Error communicating with Gemini: {e}"

def save_generated_files(response_text):
    """
    Parses the LLM response and saves files to disk.
    Look for markers like: ### FILE: data_products/domain/filename.ext
    """
    # Regex to find file blocks
    file_blocks = re.split(r'### FILE:\s*', response_text)
    
    # Skip the first split if it's empty text before the first file
    if not file_blocks[0].strip():
        file_blocks = file_blocks[1:]
    elif len(file_blocks) > 1 and "### FILE:" not in response_text[:20]:
         # Handle case where there is intro text before first file
         file_blocks = file_blocks[1:]
        
    generated_files = []

    for block in file_blocks:
        if not block.strip(): continue
        
        lines = block.strip().split('\n')
        file_path = lines[0].strip()
        
        # Determine content (everything after the first line)
        code_content = '\n'.join(lines[1:])
        
        # Clean up Markdown code fences
        code_content = re.sub(r'^```[a-zA-Z]*\n', '', code_content) # Remove start ```python
        code_content = re.sub(r'\n```$', '', code_content) # Remove end ```
        code_content = re.sub(r'^```$', '', code_content)  # Remove stray ```
        
        # Ensure directory exists
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            try:
                os.makedirs(directory)
                logging.info(f"Created directory: {directory}")
            except OSError as e:
                logging.error(f"Error creating directory {directory}: {e}")
                continue
            
        # Write file
        try:
            with open(file_path, "w") as f:
                f.write(code_content.strip())
                # Ensure final newline
                f.write('\n')
                
            logging.info(f"Saved file: {file_path}")
            generated_files.append(file_path)
        except IOError as e:
             logging.error(f"Error writing file {file_path}: {e}")
        
    return generated_files

# --- Main Loop ---
def main():
    print("--- Starburst Data Product Generator (Powered by Gemini) ---")
    logging.info("Loading project context...")
    context = load_project_context()
    logging.info("Context loaded. Ready for requests.")
    print("Example: 'Create a dataset for airline flight operations with data products for delays and passenger traffic.'")
    
    while True:
        try:
            user_input = input("\n> ")
            if user_input.lower() in ['exit', 'quit']:
                break
            
            if not user_input.strip():
                continue

            print("\nThinking and generating code... (This may take a minute)")
            response = generate_response(user_input, context)
            
            print("\n--- LLM Response ---")
            # print(response) # Uncomment to debug full text
            
            print("\n--- Saving Files ---")
            saved_files = save_generated_files(response)
            
            if saved_files:
                print(f"\nSuccessfully generated {len(saved_files)} files.")
                print("Review the files and run the generation script when ready.")
            else:
                logging.warning("No files were automatically saved. Please check the raw response or try a clearer prompt.")
                # print(f"Raw Output Snippet: {response[:500]}...")

        except KeyboardInterrupt:
            print("\nExiting...")
            break

if __name__ == "__main__":
    main()