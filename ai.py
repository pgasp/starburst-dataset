import sys
import os
import logging

# Ensure we can import from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from shared_tools.llm_utils import get_llm_model
from shared_tools.ai_utils import (
    load_project_context, 
    generate_response, 
    parse_generated_files, 
    save_files
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def main():
    print("--- Starburst Data Product Factory (CLI) ---")
    
    # 1. Initialize Model
    try:
        model = get_llm_model()
    except ValueError as e:
        print(f"Configuration Error: {e}")
        return

    # 2. Load Context
    logging.info("Loading project context...")
    context, files_loaded = load_project_context()
    logging.info(f"Loaded context from {len(files_loaded)} files.")
    
    print("\nReady. Example: 'Create a dataset for retail banking with churn analysis.'")
    print("Type 'exit' to quit.")
    
    chat_session = None

    # 3. Main Loop
    while True:
        try:
            user_input = input("\n> ")
            if user_input.lower() in ['exit', 'quit']:
                break
            
            if not user_input.strip():
                continue

            print("\nThinking...")
            response_text, chat_session = generate_response(model, user_input, context, chat_session)
            
            # 4. Guardrail & Save
            parsed_files = parse_generated_files(response_text)
            
            if parsed_files:
                print("\n--- Files Generated ---")
                saved_paths = save_files(parsed_files)
                print(f"Successfully saved {len(saved_paths)} files.")
                for p in saved_paths:
                    print(f"  - {p}")
            else:
                print("\n--- Assistant Response ---")
                print(response_text)

        except KeyboardInterrupt:
            print("\nExiting...")
            break

if __name__ == "__main__":
    main()