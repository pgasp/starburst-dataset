# src/shared_tools/env_utils.py

import os
from dotenv import load_dotenv

def load_project_env(caller_file_path):
    """
    Loads environment variables from two locations, based on the calling script's location.
    
    1. Loads the global .env from the project root (one level above the calling script's directory).
    2. Loads the local .env from the calling script's directory (e.g., e_commerce/).
    
    Local variables always override global ones.
    
    Args:
        caller_file_path (str): The absolute path of the file calling this function (i.e., __file__).
    """
    
    # 1. Get the directory of the calling script (e.g., /path/to/starburst-dataset/e_commerce)
    caller_dir = os.path.dirname(os.path.abspath(caller_file_path))
    
    # 2. FIX: Calculate the path to the project root (one level up)
    # This finds the directory containing the caller_dir (e.g., /path/to/starburst-dataset)
    project_root = os.path.abspath(os.path.join(caller_dir, '..'))
    root_env_path = os.path.join(project_root, '.env')
    
    # Load the global environment variables (lowest priority)
    if os.path.exists(root_env_path):
        # Setting verbose to True helps debug path issues if the error returns.
        load_dotenv(dotenv_path=root_env_path, verbose=False)
    
    # 3. Load the local environment variables (highest priority)
    # The local .env is expected to be in the same directory as the caller script
    local_env_path = os.path.join(caller_dir, '.env')
    if os.path.exists(local_env_path):
        # We use override=True to ensure local settings overwrite global settings
        load_dotenv(dotenv_path=local_env_path, override=True, verbose=False)