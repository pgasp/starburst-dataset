# src/shared_tools/deploy.py

import os
import sys
import json
import time
import yaml
import requests
import argparse
import re 
# Removed: from dotenv import load_dotenv (no longer needed here)

# Global State Dictionary for lazy initialization
# All shared config (session, URL, base location) will be stored here.
_STATE = {
    'session': None,
    'sb_url': None,
    'base_location': None
}

def _initialize_session():
    """
    Initializes the global session object and configuration variables
    if they haven't been initialized already. Assumes environment variables
    have been loaded by the calling script.
    """
    if _STATE['session'] is not None:
        return

    # Read Environment Variables (They MUST be present now)
    # This ensures we read the values after the main script has loaded the .env files
    sb_url = os.getenv("SB_URL")
    sb_user = os.getenv("SB_USER")
    sb_pass = os.getenv("SB_PASSWORD")
    sb_domain_location_base = os.getenv("SB_DOMAIN_LOCATION_BASE")

    if not all([sb_url, sb_user, sb_pass, sb_domain_location_base]):
        print("Error: Missing critical configuration (SB_URL, SB_USER, SB_PASSWORD, or SB_DOMAIN_LOCATION_BASE).")
        print("Ensure 'load_project_env(__file__)' was called successfully in the main script.")
        sys.exit(1)

    # Setup Global Session
    session = requests.Session()
    session.auth = (sb_user, sb_pass)
    session.headers.update({"Content-Type": "application/json"})
    
    # Store in global state
    _STATE['session'] = session
    _STATE['sb_url'] = sb_url
    _STATE['base_location'] = sb_domain_location_base

# --- Helper Functions (Updated to call _initialize_session) ---

def parse_duration_to_minutes(duration_str):
    """
    Converts a duration string (e.g., '4h', '90m', '1d') into total minutes.
    """
    if not isinstance(duration_str, str): raise ValueError("Duration must be a string.")
    match = re.match(r"(\d+)([mhd])$", duration_str.lower().strip())
    if not match: raise ValueError(f"Invalid duration format: '{duration_str}'. Must be like '30m', '4h', or '2d'.")
    value = int(match.group(1)); unit = match.group(2)
    if unit == 'm': return value
    elif unit == 'h': return value * 60
    elif unit == 'd': return value * 1440
    return value

def find_existing_product(product_name):
    _initialize_session() # Ensure session is ready
    session = _STATE['session']
    sb_url = _STATE['sb_url']

    search_options = {"searchString": product_name, "limit": 10}
    params = {"searchOptions": json.dumps(search_options)}
    url = f"{sb_url}/api/v1/dataProduct/products"
    resp = session.get(url, params=params)
    if resp.status_code == 200:
        for product in resp.json():
            if product['name'] == product_name: return product['id']
    return None

def load_yaml(filepath):
    """Loads a YAML file and performs environment variable substitution (e.g., ${VAR_NAME})."""
    try:
        with open(filepath, 'r') as f:
            yaml_content = f.read()
            # This relies on os.environ being populated by the calling script's load_project_env
            processed_content = os.path.expandvars(yaml_content) 
            return yaml.safe_load(processed_content)
    except Exception as e:
        print(f"Error reading and processing YAML file {filepath}: {e}")
        return None

def construct_payload(config, domain_id):
    # This function is unchanged as it does not rely on global session/env
    payload = {
        "name": config['name'], "catalogName": config['catalog'],
        "schemaName": config['schema'], "dataDomainId": domain_id,
        "summary": config.get('summary', ''), "description": config.get('description', ''),
        "owners": config.get('owners', []), "views": [], "materializedViews": []
    }

    # Add Materialized Views with Validation
    for mv in config.get('materialized_views', []):
        mv_props = {}
        
        # --- 1. Refresh Strategy (Mutually Exclusive) ---
        if 'refresh_interval' in mv and 'cron' in mv:
            print(f"Error: MV '{mv['name']}' cannot have both 'refresh_interval' and 'cron'."); sys.exit(1)
        if 'refresh_interval' in mv: 
            interval_str = mv['refresh_interval']
            mv_props['refresh_interval'] = interval_str
        elif 'cron' in mv: mv_props['refresh_schedule'] = mv['cron']
            
        # --- 2. MV Time Validation ---
        duration_str = mv.get('max_import_duration')
        if 'refresh_interval' in mv_props and duration_str:
            try:
                interval_min = parse_duration_to_minutes(interval_str)
                duration_min = parse_duration_to_minutes(duration_str)
                if interval_min <= duration_min * 1.10:
                    raise ValueError(f"MV '{mv['name']}': The refresh interval ({interval_str}) must be significantly LONGER than the max import duration ({duration_str}).")
            except ValueError as e:
                print(f"Deployment failed due to MV validation: {e}"); sys.exit(1)
        
        # Map properties (ensuring all are strings for the API payload)
        optional_fields = {
            'incremental_column': 'incremental_column', 'max_import_duration': 'max_import_duration',
            'grace_period': 'grace_period', 'failed_refresh_limit': 'failed_refresh_limit', 'refresh_schedule_timezone': 'refresh_schedule_timezone'
        }

        for yaml_key, api_key in optional_fields.items():
            if yaml_key in mv:
                value = mv[yaml_key]
                mv_props[api_key] = str(value) if not isinstance(value, str) else value

        mv_payload = {
            "name": mv['name'], "description": mv.get('description', ''), "definitionQuery": mv['query'],
            "columns": mv.get('columns', []), "markedForDeletion": False, "definitionProperties": mv_props
        }
        payload['materializedViews'].append(mv_payload)

    # Add standard Views (Similarly structured)
    for v in config.get('views', []):
        payload['views'].append({
            "name": v['name'], "description": v.get('description', ''),
            "definitionQuery": v['query'], "viewSecurityMode": v.get('security_mode', 'DEFINER'),
            "columns": v.get('columns', []), "markedForDeletion": False
        })

    return payload

def poll_status(status_url):
    _initialize_session() # Ensure session is ready
    session = _STATE['session']
    
    print("   > Polling status...", end="", flush=True)
    while True:
        try:
            resp = session.get(status_url)
            if resp.status_code != 200: print(f"\n   x Error polling: {resp.status_code}"); return False
            state = resp.json()
            if state.get('isFinalStatus') is True:
                print(""); return state.get('status') == 'COMPLETED'
            print(".", end="", flush=True); time.sleep(2)
        except KeyboardInterrupt: return False

def deploy_dataproduct_file(filepath):
    _initialize_session() # Ensure session is ready
    # Read variables from the initialized state
    session = _STATE['session']
    sb_url = _STATE['sb_url']
    base_location = _STATE['base_location']
    
    config = load_yaml(filepath)
    if not config: return False
    
    # 1. ENFORCE DOMAIN RULE: Read domain exclusively from YAML
    domain_name = config.get('domain')
    if not domain_name:
         raise ValueError(f"Deployment failed for {os.path.basename(filepath)}: 'domain' field is mandatory and missing.")

    print(f"\n--- Processing: {config['name']} ({os.path.basename(filepath)}) ---")

    try:
        # 2. Resolve Domain ID 
        # Pass the initialized session, URL, and base_location explicitly
        domain_id = get_or_create_domain_id(
            domain_name, 
            session, 
            sb_url, 
            base_location
        ) 
        
        # 3. Check Existence & Construct Payload
        product_id = find_existing_product(config['name'])
        payload = construct_payload(config, domain_id) 

        # 4. Create or Update
        if product_id:
            print(f"   > Updating existing product (ID: {product_id})...")
            url = f"{sb_url}/api/v1/dataProduct/products/{product_id}"
            resp = session.put(url, json=payload)
        else:
            print(f"   > Creating new product...")
            url = f"{sb_url}/api/v1/dataProduct/products"
            resp = session.post(url, json=payload)

        resp.raise_for_status()
        product_data = resp.json()
        product_id = product_data['id']

        # 5. Publish
        print("   > Triggering Publish workflow...")
        publish_url = f"{sb_url}/api/v1/dataProduct/products/{product_id}/workflows/publish"
        pub_resp = session.post(publish_url, params={"force": "true"})
        
        if pub_resp.status_code == 202:
            status_url = pub_resp.headers.get("Location")
            return poll_status(status_url)
        else:
            print(f"   x Failed to trigger publish: {pub_resp.text}"); return False

    except Exception as e:
        print(f"   x Deployment Error: {e}"); return False

def scan_and_deploy(folder_path):
    # This function primarily iterates and calls deploy_dataproduct_file,
    # so no major changes needed here.
    if not os.path.isdir(folder_path): print(f"Error: Directory '{folder_path}' does not exist."); sys.exit(1)
    files = [f for f in os.listdir(folder_path) if f.endswith(('.yaml', '.yml'))]
    if not files: print(f"No .yaml files found in {folder_path}"); return
    print(f"Found {len(files)} Data Product definition(s) in '{folder_path}'\n")
    success_count = 0
    for filename in files:
        full_path = os.path.join(folder_path, filename)
        try:
            if deploy_dataproduct_file(full_path): success_count += 1
        except ValueError as e:
             print(f"\n--- SKIPPING {os.path.basename(full_path)} ---")
             print(f"!!! Validation Failed: {e}")
            
    print(f"\nSUMMARY: Successfully deployed {success_count}/{len(files)} Data Products.")

# --- Domain Management Functions (unchanged signature, relies on caller) ---

def create_data_domain(domain_name: str, session: requests.Session, sb_url: str, base_location: str):
    """
    Creates a new data product domain with a specific schema location provided by the caller.
    
    Returns: The UUID of the created domain.
    """
    
    # 1. Generate URL-safe path: base_location + safe_domain_name + /
    safe_domain_name = domain_name.lower().replace(" ", "-").replace("&", "and")
    schema_location = f"{base_location}{safe_domain_name}/"
    
    url = f"{sb_url}/api/v1/dataProduct/domains"
    payload = {"name": domain_name, "schemaLocation": schema_location}
    
    print(f"   > Domain '{domain_name}' not found. Attempting to create it with location: {schema_location}...")
    
    resp = session.post(url, json=payload)
    
    if resp.status_code == 200:
        domain_data = resp.json()
        domain_id = domain_data['id']
        print(f"   ✓ Domain '{domain_name}' created successfully (ID: {domain_id}).")
        return domain_id
    elif resp.status_code == 409:
        # If conflict, retry lookup
        print(f"   ! Conflict detected during creation. Retrying lookup for '{domain_name}'...")
        # Since this function is part of the package, it should not access the global state directly.
        # It relies on the caller (deploy_dataproduct_file) to manage the retry logic, 
        # but for simplicity and completeness, we keep the original retry pattern which implicitly
        # calls get_or_create_domain_id which is okay since it's called with the correct
        # arguments from deploy_dataproduct_file.
        # However, to avoid circular logic, we call the logic directly.
        
        # We recursively call get_or_create_domain_id with the provided arguments
        return get_or_create_domain_id(domain_name, session, sb_url, base_location)
    else:
        raise Exception(f"Failed to create domain '{domain_name}'. Status: {resp.status_code}, Response: {resp.text}")


def get_or_create_domain_id(domain_name: str, session: requests.Session, sb_url: str, base_location: str):
    """
    Gets the domain ID by name. If not found, calls create_data_domain, passing the base_location.
    
    Returns: The UUID of the domain.
    """
    url = f"{sb_url}/api/v1/dataProduct/domains"
    
    # 1. Try to find the domain
    resp = session.get(url)
    resp.raise_for_status()
    
    for d in resp.json():
        if d['name'] == domain_name:
            print(f"   ✓ Domain '{domain_name}' found.")
            return d['id']
            
    # 2. If not found, create it, passing all necessary arguments
    return create_data_domain(domain_name, session, sb_url, base_location)

if __name__ == "__main__":
    # NOTE: The __main__ block is for standalone testing of the utility, 
    # not the primary execution method in your project.
    
    # If run directly, we must load the environment variables manually here.
    # The 'load_project_env' logic would need to be replicated or imported.
    # Since we can't easily replicate that here, we will just assume standard dotenv loading
    # if run standalone (which requires a .env in the same directory, or environment vars set).
    # Since this is a package utility, we should skip loading logic here.
    
    parser = argparse.ArgumentParser(description="Deploy Data Products from YAML definitions.")
    parser.add_argument("--folder", type=str, default="/Users/pascal.gasp/git/starburst-dataset/data_products/definitions", help="Path to the folder containing YAML files")
    
    args = parser.parse_args()
    
    # To run this standalone, you would need to manually set the environment variables
    # or uncomment the initialization call.
    # _initialize_session() 
    
    scan_and_deploy(args.folder)