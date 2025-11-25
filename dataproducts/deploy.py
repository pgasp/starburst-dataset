import os
import sys
import json
import time
import yaml
import requests
import argparse
import re 
from dotenv import load_dotenv

# 1. Load Environment Variables
#load_dotenv()

SB_URL = os.getenv("SB_URL")
SB_USER = os.getenv("SB_USER")
SB_PASS = os.getenv("SB_PASSWORD")
SB_DOMAIN_LOCATION_BASE = os.getenv("SB_DOMAIN_LOCATION_BASE") 

if not all([SB_URL, SB_USER, SB_PASS, SB_DOMAIN_LOCATION_BASE]):
    print("Error: Missing configuration. Please check your .env file for SB_HOST, SB_USER, SB_PASSWORD, AND SB_DOMAIN_LOCATION_BASE.")
    sys.exit(1)

# Setup Global Session
session = requests.Session()
session.auth = (SB_USER, SB_PASS)
session.headers.update({"Content-Type": "application/json"})

# --- Helper Functions ---

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
    search_options = {"searchString": product_name, "limit": 10}
    params = {"searchOptions": json.dumps(search_options)}
    url = f"{SB_URL}/api/v1/dataProduct/products"
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
            processed_content = os.path.expandvars(yaml_content)
            return yaml.safe_load(processed_content)
    except Exception as e:
        print(f"Error reading and processing YAML file {filepath}: {e}")
        return None

def construct_payload(config, domain_id):
    # This function is the core of the Data Product schema assembly and validation.
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
    config = load_yaml(filepath)
    if not config: return False
    
    # 1. ENFORCE DOMAIN RULE: Read domain exclusively from YAML
    domain_name = config.get('domain')
    if not domain_name:
         raise ValueError(f"Deployment failed for {os.path.basename(filepath)}: 'domain' field is mandatory and missing.")

    print(f"\n--- Processing: {config['name']} ({os.path.basename(filepath)}) ---")

    try:
        # 2. Resolve Domain ID 
        domain_id = get_or_create_domain_id(
            domain_name, 
            session, 
            SB_URL, 
            SB_DOMAIN_LOCATION_BASE
        ) 
        
        # 3. Check Existence & Construct Payload
        product_id = find_existing_product(config['name'])
        payload = construct_payload(config, domain_id) 

        # 4. Create or Update
        if product_id:
            print(f"   > Updating existing product (ID: {product_id})...")
            url = f"{SB_URL}/api/v1/dataProduct/products/{product_id}"
            resp = session.put(url, json=payload)
        else:
            print(f"   > Creating new product...")
            url = f"{SB_URL}/api/v1/dataProduct/products"
            resp = session.post(url, json=payload)

        resp.raise_for_status()
        product_data = resp.json()
        product_id = product_data['id']

        # 5. Publish
        print("   > Triggering Publish workflow...")
        publish_url = f"{SB_URL}/api/v1/dataProduct/products/{product_id}/workflows/publish"
        pub_resp = session.post(publish_url, params={"force": "true"})
        
        if pub_resp.status_code == 202:
            status_url = pub_resp.headers.get("Location")
            return poll_status(status_url)
        else:
            print(f"   x Failed to trigger publish: {pub_resp.text}"); return False

    except Exception as e:
        print(f"   x Deployment Error: {e}"); return False

def scan_and_deploy(folder_path):
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

# --- Domain Management Functions ---

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
        # If conflict, retry lookup using the updated function signature
        print(f"   ! Conflict detected during creation. Retrying lookup for '{domain_name}'...")
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
    parser = argparse.ArgumentParser(description="Deploy Data Products from YAML definitions.")
    parser.add_argument("--folder", type=str, default="/Users/pascal.gasp/git/starburst-dataset/data_products/definitions", help="Path to the folder containing YAML files")
    
    args = parser.parse_args()
    scan_and_deploy(args.folder)