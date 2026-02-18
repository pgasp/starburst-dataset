import os
import sys
import time
import yaml
import argparse
import re 
from typing import Dict, Any

# Import the new Client
from shared_tools.starburst_client import StarburstClient

# Initialize Client
_CLIENT = StarburstClient()

# ==============================================================================
# HELPERS (Logic & Validation)
# ==============================================================================

def starburst_health_check():
    """Wrapper for UI compatibility."""
    return _CLIENT.health_check()

def parse_duration_to_minutes(duration_str):
    if not isinstance(duration_str, str): raise ValueError("Duration must be a string.")
    match = re.match(r"(\d+)([mhd])$", duration_str.lower().strip())
    if not match: raise ValueError(f"Invalid duration: '{duration_str}'. Use '30m', '4h', or '2d'.")
    value = int(match.group(1)); unit = match.group(2)
    if unit == 'm': return value
    elif unit == 'h': return value * 60
    elif unit == 'd': return value * 1440
    return value

def load_yaml(filepath):
    try:
        with open(filepath, 'r') as f:
            content = os.path.expandvars(f.read()) 
            return yaml.safe_load(content)
    except Exception as e:
        print(f"Error reading YAML {filepath}: {e}")
        return None

def construct_payload(config, domain_id):
    """
    Maps YAML configuration to the Starburst API Payload structure.
    """
    payload = {
        "name": config['name'], 
        "catalogName": config['catalog'],
        "schemaName": config['schema'], 
        "dataDomainId": domain_id,
        "summary": config.get('summary', ''), 
        "description": config.get('description', ''),
        "owners": config.get('owners', []), 
        "views": [], 
        "materializedViews": []
    }

    # Process Views
    for v in config.get('views', []):
        payload['views'].append({
            "name": v['name'], 
            "description": v.get('description', ''),
            "definitionQuery": v['query'], 
            # "viewSecurityMode": v.get('security_mode', 'INVOKER'), 
            "columns": v.get('columns', []), 
            "markedForDeletion": False
        })

    # Process MVs (with validation)
    for mv in config.get('materialized_views', []):
        mv_props = {}
        refresh_val = mv.get('refresh_interval')
        cron_val = mv.get('cron')
        
        if refresh_val and cron_val:
            raise ValueError(f"MV '{mv['name']}' cannot have both 'refresh_interval' and 'cron'.")
        
        if refresh_val: mv_props['refresh_interval'] = refresh_val
        elif cron_val: mv_props['refresh_schedule'] = cron_val
            
        # Validation: Refresh vs Import Duration
        duration_str = mv.get('max_import_duration')
        if refresh_val and duration_str:
            rm = parse_duration_to_minutes(refresh_val)
            dm = parse_duration_to_minutes(duration_str)
            if rm <= dm * 1.1:
                raise ValueError(f"MV '{mv['name']}': refresh_interval must be > max_import_duration.")

        # Map other optional properties
        for key in ['incremental_column', 'grace_period', 'refresh_schedule_timezone']:
            if key in mv: mv_props[key] = str(mv[key])

        payload['materializedViews'].append({
            "name": mv['name'], 
            "description": mv.get('description', ''), 
            "definitionQuery": mv['query'], 
            "columns": mv.get('columns', []), 
            "markedForDeletion": False, 
            "definitionProperties": mv_props
        })

    return payload

def poll_workflow(status_url):
    print("   > Polling status...", end="", flush=True)
    while True:
        try:
            state = _CLIENT.get_status(status_url)
            if state.get('isFinalStatus'):
                status = state.get('status')
                print(f"\n   > Final Status: {status}")
                if status == 'ERROR':
                    print(f"   x Errors: {state.get('errors')}")
                return status == 'COMPLETED'
            print(".", end="", flush=True)
            time.sleep(2)
        except KeyboardInterrupt:
            return False

# ==============================================================================
# MAIN DEPLOYMENT LOGIC
# ==============================================================================

def deploy_single_file(filepath):
    config = load_yaml(filepath)
    if not config: return False
    
    print(f"\n--- Processing: {config['name']} ({os.path.basename(filepath)}) ---")

    try:
        # 1. Resolve Domain
        domain_name = config.get('domain')
        if not domain_name: raise ValueError("YAML missing 'domain' field.")
        
        domain_data = _CLIENT.create_domain(domain_name) # Idempotent (gets ID if exists)
        domain_id = domain_data['id']

        # 2. Check for Existing Product
        products = _CLIENT.search_products(config['name'])
        existing_id = None
        for p in products:
            if p['name'] == config['name']:
                existing_id = p['id']
                break
        
        # 3. Construct & Send Payload
        payload = construct_payload(config, domain_id)
        
        if existing_id:
            print(f"   > Updating existing product (ID: {existing_id})...")
            prod_data = _CLIENT.update_product(existing_id, payload)
        else:
            print(f"   > Creating new product...")
            prod_data = _CLIENT.create_product(payload)
            
        product_id = prod_data['id']

        # 4. Handle Tags (Optional)
        if 'tags' in config:
            _CLIENT.update_product_tags(product_id, config['tags'])

        # 5. Publish
        print("   > Triggering Publish workflow...")
        status_url = _CLIENT.trigger_publish(product_id)
        return poll_workflow(status_url)

    except Exception as e:
        print(f"   x Deployment Error: {e}")
        return False

def scan_and_deploy(folder_path):
    if not os.path.isdir(folder_path):
        print(f"Error: Directory '{folder_path}' does not exist.")
        sys.exit(1)
        
    files = [f for f in os.listdir(folder_path) if f.endswith(('.yaml', '.yml'))]
    if not files:
        print(f"No .yaml files found in {folder_path}")
        return
        
    print(f"Found {len(files)} Data Product definition(s) in '{folder_path}'\n")
    
    success = 0
    for f in files:
        full_path = os.path.join(folder_path, f)
        # Skip hidden files or context files
        if f.startswith('.'): continue
        
        try:
            if deploy_single_file(full_path):
                success += 1
        except ValueError as e:
             print(f"\n--- SKIPPING {os.path.basename(full_path)} ---")
             print(f"!!! Validation Failed: {e}")
            
    print(f"\nSUMMARY: Successfully deployed {success}/{len(files)} Data Products.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy Data Products from YAML definitions.")
    parser.add_argument("--folder", type=str, default="./definitions", help="Path to YAML folder")
    args = parser.parse_args()
    scan_and_deploy(args.folder)