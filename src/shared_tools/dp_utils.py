import os
import yaml
import logging
from typing import List, Dict, Union, Any

# Define the structure for a single Data Product entry
DataProductEntry = Dict[str, Union[str, int, List[Dict]]]

# Define the structure for a Domain entry
DomainCatalogEntry = Dict[str, Union[str, List[DataProductEntry], str]]

def _load_yaml_without_env(filepath: str) -> Union[Dict, None]:
    """
    Loads a YAML file without performing environment variable substitution.
    """
    try:
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        # We suppress verbose warnings for non-data product YAML files (like .env files)
        if not ('data_product' in filepath or '_dp' in filepath):
             logging.warning(f"Skipping YAML file {filepath} due to parsing error: {e}")
        return None

def scan_data_products_for_catalog(root_dir: str = "data_products") -> List[DomainCatalogEntry]:
    """
    Scans the data_products directory, grouping results by domain (directory).
    Each domain object includes its data script path and a list of Data Product objects.
    """
    domain_catalog: Dict[str, DomainCatalogEntry] = {}
    
    # Adjust root directory path relative to the shared_tools module location if necessary
    if not os.path.isdir(root_dir):
        root_dir = os.path.join(os.path.dirname(__file__), '..', '..', root_dir)
        
    if not os.path.isdir(root_dir):
        logging.error(f"Data products directory not found at: {root_dir}")
        return []
    
    # Iterate through immediate subdirectories of the root_dir (which represent domains)
    for domain_name in os.listdir(root_dir):
        domain_path = os.path.join(root_dir, domain_name)
        if not os.path.isdir(domain_path) or domain_name.startswith('.'):
            continue

        data_script_path = 'N/A'
        domain_products: List[DataProductEntry] = []
        
        # 1. Scan for the single data script (*_data.py)
        for file in os.listdir(domain_path):
            if file.endswith('_data.py'):
                data_script_path = os.path.relpath(os.path.join(domain_path, file))
                break

        # 2. Scan for all Data Product YAMLs in the domain folder
        for filename in os.listdir(domain_path):
            if filename.endswith(('.yaml', '.yml')) and ('data_product' in filename or '_dp' in filename):
                filepath = os.path.join(domain_path, filename)
                config = _load_yaml_without_env(filepath)
                
                if config and config.get('name') and config.get('domain'):
                    
                    # Consolidate all views (standard and materialized)
                    all_views = []
                    for view in config.get('views', []):
                        all_views.append({
                            'name': view.get('name', 'N/A'), 
                            'type': 'View', 
                            'description': view.get('description', 'N/A'),
                            'columns': view.get('columns', []) 
                        })
                        
                    for mv in config.get('materialized_views', []):
                        all_views.append({
                            'name': mv.get('name', 'N/A'), 
                            'type': 'Materialized View', 
                            'description': mv.get('description', 'N/A'),
                            'columns': mv.get('columns', [])
                        })
                        
                    # Create the Data Product entry
                    dp_entry = {
                        'name': config['name'],
                        'description': config.get('description', config.get('summary', 'No description available.')), 
                        'file_path': os.path.relpath(filepath),
                        'total_views': len(all_views),
                        'views': all_views
                    }
                    domain_products.append(dp_entry)
        
        # 3. Create the Domain Catalog Entry if any Data Products were found
        if domain_products:
            # Domain name from the YAML is authoritative
            canonical_domain_name = domain_products[0]['domain'] if domain_products and 'domain' in domain_products[0] else domain_name
            
            # Use a generic description instead of borrowing from the first DP
            domain_desc_display = f"Business Domain: {canonical_domain_name}"

            domain_catalog[domain_name] = {
                'domain_name': canonical_domain_name,
                'folder_name': domain_name,
                'data_script_path': data_script_path,
                'domain_description': domain_desc_display, 
                'data_products': domain_products
            }
                    
    return list(domain_catalog.values())