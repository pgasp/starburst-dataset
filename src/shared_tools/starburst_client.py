import os
import requests
import json
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv

class StarburstClient:
    """
    A wrapper around the Starburst Enterprise API for Data Products.
    Handles authentication, session management, and CRUD operations.
    """
    def __init__(self):
        # Initialize configuration from .env
        self._load_config()

        self.base_url = os.getenv("SB_URL")
        self.user = os.getenv("SB_USER")
        self.password = os.getenv("SB_PASSWORD")
        self.base_location = os.getenv("SB_DOMAIN_LOCATION_BASE")
        
        if self.base_url and self.base_url.endswith('/'):
            self.base_url = self.base_url[:-1]

        self.session = requests.Session()
        if self.user and self.password:
            self.session.auth = (self.user, self.password)
        self.session.headers.update({"Content-Type": "application/json"})

    def _load_config(self):
        """Initializes Starburst configuration from .env files."""
        # Path relative to src/shared_tools/
        current_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
        
        local_env_path = os.path.join(root_dir, 'data_products/.env')
        
        if os.path.exists(local_env_path):
            load_dotenv(dotenv_path=local_env_path, override=True, verbose=False)

    def health_check(self) -> tuple[bool, str]:
        """Checks connectivity to the Starburst control plane."""
        if not self.base_url:
            return False, "SB_URL not configured."
        try:
            url = f"{self.base_url}/api/v1/dataProduct/domains"
            resp = self.session.get(url, timeout=5)
            if 200 <= resp.status_code < 300:
                return True, "Connection successful."
            return False, f"API call failed: {resp.status_code}"
        except Exception as e:
            return False, f"Connection failed: {e}"

    # ==========================
    # DOMAINS
    # ==========================
    def get_domains(self) -> List[Dict]:
        """List all data product domains."""
        url = f"{self.base_url}/api/v1/dataProduct/domains"
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()

    def create_domain(self, name: str, description: str = None) -> Dict:
        """Create a data product domain."""
        url = f"{self.base_url}/api/v1/dataProduct/domains"
        
        # Auto-generate schema location if base is provided
        schema_location = None
        if self.base_location:
            safe_name = name.lower().replace(" ", "-").replace("&", "and")
            schema_location = f"{self.base_location}{safe_name}/"

        payload = {"name": name}
        if schema_location: payload["schemaLocation"] = schema_location
        if description: payload["description"] = description

        resp = self.session.post(url, json=payload)
        
        # Handle conflict (409) gracefully by fetching existing
        if resp.status_code == 409:
            # print(f"   ! Domain '{name}' exists. Fetching ID...")
            domains = self.get_domains()
            for d in domains:
                if d['name'] == name: return d
        
        resp.raise_for_status()
        return resp.json()

    # ==========================
    # DATA PRODUCTS
    # ==========================
    def search_products(self, search_string: str) -> List[Dict]:
        """Search for data products by name."""
        url = f"{self.base_url}/api/v1/dataProduct/products"
        search_options = {"searchString": search_string, "limit": 10}
        params = {"searchOptions": json.dumps(search_options)}
        
        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def get_product(self, product_id: str) -> Dict:
        """Get a specific data product."""
        url = f"{self.base_url}/api/v1/dataProduct/products/{product_id}"
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()

    def create_product(self, payload: Dict) -> Dict:
        """Create a new data product."""
        url = f"{self.base_url}/api/v1/dataProduct/products"
        resp = self.session.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def update_product(self, product_id: str, payload: Dict) -> Dict:
        """Update an existing data product."""
        url = f"{self.base_url}/api/v1/dataProduct/products/{product_id}"
        resp = self.session.put(url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def trigger_publish(self, product_id: str) -> str:
        """Triggers the publish workflow and returns the status URL."""
        url = f"{self.base_url}/api/v1/dataProduct/products/{product_id}/workflows/publish"
        resp = self.session.post(url, params={"force": "true"})
        resp.raise_for_status()
        return resp.headers.get("Location")

    def get_status(self, status_url: str) -> Dict:
        """Checks the status of an async workflow."""
        resp = self.session.get(status_url)
        resp.raise_for_status()
        return resp.json()

    # ==========================
    # TAGS & METADATA
    # ==========================
    def update_product_tags(self, product_id: str, tags: List[str]):
        """Replace tags for a product."""
        url = f"{self.base_url}/api/v1/dataProduct/tags/products/{product_id}"
        payload = [{"value": t} for t in tags]
        resp = self.session.put(url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def get_catalogs(self) -> List[Dict]:
        """Get target catalogs suitable for Data Products."""
        url = f"{self.base_url}/api/v1/dataProduct/catalogs"
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()