import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
import sys
from dotenv from dotead_doteivport load_dotenv
import logging

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
# ------------------------------

# Load environment variables
load_dotenv()
fake = Faker()

# --- External Utility Imports ---
# 1. Add the parent directory to the system path to find the 'utils' folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 2. Import the required utility functions
# NOTE: Assuming lakehouse_utils is in a 'utils' directory one level up.
def import_or_exit(module_path, import_names, error_message):
    try:
        module = __import__(module_path, fromlist=import_names)
        return [getattr(module, name) for name in import_names]
    except ImportError as e:
        logging.critical(f"{error_message}\nDetails: {e}")
        sys.exit(1)

setup_schema, upload_to_starburst_parallel = import_or_exit(
    "utils.lakehouse_utils",
    ["setup_schema", "upload_to_starburst_parallel"],
    "FATAL ERROR: Could not import utility functions from utils/lakehouse_utils. Please ensure the path and file exist."
)

deploy_dataproduct_file, = import_or_exit(
    "dataproducts.deploy",
    ["deploy_dataproduct_file"],
    "FATAL ERROR: Could not import utility functions from dataproducts/deploy. Please ensure the path and file exist."
)



# --- Configuration (Volume) ---
NUM_CLIENTS = 500
NUM_DEALS = 800
NUM_RATINGS = 1000 

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            # RAW DATA TARGET (Using ESG_RAW variables)
            "catalog": os.environ["ESG_RAW_CATALOG"], 
            "schema": os.environ["ESG_RAW_SCHEMA"],
            "location": os.environ["ESG_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for ESG: {e} ---")
        sys.exit(1)

def generate_esg_data():
    logging.info("Starting CIB ESG data generation...")
    today = datetime.now().date()
    
    # --- 1. ClientEntities (Base Dimension) ---
    clients = []
    industries = ['Energy', 'Manufacturing', 'Technology', 'Healthcare', 'Financials']
    for i in range(NUM_CLIENTS):
        clients.append({"ClientID": f"CL-{1000 + i}", "CompanyName": fake.company(), "Industry": random.choice(industries), "TotalRevenue": round(random.uniform(500000, 500000000), 2)})
    clients_df = pd.DataFrame(clients); client_ids = clients_df['ClientID'].tolist()

    # --- 2. ESG_Ratings (Time Series Data, used for latest score lookup) ---
    ratings = []
    for _ in range(NUM_RATINGS):
        esg_e = random.randint(30, 95); esg_s = random.randint(30, 95); esg_g = random.randint(30, 95)
        overall = round((esg_e + esg_s + esg_g) / 3, 2)
        ratings.append({"RatingID": f"RT-{len(ratings) + 1}", "ClientID": random.choice(client_ids), "Date": fake.date_between(start_date='-5y', end_date='today'), "OverallScore": overall, "EScore": esg_e, "SScore": esg_s, "GScore": esg_g})
    ratings_df = pd.DataFrame(ratings)

    # --- 3. Financing_Deals (Transaction Fact, links clients to E/G metrics) ---
    deals = []
    deal_types = ['Loan', 'Bond', 'Equity']; proceeds = ['General Corporate', 'Renewable Energy Project', 'Social Housing', 'M&A']
    for i in range(NUM_DEALS):
        deals.append({"DealID": f"DEAL-{2000 + i}", "ClientID": random.choice(client_ids), "DealDate": fake.date_between(start_date='-3y', end_date='today'), "DealType": random.choice(deal_types), "Amount": round(random.uniform(1000000, 500000000), 2), "Sector": random.choice(industries), "UseOfProceeds": random.choice(proceeds)})
    deals_df = pd.DataFrame(deals); deal_ids = deals_df['DealID'].tolist()

    # --- 4. E_Metrics (Environmental Data, linked to deals for project finance) ---
    e_metrics = []
    for i, did in enumerate(deal_ids):
        is_green = deals_df[deals_df['DealID'] == did]['UseOfProceeds'].iloc[0] == 'Renewable Energy Project'
        ghg_base = 5000 if not is_green else 100; renew_base = 0.1 if not is_green else 0.8
        e_metrics.append({"MetricID": f"E-{i + 1}", "DealID": did, "Year": random.randint(2022, 2025), "GHG_Emissions_Scope1": round(ghg_base * random.uniform(0.9, 1.1), 2), "GHG_Emissions_Scope2": round(ghg_base * random.uniform(0.5, 0.8), 2), "WaterUse": round(random.uniform(500, 50000), 2), "EnergyMix_Renewable": round(renew_base * random.uniform(0.8, 1.2), 2)})
    e_metrics_df = pd.DataFrame(e_metrics)

    # --- 5. S_Metrics (Social Data, used for granular social view) ---
    s_metrics = []
    for i, cid in enumerate(client_ids):
        s_metrics.append({"SocialMetricID": f"S-{i + 1}", "ClientID": cid, "Year": random.randint(2022, 2025), "EmployeeTurnover": round(random.uniform(0.05, 0.25), 2), "DiversityScore": round(random.uniform(0.30, 0.90), 2), "SafetyIncidents": random.randint(0, 5)})
    s_metrics_df = pd.DataFrame(s_metrics)

    # --- 6. G_Metrics (Governance Data, used for granular governance view) ---
    g_metrics = []
    for i, cid in enumerate(client_ids):
        g_metrics.append({"GovMetricID": f"G-{i + 1}", "ClientID": cid, "BoardDiversityPct": round(random.uniform(0.10, 0.40), 2), "AuditCommitteeIndependence": round(random.uniform(0.80, 1.00), 2), "CEO_PayRatio": random.randint(5, 50)})
    g_metrics_df = pd.DataFrame(g_metrics)

    logging.info(f"Generated {len(clients_df)} clients and {len(deals_df)} financing deals.")
    
    return {
        "client_entities": clients_df, "esg_ratings": ratings_df, "financing_deals": deals_df,
        "e_metrics": e_metrics_df, "s_metrics": s_metrics_df, "g_metrics": g_metrics_df
    }


if __name__ == "__main__":
    config = get_config()
    # Create the SQLAlchemy engine string for connection (without schema/catalog yet)
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"
    
    try:
        engine = create_engine(engine_string)
        deploy_dataproduct_file("cib_esg_dataproduct.yaml")
        # # 1. SETUP SCHEMA: Use externalized utility function
        # if setup_schema(engine, config['catalog'], config['schema'], config['location']):
            
        #     # 2. GENERATE DATA
        #     data_tables = generate_esg_data()
            
        #     # 3. UPLOAD DATA: Use externalized utility function for parallel upload
        #     upload_to_starburst_parallel(engine, config['schema'], data_tables)
        #     # 4. DEPLOY DATAPRODUCT: Use externalized utility function  
        #     deploy_dataproduct_file("cib_esg_dataproduct.yaml")
        #     logging.info("ESG data pipeline executed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
