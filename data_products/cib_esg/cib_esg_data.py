# cib_esg/cib_esg_data.py

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
import sys
import logging
import argparse

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
# ------------------------------

# --- External Utility Imports ---
try:
    from shared_tools.lakehouse_utils import setup_schema, upload_to_starburst_parallel
    from shared_tools.deploy import scan_and_deploy
    from shared_tools.env_utils import load_project_env
except ImportError as e:
    logging.critical(f"FATAL ERROR: Could not import utility functions. Did you run 'pip install -e .' from the project root? Details: {e}")
    sys.exit(1)

# Load environment variables (from project root and local .env files)
load_project_env(__file__)

fake = Faker()

# --- Configuration (Volume) ---
NUM_CLIENTS = 2000
NUM_CREDIT_APPS = 5000
NUM_DEALS = 4000

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["CIB_ESG_RAW_CATALOG"],
            "schema": os.environ["CIB_ESG_RAW_SCHEMA"],
            "location": os.environ["CIB_ESG_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for CIB ESG: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the CIB_ESG variables in your local cib_esg/.env file.")
        sys.exit(1)

def generate_cib_esg_data():
    logging.info("Starting CIB ESG data generation...")

    # --- 1. Clients Master ---
    sectors = ['Oil & Gas', 'Utilities', 'Technology', 'Healthcare', 'Industrials', 'Consumer Goods', 'Financials', 'Renewable Energy']
    clients = []
    for i in range(NUM_CLIENTS):
        clients.append({
            "client_id": f"C{1000 + i}",
            "client_name": fake.company(),
            "industry_sector": random.choice(sectors),
            "hq_country": fake.country(),
            "annual_revenue_usd": random.randint(50000000, 10000000000)
        })
    clients_master_df = pd.DataFrame(clients)
    client_ids = clients_master_df['client_id'].tolist()

    # --- 2. Credit Applications ---
    app_statuses = ['Approved', 'Rejected', 'Pending']
    credit_applications = []
    for i in range(NUM_CREDIT_APPS):
        credit_applications.append({
            "application_id": f"APP-{50000 + i}",
            "client_id": random.choice(client_ids),
            "application_amount": random.randint(1000000, 500000000),
            "status": random.choice(app_statuses),
            "application_date": fake.date_between(start_date='-2y', end_date='today')
        })
    credit_applications_df = pd.DataFrame(credit_applications)

    # --- 3. ESG Risk Ratings ---
    providers = ['MSCI', 'Sustainalytics', 'ISS']
    esg_ratings = []
    for cid in client_ids:
        has_controversy = random.choices([True, False], weights=[15, 85], k=1)[0]
        overall_score = random.randint(10, 95) if not has_controversy else random.randint(5, 50)
        esg_ratings.append({
            "client_id": cid,
            "rating_provider": random.choice(providers),
            "overall_esg_score": overall_score,
            "environmental_score": random.randint(10, 95),
            "social_score": random.randint(10, 95),
            "governance_score": random.randint(10, 95),
            "has_controversies_flag": has_controversy,
            "rating_as_of_date": fake.date_between(start_date='-1y', end_date='-1m')
        })
    esg_risk_ratings_df = pd.DataFrame(esg_ratings)

    # --- 4. Deals Master ---
    deal_types = ['Green Bond', 'Sustainability-Linked Loan', 'General Corporate Purpose', 'Social Bond']
    deals = []
    for i in range(NUM_DEALS):
        deals.append({
            "deal_id": f"DEAL-{20000 + i}",
            "client_id": random.choice(client_ids),
            "deal_type": random.choices(deal_types, weights=[20, 15, 60, 5], k=1)[0],
            "deal_value_usd": random.randint(50000000, 1000000000),
            "close_date": fake.date_time_between(start_date='-3y', end_date='now')
        })
    deals_master_df = pd.DataFrame(deals)

    # --- 5. Portfolio Emissions ---
    emissions = []
    for cid in client_ids:
        emissions.append({
            "client_id": cid,
            "reporting_year": 2023,
            "scope1_emissions_tco2e": random.randint(1000, 500000),
            "scope2_emissions_tco2e": random.randint(5000, 2000000),
            "scope3_emissions_tco2e": random.randint(10000, 10000000)
        })
    portfolio_emissions_df = pd.DataFrame(emissions)

    logging.info(f"Generated {len(clients_master_df)} clients and related ESG datasets.")

    return {
        "clients_master": clients_master_df,
        "credit_applications": credit_applications_df,
        "esg_risk_ratings": esg_risk_ratings_df,
        "deals_master": deals_master_df,
        "portfolio_emissions": portfolio_emissions_df
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate CIB ESG data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()

    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"

    try:
        engine = create_engine(engine_string)

        if not args.deploy_only:
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                data_tables = generate_cib_esg_data()
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")

        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)

        logging.info("CIB ESG data pipeline executed successfully.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
