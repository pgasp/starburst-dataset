# credit_risk_management/credit_risk_management_data.py

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
import uuid

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

fake = Faker('fr_FR') # Use French locale for more relevant company names

# --- Configuration (Volume) ---
NUM_COUNTERPARTIES = 5000
NUM_EXPOSURES = 25000
NUM_COLLATERALS = 15000
NUM_PROVISIONS = 25000
NUM_SECURITIZATION_DEALS = 50

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["RISK_RAW_CATALOG"],
            "schema": os.environ["RISK_RAW_SCHEMA"],
            "location": os.environ["RISK_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for CREDIT RISK: {e} ---")
        logging.error("Please check for SB_HOST in your root .env, and the RISK_ variables in your local credit_risk_management/.env file.")
        sys.exit(1)

def generate_risk_data():
    logging.info("Starting Bâle IV credit risk data generation...")

    # --- 1. Contreparties (Counterparties) ---
    counterparties = []
    basel_segments = ['Corporate', 'Sovereign', 'Retail', 'SME', 'Bank']
    ratings = ['AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC']
    for i in range(NUM_COUNTERPARTIES):
        counterparties.append({
            "counterparty_id": f"CTPY-{1000 + i}",
            "counterparty_name": fake.company(),
            "basel_iv_segment": random.choice(basel_segments),
            "internal_rating": random.choice(ratings[2:]),
            "external_rating_sp": random.choice(ratings),
            "country_iso": fake.country_code()
        })
    df_counterparties = pd.DataFrame(counterparties)
    counterparty_ids = df_counterparties['counterparty_id'].tolist()

    # --- 2. Expositions (Exposures) ---
    exposures = []
    product_types = ['Loan', 'Derivative', 'Revolving Credit Facility', 'Trade Finance']
    for i in range(NUM_EXPOSURES):
        exposures.append({
            "exposure_id": str(uuid.uuid4()),
            "counterparty_id": random.choice(counterparty_ids),
            "product_type": random.choice(product_types),
            "gross_exposure_eur": round(random.uniform(10000, 5000000), 2),
            "ccf_credit_conversion_factor": random.choices([0.2, 0.5, 1.0], weights=[30, 30, 40])[0],
            "pd_probability_of_default": round(random.uniform(0.001, 0.2), 4),
            "lgd_loss_given_default": round(random.uniform(0.1, 0.9), 2),
            "origination_date": fake.date_between(start_date='-5y', end_date='today')
        })
    df_expositions = pd.DataFrame(exposures)
    exposure_ids = df_expositions['exposure_id'].tolist()

    # --- 3. Garanties et Sûretés (Collaterals & Guarantees) ---
    collaterals = []
    collateral_types = ['Real Estate', 'Cash', 'Financial Instruments', 'Receivables', 'Third-Party Guarantee']
    for i in range(NUM_COLLATERALS):
        collaterals.append({
            "mitigant_id": f"CRM-{5000 + i}",
            "exposure_id": random.choice(exposure_ids),
            "mitigant_type": random.choice(collateral_types),
            "market_value_eur": round(random.uniform(5000, 1000000), 2),
            "guarantor_id": random.choice(counterparty_ids) if random.random() > 0.5 else None,
            "valuation_date": fake.date_between(start_date='-90d', end_date='today')
        })
    df_collaterals = pd.DataFrame(collaterals)

    # --- 4. Provisions IFRS9 ---
    provisions = []
    for exp_id in exposure_ids:
        provisions.append({
            "provision_id": str(uuid.uuid4()),
            "exposure_id": exp_id,
            "ifrs9_stage": random.choices([1, 2, 3], weights=[85, 10, 5])[0],
            "ecl_expected_credit_loss_eur": round(random.uniform(100, 50000), 2),
            "calculation_date": fake.date_this_month()
        })
    df_provisions = pd.DataFrame(provisions)

    # --- 5. Titrisation & Tranches (Securitization) ---
    tranches = []
    tranche_types = ['Senior', 'Mezzanine', 'Junior', 'Equity']
    for i in range(NUM_SECURITIZATION_DEALS):
        deal_id = f"SEC-DEAL-{200 + i}"
        for tranche_type in tranche_types:
            tranches.append({
                "tranche_id": str(uuid.uuid4()),
                "deal_id": deal_id,
                "tranche_type": tranche_type,
                "retained_amount_eur": round(random.uniform(1000000, 20000000), 2) if random.random() > 0.3 else 0,
                "notional_amount_eur": round(random.uniform(25000000, 100000000), 2),
                "issue_date": fake.date_between(start_date='-2y', end_date='-6m')
            })
    df_tranches = pd.DataFrame(tranches)

    logging.info(f"Generated {len(df_counterparties)} counterparties and {len(df_expositions)} exposures.")
    return {
        "raw_counterparties": df_counterparties,
        "raw_expositions": df_expositions,
        "raw_collaterals_guarantees": df_collaterals,
        "raw_provisions_ifrs9": df_provisions,
        "raw_securitization_tranches": df_tranches
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Bâle IV Credit Risk data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()

    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"

    try:
        engine = create_engine(engine_string)

        if not args.deploy_only:
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                data_tables = generate_risk_data()
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")

        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)

        logging.info("Credit Risk Management data pipeline executed successfully.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
