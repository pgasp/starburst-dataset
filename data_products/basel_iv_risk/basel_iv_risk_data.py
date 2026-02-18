# basel_iv_risk/basel_iv_risk_data.py

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, date, timedelta
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

fake = Faker('fr_FR') # Use French locale for more relevant company names

# --- Configuration (Volume) ---
NUM_CUSTOMERS = 5000
NUM_LOANS = 15000
NUM_COLLATERALS = 10000
NUM_GUARANTEES = 2000
NUM_SECURITIZATIONS = 500

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["BASEL_RAW_CATALOG"],
            "schema": os.environ["BASEL_RAW_SCHEMA"],
            "location": os.environ["BASEL_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for BASEL IV: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the BASEL variables in your local basel_iv_risk/.env file.")
        sys.exit(1)

def generate_basel_iv_data():
    logging.info("Starting Basel IV Credit Risk data generation with improved logic...")

    # --- 1. Customers (Counterparties) ---
    counterparty_types = ['Corporate', 'SME', 'Retail', 'Sovereign']
    business_segments = ['Commercial Real Estate', 'Large Corporate', 'Trade Finance', 'Retail Banking', 'Public Sector']
    external_ratings = ['AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC']
    customers = []
    for i in range(NUM_CUSTOMERS):
        internal_rating = random.randint(1, 10)
        # IMPROVEMENT: Correlate PD with Internal Rating. Higher rating (worse) -> higher PD.
        base_pd = 0.0005 * (internal_rating ** 2)
        pd_internal = round(base_pd + random.uniform(-0.005, 0.005), 4)
        pd_internal = max(0.0001, min(pd_internal, 0.9)) # Clamp PD to a realistic range

        customers.append({
            'CustomerID': f'CUST-{i + 1001}',
            'CustomerName': fake.company() if random.random() > 0.1 else fake.name(),
            'CounterpartyType': random.choices(counterparty_types, weights=[0.3, 0.2, 0.4, 0.1], k=1)[0],
            'BusinessSegment': random.choice(business_segments),
            'InternalRating': internal_rating,
            'ExternalRating': random.choice(external_ratings),
            'PD_Internal': pd_internal
        })
    customers_df = pd.DataFrame(customers)
    customer_ids = customers_df['CustomerID'].tolist()

    # --- 2. Loans (Exposures) ---
    loan_types = {'Mortgage': 0.20, 'Term Loan': 1.0, 'Revolving Credit': 0.50, 'Project Finance': 1.0}
    loans = []
    for i in range(NUM_LOANS):
        loan_type = random.choice(list(loan_types.keys()))
        commitment = random.randint(50000, 10000000)
        drawn = commitment * random.uniform(0.1, 1.0) if loan_type != 'Term Loan' else commitment
        loans.append({
            'LoanID': f'LOAN-{i + 5001}',
            'CustomerID': random.choice(customer_ids),
            'LoanType': loan_type,
            'CommitmentAmount': float(commitment),
            'DrawnAmount': float(drawn),
            'CCF': loan_types[loan_type], # Credit Conversion Factor
            'LGD': round(random.uniform(0.10, 0.75), 2) # Loss Given Default
        })
    loans_df = pd.DataFrame(loans)
    loan_ids = loans_df['LoanID'].tolist()

    # --- 3. Collateral (Suretés) ---
    collateral_types = {'Real Estate': 0.15, 'Financial Instruments': 0.25, 'Cash': 0.0}
    collaterals = []
    loan_ids_with_collateral = random.sample(loan_ids, k=NUM_COLLATERALS)
    for i, loan_id in enumerate(loan_ids_with_collateral):
        collateral_type = random.choice(list(collateral_types.keys()))
        collaterals.append({
            'CollateralID': f'COL-{i + 2001}',
            'LoanID': loan_id,
            'CollateralType': collateral_type,
            'MarketValue': float(random.randint(25000, 2000000)),
            'RegulatoryHaircut': collateral_types[collateral_type]
        })
    collateral_df = pd.DataFrame(collaterals)

    # --- 4. Guarantees ---
    guarantor_types = ['Sovereign', 'Corporate']
    guarantees = []
    loan_ids_with_guarantee = random.sample(loan_ids, k=NUM_GUARANTEES)
    for i, loan_id in enumerate(loan_ids_with_guarantee):
        guarantees.append({
            'GuaranteeID': f'GUAR-{i+8001}',
            'LoanID': loan_id,
            'GuarantorName': fake.company(),
            'GuarantorType': random.choice(guarantor_types),
            'GuaranteedAmount': float(random.randint(10000, 500000))
        })
    guarantees_df = pd.DataFrame(guarantees)

    # --- 5. Provisions (IFRS9) ---
    provisioning_stages = ['Stage 1', 'Stage 2', 'Stage 3']
    provisions = []
    today = date.today()
    for loan_id in loan_ids:
        stage = random.choices(provisioning_stages, weights=[0.85, 0.10, 0.05], k=1)[0]
        loan_amount = loans_df[loans_df['LoanID'] == loan_id]['DrawnAmount'].iloc[0]
        ecl_factor = {'Stage 1': 0.01, 'Stage 2': 0.15, 'Stage 3': 0.50}
        # IMPROVEMENT: Use varied reporting dates for time-series analysis potential
        reporting_date = today - timedelta(days=random.randint(0, 365))
        provisions.append({
            'ProvisionID': f'PROV-{random.randint(10000, 99999)}',
            'LoanID': loan_id,
            'ReportingDate': reporting_date,
            'ProvisioningStage': stage,
            'ECL_Amount': round(loan_amount * ecl_factor[stage] * random.uniform(0.8, 1.2), 2)
        })
    provisions_ifrs9_df = pd.DataFrame(provisions)

    # --- 6. Securitization Positions ---
    tranche_types = {'Senior': 0.20, 'Mezzanine': 0.60, 'Junior': 1.25}
    securitizations = []
    for i in range(NUM_SECURITIZATIONS):
        tranche = random.choice(list(tranche_types.keys()))
        securitizations.append({
            'PositionID': f'SECPOS-{i + 3001}',
            'DealName': f'DEAL-{random.randint(100, 120)}',
            'TrancheType': tranche,
            'RetainedAmount': float(random.randint(1000000, 25000000)),
            'RiskWeight_SA': tranche_types[tranche]
        })
    securitization_positions_df = pd.DataFrame(securitizations)

    logging.info(f"Generated {len(customers_df)} customers, {len(loans_df)} loans, and {len(collateral_df)} collateral records.")

    return {
        "customers": customers_df,
        "loans": loans_df,
        "collateral": collateral_df,
        "guarantees": guarantees_df,
        "provisions_ifrs9": provisions_ifrs9_df,
        "securitization_positions": securitization_positions_df
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Basel IV data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()

    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"

    try:
        engine = create_engine(engine_string)

        if not args.deploy_only:
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                data_tables = generate_basel_iv_data()
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")

        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)

        logging.info("Basel IV Credit Risk data pipeline executed successfully.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
