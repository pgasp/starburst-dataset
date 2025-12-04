# risk_stress_test/risk_stress_test_data.py

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
import sys
# NEW IMPORT
import argparse
# Removed: from dotenv import load_dotenv (Now imported via env_utils)
import logging

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
# ------------------------------

# --- External Utility Imports (CLEANED) ---
# This assumes shared_tools has been installed via 'pip install -e .'
try:
    from shared_tools.lakehouse_utils import setup_schema, upload_to_starburst_parallel
    from shared_tools.deploy import scan_and_deploy
    # NEW: Import the environment loader function
    from shared_tools.env_utils import load_project_env 
except ImportError as e:
    logging.critical(f"FATAL ERROR: Could not import utility functions. Did you run 'pip install -e .' from the project root? Details: {e}")
    sys.exit(1)

# NEW CLEAN LOGIC: Load global, then local envs using the utility function.
# This ensures os.environ is populated before get_config runs.
load_project_env(__file__)

fake = Faker()

# --- Configuration (Volume) ---
NUM_LOANS = 20000
NUM_POSITIONS = 5000
NUM_MARKET_DATA = 50000 
NUM_SCENARIOS = 10

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        # These variables are now guaranteed to be in os.environ 
        # because load_project_env(__file__) was called above.
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            # RAW DATA TARGET (Using RISK_RAW variables)
            "catalog": os.environ["RISK_RAW_CATALOG"], 
            "schema": os.environ["RISK_RAW_SCHEMA"],
            "location": os.environ["RISK_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        # Re-raise the error with a more specific check, though the utility should handle missing globals.
        logging.error(f"--- ERROR: Missing configuration for RISK & STRESS TEST: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the RISK variables in your local risk_stress_test/.env file.")
        sys.exit(1)

def generate_risk_data():
    logging.info("Starting Risk & Stress Test data generation...")
    today = datetime.now().date()
    
    # --- 1. Loan_Portfolios (Credit Risk Base Data) ---
    loan_types = ['Residential Mortgage', 'Commercial Real Estate', 'Corporate Loan', 'SME Loan']
    risk_grades = ['A', 'B', 'C', 'D', 'E']
    loan_portfolios = []
    for i in range(NUM_LOANS):
        grade = random.choice(risk_grades)
        loan_portfolios.append({
            "LoanID": f"LST-{60000 + i}", 
            "LoanType": random.choice(loan_types), 
            "PrincipalAmount": round(random.uniform(100000, 5000000), 2),
            "RiskGrade": grade,
            "LTV_Ratio": round(random.uniform(0.4, 0.9), 2),
            "PD_Score": round(0.01 * risk_grades.index(grade) + random.uniform(0.01, 0.05), 4),
            "IsDefaulted": random.choices([True, False], weights=[risk_grades.index(grade) + 1, 5 - risk_grades.index(grade)], k=1)[0],
            "Region": fake.city_prefix()
        })
    loan_portfolios_df = pd.DataFrame(loan_portfolios)

    # --- 2. Trading_Positions (Market Risk Base Data) ---
    asset_classes = ['Equity', 'FX', 'Commodity', 'Fixed Income']
    trading_positions = []
    for i in range(NUM_POSITIONS):
        trading_positions.append({
            "PositionID": f"POS-{70000 + i}", 
            "AssetClass": random.choice(asset_classes), 
            "InstrumentID": fake.bothify(text='??####.STK'),
            "NotionalValue": round(random.uniform(10000, 1000000), 2) * random.choice([1, -1]),
            "Delta": round(random.uniform(0.5, 1.5), 2),
            "Vega": round(random.uniform(0.1, 0.5), 2),
            "ValuationDate": today
        })
    trading_positions_df = pd.DataFrame(trading_positions)

    # --- 3. Market_Data (Risk Factor Inputs) ---
    risk_factors = ['Stock Index', 'Interest Rate', 'FX Rate', 'Commodity Price']
    market_data = []
    for factor in risk_factors:
        for i in range(1, 50 * len(asset_classes) + 1):
            date = today - timedelta(days=i // len(asset_classes))
            market_data.append({
                "Date": date, 
                "RiskFactor": factor, 
                "Ticker": fake.bothify(text='??####.STK') if factor == 'Stock Index' else factor,
                "Value": round(random.uniform(50, 150), 4),
                "Volatility": round(random.uniform(0.01, 0.05), 4)
            })
    market_data_df = pd.DataFrame(market_data)

    # --- 4. Stress_Scenarios (Stress Test Parameters) ---
    scenario_types = ['Recession', 'Market Crash', 'Inflation Spike', 'Geopolitical Shock']
    stress_scenarios = []
    for i in range(NUM_SCENARIOS):
        stress_scenarios.append({
            "ScenarioID": f"SCN-{i + 1}",
            "ScenarioName": random.choice(scenario_types) + f" V{random.randint(1, 5)}",
            "DateCreated": today,
            "ScenarioDescription": fake.catch_phrase(),
            "Impact_Equity": round(random.uniform(-0.3, 0), 2),
            "Impact_IR_Shift_bps": random.randint(50, 200)
        })
    stress_scenarios_df = pd.DataFrame(stress_scenarios)
    
    logging.info(f"Generated {len(loan_portfolios_df)} loans, {len(trading_positions_df)} positions, {len(market_data_df)} market data points, and {len(stress_scenarios_df)} scenarios.")
    
    return {
        "loan_portfolios": loan_portfolios_df, 
        "trading_positions": trading_positions_df, 
        "market_data": market_data_df,
        "stress_scenarios": stress_scenarios_df
    }


if __name__ == "__main__":
    # 1. Argument Parsing (NEW)
    parser = argparse.ArgumentParser(description="Generate Risk & Stress Test data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()
    
    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"
    
    try:
        engine = create_engine(engine_string)
        
        if not args.deploy_only:
            # 1. SETUP SCHEMA
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                
                # 2. GENERATE DATA
                data_tables = generate_risk_data()
                
                # 3. UPLOAD DATA
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")
            
        # 4. DEPLOY DATAPRODUCTS
        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)
            
        logging.info("Risk & Stress Test data pipeline executed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")