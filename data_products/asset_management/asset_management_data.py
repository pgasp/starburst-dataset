# asset_management/asset_management_data.py

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta, date
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
Faker.seed(42)
random.seed(42)
np.random.seed(42)


# --- Configuration (Volume) ---
NUM_FUNDS = 50
NUM_SECURITIES = 250
DAYS_OF_DATA = 300
POSITIONS_PER_FUND = 100
TRADES_PER_DAY = 1000
CASH_FLOWS_PER_DAY = 100

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["ASSET_MGMT_RAW_CATALOG"],
            "schema": os.environ["ASSET_MGMT_RAW_SCHEMA"],
            "location": os.environ["ASSET_MGMT_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for ASSET MANAGEMENT: {e} ---")
        logging.error("Please check for SB_HOST in your root .env, and the ASSET_MGMT variables in your local asset_management/.env file.")
        sys.exit(1)

def generate_asset_management_data():
    logging.info("Starting Asset Management data generation...")

    # --- 1. Master Data: Funds & Securities ---
    funds = []
    fund_strategies = ['Global Equity', 'Euro Bond', 'Emerging Markets', 'US Small Cap', 'Convertible Bond', 'Money Market']
    for i in range(NUM_FUNDS):
        funds.append({
            "Fund_Code": f"FND{i+1:03}",
            "Fund_Name": f"{random.choice(fund_strategies)} Fund {fake.lexify('???').upper()}",
            "Base_Currency": random.choice(['EUR', 'USD', 'GBP'])
        })
    funds_df = pd.DataFrame(funds)
    fund_codes = funds_df['Fund_Code'].tolist()

    securities = []
    asset_classes = ['Equity', 'Bond', 'Cash Equivalent', 'ETF', 'Convertible']
    for i in range(NUM_SECURITIES):
        country_code = fake.country_code()
        isin_body = fake.bothify(text='?????????').upper()
        isin_check_digit = str(random.randint(0, 9))
        
        securities.append({
            "ISIN": f"{country_code}{isin_body}{isin_check_digit}",
            "Security_Name": fake.company(),
            "Asset_Class": random.choice(asset_classes),
            "Country": country_code,
            "Currency": random.choice(['EUR', 'USD', 'GBP', 'JPY', 'CHF'])
        })
    securities_df = pd.DataFrame(securities)
    isin_list = securities_df['ISIN'].tolist()

    # --- 2. Daily Transactional Data Generation ---
    all_positions, all_cash_flows, all_trades, all_fx_rates = [], [], [], []
    today = date.today()

    for day in range(DAYS_OF_DATA):
        report_date = today - timedelta(days=day)
        logging.info(f"Generating data for {report_date}...")

        # Generate FX rates for the day
        for pair in ['EUR/USD', 'GBP/USD', 'JPY/USD', 'CHF/USD']:
            all_fx_rates.append({
                "Report_Date": report_date,
                "Currency_Pair": pair,
                "Spot_Rate": round(random.uniform(0.8, 1.5), 4) + (day * 0.0001), # Add slight drift
                "Forward_Points": round(random.uniform(-10, 10), 2)
            })

        # Generate Daily Positions for each fund
        for fund_code in fund_codes:
            fund_holdings = securities_df.sample(n=POSITIONS_PER_FUND)
            fund_nav = 0
            for _, security in fund_holdings.iterrows():
                market_price = round(random.uniform(10, 1000), 2)
                quantity = random.randint(100, 10000)
                market_value = market_price * quantity
                fund_nav += market_value # Simplified NAV calculation
                position = {
                    "Fund_Code": fund_code,
                    "Report_Date": report_date,
                    "ISIN": security['ISIN'],
                    "Security_Name": security['Security_Name'],
                    "Asset_Class": security['Asset_Class'],
                    "Country": security['Country'],
                    "Currency": security['Currency'],
                    "Quantity": quantity,
                    "Market_Price_Local": market_price,
                    "Market_Value_Local": market_value,
                    "Accrued_Interest": round(random.uniform(0, 500), 2) if security['Asset_Class'] == 'Bond' else 0.0
                }
                all_positions.append(position)
            
            # Attach NAV and Units to each position record for that fund/day
            total_units = round(fund_nav / random.uniform(10, 150), 2) # Derive units from NAV
            for pos in all_positions:
                if pos['Fund_Code'] == fund_code and pos['Report_Date'] == report_date:
                    pos['NAV'] = round(fund_nav, 2)
                    pos['Units'] = total_units

        # Generate Daily Cash Flows (total for the day, assigned randomly to funds)
        for _ in range(CASH_FLOWS_PER_DAY):
            all_cash_flows.append({
                "Flow_ID": fake.uuid4(),
                "Fund_Code": random.choice(fund_codes),
                "Report_Date": report_date,
                "Flow_Type": random.choice(['Subscription', 'Redemption']),
                "Amount": round(random.uniform(1000, 500000), 2),
                "Source": fake.company() + " Asset Management"
            })

        # Generate Daily Trades (total for the day, assigned randomly to funds)
        for _ in range(TRADES_PER_DAY):
            trade_date = report_date - timedelta(days=random.randint(1, 3))
            settle_date = trade_date + timedelta(days=2)
            # Introduce some late settlements for reconciliation purposes
            status = 'Settled' if settle_date < report_date else 'Unsettled'
            if status == 'Unsettled' and (report_date - settle_date).days > 0 and random.random() < 0.1:
                status = 'Unsettled_Late'

            all_trades.append({
                "Trade_ID": fake.uuid4(),
                "Fund_Code": random.choice(fund_codes),
                "Trade_Date": trade_date,
                "Settle_Date": settle_date,
                "ISIN": random.choice(isin_list),
                "Buy_Sell": random.choice(['BUY', 'SELL']),
                "Quantity": random.randint(100, 5000),
                "Trade_Price": round(random.uniform(10, 1000), 2),
                "Counterparty": fake.company() + " Investments",
                "Status": status
            })


    daily_nav_positions_df = pd.DataFrame(all_positions)
    daily_cash_flows_df = pd.DataFrame(all_cash_flows)
    trades_df = pd.DataFrame(all_trades)
    daily_fx_rates_df = pd.DataFrame(all_fx_rates)

    logging.info(f"Generated {len(daily_nav_positions_df)} position records across {DAYS_OF_DATA} days.")
    logging.info(f"Generated {len(daily_cash_flows_df)} cash flow records.")
    logging.info(f"Generated {len(trades_df)} trade records.")

    return {
        "funds_master": funds_df,
        "securities_master": securities_df,
        "daily_nav_positions": daily_nav_positions_df,
        "daily_cash_flows": daily_cash_flows_df,
        "trades": trades_df,
        "daily_fx_rates": daily_fx_rates_df
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Asset Management data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()

    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"

    try:
        engine = create_engine(engine_string)

        if not args.deploy_only:
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                data_tables = generate_asset_management_data()
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")

        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)

        logging.info("Asset Management data pipeline executed successfully.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
