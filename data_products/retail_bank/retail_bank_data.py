# retail_bank/retail_bank_data.py

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
import sys
# ADDED argparse for command-line options
import argparse 
from dotenv import load_dotenv
import logging

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
# ------------------------------


# --- External Utility Imports (CLEANED) ---
try:
    from shared_tools.lakehouse_utils import setup_schema, upload_to_starburst_parallel
    from shared_tools.deploy import scan_and_deploy
    from shared_tools.env_utils import load_project_env
except ImportError as e:
    logging.critical(f"FATAL ERROR: Could not import utility functions. Did you run 'pip install -e .' from the project root? Details: {e}")
    sys.exit(1)

# Load environment variables
load_project_env(__file__)
fake = Faker()

# --- Configuration (Volume) ---
NUM_CUSTOMERS = 10000
NUM_ACCOUNTS = 15000
NUM_TRANSACTIONS = 100000
NUM_BRANCHES = 50
NUM_LOANS = 5000 
NUM_CAMPAIGNS = 20000 

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            # RAW DATA TARGET (Using RETAIL_RAW variables)
            "catalog": os.environ["RETAIL_RAW_CATALOG"], 
            "schema": os.environ["RETAIL_RAW_SCHEMA"],
            "location": os.environ["RETAIL_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for RETAIL: {e} ---")
        logging.error("Please check for SB_HOST, RETAIL_RAW_CATALOG, RETAIL_RAW_SCHEMA, and RETAIL_SB_SCHEMA_LOCATION in your .env file.")
        sys.exit(1)

def generate_retail_data():
    logging.info("Starting Retail Banking data generation...")
    today = datetime.now().date()
    
    # --- 1. Branches (Dimension) ---
    branches = []
    regions = ['Northeast', 'Southeast', 'Midwest', 'West']
    for i in range(NUM_BRANCHES):
        branches.append({"BranchID": f"BR-{100 + i}", "BranchName": f"{fake.city()} Branch", "Region": random.choice(regions)})
    branches_df = pd.DataFrame(branches); branch_ids = branches_df['BranchID'].tolist()

    # --- 2. Customers (Base Dimension) ---
    customers = []
    for i in range(NUM_CUSTOMERS):
        customers.append({
            "CustomerID": f"CUST-{10000 + i}", 
            "FirstName": fake.first_name(), 
            "LastName": fake.last_name(), 
            "Email": fake.email(),
            "City": fake.city(),
            "Region": random.choice(regions)
        })
    customers_df = pd.DataFrame(customers); customer_ids = customers_df['CustomerID'].tolist()

    # --- 3. Accounts (Dimension/Fact) ---
    accounts = []
    account_types = ['Checking', 'Savings', 'Credit Card', 'Money Market']
    for i in range(NUM_ACCOUNTS):
        cid = random.choice(customer_ids)
        open_date = fake.date_between(start_date='-10y', end_date='today')
        accounts.append({
            "AccountID": f"ACC-{200000 + i}", 
            "CustomerID": cid, 
            "DateOpened": open_date,
            "AccountType": random.choice(account_types), 
            "Balance": round(random.uniform(100, 500000), 2)
        })
    accounts_df = pd.DataFrame(accounts); account_ids = accounts_df['AccountID'].tolist()

    # --- 4. Transactions (Fact) ---
    transactions = []
    transaction_types = ['Deposit', 'Withdrawal', 'Transfer', 'Payment', 'ATM', 'Fraud']
    for i in range(NUM_TRANSACTIONS):
        aid = random.choice(account_ids)
        start_date = today - timedelta(days=365)
        # Weighted random choice, 'Fraud' is rare (1%)
        tx_type = random.choices(transaction_types, weights=[40, 30, 10, 10, 9, 1], k=1)[0]
        
        transactions.append({
            "TransactionID": f"TX-{3000000 + i}", 
            "AccountID": aid, 
            "TransactionDate": fake.date_time_between(start_date=start_date),
            "TransactionType": tx_type, 
            "Amount": round(random.uniform(1, 5000), 2),
            "BranchID": random.choice(branch_ids) if random.random() < 0.7 else np.nan
        })
    transactions_df = pd.DataFrame(transactions)
    # Ensure BranchID is handled correctly for upload, replacing nan with None
    transactions_df['BranchID'] = transactions_df['BranchID'].astype(str).replace('nan', None)

    # --- 5. Loans (NEW) ---
    loans = []
    loan_types = ['Mortgage', 'Auto', 'Personal', 'Home Equity']
    loan_statuses = ['Active', 'Paid', 'Default']
    for i in range(NUM_LOANS):
        # A small fraction of loans are defaulted (5%)
        status = random.choices(loan_statuses, weights=[75, 20, 5], k=1)[0]
        principal = round(random.uniform(5000, 1000000), 2)
        loans.append({
            "LoanID": f"L-{40000 + i}",
            "CustomerID": random.choice(customer_ids),
            "LoanType": random.choice(loan_types),
            "Principal": principal,
            "InterestRate": round(random.uniform(2.5, 12.0), 2) / 100, # stored as a ratio
            "TermMonths": random.choices([12, 36, 60, 180, 360], weights=[10, 20, 30, 10, 30], k=1)[0],
            "OriginationDate": fake.date_between(start_date='-5y', end_date='-30d'),
            "LoanStatus": status
        })
    loans_df = pd.DataFrame(loans)

    # --- 6. Marketing Campaigns (NEW) ---
    campaigns = []
    campaign_names = ['Q3_CreditCard_Promo', 'Mortgage_Refi_Q1', 'Savings_Goal_Reminder', 'Financial_Wellness_Tip']
    channels = ['Email', 'App_Notification', 'SMS']
    responses = ['Responded', 'Clicked', 'Ignored']
    
    for i in range(NUM_CAMPAIGNS):
        cid = random.choice(customer_ids)
        # Responses are rare: Ignored (80%), Clicked (15%), Responded (5%)
        response = random.choices(responses, weights=[5, 15, 80], k=1)[0]
        
        campaigns.append({
            "CampaignID": f"CAMP-{500000 + i}",
            "CustomerID": cid,
            "CampaignName": random.choice(campaign_names),
            "Channel": random.choice(channels),
            "DateSent": fake.date_time_between(start_date='-1y', end_date='now'),
            "Response": response
        })
    marketing_campaigns_df = pd.DataFrame(campaigns)

    logging.info(f"Generated {len(customers_df)} customers, {len(accounts_df)} accounts, {len(transactions_df)} transactions, {len(loans_df)} loans, and {len(marketing_campaigns_df)} campaign records.")
    
    return {
        "customers": customers_df, "accounts": accounts_df, 
        "transactions": transactions_df, "branches": branches_df,
        "loans": loans_df, "marketing_campaigns": marketing_campaigns_df 
    }


if __name__ == "__main__":
    # --- 1. Argument Parsing (NEW) ---
    parser = argparse.ArgumentParser(description="Generate Retail Banking data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()

    config = get_config()
    # Create the SQLAlchemy engine string for connection (without schema/catalog yet)
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"
    
    try:
        engine = create_engine(engine_string)
        
        if not args.deploy_only:
            # 1. SETUP SCHEMA
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                
                # 2. GENERATE DATA
                data_tables = generate_retail_data()
                
                # 3. UPLOAD DATA (Now includes 6 tables)
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")
        
        # 4. DEPLOY DATAPRODUCTS (This step is run regardless of deploy_only flag)
        # FIX: Use the correct, relative path for scan_and_deploy
        deploy_path = os.path.dirname(os.path.abspath(__file__))
        
        scan_and_deploy(deploy_path)
        
        logging.info("Retail data pipeline executed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")