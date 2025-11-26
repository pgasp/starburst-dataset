# telecom/telecom_data.py

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
import sys
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

# NEW CLEAN LOGIC: Load global, then local envs using the utility function.
load_project_env(__file__)

fake = Faker()

# --- Configuration (Volume) ---
NUM_CUSTOMERS = 20000
NUM_SUBSCRIPTIONS = 25000 
NUM_INVOICES = 30000
NUM_USAGE = 60000 
NUM_PERFORMANCE = 15000
NUM_FAULTS = 10000 

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            # RAW DATA TARGET (Using TELECOM_RAW variables)
            "catalog": os.environ["TELECOM_RAW_CATALOG"], 
            "schema": os.environ["TELECOM_RAW_SCHEMA"],
            "location": os.environ["TELECOM_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for TELECOM: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the TELECOM variables in your local telecom/.env file.")
        sys.exit(1)

def generate_telecom_data():
    logging.info("Starting Telecom (OSS/BSS) data generation...")
    today = datetime.now().date()
    
    # --- 1. Customers (BSS Dimension) ---
    segments = ['Residential', 'Business-SME', 'Business-Enterprise']
    customers = []
    for i in range(NUM_CUSTOMERS):
        customers.append({
            "CustomerID": f"TELCUST-{10000 + i}", 
            "JoinedDate": fake.date_between(start_date='-5y', end_date='-60d'),
            "CustomerSegment": random.choice(segments),
            "ChurnRiskScore": round(random.uniform(0.1, 0.9), 2),
            "City": fake.city()
        })
    customers_df = pd.DataFrame(customers); customer_ids = customers_df['CustomerID'].tolist()

    # --- 2. Service_Subscriptions (BSS Dimension) ---
    products = ['Mobile-Pro', 'Mobile-Basic', 'Fiber-Gold', 'Fiber-Bronze']
    subscriptions = []
    for i in range(NUM_SUBSCRIPTIONS):
        cid = random.choice(customer_ids)
        subscriptions.append({
            "SubscriptionID": f"SUB-{200000 + i}",
            "CustomerID": cid,
            "ProductID": random.choice(products),
            "MonthlyFee": round(random.uniform(20, 150), 2),
            "ServiceStartDate": fake.date_between(start_date='-3y', end_date='-30d'),
            "ContractStatus": random.choices(['Active', 'Suspended', 'Cancelled'], weights=[90, 5, 5], k=1)[0]
        })
    subscriptions_df = pd.DataFrame(subscriptions); subscription_ids = subscriptions_df['SubscriptionID'].tolist()

    # --- 3. Invoices (BSS Fact) ---
    invoice_statuses = ['Paid', 'Overdue', 'Pending']
    invoices = []
    for i in range(NUM_INVOICES):
        sid = random.choice(subscription_ids)
        status = random.choices(invoice_statuses, weights=[85, 10, 5], k=1)[0]
        invoices.append({
            "InvoiceID": f"INV-{300000 + i}",
            "SubscriptionID": sid,
            "InvoiceDate": fake.date_between(start_date='-1y', end_date='-7d'),
            "BillingCycle": random.choice(['Monthly', 'Quarterly']),
            "AmountDue": round(random.uniform(25, 175), 2),
            "PaymentStatus": status,
            "DaysLate": random.randint(1, 45) if status == 'Overdue' else 0
        })
    invoices_df = pd.DataFrame(invoices)
    
    # --- 4. Customer_Usage (OSS Fact - Links to BSS via SubscriptionID) ---
    usage = []
    usage_types = ['Data_MB', 'Voice_Mins', 'SMS_Count']
    for i in range(NUM_USAGE):
        sid = random.choice(subscription_ids)
        usage_type = random.choice(usage_types)
        usage.append({
            "UsageRecordID": f"USG-{4000000 + i}",
            "SubscriptionID": sid,
            "UsageDate": fake.date_time_between(start_date='-30d', end_date='now'),
            "UsageType": usage_type,
            "UsageValue": round(random.uniform(1, 1000) * (2 if usage_type=='Data_MB' else 1), 2),
            "CellSiteID": f"CELL-{random.randint(100, 300)}"
        })
    usage_df = pd.DataFrame(usage); cell_site_ids = usage_df['CellSiteID'].unique().tolist()
    
    # --- 5. Network_Performance (OSS Fact - Cell Site Level) ---
    performance = []
    for i in range(NUM_PERFORMANCE):
        performance.append({
            "RecordID": f"PERF-{50000 + i}",
            "CellSiteID": random.choice(cell_site_ids),
            "Timestamp": fake.date_time_between(start_date='-30d', end_date='now'),
            "DownloadSpeed_Mbps": round(random.uniform(5, 100), 2),
            "Latency_ms": round(random.uniform(10, 150), 2),
            "PacketLoss_Pct": round(random.uniform(0, 5), 2)
        })
    performance_df = pd.DataFrame(performance)
    
    # --- 6. Fault_Records (OSS Fact) ---
    fault_severities = ['Critical', 'Major', 'Minor']
    fault_records = []
    for i in range(NUM_FAULTS):
        fault_records.append({
            "FaultID": f"FLT-{6000 + i}",
            "CellSiteID": random.choice(cell_site_ids),
            "FaultTime": fake.date_time_between(start_date='-60d', end_date='now'),
            "Severity": random.choices(fault_severities, weights=[10, 30, 60], k=1)[0],
            "Description": fake.bs(),
            "ResolutionTime_min": random.randint(10, 480)
        })
    fault_records_df = pd.DataFrame(fault_records)

    logging.info(f"Generated {len(customers_df)} customers, {len(subscriptions_df)} subscriptions, and approximately 100k records across usage, performance, and faults.")
    
    return {
        "customers": customers_df, 
        "service_subscriptions": subscriptions_df, 
        "invoices": invoices_df,
        "customer_usage": usage_df,
        "network_performance": performance_df,
        "fault_records": fault_records_df
    }


if __name__ == "__main__":
    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"
    
    try:
        engine = create_engine(engine_string)
        
        # 1. SETUP SCHEMA
        if setup_schema(engine, config['catalog'], config['schema'], config['location']):
            
            # 2. GENERATE DATA
            data_tables = generate_telecom_data()
            
            # 3. UPLOAD DATA (6 tables)
            upload_to_starburst_parallel(engine, config['schema'], data_tables)
            
            # 4. DEPLOY DATAPRODUCTS: Scans the directory and deploys all three YAML files.
            scan_and_deploy("./telecom")
            
            logging.info("Telecom (OSS/BSS) data pipeline executed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")