# telecommunications/telecommunications_data.py

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

fake = Faker('es_ES') # Use Spanish localization for more realistic names

# --- Configuration (Volume) ---
NUM_CUSTOMERS = 1500
NUM_SUBSCRIPTIONS = 2000
NUM_INVOICES = 2500
NUM_EQUIPMENT = 500
NUM_TRAFFIC_LOGS = 1000
NUM_SERVICE_TICKETS = 100

def get_config():
    """Loads configuration for the RAW data target from environment variables."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["TELECOM_RAW_CATALOG"],
            "schema": os.environ["TELECOM_RAW_SCHEMA"],
            "location": os.environ["TELECOM_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for TELECOM: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the TELECOM variables in your local telecommunications/.env file.")
        sys.exit(1)

def generate_telecom_data():
    """Generates synthetic data for BSS and OSS systems."""
    logging.info("Starting Telecommunications OSS/BSS data generation...")

    # --- 1. BSS: Customers ---
    customers = []
    account_statuses = ['Active', 'Suspended', 'Deactivated']
    for i in range(NUM_CUSTOMERS):
        customers.append({
            "CustomerID": f"CUST-{1000+i}",
            "FullName": fake.name(),
            "Email": fake.email(),
            "Address": fake.address(),
            "JoinDate": fake.date_between(start_date='-5y', end_date='-1d'),
            "AccountStatus": random.choices(account_statuses, weights=[90, 8, 2], k=1)[0]
        })
    bss_customers_df = pd.DataFrame(customers)
    customer_ids = bss_customers_df['CustomerID'].tolist()

    # --- 2. BSS: Subscriptions ---
    subscriptions = []
    plans = {
        'Fibra 1Gbps': 50.00, 'Fibra 600Mbps': 40.00, 'ADSL 20Mbps': 30.00,
        'Móvil Ilimitado': 35.00, 'Móvil 50GB': 25.00,
        'Fusión Total Plus': 120.00, 'Fusión Base': 75.00
    }
    plan_names = list(plans.keys())
    sub_statuses = ['Active', 'Canceled', 'Pending Activation']
    for i in range(NUM_SUBSCRIPTIONS):
        plan_name = random.choice(plan_names)
        subscriptions.append({
            "SubscriptionID": f"SUB-{20000+i}",
            "CustomerID": random.choice(customer_ids),
            "PlanName": plan_name,
            "MonthlyCharge": plans[plan_name],
            "StartDate": fake.date_time_between(start_date='-4y', end_date='-1d'),
            "Status": random.choices(sub_statuses, weights=[85, 13, 2], k=1)[0]
        })
    bss_subscriptions_df = pd.DataFrame(subscriptions)
    subscription_ids = bss_subscriptions_df['SubscriptionID'].tolist()

    # --- 3. BSS: Billing Invoices ---
    invoices = []
    payment_statuses = ['Paid', 'Due', 'Overdue']
    for i in range(NUM_INVOICES):
        invoice_date = fake.date_time_between(start_date='-2y', end_date='now')
        status = random.choices(payment_statuses, weights=[92, 5, 3], k=1)[0]
        invoices.append({
            "InvoiceID": f"INV-{500000+i}",
            "SubscriptionID": random.choice(subscription_ids),
            "InvoiceDate": invoice_date.date(),
            "AmountDue": round(random.uniform(25.0, 150.0), 2),
            "PaymentStatus": status,
            "PaymentDate": (invoice_date + timedelta(days=random.randint(5, 25))).date() if status == 'Paid' else None
        })
    bss_billing_invoices_df = pd.DataFrame(invoices)

    # --- 4. OSS: Network Equipment ---
    equipment = []
    equip_types = ['CellTower', 'Router', 'Switch', 'FiberNode', 'DSLAM']
    equip_statuses = ['Online', 'Offline', 'Maintenance']
    for i in range(NUM_EQUIPMENT):
        equipment.append({
            "EquipmentID": f"EQ-{5000+i}",
            "EquipmentType": random.choice(equip_types),
            "Location": f"{fake.city()}, {fake.country()}",
            "InstallDate": fake.date_time_between(start_date='-8y', end_date='-6m'),
            "Status": random.choices(equip_statuses, weights=[96, 3, 1], k=1)[0]
        })
    oss_network_equipment_df = pd.DataFrame(equipment)
    equipment_ids = oss_network_equipment_df['EquipmentID'].tolist()

    # --- 5. OSS: Network Traffic ---
    traffic_logs = []
    traffic_types = ['VideoStreaming', 'WebBrowsing', 'VoIP', 'Gaming', 'FileUpload']
    for i in range(NUM_TRAFFIC_LOGS):
        traffic_logs.append({
            "LogID": f"LOG-{2000000+i}",
            "SubscriptionID": random.choice(subscription_ids), # Link to BSS
            "EquipmentID": random.choice(equipment_ids),
            "Timestamp": fake.date_time_between(start_date='-48h', end_date='now'),
            "DataVolume_GB": round(random.uniform(0.01, 5.5), 4),
            "TrafficType": random.choice(traffic_types)
        })
    oss_network_traffic_df = pd.DataFrame(traffic_logs)

    # --- 6. OSS: Service Tickets ---
    tickets = []
    issue_types = ['Slow Speed', 'No Signal', 'Dropped Calls', 'Billing Inquiry']
    ticket_statuses = ['Closed', 'Open', 'In Progress']
    for i in range(NUM_SERVICE_TICKETS):
        status = random.choices(ticket_statuses, weights=[80, 10, 10], k=1)[0]
        open_date = fake.date_time_between(start_date='-1y', end_date='now')
        tickets.append({
            "TicketID": f"TKT-{8000+i}",
            "CustomerID": random.choice(customer_ids),
            "EquipmentID": random.choice(equipment_ids),
            "IssueType": random.choice(issue_types),
            "OpenDate": open_date,
            "CloseDate": open_date + timedelta(hours=random.randint(1, 72)) if status == 'Closed' else None,
            "Status": status
        })
    oss_service_tickets_df = pd.DataFrame(tickets)

    # --- 7. NEW: OSS Cell Tower Details ---
    cell_tower_details = []
    cell_tower_ids = oss_network_equipment_df[oss_network_equipment_df['EquipmentType'] == 'CellTower']['EquipmentID'].tolist()
    bands = ['5G-NR', 'LTE-A', 'LTE', 'UMTS']
    for tower_id in cell_tower_ids:
        cell_tower_details.append({
            "EquipmentID": tower_id,
            "Band": random.choice(bands),
            "Power_dBm": random.randint(20, 43),
            "ConnectedUsers": random.randint(5, 500),
            "LastCheckTime": datetime.now()
        })
    oss_cell_tower_details_df = pd.DataFrame(cell_tower_details)

    # --- ROBUSTNESS FIX ---
    # Pre-convert datetime columns with potential nulls to strings to avoid upload errors.
    bss_billing_invoices_df['PaymentDate'] = bss_billing_invoices_df['PaymentDate'].apply(
        lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None
    )
    oss_service_tickets_df['OpenDate'] = pd.to_datetime(oss_service_tickets_df['OpenDate'])
    oss_service_tickets_df['CloseDate'] = pd.to_datetime(oss_service_tickets_df['CloseDate'])
    for col in ['OpenDate', 'CloseDate']:
        oss_service_tickets_df[col] = oss_service_tickets_df[col].apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None
        )

    logging.info(f"Generated {len(bss_customers_df)} customers, {len(oss_network_traffic_df)} traffic logs, and {len(oss_cell_tower_details_df)} cell tower records.")

    return {
        "bss_customers": bss_customers_df,
        "bss_subscriptions": bss_subscriptions_df,
        "bss_billing_invoices": bss_billing_invoices_df,
        "oss_network_equipment": oss_network_equipment_df,
        "oss_network_traffic": oss_network_traffic_df,
        "oss_service_tickets": oss_service_tickets_df,
        "oss_cell_tower_details": oss_cell_tower_details_df, # Add new table
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Telecom data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()

    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"

    try:
        engine = create_engine(engine_string)

        if not args.deploy_only:
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                data_tables = generate_telecom_data()
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")

        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)

        logging.info("Telecommunications data pipeline executed successfully.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
