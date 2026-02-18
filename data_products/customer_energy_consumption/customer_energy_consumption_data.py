# customer_energy_consumption/customer_energy_consumption_data.py

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
from dateutil.relativedelta import relativedelta

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

# Utilisation de la locale française pour générer des données réalistes
fake = Faker('fr_FR')

# --- MODIFICATION: Configuration (Volume) réduite pour des tests rapides ---
NUM_CUSTOMERS = 1000
NUM_ACCOUNTS = 1200
NUM_METER_READINGS = 75000 
NUM_INTERVENTIONS = 5000
NUM_INVOICES = 3000

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["ENERGY_RAW_CATALOG"],
            "schema": os.environ["ENERGY_RAW_SCHEMA"],
            "location": os.environ["ENERGY_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for ENERGY CONSUMPTION: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the ENERGY variables in your local customer_energy_consumption/.env file.")
        sys.exit(1)

def get_tariff_period(timestamp):
    """Détermine si un horodatage correspond aux Heures Pleines ou Creuses."""
    # Heures Creuses: 22h - 6h tous les jours
    if timestamp.hour >= 22 or timestamp.hour < 6:
        return 'Heures Creuses'
    # Heures Pleines: le reste du temps
    return 'Heures Pleines'

def generate_energy_data():
    logging.info("Starting Customer Energy Consumption data generation (in French)...")

    # --- 1. CRM_Customers ---
    customers = []
    for i in range(NUM_CUSTOMERS):
        customers.append({
            "CustomerID": f"CUST-{1000 + i}",
            "FirstName": fake.first_name(),
            "LastName": fake.last_name(),
            "Email": fake.email(),
            "PhoneNumber": fake.phone_number(),
            "Address": fake.street_address(),
            "City": fake.city(),
            "PostalCode": fake.postcode(),
            "JoinDate": fake.date_between(start_date='-5y', end_date='-1m')
        })
    crm_customers_df = pd.DataFrame(customers)
    customer_ids = crm_customers_df['CustomerID'].tolist()

    # --- 2. Billing_Accounts ---
    accounts = []
    contract_types = ['Base', 'Heures Pleines/Heures Creuses', 'Tempo']
    statuses = ['Actif', 'Résilié', 'Suspendu']
    for i in range(NUM_ACCOUNTS):
        accounts.append({
            "AccountID": f"ACCT-{20000 + i}",
            "CustomerID": random.choice(customer_ids),
            "ContractType": random.choices(contract_types, weights=[60, 35, 5], k=1)[0],
            "StartDate": fake.date_between(start_date='-4y', end_date='-1w'),
            "Status": random.choices(statuses, weights=[90, 8, 2], k=1)[0],
            "BillingCycleDay": random.randint(1, 28)
        })
    billing_accounts_df = pd.DataFrame(accounts)
    active_account_ids = billing_accounts_df[billing_accounts_df['Status'] == 'Actif']['AccountID'].tolist()
    
    # S'assurer qu'il y a au moins un compte actif pour éviter les erreurs
    if not active_account_ids:
        logging.warning("No active accounts generated in this small sample. Forcing one to be active.")
        if len(billing_accounts_df) > 0:
            billing_accounts_df.loc[0, 'Status'] = 'Actif'
            active_account_ids = billing_accounts_df[billing_accounts_df['Status'] == 'Actif']['AccountID'].tolist()
        else:
            logging.error("Cannot generate any data as no accounts were created.")
            return {}


    # --- 3. Smart_Meter_Readings ---
    meter_readings = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    for _ in range(NUM_METER_READINGS):
        timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)
        tariff_period = get_tariff_period(timestamp)
        
        # Simuler une consommation plus élevée en Heures Pleines et en hiver
        base_consumption = random.uniform(0.1, 0.8)
        if tariff_period == 'Heures Pleines':
            base_consumption *= 2.5
        if timestamp.month in [1, 2, 11, 12]:
            base_consumption *= 1.5
        
        meter_readings.append({
            "ReadingID": fake.uuid4(),
            "AccountID": random.choice(active_account_ids),
            "Timestamp": timestamp,
            "Consumption_kWh": round(base_consumption, 4),
            "TariffPeriod": tariff_period # NOUVELLE COLONNE
        })
    smart_meter_readings_df = pd.DataFrame(meter_readings)

    # --- 4. Support_Interventions ---
    interventions = []
    intervention_types = ['Problème Technique', 'Question sur la Facturation', 'Souscription Nouveau Service', 'Contestation de Relevé']
    intervention_status = ['Clôturé', 'Ouvert', 'En attente technicien']
    for i in range(NUM_INTERVENTIONS):
        interventions.append({
            "InterventionID": f"INT-{50000 + i}",
            "AccountID": random.choice(active_account_ids),
            "InterventionType": random.choice(intervention_types),
            "RequestDate": fake.date_time_between(start_date='-1y', end_date='now'),
            "Status": random.choices(intervention_status, weights=[85, 10, 5], k=1)[0],
            "ResolutionComment": fake.sentence() if random.random() > 0.3 else None
        })
    support_interventions_df = pd.DataFrame(interventions)

    # --- 5. Billing_Invoices ---
    invoices = []
    payment_statuses = ['Payée', 'En attente', 'En retard']
    for i in range(NUM_INVOICES):
        invoice_date = fake.date_between(start_date='-1y', end_date='-1d')
        billing_period_end = invoice_date.replace(day=1) - timedelta(days=1)
        billing_period_start = billing_period_end.replace(day=1)
        consumption = round(random.uniform(150, 800), 2)
        price_per_kwh = round(random.uniform(0.15, 0.25), 4)
        status = random.choices(payment_statuses, weights=[90, 5, 5], k=1)[0]
        
        payment_date = None
        if status == 'Payée':
            payment_date = fake.date_between_dates(date_start=invoice_date, date_end=invoice_date + timedelta(days=20))

        invoices.append({
            "InvoiceID": f"FACT-{20240000 + i}",
            "AccountID": random.choice(active_account_ids),
            "InvoiceDate": invoice_date,
            "BillingPeriodStart": billing_period_start,
            "BillingPeriodEnd": billing_period_end,
            "Consumption_kWh": consumption,
            "Price_per_kWh": price_per_kwh,
            "AmountBilled": round(consumption * price_per_kwh, 2),
            "DueDate": invoice_date + timedelta(days=30),
            "PaymentStatus": status,
            "PaymentDate": payment_date
        })
    billing_invoices_df = pd.DataFrame(invoices)

    logging.info(f"Generated {len(crm_customers_df)} customers, {len(billing_accounts_df)} accounts, {len(smart_meter_readings_df)} meter readings, {len(support_interventions_df)} interventions, and {len(billing_invoices_df)} invoices.")

    return {
        "crm_customers": crm_customers_df,
        "billing_accounts": billing_accounts_df,
        "smart_meter_readings": smart_meter_readings_df,
        "support_interventions": support_interventions_df,
        "billing_invoices": billing_invoices_df
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Customer Energy Consumption data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()

    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"

    try:
        engine = create_engine(engine_string)

        if not args.deploy_only:
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                data_tables = generate_energy_data()
                if data_tables: # Check if data generation was successful
                    upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")

        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)

        logging.info("Customer Energy Consumption data pipeline executed successfully.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
