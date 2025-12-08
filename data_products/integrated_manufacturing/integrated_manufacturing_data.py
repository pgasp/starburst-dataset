# integrated_manufacturing/integrated_manufacturing_data.py

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
    # Assumes shared_tools has been installed via 'pip install -e .'
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
NUM_PARTS = 10000 
NUM_MACHINES = 100
NUM_WORK_ORDERS = 20000 
NUM_INSPECTIONS = 80000 
NUM_TELEMETRY = 500000 
NUM_SCM_ORDERS = 10000

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["MANUF_RAW_CATALOG"], 
            "schema": os.environ["MANUF_RAW_SCHEMA"],
            "location": os.environ["MANUF_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for MANUFACTURING: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the MANUFACTURING variables in your local integrated_manufacturing/.env file.")
        sys.exit(1)

def generate_manufacturing_data():
    logging.info("Starting Integrated Manufacturing data generation...")
    
    # --- 1. PLM_Product_Master (Parts Master) ---
    parts = []
    part_types = ['Engine Assembly', 'Chassis Frame', 'Sensor', 'Control Unit', 'Raw Material']
    for i in range(NUM_PARTS):
        parts.append({
            "PartID": f"PLM-{i + 1}", 
            "PartRevision": random.choice(['A', 'B', 'C']),
            "PartName": fake.word().capitalize() + " " + random.choice(['Module', 'Bracket', 'IC', 'Block']),
            "PartType": random.choice(part_types),
            "UnitCost_ERP": round(random.uniform(10, 5000), 2),
            "DrawingURL": fake.url()
        })
    plm_product_master_df = pd.DataFrame(parts); part_ids = plm_product_master_df['PartID'].tolist()

    # --- 2. MES_Work_Orders ---
    statuses = ['Scheduled', 'In_Progress', 'Completed', 'Canceled']
    work_orders = []
    for i in range(NUM_WORK_ORDERS):
        work_orders.append({
            "WorkOrderID": f"WO-{i + 1}",
            "TopLevelPartID": random.choice(part_ids),
            "PlannedQty": random.randint(10, 500),
            "ActualQty": None,
            "MachineID": f"MCH-{random.randint(1, NUM_MACHINES)}",
            "Status": random.choice(statuses),
            "ScheduledStart": fake.date_time_between(start_date='-30d', end_date='-7d')
        })
    mes_work_orders_df = pd.DataFrame(work_orders); wo_ids = mes_work_orders_df['WorkOrderID'].tolist()
    
    for index, row in mes_work_orders_df.iterrows():
        if row['Status'] == 'Completed':
            mes_work_orders_df.at[index, 'ActualQty'] = row['PlannedQty'] + random.randint(-5, 5)

    # --- 3. QMS_Inspection_Records ---
    check_types = ['Dimensional', 'Surface Finish', 'Torque Spec', 'Visual']
    qms_inspection_records = []
    for i in range(NUM_INSPECTIONS):
        qms_inspection_records.append({
            "InspectionID": f"INSP-{i + 1}",
            "WorkOrderID": random.choice(wo_ids),
            "ComponentPartID": random.choice(part_ids),
            "CheckType": random.choice(check_types),
            "Result": random.choices(['PASS', 'FAIL'], weights=[95, 5], k=1)[0],
            "InspectorID": f"USR-{random.randint(10, 50)}",
            "Timestamp": fake.date_time_between(start_date='-30d', end_date='now')
        })
    qms_inspection_records_df = pd.DataFrame(qms_inspection_records)

    # --- 4. SCADA_Sensor_Telemetry (FUTURE-DATED FOR PREDICTIVE DEMO) ---
    sensor_types = ['Spindle_Temp', 'Tool_Vibration', 'Pressure_Bar', 'Motor_Amps']
    
    # Define future date range variables (Current time up to 1 year ahead)
    start_future = datetime.now()
    end_future = datetime.now() + timedelta(days=365)
    
    scada_sensor_telemetry = []
    for i in range(NUM_TELEMETRY):
        sensor_type = random.choice(sensor_types)
        is_anomaly = random.choices([True, False], weights=[3, 97], k=1)[0]
        
        scada_sensor_telemetry.append({
            "ReadingTime": fake.date_time_between(start_date=start_future, end_date=end_future), # CORRECTED LINE
            "MachineID": f"MCH-{random.randint(1, NUM_MACHINES)}",
            "SensorType": sensor_type,
            "Value": round(random.uniform(5, 150), 4),
            "Alarm_Flag": is_anomaly
        })
    scada_sensor_telemetry_df = pd.DataFrame(scada_sensor_telemetry)

    # --- 5. CRM_Customer_Orders ---
    customer_types = ['Dealer', 'Direct Retail']
    crm_customer_orders = []
    for i in range(10000):
        crm_customer_orders.append({
            "OrderID": f"CUSTORD-{i + 1}",
            "ERP_TopLevelPartID": random.choice(part_ids),
            "CustomerType": random.choice(customer_types),
            "QuantityOrdered": random.randint(1, 50),
            "OrderDate": fake.date_between(start_date='-90d', end_date='-30d'),
            "SalesValue": round(random.uniform(1000, 50000), 2)
        })
    crm_customer_orders_df = pd.DataFrame(crm_customer_orders)
    
    # --- 6. SCM_Supplier_Invoices ---
    scm_supplier_invoices = []
    for i in range(NUM_SCM_ORDERS):
        scm_supplier_invoices.append({
            "InvoiceID": f"SUPINV-{i + 1}",
            "PartID": random.choice(part_ids),
            "Supplier": fake.company(),
            "PurchasePrice": round(random.uniform(10, 1000), 2),
            "DeliveryLeadTime_Days": random.randint(3, 45),
            "InvoiceDate": fake.date_between(start_date='-1y', end_date='now')
        })
    scm_supplier_invoices_df = pd.DataFrame(scm_supplier_invoices)

    logging.info(f"Generated {len(plm_product_master_df)} parts, {len(mes_work_orders_df)} work orders, and {len(scada_sensor_telemetry_df)} machine readings (future-dated).")
    
    return {
        "plm_product_master": plm_product_master_df, 
        "mes_work_orders": mes_work_orders_df, 
        "qms_inspection_records": qms_inspection_records_df,
        "scada_sensor_telemetry": scada_sensor_telemetry_df,
        "crm_customer_orders": crm_customer_orders_df,
        "scm_supplier_invoices": scm_supplier_invoices_df
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Integrated Manufacturing data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()

    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"
    
    try:
        engine = create_engine(engine_string)
        
        if not args.deploy_only:
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                data_tables = generate_manufacturing_data()
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")
            
        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)
            
        logging.info("Integrated Manufacturing data pipeline executed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")