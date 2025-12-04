# automotive/automotive_data.py

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
import sys
import logging
# NEW IMPORT
import argparse

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
NUM_PARTS = 2000
NUM_STOCK_LOCATIONS = 50
NUM_PRODUCTION_RUNS = 1000
NUM_ASSEMBLY_STEPS = 50000 
NUM_SENSOR_READINGS = 150000 

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            # RAW DATA TARGET (Using AUTOMOTIVE_RAW variables)
            "catalog": os.environ["AUTOMOTIVE_RAW_CATALOG"], 
            "schema": os.environ["AUTOMOTIVE_RAW_SCHEMA"],
            "location": os.environ["AUTOMOTIVE_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for AUTOMOTIVE: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the AUTOMOTIVE variables in your local automotive/.env file.")
        sys.exit(1)

def generate_automotive_data():
    logging.info("Starting Automotive Manufacturing data generation...")
    today = datetime.now().date()
    
    # --- 1. Parts (Inventory Dimension) ---
    part_types = ['Chassis', 'Engine', 'Interior', 'Electronics', 'Exterior']
    parts = []
    for i in range(NUM_PARTS):
        parts.append({
            "PartID": f"PART-{1000 + i}", 
            "PartName": fake.word().capitalize() + " " + random.choice(['Bracket', 'Sensor', 'Module', 'Panel']),
            "PartType": random.choice(part_types),
            "Cost": round(random.uniform(5, 500), 2),
            "Supplier": fake.company(),
            "MaxStockLevel": random.randint(500, 5000)
        })
    parts_df = pd.DataFrame(parts); part_ids = parts_df['PartID'].tolist()

    # --- 2. Inventory_Levels (Logistics Fact) ---
    locations = [f"ZONE-{i}" for i in range(1, NUM_STOCK_LOCATIONS + 1)]
    inventory_levels = []
    for _ in range(NUM_PARTS * 2): # Two locations per part on average
        inventory_levels.append({
            "InventoryID": f"INV-{len(inventory_levels) + 1}",
            "PartID": random.choice(part_ids),
            "LocationID": random.choice(locations),
            "CurrentStock": random.randint(0, 1500),
            "LastUpdated": fake.date_time_between(start_date='-7d', end_date='now')
        })
    inventory_levels_df = pd.DataFrame(inventory_levels)

    # --- 3. Production_Runs (Assembly Dimension - The car being built) ---
    models = ['SedanX', 'TruckY', 'SUVZ']
    production_runs = []
    for i in range(NUM_PRODUCTION_RUNS):
        production_runs.append({
            "RunID": f"RUN-{3000 + i}",
            "VIN_Serial": fake.bothify(text='############'),
            "Model": random.choice(models),
            "StartTime": fake.date_time_between(start_date='-30d', end_date='now'),
            "LineID": f"LINE-{random.randint(1, 5)}",
            "TargetBuildTime_hrs": random.randint(10, 30)
        })
    production_runs_df = pd.DataFrame(production_runs); run_ids = production_runs_df['RunID'].tolist()

    # --- 4. Assembly_Steps (Efficiency Fact) ---
    steps = ['Chassis Welding', 'Engine Install', 'Interior Trim', 'Paint Booth', 'Final Inspection']
    assembly_steps = []
    for i in range(NUM_ASSEMBLY_STEPS):
        run_id = random.choice(run_ids)
        step_name = random.choice(steps)
        start_time = fake.date_time_between(start_date='-30d', end_date='now')
        assembly_steps.append({
            "StepID": f"STEP-{40000 + i}",
            "RunID": run_id,
            "StepName": step_name,
            "WorkerID": f"WKR-{random.randint(100, 200)}",
            "StartTime": start_time,
            "EndTime": start_time + timedelta(minutes=random.randint(5, 60)),
            "IsRework": random.choices([True, False], weights=[5, 95], k=1)[0]
        })
    assembly_steps_df = pd.DataFrame(assembly_steps)
    
    # --- 5. Quality_Sensor_Data (Quality Fact) ---
    sensor_types = ['Vibration', 'Temperature', 'Torque']
    quality_sensor_data = []
    for i in range(NUM_SENSOR_READINGS):
        run_id = random.choice(run_ids)
        quality_sensor_data.append({
            "ReadingID": f"SENSOR-{500000 + i}",
            "RunID": run_id,
            "SensorType": random.choice(sensor_types),
            "MeasurementTime": fake.date_time_between(start_date='-30d', end_date='now'),
            "ReadingValue": round(random.uniform(50, 150), 3),
            "IsAnomaly": random.choices([True, False], weights=[2, 98], k=1)[0],
            "AssemblyStep": random.choice(steps)
        })
    quality_sensor_data_df = pd.DataFrame(quality_sensor_data)

    logging.info(f"Generated {len(parts_df)} parts, {len(production_runs_df)} runs, and {len(assembly_steps_df) + len(quality_sensor_data_df)} manufacturing records.")
    
    return {
        "parts": parts_df, 
        "inventory_levels": inventory_levels_df, 
        "production_runs": production_runs_df,
        "assembly_steps": assembly_steps_df,
        "quality_sensor_data": quality_sensor_data_df
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Automotive Manufacturing data and deploy Data Products.")
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
                data_tables = generate_automotive_data()
                
                # 3. UPLOAD DATA (5 tables)
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")
            
        # 4. DEPLOY DATAPRODUCTS: Scans the directory and deploys all three YAML files.
        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)
            
        logging.info("Automotive Manufacturing data pipeline executed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")