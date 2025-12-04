# clariane/clariane_data.py

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

load_project_env(__file__)

fake = Faker()

# --- Configuration (Volume) ---
NUM_FACILITIES = 50
NUM_RESIDENTS = 150000
NUM_STAFF = 10000
NUM_CARE_EVENTS = 1000000 
NUM_TRAINING = 20000

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["CLARIANE_RAW_CATALOG"], 
            "schema": os.environ["CLARIANE_RAW_SCHEMA"],
            "location": os.environ["CLARIANE_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for CLARIANE: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the CLARIANE variables in your local clariane/.env file.")
        sys.exit(1)

def generate_clariane_data():
    logging.info("Starting Clariane Health & Care data generation...")
    
    # --- 1. Facilities (Dimension) ---
    activity_types = ['Long-Term Care', 'Specialty Care', 'Community Care']
    facilities = []
    for i in range(NUM_FACILITIES):
        facilities.append({
            "FacilityID": f"FAC-{i + 1}", 
            "FacilityName": fake.company() + " Residence",
            "ActivityType": random.choice(activity_types),
            "Country": random.choice(['France', 'Germany', 'Italy', 'Belgium']),
            "Capacity": random.randint(50, 200)
        })
    facilities_df = pd.DataFrame(facilities); facility_ids = facilities_df['FacilityID'].tolist()

    # --- 2. Residents (Dimension) ---
    vulnerability_levels = ['Low', 'Medium', 'High']
    residents = []
    for i in range(NUM_RESIDENTS):
        facilities_id = random.choice(facility_ids)
        residents.append({
            "ResidentID": f"RES-{i + 1}",
            "FacilityID": facilities_id,
            "AdmissionDate": fake.date_between(start_date='-3y', end_date='-90d'),
            "VulnerabilityLevel": random.choices(vulnerability_levels, weights=[40, 40, 20], k=1)[0],
            "CarePlanID": f"PLAN-{random.randint(1000, 9999)}",
            "PrimaryDiagnosis": random.choice(['Alzheimers', 'Mobility', 'Rehabilitation', 'Palliative'])
        })
    residents_df = pd.DataFrame(residents); resident_ids = residents_df['ResidentID'].tolist()
    
    # --- 3. Staff (Dimension) ---
    roles = ['Nurse', 'Caregiver', 'Doctor', 'Administrator', 'Specialist']
    staff = []
    for i in range(NUM_STAFF):
        staff.append({
            "StaffID": f"STF-{i + 1}",
            "FacilityID": random.choice(facility_ids),
            "Role": random.choices(roles, weights=[30, 40, 5, 5, 20], k=1)[0],
            "EmploymentStatus": random.choices(['Full-Time', 'Part-Time'], weights=[80, 20], k=1)[0],
            "HireDate": fake.date_between(start_date='-10y', end_date='-1y'),
            "MonthlySalary": round(random.uniform(2000, 8000), 2)
        })
    staff_df = pd.DataFrame(staff); staff_ids = staff_df['StaffID'].tolist()

    # --- 4. Care_Events (Fact) ---
    event_types = ['Medication Admin', 'Assistance', 'Vital Check', 'Fall Incident', 'Training']
    care_events = []
    for i in range(NUM_CARE_EVENTS):
        event_type = random.choice(event_types)
        is_incident = event_type == 'Fall Incident'
        care_events.append({
            "EventID": f"EVT-{i + 1}",
            "ResidentID": random.choice(resident_ids),
            "StaffID": random.choice(staff_ids),
            "Timestamp": fake.date_time_between(start_date='-60d', end_date='now'),
            "EventType": event_type,
            "Duration_min": random.randint(5, 60),
            "IsCriticalIncident": is_incident
        })
    care_events_df = pd.DataFrame(care_events)

    # --- 5. Training_Records (HR Fact) ---
    training_modules = ['First Aid', 'Ethics & Compliance', 'Specialty Care', 'Vulnerability Mgmt']
    training_records = []
    for i in range(NUM_TRAINING):
        training_records.append({
            "TrainingID": f"TRN-{i + 1}",
            "StaffID": random.choice(staff_ids),
            "Module": random.choice(training_modules),
            "CompletionDate": fake.date_time_between(start_date='-1y', end_date='now'),
            "Score": random.randint(70, 100),
            "IsMandatory": random.choices([True, False], weights=[70, 30], k=1)[0]
        })
    training_records_df = pd.DataFrame(training_records)

    logging.info(f"Generated {len(facilities_df)} facilities, {len(residents_df)} residents, and {len(care_events_df)} care events.")
    
    return {
        "facilities": facilities_df, 
        "residents": residents_df, 
        "staff": staff_df,
        "care_events": care_events_df,
        "training_records": training_records_df
    }


if __name__ == "__main__":
    # 1. Argument Parsing (NEW)
    parser = argparse.ArgumentParser(description="Generate Clariane data and deploy Data Products.")
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
                data_tables = generate_clariane_data()
                
                # 3. UPLOAD DATA (5 tables)
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")
            
        # 4. DEPLOY DATAPRODUCTS
        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)
            
        logging.info("Clariane data pipeline executed successfully.")
          
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")