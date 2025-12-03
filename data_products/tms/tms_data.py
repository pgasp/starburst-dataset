# tms/tms_data.py

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

load_project_env(__file__)

fake = Faker()

# --- Configuration (Volume) ---
NUM_VEHICLES = 500
NUM_CARRIERS = 50
NUM_TRIPS = 10000
NUM_SHIPMENTS = 50000 
NUM_TRACKING_POINTS = 100000 
NUM_INVOICES = 10000

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["TMS_RAW_CATALOG"], 
            "schema": os.environ["TMS_RAW_SCHEMA"],
            "location": os.environ["TMS_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for TMS: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the TMS variables in your local tms/.env file.")
        sys.exit(1)

def generate_tms_data():
    logging.info("Starting Transportation Management System (TMS) data generation...")
    
    # --- 1. Carriers (Dimension) ---
    carriers = [f"CARRIER-{i}" for i in range(1, NUM_CARRIERS + 1)]
    
    # --- 2. Vehicles (Dimension) ---
    vehicle_types = ['Semi-Truck', 'Box-Truck', 'Van']
    vehicles = []
    for i in range(NUM_VEHICLES):
        vehicles.append({
            "VehicleID": f"VEH-{i + 1}",
            "CarrierID": random.choice(carriers),
            "VehicleType": random.choice(vehicle_types),
            "FuelTankCapacity_L": random.randint(300, 1000),
            "AvgFuelConsumption_kml": round(random.uniform(2.5, 5.5), 2)
        })
    vehicles_df = pd.DataFrame(vehicles); vehicle_ids = vehicles_df['VehicleID'].tolist()

    # --- 3. Trips (Route/Asset Fact) ---
    trip_ids = [f"TRIP-{i + 1}" for i in range(NUM_TRIPS)]
    regions = ['Northeast', 'Midwest', 'WestCoast', 'Southeast']
    trips = []
    for trip_id in trip_ids:
        planned_miles = random.randint(100, 3000)
        planned_start = fake.date_time_between(start_date='-60d', end_date='now')
        
        trips.append({
            "TripID": trip_id,
            "VehicleID": random.choice(vehicle_ids),
            "OriginRegion": random.choice(regions),
            "DestinationRegion": random.choice(regions),
            "PlannedDistance_mi": planned_miles,
            "PlannedStartTime": planned_start,
            "PlannedDeliveryTime": planned_start + timedelta(hours=planned_miles / 50 + random.randint(1, 10)),
            "ActualDeliveryTime": None,
            "TripStatus": random.choices(['Completed', 'InTransit', 'Delayed', 'Cancelled'], weights=[70, 20, 5, 5], k=1)[0]
        })
    trips_df = pd.DataFrame(trips)
    
    # Update actual delivery time for completed/delayed trips
    for index, row in trips_df.iterrows():
        if row['TripStatus'] == 'Completed' or row['TripStatus'] == 'Delayed':
            actual_time = row['PlannedDeliveryTime'] + timedelta(minutes=random.randint(-120, 240))
            trips_df.at[index, 'ActualDeliveryTime'] = actual_time
            
    # --- 4. Shipments (Load Fact - Links to Trip) ---
    shipments = []
    for i in range(NUM_SHIPMENTS):
        shipments.append({
            "ShipmentID": f"LOAD-{i + 1}",
            "TripID": random.choice(trip_ids),
            "Weight_lbs": random.randint(100, 40000),
            "Commodity": fake.word().capitalize(),
            "CustomerAccount": f"CUST-{random.randint(1000, 9999)}"
        })
    shipments_df = pd.DataFrame(shipments)

    # --- 5. Tracking/Telemetry (Real-time Fact) ---
    tracking_points = []
    for i in range(NUM_TRACKING_POINTS):
        trip_id = random.choice(trips_df[trips_df['TripStatus'] == 'InTransit']['TripID'].tolist() or trips_df['TripID'].tolist())
        
        tracking_points.append({
            "TrackingID": f"TRK-{i + 1}",
            "TripID": trip_id,
            "Timestamp": fake.date_time_between(start_date='-2d', end_date='now'),
            "Latitude": round(random.uniform(30, 45), 6),
            "Longitude": round(random.uniform(-110, -80), 6),
            "Speed_mph": random.randint(0, 75),
            "FuelLevel_pct": random.randint(5, 100),
            "CurrentOdometer_mi": random.randint(50000, 500000)
        })
    tracking_points_df = pd.DataFrame(tracking_points)

    # --- 6. Costs/Invoices (Financial Fact) ---
    invoices = []
    for i in range(NUM_INVOICES):
        trip_id = random.choice(trips_df['TripID'].tolist())
        invoices.append({
            "InvoiceID": f"INV-{i + 1}",
            "TripID": trip_id,
            "BaseFreightCost": round(random.uniform(500, 5000), 2),
            "FuelSurcharge": round(random.uniform(50, 500), 2),
            "TotalInvoiceAmount": None,
            "PaymentStatus": random.choices(['Paid', 'Pending', 'Disputed'], weights=[80, 15, 5], k=1)[0]
        })
    invoices_df = pd.DataFrame(invoices)
    invoices_df['TotalInvoiceAmount'] = invoices_df['BaseFreightCost'] + invoices_df['FuelSurcharge']
    
    logging.info(f"Generated {len(trips_df)} trips, {len(shipments_df)} loads, and {len(tracking_points_df)} tracking records.")
    
    return {
        "carriers": pd.DataFrame({'CarrierID': carriers}), 
        "vehicles": vehicles_df, 
        "trips": trips_df,
        "shipments": shipments_df,
        "tracking_telemetry": tracking_points_df,
        "invoices": invoices_df
    }


if __name__ == "__main__":
    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"
    
    try:
        engine = create_engine(engine_string)
        
        # 1. SETUP SCHEMA
        if setup_schema(engine, config['catalog'], config['schema'], config['location']):
            
            # 2. GENERATE DATA
            data_tables = generate_tms_data()
            
            # 3. UPLOAD DATA (6 tables)
            upload_to_starburst_parallel(engine, config['schema'], data_tables)
            
            # 4. DEPLOY DATAPRODUCTS
            scan_and_deploy("./tms")
            
            logging.info("TMS data pipeline executed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")