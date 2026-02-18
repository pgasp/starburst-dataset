# airline_operations/airline_operations_data.py

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
import uuid

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

# --- Configuration (Volume) ---
NUM_FLIGHTS = 20000
NUM_BOOKINGS_PER_FLIGHT_RANGE = (80, 250)

# --- Static Reference Data ---
AIRPORTS = [
    {"code": "JFK", "name": "John F. Kennedy International Airport", "city": "New York", "country": "USA"},
    {"code": "LAX", "name": "Los Angeles International Airport", "city": "Los Angeles", "country": "USA"},
    {"code": "ORD", "name": "O'Hare International Airport", "city": "Chicago", "country": "USA"},
    {"code": "DFW", "name": "Dallas/Fort Worth International Airport", "city": "Dallas", "country": "USA"},
    {"code": "DEN", "name": "Denver International Airport", "city": "Denver", "country": "USA"},
    {"code": "LHR", "name": "Heathrow Airport", "city": "London", "country": "UK"},
    {"code": "CDG", "name": "Charles de Gaulle Airport", "city": "Paris", "country": "France"},
    {"code": "HND", "name": "Haneda Airport", "city": "Tokyo", "country": "Japan"},
    {"code": "DXB", "name": "Dubai International Airport", "city": "Dubai", "country": "UAE"},
    {"code": "SFO", "name": "San Francisco International Airport", "city": "San Francisco", "country": "USA"},
]

AIRLINES = [
    {"code": "UA", "name": "United Airlines"},
    {"code": "AA", "name": "American Airlines"},
    {"code": "DL", "name": "Delta Air Lines"},
    {"code": "BA", "name": "British Airways"},
    {"code": "AF", "name": "Air France"},
]

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["AIRLINE_RAW_CATALOG"],
            "schema": os.environ["AIRLINE_RAW_SCHEMA"],
            "location": os.environ["AIRLINE_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for AIRLINE OPS: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the AIRLINE variables in your local airline_operations/.env file.")
        sys.exit(1)

def generate_airline_data():
    logging.info("Starting Airline Operations data generation...")

    # --- 1. Airports ---
    airports_df = pd.DataFrame(AIRPORTS)
    airport_codes = airports_df['code'].tolist()

    # --- 2. Airlines ---
    airlines_df = pd.DataFrame(AIRLINES)
    airline_codes = airlines_df['code'].tolist()

    # --- 3. Flights ---
    flights = []
    for i in range(NUM_FLIGHTS):
        origin, dest = random.sample(airport_codes, 2)
        airline_code = random.choice(airline_codes)
        scheduled_departure = fake.date_time_between(start_date='-90d', end_date='+30d')
        flight_duration_hours = random.uniform(1, 10)
        scheduled_arrival = scheduled_departure + timedelta(hours=flight_duration_hours)

        departure_delay = 0
        arrival_delay = 0
        cancelled = random.choices([True, False], weights=[5, 95], k=1)[0]
        actual_departure = None
        actual_arrival = None

        if not cancelled:
            delay_chance = random.random()
            if delay_chance < 0.20: # 20% chance of delay
                departure_delay = random.randint(5, 180)
            
            # Arrival delay is correlated with departure delay, but can vary
            arrival_delay = max(0, departure_delay + random.randint(-15, 25))
            actual_departure = scheduled_departure + timedelta(minutes=departure_delay)
            actual_arrival = scheduled_arrival + timedelta(minutes=arrival_delay)

        flights.append({
            "FlightID": str(uuid.uuid4()),
            "FlightNumber": f"{airline_code}{random.randint(100, 2999)}",
            "AirlineCode": airline_code,
            "OriginAirportCode": origin,
            "DestinationAirportCode": dest,
            "ScheduledDepartureTime": scheduled_departure,
            "ActualDepartureTime": actual_departure,
            "ScheduledArrivalTime": scheduled_arrival,
            "ActualArrivalTime": actual_arrival,
            "DepartureDelayMinutes": departure_delay,
            "ArrivalDelayMinutes": arrival_delay,
            "DistanceMiles": int(flight_duration_hours * 500), # Approximation
            "Cancelled": cancelled
        })
    flights_df = pd.DataFrame(flights)

    # --- 4. Bookings ---
    bookings = []
    active_flights = flights_df[flights_df['Cancelled'] == False]
    for _, flight in active_flights.iterrows():
        num_bookings = random.randint(*NUM_BOOKINGS_PER_FLIGHT_RANGE)
        for _ in range(num_bookings):
            bookings.append({
                "BookingID": str(uuid.uuid4()),
                "FlightID": flight['FlightID'],
                "PassengerID": str(uuid.uuid4()),
                "SeatNumber": f"{random.randint(1, 40)}{random.choice(['A', 'B', 'C', 'D', 'E', 'F'])}",
                "BookingTimestamp": fake.date_time_between(start_date=flight['ScheduledDepartureTime'] - timedelta(days=90), end_date=flight['ScheduledDepartureTime'] - timedelta(days=1)),
                "Fare": round(random.uniform(150.0, 1200.0), 2)
            })
    bookings_df = pd.DataFrame(bookings)

    logging.info(f"Generated {len(airlines_df)} airlines, {len(airports_df)} airports, {len(flights_df)} flights, and {len(bookings_df)} bookings.")

    return {
        "airlines": airlines_df,
        "airports": airports_df,
        "flights": flights_df,
        "bookings": bookings_df
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Airline Operations data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()

    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"

    try:
        engine = create_engine(engine_string)

        if not args.deploy_only:
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                data_tables = generate_airline_data()
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")

        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)

        logging.info("Airline Operations data pipeline executed successfully.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
