# observability/observability_data.py

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

# Load environment variables from project root and local .env
load_project_env(__file__)

fake = Faker()

# --- Configuration (Volume) ---
NUM_TRACES = 25000
NUM_COMPUTE_METRICS = 100000
NUM_DB_LOGS = 50000
NUM_NETWORK_RECORDS = 75000
NUM_AUTH_LOGS = 1000
NUM_WAF_LOGS = 500
NUM_BILLING_RECORDS_PER_DAY = 20 # One per server

def get_config():
    """Loads configuration for the RAW TARGET from environment variables."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["OBS_RAW_CATALOG"],
            "schema": os.environ["OBS_RAW_SCHEMA"],
            "location": os.environ["OBS_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for OBSERVABILITY: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the OBSERVABILITY variables in your local observability/.env file.")
        sys.exit(1)

def generate_observability_data():
    logging.info("Starting Full-Stack Observability data generation for the last 30 days...")

    # Define historical date range variables for realistic monitoring
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # Define shared component lists
    hosts = [f"app-server-{i}.prod.local" for i in range(1, 21)]
    services = ['cart-service', 'payment-service', 'auth-service', 'recommendation-engine']
    endpoints = ['/api/v1/checkout', '/api/v1/process', '/api/v1/login', '/api/v1/suggestions']
    dbs = ['orders_db', 'users_db', 'inventory_db']
    devices = [f"core-router-{i}" for i in range(1, 6)] + [f"tor-switch-{i}" for i in range(1, 11)]
    user_ids = [f"user_{random.randint(100, 999)}" for _ in range(500)]

    # --- 1. Application Layer: Request Traces (ENRICHED) ---
    traces = []
    for _ in range(NUM_TRACES):
        status = random.choices([200, 201, 500, 503, 404], weights=[85, 10, 2, 1, 2], k=1)[0]
        traces.append({
            "trace_id": str(uuid.uuid4()),
            "timestamp": fake.date_time_between(start_date=start_date, end_date=end_date),
            "service_name": random.choice(services),
            "endpoint": random.choice(endpoints),
            "server_hostname": random.choice(hosts), # ENRICHMENT: Link trace to compute
            "http_status": status,
            "latency_ms": random.randint(50, 200) if status < 500 else random.randint(500, 5000),
            "customer_id": f"cust_{random.randint(1000, 5000)}",
            "cart_value": round(random.uniform(10.50, 850.00), 2) if status < 400 else 0.0,
            "error_message": "Service Unavailable" if status >= 500 else None
        })
    app_traces_df = pd.DataFrame(traces)

    # --- 2. Compute Layer: Server Metrics ---
    metrics = []
    for _ in range(NUM_COMPUTE_METRICS):
        metrics.append({
            "timestamp": fake.date_time_between(start_date=start_date, end_date=end_date),
            "server_hostname": random.choice(hosts),
            "cpu_utilization_pct": round(random.uniform(10.0, 99.5), 2),
            "memory_utilization_pct": round(random.uniform(25.0, 95.0), 2),
            "disk_io_mbs": round(random.uniform(5.0, 300.0), 2)
        })
    compute_metrics_df = pd.DataFrame(metrics)

    # --- 3. Data Layer: Database Query Logs ---
    db_logs = []
    for _ in range(NUM_DB_LOGS):
        db_logs.append({
            "query_id": str(uuid.uuid4()),
            "timestamp": fake.date_time_between(start_date=start_date, end_date=end_date),
            "database_name": random.choice(dbs),
            "query_hash": fake.md5(),
            "execution_time_ms": random.randint(10, 800),
            "lock_wait_ms": random.choices([0, random.randint(50, 1000)], weights=[95, 5], k=1)[0]
        })
    db_query_logs_df = pd.DataFrame(db_logs)

    # --- 4. Network/Hardware Layer: Device Health ---
    net_health = []
    for _ in range(NUM_NETWORK_RECORDS):
        net_health.append({
            "timestamp": fake.date_time_between(start_date=start_date, end_date=end_date),
            "device_id": random.choice(devices),
            "device_type": 'router' if 'router' in random.choice(devices) else 'switch',
            "throughput_gbps": round(random.uniform(1.0, 40.0), 2),
            "health_status": random.choices(['HEALTHY', 'DEGRADED', 'OFFLINE'], weights=[97, 2, 1], k=1)[0]
        })
    network_device_health_df = pd.DataFrame(net_health)
    
    # --- 5. Security Layer: Authentication Logs ---
    auth_logs = []
    suspicious_ips = [fake.ipv4() for _ in range(20)]
    for _ in range(NUM_AUTH_LOGS):
        is_suspicious = random.random() < 0.1
        auth_logs.append({
            "log_id": str(uuid.uuid4()),
            "timestamp": fake.date_time_between(start_date=start_date, end_date=end_date),
            "user_id": random.choice(user_ids),
            "ip_address": random.choice(suspicious_ips) if is_suspicious else fake.ipv4(),
            "user_agent": fake.user_agent(),
            "login_successful": False if is_suspicious else random.choices([True, False], weights=[98, 2], k=1)[0],
        })
    auth_logs_df = pd.DataFrame(auth_logs)

    # --- 6. Security Layer: Web Application Firewall (WAF) Logs ---
    waf_logs = []
    attack_types = ['SQL Injection', 'Cross-Site Scripting (XSS)', 'Path Traversal']
    for _ in range(NUM_WAF_LOGS):
         waf_logs.append({
            "event_id": str(uuid.uuid4()),
            "timestamp": fake.date_time_between(start_date=start_date, end_date=end_date),
            "client_ip": random.choice(suspicious_ips),
            "http_method": random.choice(['GET', 'POST']),
            "request_uri": fake.uri_path(),
            "action": 'BLOCK',
            "attack_type": random.choice(attack_types),
            "rule_id": f"WAF_RULE_{random.randint(1001, 1099)}"
        })
    waf_logs_df = pd.DataFrame(waf_logs)

    # --- 7. **NEW** FinOps Layer: Cloud Billing Data ---
    billing_data = []
    service_map = {
        'cart-service': 'e-commerce-prod', 'payment-service': 'e-commerce-prod',
        'auth-service': 'security-prod', 'recommendation-engine': 'data-science-prod'
    }
    for day in range(30):
        billing_date = end_date - timedelta(days=day)
        for host in hosts:
            service = next((s for s in services if s in host or random.random() > 0.5), random.choice(services))
            billing_data.append({
                "billing_record_id": str(uuid.uuid4()),
                "usage_date": billing_date.date(),
                "resource_id": host,
                "service_tag": service_map.get(service, 'general-compute'),
                "cost_usd": round(random.uniform(15.50, 80.00), 2),
                "usage_type": "EC2:BoxUsage:t3.xlarge"
            })
    cloud_billing_data_df = pd.DataFrame(billing_data)

    logging.info(f"Generated {len(app_traces_df)} app traces, {len(compute_metrics_df)} compute metrics, and {len(cloud_billing_data_df)} billing records.")

    return {
        "app_traces": app_traces_df,
        "compute_metrics": compute_metrics_df,
        "db_query_logs": db_query_logs_df,
        "network_device_health": network_device_health_df,
        "auth_logs": auth_logs_df,
        "waf_logs": waf_logs_df,
        "cloud_billing_data": cloud_billing_data_df
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Observability data and deploy Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Skip schema setup and data ingestion, only deploy Data Products.')
    args = parser.parse_args()

    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"

    try:
        engine = create_engine(engine_string)

        if not args.deploy_only:
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                data_tables = generate_observability_data()
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("Schema setup failed. Cannot proceed to deploy Data Products.")
                sys.exit(1)
        else:
            logging.info("Deployment only mode: Skipping schema setup and data generation/upload.")

        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)

        logging.info("Observability data pipeline executed successfully.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
