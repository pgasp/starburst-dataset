# e_commerce/e_commerce_data.py

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
NUM_CUSTOMERS = 10000
NUM_PRODUCTS = 2000
NUM_ORDERS = 50000
NUM_SESSIONS = 150000

def get_config():
    """Loads configuration from environment variables for the RAW TARGET."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            # RAW DATA TARGET (Using ECOMMERCE_RAW variables)
            "catalog": os.environ["ECOMMERCE_RAW_CATALOG"], 
            "schema": os.environ["ECOMMERCE_RAW_SCHEMA"],
            "location": os.environ["ECOMMERCE_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERROR: Missing configuration for E-COMMERCE: {e} ---")
        logging.error("Please check for SB_HOST in your root .env file, and the ECOMMERCE variables in your local e_commerce/.env file.")
        sys.exit(1)

def generate_ecommerce_data():
    logging.info("Starting E-Commerce data generation...")
    today = datetime.now().date()
    
    # --- 1. Customers (Dimension) ---
    regions = ['US_West', 'US_East', 'EU_North', 'APAC']
    customers = []
    for i in range(NUM_CUSTOMERS):
        customers.append({
            "CustomerID": f"CUST-{10000 + i}", 
            "JoinedDate": fake.date_between(start_date='-5y', end_date='-60d'),
            "CustomerRegion": random.choice(regions),
            "MarketingOptIn": random.choice([True, False]),
            "LastLoginDate": fake.date_time_between(start_date='-30d', end_date='now')
        })
    customers_df = pd.DataFrame(customers); customer_ids = customers_df['CustomerID'].tolist()

    # --- 2. Products (Dimension) ---
    categories = ['Electronics', 'Apparel', 'Home Goods', 'Books', 'Groceries']
    products = []
    for i in range(NUM_PRODUCTS):
        products.append({
            "ProductID": f"PROD-{2000 + i}", 
            "ProductName": fake.catch_phrase(),
            "Category": random.choice(categories),
            "Price": round(random.uniform(5, 500), 2),
            "StockQuantity": random.randint(0, 500),
            "SupplierID": f"SUP-{random.randint(10, 50)}"
        })
    products_df = pd.DataFrame(products); product_ids = products_df['ProductID'].tolist()

    # --- 3. Orders (Fact) ---
    order_statuses = ['Completed', 'Shipped', 'Pending', 'Cancelled']
    orders = []
    for i in range(NUM_ORDERS):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        
        # Determine how many items were purchased (1 to 5)
        num_items = random.randint(1, 5)
        order_items = random.choices(product_ids, k=num_items)
        
        total_price = 0
        for prod_id in order_items:
            price = products_df[products_df['ProductID'] == prod_id]['Price'].iloc[0]
            total_price += price
        
        orders.append({
            "OrderID": f"ORD-{30000 + i}", 
            "CustomerID": customer_id, 
            "OrderDate": order_date,
            "TotalAmount": round(total_price * random.uniform(0.9, 1.1), 2), # Apply small variance/discount
            "ShippingCost": round(random.uniform(5, 20), 2),
            "OrderStatus": random.choices(order_statuses, weights=[80, 10, 5, 5], k=1)[0],
            "ShippedDate": order_date + timedelta(days=random.randint(1, 7)) if random.random() < 0.9 else None
        })
    orders_df = pd.DataFrame(orders)

    # --- 4. Web Sessions (Fact) ---
    event_types = ['PageView', 'ProductView', 'AddToCart', 'Checkout']
    web_sessions = []
    for i in range(NUM_SESSIONS):
        web_sessions.append({
            "SessionID": f"SESS-{400000 + i}", 
            "CustomerID": random.choice(customer_ids) if random.random() < 0.8 else None, # 20% anonymous
            "SessionStart": fake.date_time_between(start_date='-30d', end_date='now'),
            "DeviceType": random.choice(['Mobile', 'Desktop', 'Tablet']),
            "EventType": random.choice(event_types),
            "ProductID": random.choice(product_ids) if random.random() < 0.6 else None # Not every event is product-related
        })
    web_sessions_df = pd.DataFrame(web_sessions)
    
    logging.info(f"Generated {len(customers_df)} customers, {len(products_df)} products, {len(orders_df)} orders, and {len(web_sessions_df)} web sessions.")
    
    return {
        "customers": customers_df, 
        "products": products_df, 
        "orders": orders_df,
        "web_sessions": web_sessions_df
    }


if __name__ == "__main__":
    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"
    
    try:
        engine = create_engine(engine_string)
        
        # 1. SETUP SCHEMA
        if setup_schema(engine, config['catalog'], config['schema'], config['location']):
            
            # 2. GENERATE DATA
            data_tables = generate_ecommerce_data()
            
            # 3. UPLOAD DATA
            upload_to_starburst_parallel(engine, config['schema'], data_tables)
            
            # 4. DEPLOY DATAPRODUCTS: Scans the directory and deploys both YAML files.
            scan_and_deploy("./e_commerce")
            
            logging.info("E-Commerce data pipeline executed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")