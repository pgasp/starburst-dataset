# Starburst Data Product Pipelines

The **Starburst Data Product Pipelines** project is a comprehensive framework for generating multi-domain synthetic data and deploying pre-defined semantic layers (Data Products) on a Starburst Enterprise (Trino) cluster.

It is ideal for demonstrating, benchmarking, or developing data products in environments that separate raw data ingestion from semantic consumption layers.

## Features

* **Multi-Domain Data Generation:** Scripts for six distinct business domains to create large volumes of realistic, synthetic raw data.
* **Automated Data Lake Setup:** Utilities to drop and recreate Iceberg schemas (or equivalent) for clean, repeatable pipeline runs.
* **Parallel Data Ingestion:** Efficiently uploads generated Pandas DataFrames to the data lakehouse using parallel processing.
* **Data Product Deployment:** Automatically reads YAML definitions, substitutes environment variables, and calls the Starburst Data Product API to create and publish semantic views.
* **Layered Configuration:** Uses `python-dotenv` to manage global and domain-specific environment variables, ensuring local settings override global ones.

### Supported Business Domains

The project includes full end-to-end data pipelines for the following domains, each generating raw tables and deploying a set of Data Products:

| Domain | Focus / Data Products | Raw Data Tables |
| :--- | :--- | :--- |
| **Retail Banking** | Customer 360, Credit Risk, Branch Performance | `customers`, `accounts`, `transactions`, `loans`, `branches`, `marketing_campaigns` |
| **E-Commerce** | Customer Analytics (CLV), Inventory & Fulfillment | `customers`, `products`, `orders`, `web_sessions` |
| **Telecom** | BSS (Revenue), OSS (Network Health), Federated Churn | `customers`, `service_subscriptions`, `invoices`, `customer_usage`, `network_performance`, `fault_records` |
| **Automotive** | Inventory & Logistics, Factory Efficiency, Quality Control | `parts`, `inventory_levels`, `production_runs`, `assembly_steps`, `quality_sensor_data` |
| **Risk & Stress Test** | Credit Risk Portfolio, Market Risk VaR/Greeks | `loan_portfolios`, `trading_positions`, `market_data`, `stress_scenarios` |
| **CIB ESG** | Client Risk Screening, Financed Emissions, S/G Metrics | `client_entities`, `esg_ratings`, `financing_deals`, `e_metrics`, `s_metrics`, `g_metrics` |

---

## Getting Started

### Prerequisites

You will need the following installed:

* Python 3.8+
* `git`
* Access to a Starburst Enterprise cluster and credentials with API access to create Schemas and Data Products.
* Access to an S3/compatible object storage bucket for the Iceberg table location.

### Installation

1.  **Clone the repository:**
    ```bash
    git clone [your-repo-url]/starburst-dataset.git
    cd starburst-dataset
    ```
2.  **Install Python Dependencies:**
    The project uses an editable installation (`-e .`) to make the `shared_tools` package available to all domain scripts, along with required libraries.
    ```bash
    pip install -e .
    pip install -r requirements.txt
    ```

### Configuration

The project uses a layered `.env` file structure handled by `env_utils.py`.

1.  **Global Configuration (`.env`)**
    This file must be created in the project root (`starburst-dataset/`). It holds your general Starburst connection details and the base location for Data Product domains.

    ```ini
    # starburst-dataset/.env
    SB_HOST=ai-workshop.enablement.starburstdata.net
    SB_URL=[https://ai-workshop.enablement.starburstdata.net](https://ai-workshop.enablement.starburstdata.net)
    SB_PORT=443
    SB_USER=YOUR_USERNAME
    SB_PASSWORD=YOUR_PASSWORD
    SB_DOMAIN_LOCATION_BASE = "s3://your-bucket-name/dataproduct/" # UPDATE THIS
    ```
    *Note: The `SB_DOMAIN_LOCATION_BASE` should point to a storage path where Starburst can manage new domain schemas.*

2.  **Domain-Specific Configuration (`domain_folder/.env`)**
    Each domain folder (e.g., `e_commerce/`) contains its own `.env` file for defining the RAW data target and the semantic view destinations. These values will **override** or supplement the global `.env` settings.

    *Example (`e_commerce/.env`)*:
    ```ini
    # ECOMMERCE_RAW variables define where the generated data is deposited.
    ECOMMERCE_RAW_CATALOG=iceberg      
    ECOMMERCE_RAW_SCHEMA=ecommerce_raw
    ECOMMERCE_SB_SCHEMA_LOCATION=s3://your-bucket-name/iceberg/e_commerce/ 

    # ECOMMERCE_ANALYTICS variables define where the semantic views are published.
    ECOMMERCE_ANALYTICS_DP_CATALOG=dataproduct       
    ECOMMERCE_ANALYTICS_DP_SCHEMA=ecommerce_analytics
    ```

## Usage

To run a pipeline, execute the main Python script in the domain folder. The script performs the entire end-to-end flow:

1.  Loads configuration (global + local `.env`).
2.  Creates the SQL engine connection.
3.  Calls `setup_schema` (drops/recreates the raw schema).
4.  Calls `generate_*_data` (creates in-memory DataFrames).
5.  Calls `upload_to_starburst_parallel` (inserts raw data).
6.  Calls `scan_and_deploy` (publishes the Data Product YAML files).

### Example: Run the E-Commerce Pipeline

```bash
# Execute the main data script in the e_commerce directory
python starburst-dataset/e_commerce/e_commerce_data.py