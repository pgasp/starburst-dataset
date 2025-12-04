# Starburst Dataset Framework

This project is a comprehensive framework designed to generate synthetic raw datasets across various industry domains and automatically deploy a semantic layer on top of them using Starburst Data Products (DPs).

The goal is to demonstrate end-to-end data pipelines for creating ready-to-use, well-governed data assets.

## Project Overview & Technology Stack

The project leverages the following technologies:
- **Python**, **Pandas**, and the **Faker** library for creating realistic, high-volume synthetic data.
- Integration with **Trino/Starburst**, typically targeting an **Iceberg catalog** for raw data storage.

### Main Components:
1. **Shared Utilities (`shared_tools`)**:  
    A Python package containing reusable logic for:
    - Environment loading.
    - Parallel data ingestion to Starburst/Trino using `pandas.to_sql`.
    - API calls for Data Product creation and publishing.

2. **Data Domain Folders**:  
    Separate directories for each business unit (e.g., `telecom`, `automotive`, `retail_bank`), each with its own configuration and deployment files.

3. **Data Product YAMLs**:  
    Declarative files defining the semantic layer as Views and/or Materialized Views over the raw data.

---

## Data Pipeline Workflow

Each domain-specific Python script (e.g., `telecom/telecom_data.py`) follows a four-step pipeline:

1. **Setup Schema**:  
    - Connects to Starburst/Trino.  
    - Executes `DROP SCHEMA IF EXISTS` and `CREATE SCHEMA` commands to prepare the raw data location (e.g., an S3 bucket defined in `.env` files).

2. **Generate Data**:  
    - Calls a function (e.g., `generate_*_data`) using Pandas and Faker to create synthetic datasets for multiple tables (e.g., `customers`, `subscriptions`, `invoices`).

3. **Upload Data**:  
    - Uses the shared `upload_to_starburst_parallel` utility to ingest the generated Pandas DataFrames into the raw schema concurrently.

4. **Deploy Data Products**:  
    - Calls the `scan_and_deploy` utility, which:
      - Reads the YAML definitions in the domain folder.
      - Translates them into a payload.
      - Uses the Starburst API to create and publish the semantic views.

---  
This framework provides a robust solution for generating, managing, and deploying synthetic datasets with a semantic layer, enabling efficient data pipeline demonstrations across various industries.  