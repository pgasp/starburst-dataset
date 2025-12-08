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

----
# 🚀 How to Use It
## Prompt example :
### Supply Chain Logistics
`Create a Data Product for Supply Chain Logistics. Entities: Shipments, Warehouses, Carriers, Routes. Goal: Track On-Time Delivery Rate and Average Cost per Mile. Note: Use the existing project structure.`


```console
Please generate a complete dataset and two distinct Data Products for Energy Consumption Monitoring in a Smart Grid context.
    1. The Dataset (Synthetic Data) Create a Python generation script (energy_data.py) for a relational schema with these key entities:
        Smart Meters: (MeterID, Location, CustomerType, InstallationDate)
        Substations: (SubstationID, Region, Capacity_MW)
        Meter Readings: (ReadingID, MeterID (FK), Timestamp, Usage_kWh, Voltage_V) - High volume time-series data.
        Weather Data: (WeatherID, Region (FK), Date, Temperature_C, Humidity) - For correlation analysis.
        Tariff Plans: (PlanID, Name, PeakRate, OffPeakRate)
    2. The Data Products (Semantic Layer) Provide two separate YAML definitions (peak_usage_dp.yaml and forecast_dp.yaml) adhering to the project's strict naming and security conventions:
        Data Product A: Peak Usage Analysis (Operations Focus)
            Goal: Identify grid strain and load balancing needs.
            View 1: v_regional_peak_load - Aggregates max usage per substation/region per hour.
            View 2: v_high_usage_customers - Lists customers exceeding a threshold during peak hours (e.g., 6 PM - 9 PM).
    Data Product B: Consumption Forecasting (Planning Focus)
            Goal: Predict future demand based on historical trends and weather.
            Materialized View: mv_daily_usage_trends - Aggregates daily total consumption per region joined with average temperature. Set a refresh_interval of 24h.
            View: v_weather_impact_correlation - Calculates the correlation between temperature drops/spikes and energy usage spikes.
    Constraints:
            Ensure the domain is set to 'Energy & Utilities'.
```
