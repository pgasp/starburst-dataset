# utils/lakehouse_utils.py

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Union
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype, is_string_dtype, is_bool_dtype

# --- Helper 1: Robust Type Mapping (Kept for consistency/future use) ---
def map_dtype_to_trino(dtype) -> str:
    """Maps DType objects to Trino/Starburst SQL types using Pandas API checks."""
    if is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP WITH TIME ZONE' 
    if is_bool_dtype(dtype):
        return 'BOOLEAN'
    if is_string_dtype(dtype):
        return 'VARCHAR'
    if is_numeric_dtype(dtype):
        try:
            if np.issubdtype(dtype, np.integer):
                return 'BIGINT'
            elif np.issubdtype(dtype, np.floating):
                return 'DOUBLE'
        except TypeError:
            return 'DOUBLE' 
        return 'DOUBLE' 
    return 'VARCHAR'

# --- Utility 1: Schema Setup ---
def setup_schema(engine: Engine, catalog: str, schema: str, location: str) -> bool:
    """Drops and recreates the target schema in Starburst/Trino."""
    schema_full_name = f'"{catalog}"."{schema}"'
    drop_sql = text(f"DROP SCHEMA IF EXISTS {schema_full_name} CASCADE")
    create_sql = text(f"CREATE SCHEMA {schema_full_name} WITH (location = '{location}')")
    try:
        print(f"\n--- Setting up schema: {schema_full_name} ---")
        with engine.connect() as conn:
            conn.execute(drop_sql)
            print(f"  > Schema {schema_full_name} dropped if exists.")
            conn.execute(create_sql)
            conn.commit()
            print(f"  > Schema {schema_full_name} created with location '{location}'.")
        return True
    except Exception as e:
        print(f"❌ Schema setup failed: {e}"); return False

# --- Utility 2: Single Table Upload Helper (df.to_sql with method="multi") ---
def upload_single_table_batched(table_name: str, df: pd.DataFrame, engine: Engine, schema: str, batch_size: int) -> Dict[str, Union[str, int]]:
    """
    Helper function to upload a single DataFrame using the df.to_sql method with chunksize 
    and method="multi".
    """
    total_rows = len(df)
    
    try:
        print(f"  [THREAD: {table_name}] Starting df.to_sql batch upload (Chunk: {batch_size}, Rows: {total_rows})...")
        
        # 1. CRITICAL STEP: Type cleanup
        # This converts problematic Pandas Extension Types to standard NumPy types.
        df_cleaned = df.convert_dtypes()
        
        for col in df_cleaned.columns:
            dtype_name = str(df_cleaned[col].dtype)
            
            # Convert nullable numerics to np.float64 (standard NumPy float supports NaN/NULL)
            if 'Int64Dtype' in dtype_name or 'Float64Dtype' in dtype_name:
                df_cleaned[col] = df_cleaned[col].astype(np.float64) 
            # Convert nullable StringDtype to standard str
            elif 'StringDtype' in dtype_name:
                df_cleaned[col] = df_cleaned[col].astype(str)
        
        # 2. Optimized df.to_sql call
        df_cleaned.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="replace", # Ensures a fresh, clean table structure
            index=False,
            chunksize=batch_size, # Splits the DataFrame into memory-safe batches
            method="multi"        # Optimizes INSERT statements for bulk loading
        )
        
        print(f"  [THREAD: {table_name}] ✅ Successfully uploaded {total_rows} rows via df.to_sql batching.")
        
        return {
            "table": table_name,
            "status": "SUCCESS",
            "rows": total_rows
        }
    except Exception as e:
        print(f"  [THREAD: {table_name}] ❌ Upload failed for {table_name}: {e}")
        return {
            "table": table_name,
            "status": "FAILED",
            "error": str(e)
        }

# --- Utility 3: Parallel Upload Manager ---
def upload_to_starburst_parallel(engine: Engine, schema: str, dataframes_dict: Dict[str, pd.DataFrame], max_workers: int = 6, batch_size: int = 10000):
    """Manages the parallel upload of all DataFrames."""
    results = []
    num_tables = len(dataframes_dict)
    workers = min(max_workers, num_tables)
    
    print(f"\n🚀 Starting parallel upload of {num_tables} tables with {workers} workers.")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_table = {
            # Submits tasks for each table to run concurrently
            executor.submit(upload_single_table_batched, table_name, df, engine, schema, batch_size): table_name
            for table_name, df in dataframes_dict.items()
        }

        for future in as_completed(future_to_table):
            result = future.result()
            results.append(result)
            
            if result['status'] == 'SUCCESS':
                print(f"✅ Completed upload for {result['table']} with {result['rows']} rows.")
            else:
                print(f"❌ ERROR uploading {result['table']}: {result['error']}")

    print("--- All parallel uploads completed. ---")
    return results