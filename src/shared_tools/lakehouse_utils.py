# src/shared_tools/lakehouse_utils.py

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from concurrent.futures import ProcessPoolExecutor, as_completed # CHANGED TO ProcessPoolExecutor
from typing import Dict, Any, Union
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype, is_string_dtype, is_bool_dtype
from pystarburst import Session 
from trino.auth import BasicAuthentication 

# --- Configuration Constant for Chunking ---
BATCH_SIZE_ROWS = 3000 

# --- Utility 1 & Helper 1 (Unchanged) ---

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

def setup_schema(engine: Engine, catalog: str, schema: str, location: str) -> bool:
    """Drops and recreates the target schema in Starburst/Trino."""
    schema_full_name = f'"{catalog}"."{schema}"'
    drop_sql = text(f"DROP SCHEMA IF EXISTS {schema_full_name} CASCADE")
    create_sql = text(f"CREATE SCHEMA {schema_full_name} WITH (location = '{location}')")
    try:
        print(f"\n--- Setting up schema: {schema_full_name} ---")
        with engine.connect() as conn:
            conn.execute(drop_sql)
            conn.execute(create_sql)
            conn.commit()
            print(f"  > Schema {schema_full_name} created with location '{location}'.")
        return True
    except Exception as e:
        print(f"❌ Schema setup failed: {e}"); return False

# --- Utility 2: Single Table Upload Helper (CORE PYSTARBURST LOGIC) ---
def upload_single_table_pystarburst(client: Session, table_name: str, df: pd.DataFrame, schema: str) -> Dict[str, Union[str, int]]:
    """
    Helper function to upload a single Pandas DataFrame using batched PyStarburst calls,
    with explicit datetime conversion to avoid type inference errors.
    """
    total_rows = len(df)
    df_clean = df.copy() 

    # FIX: Explicitly convert datetime columns to string before list conversion
    for col in df_clean.select_dtypes(include=['datetime64', 'datetime64[ns]']).columns:
        df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    try:
        print(f"  [PROCESS: {table_name}] Starting batched pystarburst upload (Rows: {total_rows})...")
        
        n_chunks = (total_rows + BATCH_SIZE_ROWS - 1) // BATCH_SIZE_ROWS
        
        for i in range(n_chunks):
            start_idx = i * BATCH_SIZE_ROWS
            end_idx = min((i + 1) * BATCH_SIZE_ROWS, total_rows)
            chunk_df = df_clean.iloc[start_idx:end_idx]
            
            mode = 'overwrite' if i == 0 else 'append'
            
            # 1. Convert Pandas DF chunk to PyStarburst DF (PS DF)
            ps_df = client.create_dataframe(chunk_df.values.tolist(), schema=chunk_df.columns.tolist())
            
            # 2. Write the PS DF chunk to the target table
            ps_df.write.save_as_table(
                f'{schema}.{table_name}',
                mode=mode,
                table_properties={'format': 'parquet'} 
            )
            print(f"  [PROCESS: {table_name}] Chunk {i+1}/{n_chunks} uploaded successfully ({len(chunk_df)} rows).")

        print(f"  [PROCESS: {table_name}] ✅ Successfully uploaded {total_rows} rows via batched pystarburst.")
        
        return {
            "table": table_name,
            "status": "SUCCESS",
            "rows": total_rows
        }
    except Exception as e:
        print(f"  [PROCESS: {table_name}] ❌ Upload failed for {table_name}: {e}")
        return {
            "table": table_name,
            "status": "FAILED",
            "error": str(e)
        }

# --- Utility 3: Multi-Process Wrapper (NEW) ---
def _upload_single_table_wrapper(conn_params: Dict[str, Union[str, int]], table_name: str, df: pd.DataFrame, schema: str):
    """
    Wrapper function that runs in a separate process, initializes its own Session,
    and calls the main upload logic.
    """
    try:
        # 1. Initialize a NEW PyStarburst Session for this process (Thread-safe)
        sb_client = Session.builder.configs(conn_params).create()
        
        # 2. Execute the single-table upload
        result = upload_single_table_pystarburst(sb_client, table_name, df, schema)
        
        # 3. Close the Session
        sb_client.close()
        return result
    except Exception as e:
        # Handle exceptions during connection setup
        return {
            "table": table_name,
            "status": "FAILED",
            "error": f"Process setup failed: {e}"
        }

# --- Utility 4: Parallel Upload Manager (RE-ENABLED PARALLELISM) ---
def upload_to_starburst_parallel(engine: Engine, schema: str, dataframes_dict: Dict[str, pd.DataFrame], max_workers: int = 6):
    """
    Manages the parallel upload of all DataFrames using multi-processing (ProcessPoolExecutor).
    """
    results = []
    num_tables = len(dataframes_dict)
    workers = min(max_workers, num_tables)
    
    # 1. Extract *serializable* connection details from the SQLAlchemy Engine URL
    try:
        url_parts = engine.url
        user, password = url_parts.username, url_parts.password
        host, port = url_parts.host, url_parts.port
        catalog = url_parts.database
        
        if not (host and user and password):
             raise ValueError("Missing connection details in SQLAlchemy Engine URL.")

        # Connection parameters dictionary (passed to each process)
        conn_params = {
            "host": host, 
            "port": port, 
            "user": user, 
            "catalog": catalog,
            "http_scheme": "https",
            # Pass the authentication object to be reconstructed in the child process
            "auth": BasicAuthentication(user, password) 
        }
    except Exception as e:
        print(f"❌ Failed to extract connection details: {e}")
        return [{"table": "Client Setup", "status": "FAILED", "error": f"Client initialization failed: {e}"}]

    print(f"\n🚀 Starting PARALLEL upload of {num_tables} tables with {workers} processes using pystarburst.")

    # 2. Use ProcessPoolExecutor for true parallelism (thread-safe sessions)
    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_table = {
            executor.submit(_upload_single_table_wrapper, conn_params, table_name, df, schema): table_name
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
