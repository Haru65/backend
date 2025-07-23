import pandas as pd
import os
from sqlalchemy import create_engine, text

# === DATABASE CONFIG ===
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'prototype3',
    'user': 'postgres',
    'password': 'aditya4f'
}

# === ETL CSV FILES ===
etl_files = {
    "process_master.csv": "PROCESS_MASTER",
    "routing_table.csv": "ROUTING_TABLE",
    "order_fact.csv": "ORDER_FACT",
    "jobs_fact.csv": "JOBS_FACT"
}

def get_db_connection():
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def create_database_tables(engine):
    print("[INFO] Creating database tables...")
    sql = """
    DROP TABLE IF EXISTS jobs CASCADE;
    DROP TABLE IF EXISTS routing CASCADE;
    DROP TABLE IF EXISTS orders CASCADE;
    DROP TABLE IF EXISTS processes CASCADE;
    DROP TABLE IF EXISTS items CASCADE;

    CREATE TABLE items (
        item_code VARCHAR(100) PRIMARY KEY
    );

    CREATE TABLE processes (
        process_code VARCHAR(100) PRIMARY KEY,
        process_full_name VARCHAR(300),
        process_owner VARCHAR(150),
        can_parallelize BOOLEAN DEFAULT FALSE
    );

    CREATE TABLE routing (
        id SERIAL PRIMARY KEY,
        item_code VARCHAR(100) REFERENCES items(item_code) ON DELETE CASCADE,
        sequence_no INTEGER,
        process_code VARCHAR(100) REFERENCES processes(process_code) ON DELETE CASCADE,
        lead_time_days DECIMAL(12,4)
    );

    CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        item_code VARCHAR(100) REFERENCES items(item_code) ON DELETE CASCADE,
        customer_name VARCHAR(300),
        po_date DATE,
        so_creation_date DATE,
        qty_ordered DECIMAL(15,4),
        qty_dispatched DECIMAL(15,4),
        qty_open DECIMAL(15,4),
        order_status VARCHAR(100)
    );

    CREATE TABLE jobs (
        id SERIAL PRIMARY KEY,
        item_code VARCHAR(100) REFERENCES items(item_code) ON DELETE CASCADE,
        process_code VARCHAR(100),
        stage VARCHAR(100),
        qty DECIMAL(15,4),
        tonnage DECIMAL(15,6),
        employee VARCHAR(200),
        datamatrix_serial VARCHAR(200),
        job_date TIMESTAMP,
        aging FLOAT
    );
    """
    with engine.begin() as conn:
        for stmt in sql.strip().split(';'):
            if stmt.strip():
                conn.execute(text(stmt))
    print(" Tables created successfully")

def clean_dataframe_for_db(df, tag):
    """Minimal cleaning that preserves data"""
    print(f"\n[DEBUG] === {tag} CLEANING ===")
    print(f"[DEBUG] {tag} - Original shape: {df.shape}")
    
    if df.empty:
        print(f"[DEBUG] {tag} - Input DataFrame is empty!")
        return df
    
    print(f"[DEBUG] {tag} - Columns: {list(df.columns)}")
    print(f"[DEBUG] {tag} - Sample data:\n{df.head(2)}")
    
    # Only do minimal cleaning
    df = df.copy()
    
    # Remove completely empty rows
    original_len = len(df)
    df = df.dropna(how='all')
    print(f"[DEBUG] {tag} - After dropna: {len(df)} (removed {original_len - len(df)} empty rows)")
    
    # Only check for required columns existence, don't filter aggressively
    required_columns = {
        "PROCESS_MASTER": ["PROCESS_CODE"],
        "ROUTING_TABLE": ["ITEM_CODE", "PROCESS_CODE"],
        "ORDER_FACT": ["ITEM_CODE"],
        "JOBS_FACT": ["ITEM_CODE"]
    }
    
    missing_cols = []
    for key in required_columns.get(tag, []):
        if key not in df.columns:
            missing_cols.append(key)
    
    if missing_cols:
        print(f"[DEBUG] {tag} - Missing required columns: {missing_cols}")
        return pd.DataFrame()  # Return empty if missing required columns
    
    # Remove duplicates
    before_dedup = len(df)
    df = df.drop_duplicates()
    print(f"[DEBUG] {tag} - After deduplication: {len(df)} (removed {before_dedup - len(df)})")
    
    print(f"[DEBUG] {tag} - FINAL: {df.shape} rows")
    return df

def insert_individual_csvs_to_postgres(file_map, engine):
    print(" Loading data from ETL CSVs...")
    create_database_tables(engine)
    
    processed = {}
    items_set = set()
    
    # STEP 1: First collect ALL item codes from all files
    print("\n=== STEP 1: Collecting all item codes ===")
    for fname, tag in file_map.items():
        if not os.path.exists(fname):
            continue
        try:
            df = pd.read_csv(fname)
            for col in ['ITEM_CODE', 'Item Code', 'item_code']:
                if col in df.columns:
                    items_set.update(df[col].dropna().astype(str).str.strip().tolist())
        except Exception as e:
            print(f"Error reading {fname}: {e}")
    
    # STEP 2: Insert items FIRST
    print(f"\n=== STEP 2: Inserting {len(items_set)} items ===")
    if items_set:
        items_df = pd.DataFrame({"item_code": list(items_set)}).drop_duplicates()
        try:
            items_df.to_sql("items", engine, index=False, if_exists="append", method="multi")
            processed["items"] = len(items_df)
            print(f" Inserted {len(items_df)} items")
        except Exception as e:
            print(f" Failed to insert items: {e}")
            return processed
    
    # STEP 3: Process each file
    for fname, tag in file_map.items():
        print(f"\n=== STEP 3: Processing {fname} ({tag}) ===")
        
        if not os.path.exists(fname):
            print(f" Skipped {fname}: File not found")
            continue
            
        try:
            df = pd.read_csv(fname)
            print(f"[INFO] Read {fname}: {df.shape}")
            
            if df.empty:
                print(f" Skipped {fname}: Empty file")
                continue
                
        except Exception as e:
            print(f" Failed to read {fname}: {e}")
            continue
        
        # Clean the dataframe (minimal cleaning)
        df = df.copy()
        df = df.dropna(how='all')
        df = df.drop_duplicates()
        
        if df.empty:
            print(f" Skipped {fname}: No valid data after cleaning")
            continue
        
        print(f" Attempting to insert {len(df)} rows from {fname}")
        
        # Insert data with explicit transaction handling
        try:
            with engine.begin() as conn:  # Explicit transaction
                if tag == "PROCESS_MASTER":
                    df.columns = [col.lower() for col in df.columns]
                    df.to_sql("processes", conn, index=False, if_exists='append', method='multi')
                    processed["processes"] = len(df)
                    print(f" Inserted {len(df)} processes")
                    
                elif tag == "ROUTING_TABLE":
                    required_cols = ["ITEM_CODE", "PROCESS_CODE", "LEAD_TIME_DAYS", "SEQUENCE"]
                    
                    if all(col in df.columns for col in required_cols):
                        routing_df = df[required_cols].copy()
                        routing_df.columns = ["item_code", "process_code", "lead_time_days", "sequence_no"]
                        
                        # Clean data types
                        routing_df["lead_time_days"] = pd.to_numeric(routing_df["lead_time_days"], errors='coerce')
                        routing_df["sequence_no"] = pd.to_numeric(routing_df["sequence_no"], errors='coerce')
                        routing_df = routing_df.dropna()
                        
                        # Filter out items that don't exist in items table
                        valid_items = set(items_df["item_code"].tolist())
                        routing_df = routing_df[routing_df["item_code"].isin(valid_items)]
                        
                        if not routing_df.empty:
                            routing_df.to_sql("routing", conn, index=False, if_exists='append', method='multi')
                            processed["routing"] = len(routing_df)
                            print(f" Inserted {len(routing_df)} routing records")
                        else:
                            print(" No valid routing records after filtering")
                    else:
                        print(f" Missing routing columns. Have: {list(df.columns)}")

                elif tag == "ORDER_FACT":
                    required_cols = ['ITEM_CODE', 'CUSTOMER_NAME', 'PO_DATE', 'SO_CREATION_DATE',
                                   'QTY_ORDERED', 'QTY_DISPATCHED', 'QTY_OPEN', 'ORDER_STATUS']
                    available_cols = [col for col in required_cols if col in df.columns]
                    
                    if len(available_cols) >= 4:
                        orders_df = df[available_cols].copy()
                        orders_df.columns = [col.lower() for col in orders_df.columns]
                        
                        # Convert dates
                        for date_col in ['po_date', 'so_creation_date']:
                            if date_col in orders_df.columns:
                                orders_df[date_col] = pd.to_datetime(orders_df[date_col], errors='coerce')
                        
                        # Convert quantities
                        for qty_col in ['qty_ordered', 'qty_dispatched', 'qty_open']:
                            if qty_col in orders_df.columns:
                                orders_df[qty_col] = pd.to_numeric(orders_df[qty_col], errors='coerce').fillna(0)
                        
                        # Filter out items that don't exist
                        valid_items = set(items_df["item_code"].tolist())
                        orders_df = orders_df[orders_df["item_code"].isin(valid_items)]
                        
                        if not orders_df.empty:
                            orders_df.to_sql("orders", conn, index=False, if_exists='append', method="multi")
                            processed["orders"] = len(orders_df)
                            print(f" Inserted {len(orders_df)} orders")
                        else:
                            print(" No valid orders after filtering")
                    else:
                        print(f" Missing order columns. Have: {list(df.columns)}")

                elif tag == "JOBS_FACT":
                    required_cols = ['ITEM_CODE', 'PROCESS', 'STAGE', 'QTY', 'TONNAGE',
                                   'EMPLOYEE', 'DATAMATRIX_SERIAL', 'DATE', 'AGING']
                    available_cols = [col for col in required_cols if col in df.columns]
                    
                    if 'ITEM_CODE' in available_cols:
                        jobs_df = df[available_cols].copy()
                        jobs_df.columns = [col.lower() for col in jobs_df.columns]
                        
                        # Rename columns to match database
                        if 'date' in jobs_df.columns:
                            jobs_df = jobs_df.rename(columns={'date': 'job_date'})
                        if 'process' in jobs_df.columns:
                            jobs_df = jobs_df.rename(columns={'process': 'process_code'})
                        
                        # Convert data types
                        if 'job_date' in jobs_df.columns:
                            jobs_df['job_date'] = pd.to_datetime(jobs_df['job_date'], errors='coerce')
                        
                        for num_col in ['qty', 'tonnage', 'aging']:
                            if num_col in jobs_df.columns:
                                jobs_df[num_col] = pd.to_numeric(jobs_df[num_col], errors='coerce')
                        
                        # CRITICAL: Filter out items that don't exist in items table
                        valid_items = set(items_df["item_code"].tolist())
                        before_filter = len(jobs_df)
                        jobs_df = jobs_df[jobs_df["item_code"].isin(valid_items)]
                        after_filter = len(jobs_df)
                        
                        print(f"[DEBUG] Jobs before item filter: {before_filter}")
                        print(f"[DEBUG] Jobs after item filter: {after_filter}")
                        print(f"[DEBUG] Filtered out {before_filter - after_filter} jobs with invalid item codes")
                        
                        if not jobs_df.empty:
                            jobs_df.to_sql("jobs", conn, index=False, if_exists='append', method="multi")
                            processed["jobs"] = len(jobs_df)
                            print(f" Inserted {len(jobs_df)} jobs")
                        else:
                            print(" No valid jobs after filtering")
                    else:
                        print(f" Missing job columns. Have: {list(df.columns)}")

        except Exception as e:
            print(f" Failed to insert {tag}: {e}")
            import traceback
            traceback.print_exc()

    print("\nDatabase Insertion Summary:")
    for k, v in processed.items():
        print(f"   {k}: {v} rows")
    
    return processed

def get_table_info(engine):
    tables = ['items', 'processes', 'routing', 'orders', 'jobs']
    info = {}
    with engine.begin() as conn:
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            info[table] = result.scalar()
    return info

def insert_all_to_postgres():
    """Main function for FastAPI integration"""
    engine = create_engine(get_db_connection())
    insert_individual_csvs_to_postgres(etl_files, engine)
    return get_table_info(engine)

if __name__ == "__main__":
    engine = create_engine(get_db_connection())
    result = insert_individual_csvs_to_postgres(etl_files, engine)
    
    print("\n Final Table Counts:")
    info = get_table_info(engine)
    for table, count in info.items():
        print(f"  {table}: {count} rows")
