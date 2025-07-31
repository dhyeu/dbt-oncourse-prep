import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# Load credentials
load_dotenv()

# Snowflake credentials
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

# Explicit context (needed!)
cursor.execute("USE DATABASE dbt_db;")
cursor.execute("USE SCHEMA analytics;")
cursor.execute("USE WAREHOUSE dbt_wh;")

def upload_csv_to_table(csv_path, table_name):
    # Read CSV
    df = pd.read_csv(csv_path)

    # Create or replace table (basic types — customize later if needed)
    create_stmt = f"CREATE OR REPLACE TABLE {table_name} (\n" + ",\n".join(
        [f"{col} VARCHAR" for col in df.columns]) + "\n);"
    cursor.execute(create_stmt)

    # Upload rows
    for _, row in df.iterrows():
        values = "', '".join(str(x).replace("'", "") for x in row.values)
        insert_stmt = f"INSERT INTO {table_name} VALUES ('{values}');"
        cursor.execute(insert_stmt)

    print(f"✅ Uploaded {len(df)} rows to {table_name}")

# Ingest CSVs
upload_csv_to_table("customers.csv", "raw_customers")
upload_csv_to_table("orders.csv", "raw_orders")
upload_csv_to_table("events.csv", "raw_events")

cursor.close()
conn.close()