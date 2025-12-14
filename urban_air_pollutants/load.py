# 3️⃣ Load (load.py) – Supabase
# Create table:
# air_quality_data (
#     id BIGSERIAL PRIMARY KEY,
#     city TEXT,
#     time TIMESTAMP,
#     pm10 FLOAT,
#     pm2_5 FLOAT,
#     carbon_monoxide FLOAT,
#     nitrogen_dioxide FLOAT,
#     sulphur_dioxide FLOAT,
#     ozone FLOAT,
#     uv_index FLOAT,
#     aqi_category TEXT,
#     severity_score FLOAT,
#     risk_flag TEXT,
#     hour INTEGER)
# Load Requirements
# Batch insert records (batch size = 200)
# Auto-convert NaN → NULL
# Convert datetime to ISO formatted strings
# Retry failed batches (2 retries)
# Print summary of inserted rows

import pandas as pd
import numpy as np
from supabase import create_client, Client
import time

# ---------------------------
# Supabase Setup
# ---------------------------
SUPABASE_URL = "https://brzhhzroggpnyzjtuqlp.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJyemhoenJvZ2dwbnl6anR1cWxwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjUzMzY1OTUsImV4cCI6MjA4MDkxMjU5NX0.v0UjfKrDST3VQ1EaV5OugmImKEWwsPCeMWD6aKD1Lxw"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "air_quality_data"
BATCH_SIZE = 200


# ---------------------------
# Helper: Convert NaN → None
# ---------------------------
def clean_record(row):
    record = row.to_dict()

    # Convert NaN to None
    for k, v in record.items():
        if isinstance(v, float) and np.isnan(v):
            record[k] = None

    # Convert datetime to ISO format if needed
    if isinstance(record["time"], pd.Timestamp):
        record["time"] = record["time"].isoformat()

    return record


# ---------------------------
# Insert batch with retry logic
# ---------------------------
def insert_batch(batch_records, retries=2):
    for attempt in range(retries + 1):
        try:
            response = supabase.table(TABLE_NAME).insert(batch_records).execute()

            if response.data is not None:
                return True

            print(f"⚠️ Insert returned no data (attempt {attempt+1}). Retrying...")
        except Exception as e:
            print(f"❌ Batch insert failed (attempt {attempt+1}): {e}")

        time.sleep(1)

    return False

def create_table_if_not_exists():
    print("🛠 Checking/Creating table in Supabase...")

    create_sql = """
    CREATE TABLE IF NOT EXISTS air_quality_data (
        id BIGSERIAL PRIMARY KEY,
        city TEXT,
        time TIMESTAMP,
        pm10 FLOAT,
        pm2_5 FLOAT,
        carbon_monoxide FLOAT,
        nitrogen_dioxide FLOAT,
        sulphur_dioxide FLOAT,
        ozone FLOAT,
        uv_index FLOAT,
        aqi_category TEXT,
        severity_score FLOAT,
        risk_flag TEXT,
        hour INTEGER
    );
    """

    try:
        supabase.rpc("exec_sql", {"sql": create_sql}).execute()
        print("✔ Table created or already exists.")
    except Exception as e:
        print("❌ Error creating table:", e)

# ---------------------------
# Load Pipeline
# ---------------------------
def load_to_supabase(csv_path="data/staged/air_quality_transformed.csv"):
    print("\n🚀 Loading transformed data into Supabase...")

    # Read CSV
    df = pd.read_csv(csv_path)

    total_rows = len(df)
    print(f"📄 Total rows to insert: {total_rows}")

    # Convert time column to datetime
    df["time"] = pd.to_datetime(df["time"])

    success_count = 0
    fail_count = 0

    # Create batches
    for i in range(0, total_rows, BATCH_SIZE):
        batch_df = df.iloc[i : i + BATCH_SIZE]
        batch_records = [clean_record(row) for _, row in batch_df.iterrows()]

        print(f"\n📦 Inserting batch {i//BATCH_SIZE + 1} "
              f"({len(batch_records)} rows)...")

        # Insert with retry
        ok = insert_batch(batch_records)

        if ok:
            print("✅ Batch inserted successfully.")
            success_count += len(batch_records)
        else:
            print("❌ Batch failed after retries.")
            fail_count += len(batch_records)

    print("\n==========================")
    print("📊 LOAD SUMMARY")
    print("==========================")
    print(f"✅ Successfully inserted: {success_count}")
    print(f"❌ Failed inserts:       {fail_count}")
    print("==========================")

    return success_count, fail_count


# ---------------------------
# Main Trigger
# ---------------------------
if __name__ == "__main__":
    load_to_supabase()
