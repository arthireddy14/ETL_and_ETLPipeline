# 3️⃣ LOAD TO SUPABASE (load.py)
# Create a table:
# id BIGSERIAL PRIMARY KEY,
# tenure INTEGER,
# MonthlyCharges FLOAT,
# TotalCharges FLOAT,
# Churn TEXT,
# InternetService TEXT,
# Contract TEXT,
# PaymentMethod TEXT,
# tenure_group TEXT,
# monthly_charge_segment TEXT,
# has_internet_service INTEGER,
# is_multi_line_user INTEGER,
# contract_type_code INTEGER
# Load Data
# Batch size = 200 records at a time
# Convert NaN → None
# Insert with error handling + retry logic


# ===========================
# Purpose: Load transformed churn dataset into Supabase using Supabase client
# ===========================

import os
import time
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv


# ---------------------------------------------------
# Initialize Supabase client
# ---------------------------------------------------
def get_supabase_client() -> Client:
    """Initialize and return Supabase client using .env file."""

    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


# ---------------------------------------------------
# Step 1: Create table (if RPC is configured)
# ---------------------------------------------------
def create_table_if_not_exists():
    """Create telco_customer_data table if not exists using RPC."""
    try:
        supabase = get_supabase_client()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS public.telco_customer_data (
            id BIGSERIAL PRIMARY KEY,
            tenure INTEGER,
            MonthlyCharges FLOAT,
            TotalCharges FLOAT,
            Churn TEXT,
            InternetService TEXT,
            Contract TEXT,
            PaymentMethod TEXT,
            tenure_group TEXT,
            monthly_charge_segment TEXT,
            has_internet_service INTEGER,
            is_multi_line_user INTEGER,
            contract_type_code INTEGER
        );
        """

        try:
            supabase.rpc("execute_sql", {"query": create_table_sql}).execute()
            print("✅ Table 'telco_customer_data' created or already exists.")
        except Exception as e:
            print(f"ℹ️ RPC not configured or failed → {e}")
            print("ℹ️ Table must already exist OR create manually in Supabase.")

    except Exception as e:
        print(f"⚠️ Error checking/creating table: {e}")


# ---------------------------------------------------
# Step 2: Load CSV → Supabase (Batch Insert + Retry)
# ---------------------------------------------------
def load_to_supabase(staged_path: str, table_name: str = "telco_customer_data"):
    """Load transformed CSV into Supabase in batches with retry logic."""

    # Convert to absolute path
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), staged_path)
        )

    print(f"🔍 Looking for data file at: {staged_path}")

    if not os.path.exists(staged_path):
        print(f"❌ File not found: {staged_path}")
        return

    try:
        supabase = get_supabase_client()

        df = pd.read_csv(staged_path)
        df = df.where(pd.notnull(df), None)  # NaN → None
        total_rows = len(df)

        print(f"📊 Preparing to load {total_rows} rows...")

        batch_size = 200
        max_retries = 3

        for start in range(0, total_rows, batch_size):
            batch_df = df.iloc[start : start + batch_size].copy()
            records = batch_df.to_dict("records")

            # -------- Retry Mechanism ----------
            for retry in range(max_retries):
                try:
                    response = supabase.table(table_name).insert(records).execute()

                    if hasattr(response, "error") and response.error:
                        raise Exception(response.error)

                    end = min(start + batch_size, total_rows)
                    print(f"✅ Inserted rows {start + 1}-{end}/{total_rows}")
                    break  # Successful → stop retry loop

                except Exception as e:
                    print(f"⚠️ Error inserting batch {start//batch_size + 1}: {e}")

                    if retry < max_retries - 1:
                        print(f"🔁 Retrying ({retry + 1}/{max_retries})...")
                        time.sleep(2)
                    else:
                        print("❌ Failed after retries. Skipping batch.")
                        break

        print(f"🎯 Finished loading data into '{table_name}'.")

    except Exception as e:
        print(f"❌ Error loading data: {e}")


# ---------------------------------------------------
# Step 3: Run as script
# ---------------------------------------------------
if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "churn_transformed.csv")

    create_table_if_not_exists()
    load_to_supabase(staged_csv_path)
