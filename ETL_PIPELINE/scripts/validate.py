# VALIDATION SCRIPT (validate.py)
# After load, write a script that checks:
# No missing values in:
# tenure, MonthlyCharges, TotalCharges
# Unique count of rows = original dataset
# Row count matches Supabase table
# All segments (tenure_group, monthly_charge_segment) exist
# Contract codes are only {0,1,2}
# Print a validation summary.
 
 
 # ===========================================
# validate.py
# Purpose: Validate the Supabase-loaded churn dataset
# ===========================================

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv


# -------------------------------------------------------
# Initialize Supabase Client
# -------------------------------------------------------
def get_supabase_client():
    load_dotenv()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


# -------------------------------------------------------
# Fetch all rows from Supabase table
# -------------------------------------------------------
def fetch_supabase_data(table_name="telco_customer_data"):
    supabase = get_supabase_client()
    data = supabase.table(table_name).select("*").execute()
    return pd.DataFrame(data.data)


# -------------------------------------------------------
# VALIDATION FUNCTION
# -------------------------------------------------------
def run_validation(
    original_csv="../data/staged/churn_transformed.csv",
    table_name="telco_customer_data",
):
    print("\n🔍 Starting Validation...\n")

    # Load original dataset
    df_original = pd.read_csv(original_csv)
    df_supabase = fetch_supabase_data(table_name)

    summary = {}

    # ----------------------------------------
    # 1. Check Missing Values in key columns
    # ----------------------------------------
    required_cols = ["tenure", "MonthlyCharges", "TotalCharges"]
    missing_dict = {
        col: df_supabase[col].isna().sum() if col in df_supabase else "Column Missing"
        for col in required_cols
    }

    summary["missing_values"] = missing_dict

    # ----------------------------------------
    # 2. Unique count of rows = original dataset
    # ----------------------------------------
    summary["original_rows"] = len(df_original)
    summary["supabase_rows"] = len(df_supabase)

    # ----------------------------------------
    # 3. All segment categories exist
    # ----------------------------------------
    expected_tenure_groups = {
        "0-12",
        "12-24",
        "24-48",
        "48-60",
        "60-72",
        "72+",
    }  # Example buckets
    expected_monthly_charge_segments = {"Low", "Medium", "High"}

    if "tenure_group" in df_supabase:
        tenure_groups_present = set(df_supabase["tenure_group"].dropna().unique())
    else:
        tenure_groups_present = set()

    if "monthly_charge_segment" in df_supabase:
        charge_segments_present = set(
            df_supabase["monthly_charge_segment"].dropna().unique()
        )
    else:
        charge_segments_present = set()

    summary["tenure_groups_present"] = tenure_groups_present
    summary["missing_tenure_groups"] = expected_tenure_groups - tenure_groups_present

    summary["charge_segments_present"] = charge_segments_present
    summary["missing_charge_segments"] = (
        expected_monthly_charge_segments - charge_segments_present
    )

    # ----------------------------------------
    # 4. Contract type codes are only {0,1,2}
    # ----------------------------------------
    if "contract_type_code" in df_supabase:
        invalid_codes = set(df_supabase["contract_type_code"].dropna().unique()) - {
            0,
            1,
            2,
        }
        summary["invalid_contract_codes"] = invalid_codes
    else:
        summary["invalid_contract_codes"] = "Column missing"

    # ----------------------------------------
    # 5. Print Summary
    # ----------------------------------------
    print("===== 🧪 VALIDATION SUMMARY =====\n")

    # Missing Values
    print("📌 Missing Values Check:")
    for col, miss in summary["missing_values"].items():
        if miss == 0:
            print(f"   ✅ {col}: No missing values")
        else:
            print(f"   ❌ {col}: {miss} missing values")

    # Row Count Check
    print("\n📌 Row Count Check:")
    if summary["original_rows"] == summary["supabase_rows"]:
        print(f"   ✅ Row count matches ({summary['supabase_rows']})")
    else:
        print(
            f"   ❌ Row count mismatch → Original: {summary['original_rows']} | Supabase: {summary['supabase_rows']}"
        )

    # Segment Validation
    print("\n📌 Tenure Group Segments:")
    print(f"   Present: {summary['tenure_groups_present']}")
    if summary["missing_tenure_groups"]:
        print(f"   ❌ Missing: {summary['missing_tenure_groups']}")
    else:
        print("   ✅ All segments present")

    print("\n📌 Monthly Charge Segments:")
    print(f"   Present: {summary['charge_segments_present']}")
    if summary["missing_charge_segments"]:
        print(f"   ❌ Missing: {summary['missing_charge_segments']}")
    else:
        print("   ✅ All segments present")

    # Contract Type Code Validation
    print("\n📌 Contract Type Code Check:")
    if summary["invalid_contract_codes"]:
        print(f"   ❌ Invalid codes found: {summary['invalid_contract_codes']}")
    else:
        print("   ✅ All contract codes valid (0,1,2)")

    print("\n🎯 Validation Complete.\n")

    return summary


# -------------------------------------------------------
# Run script
# -------------------------------------------------------
if __name__ == "__main__":
    run_validation()

