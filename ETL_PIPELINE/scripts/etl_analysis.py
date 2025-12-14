# ANALYSIS REPORT (etl_analysis.py)
# Read table from Supabase and generate:
# 📊 Metrics
# Churn percentage
# Average monthly charges per contract
# Count of new, regular, loyal, champion customers
# Internet service distribution
# Pivot table: Churn vs Tenure Group
# Optional visualizations:
# Churn rate by Monthly Charge Segment
# Histogram of TotalCharges
# Bar plot of Contract types
# Save output CSV into:
# data/processed/analysis_summary.csv

# ============================================================
# etl_analysis.py
# Purpose: Read churn table from Supabase → Run analysis →
# Generate metrics, visualizations, and save summary CSV.
# ============================================================

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import matplotlib.pyplot as plt


# ------------------------------------------------------------
# Supabase Client
# ------------------------------------------------------------
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


def fetch_data(table="telco_customer_data"):
    supabase = get_supabase_client()
    res = supabase.table(table).select("*").execute()
    df = pd.DataFrame(res.data)
    return df


# ------------------------------------------------------------
# Customer Category Logic (New, Regular, Loyal, Champion)
# ------------------------------------------------------------
def categorize_customers(tenure):
    if tenure < 12:
        return "New"
    elif 12 <= tenure < 36:
        return "Regular"
    elif 36 <= tenure < 60:
        return "Loyal"
    else:
        return "Champion"


# ------------------------------------------------------------
# MAIN ANALYSIS FUNCTION
# ------------------------------------------------------------
def run_analysis(
    output_csv="data/processed/analysis_summary.csv",
    table_name="telco_customer_data",
    save_plots=True,
):
    print("\n📊 Loading data from Supabase...\n")

    df = fetch_data(table_name)

    if df.empty:
        print("❌ No data found in Supabase table.")
        return

    # --------------------------------------------------------
    # Add Category Column
    # --------------------------------------------------------
    df["customer_segment"] = df["tenure"].apply(categorize_customers)

    # --------------------------------------------------------
    # METRICS
    # --------------------------------------------------------

    # 1. Churn %
    churn_rate = (df["Churn"].str.lower().eq("yes").mean()) * 100

    # 2. Average monthly charges per contract
    avg_monthly_by_contract = (
        df.groupby("Contract")["MonthlyCharges"].mean().round(2)
    )

    # 3. Customer segment counts
    segment_counts = df["customer_segment"].value_counts()

    # 4. Internet service distribution
    internet_dist = df["InternetService"].value_counts()

    # 5. Pivot: Churn vs Tenure Group
    pivot_churn_tenure = pd.pivot_table(
        df,
        values="id",
        index="tenure_group",
        columns="Churn",
        aggfunc="count",
        fill_value=0,
    )

    # --------------------------------------------------------
    # CREATE SUMMARY FOR CSV
    # --------------------------------------------------------
    summary = {
        "churn_percentage": round(churn_rate, 2),
        "avg_monthly_charge_month-to-month": avg_monthly_by_contract.get(
            "Month-to-month", None
        ),
        "avg_monthly_charge_one_year": avg_monthly_by_contract.get("One year", None),
        "avg_monthly_charge_two_year": avg_monthly_by_contract.get("Two year", None),
        "count_new_customers": segment_counts.get("New", 0),
        "count_regular_customers": segment_counts.get("Regular", 0),
        "count_loyal_customers": segment_counts.get("Loyal", 0),
        "count_champion_customers": segment_counts.get("Champion", 0),
        "dsl_users": internet_dist.get("DSL", 0),
        "fiber_users": internet_dist.get("Fiber optic", 0),
        "no_internet_users": internet_dist.get("No", 0),
    }

    summary_df = pd.DataFrame(summary.items(), columns=["metric", "value"])

    # Ensure folder exists
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    summary_df.to_csv(output_csv, index=False)
    print(f"✅ Analysis saved to: {output_csv}")

    # --------------------------------------------------------
    # OPTIONAL VISUALIZATIONS
    # --------------------------------------------------------
    if save_plots:
        print("\n📈 Generating charts...")

        # 1. Churn Rate by Monthly Charge Segment
        plt.figure(figsize=(6, 4))
        df.groupby("monthly_charge_segment")["Churn"].apply(
            lambda x: (x.str.lower() == "yes").mean()
        ).plot(kind="bar")
        plt.title("Churn Rate by Monthly Charge Segment")
        plt.ylabel("Churn Rate")
        plt.tight_layout()
        plt.savefig("data/processed/churn_by_charge_segment.png")

        # 2. Histogram of Total Charges
        plt.figure(figsize=(6, 4))
        df["TotalCharges"].astype(float).plot(kind="hist", bins=30)
        plt.title("Distribution of Total Charges")
        plt.xlabel("Total Charges")
        plt.tight_layout()
        plt.savefig("data/processed/hist_total_charges.png")

        # 3. Contract Types
        plt.figure(figsize=(6, 4))
        df["Contract"].value_counts().plot(kind="bar")
        plt.title("Contract Type Distribution")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig("data/processed/contract_distribution.png")

        print("🎨 Charts saved in data/processed/")

    # --------------------------------------------------------
    # PRINT SUMMARY
    # --------------------------------------------------------
    print("\n===== ANALYSIS SUMMARY =====")
    print(f"📌 Churn Rate: {summary['churn_percentage']}%")
    print("\n📌 Avg Monthly Charges by Contract:")
    print(avg_monthly_by_contract)
    print("\n📌 Customer Segments:")
    print(segment_counts)
    print("\n📌 Internet Service Distribution:")
    print(internet_dist)
    print("\n📌 Churn vs Tenure Group (Pivot):")
    print(pivot_churn_tenure)

    print("\n🎯 Analysis Completed.\n")

    return summary_df


# ------------------------------------------------------------
# Run Script
# ------------------------------------------------------------
if __name__ == "__main__":
    run_analysis()


