
# 🟩 4️⃣ Analysis (etl_analysis.py)
# Read the loaded data from Supabase and perform:
# A. KPI Metrics
# City with highest average PM2.5
# City with the highest severity score
# Percentage of High/Moderate/Low risk hours
# Hour of day with worst AQI
# B. City Pollution Trend Report
# For each city:
# time → pm2_5, pm10, ozone
# C. Export Outputs
# Save the following CSVs into data/processed/:
# summary_metrics.csv
# city_risk_distribution.csv
# pollution_trends.csv
# D. Visualizations
# Save the following PNG plots:
# Histogram of PM2.5
# Bar chart of risk flags per city
# Line chart of hourly PM2.5 trends
# Scatter: severity_score vs pm2_5



#!/usr/bin/env python3
"""
etl_analysis.py

Reads air_quality_data from Supabase, computes KPIs, saves CSV summaries and plots.

Outputs (saved to data/processed/):
- summary_metrics.csv
- city_risk_distribution.csv
- pollution_trends.csv
- pm25_histogram.png
- risk_flags_per_city.png
- hourly_pm25_trends.png
- severity_vs_pm25_scatter.png
"""

import os
import logging
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from supabase import create_client
from dotenv import load_dotenv

# -------------------------
# Config & setup
# -------------------------
load_dotenv()  # loads .env if present

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Please set SUPABASE_URL and SUPABASE_KEY environment variables (service role key).")

OUTPUT_DIR = "data/processed/"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/etl_analysis.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------
# Supabase client
# -------------------------
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------
# Helper: fetch table into DataFrame
# -------------------------


def fetch_air_quality_table():
    url = "https://brzhhzroggpnyzjtuqlp.supabase.co"
    key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJyemhoenJvZ2dwbnl6anR1cWxwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjUzMzY1OTUsImV4cCI6MjA4MDkxMjU5NX0.v0UjfKrDST3VQ1EaV5OugmImKEWwsPCeMWD6aKD1Lxw"
    client = create_client(url, key)

    # New SDK returns APIResponse object
    res = client.table("air_quality_data").select("*").execute()

    # Extract data safely
    try:
        data = res.data
    except:
        raise Exception("Supabase: unable to read .data from response")

    if not data or len(data) == 0:
        raise Exception("Supabase returned empty data")

    df = pd.DataFrame(data)
    return df


# -------------------------
# Analysis functions
# -------------------------
def prepare_df(df):
    # Standardize column names (lowercase)
    df.columns = [c.lower() for c in df.columns]

    # Ensure datetime
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Numeric conversions
    pollutant_cols = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index", "severity_score"]
    for col in pollutant_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ensure risk flag column name
    if "risk_flag" not in df.columns and "risk_level" in df.columns:
        df["risk_flag"] = df["risk_level"]

    # Hour column
    if "hour" not in df.columns:
        if "time" in df.columns:
            df["hour"] = df["time"].dt.hour
        else:
            df["hour"] = np.nan

    # Drop rows with no city or no time
    df = df.dropna(subset=["city", "time"], how="any").reset_index(drop=True)

    return df

def compute_kpis(df):
    kpis = {}

    # A. City with highest average PM2.5
    if "pm2_5" in df.columns:
        avg_pm25 = df.groupby("city")["pm2_5"].mean().dropna()
        if not avg_pm25.empty:
            best = avg_pm25.idxmax()
            kpis["city_highest_avg_pm2_5"] = best
            kpis["city_highest_avg_pm2_5_value"] = round(avg_pm25.max(), 3)
        else:
            kpis["city_highest_avg_pm2_5"] = None
            kpis["city_highest_avg_pm2_5_value"] = None

    # B. City with the highest severity score (mean)
    if "severity_score" in df.columns:
        avg_severity = df.groupby("city")["severity_score"].mean().dropna()
        if not avg_severity.empty:
            best_s = avg_severity.idxmax()
            kpis["city_highest_severity"] = best_s
            kpis["city_highest_severity_value"] = round(avg_severity.max(), 3)
        else:
            kpis["city_highest_severity"] = None
            kpis["city_highest_severity_value"] = None

    # C. Percentage of High/Moderate/Low risk hours
    if "risk_flag" in df.columns:
        counts = df["risk_flag"].value_counts(dropna=False)
        total = counts.sum()
        perc = {label: round((counts.get(label, 0) / total) * 100, 2) for label in ["High Risk", "Moderate Risk", "Low Risk"]}
        kpis["risk_percentages"] = perc
    else:
        kpis["risk_percentages"] = {}

    # D. Hour of day with worst AQI (use pm2_5 mean across hours)
    if "pm2_5" in df.columns and "hour" in df.columns:
        hourly = df.groupby("hour")["pm2_5"].mean().dropna()
        if not hourly.empty:
            worst_hour = int(hourly.idxmax())
            kpis["worst_aqi_hour"] = worst_hour
            kpis["worst_aqi_hour_pm2_5"] = round(hourly.max(), 3)
        else:
            kpis["worst_aqi_hour"] = None
            kpis["worst_aqi_hour_pm2_5"] = None

    return kpis

def city_risk_distribution(df):
    # Count risk flags per city
    if "risk_flag" not in df.columns:
        return pd.DataFrame()
    dist = df.groupby(["city", "risk_flag"]).size().reset_index(name="count")
    # Convert counts to percentages per city
    totals = dist.groupby("city")["count"].transform("sum")
    dist["percent"] = (dist["count"] / totals * 100).round(2)
    return dist

def pollution_trends(df):
    # For each city create a time-series with pm2_5, pm10, ozone
    # We'll return a flat table: city, time, pm2_5, pm10, ozone
    cols = ["city", "time", "pm2_5", "pm10", "ozone"]
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    trends = df[cols].sort_values(["city", "time"]).reset_index(drop=True)
    return trends

# -------------------------
# Visualization functions
# -------------------------
def plot_pm25_histogram(df, outpath):
    plt.figure(figsize=(8, 5))
    data = df["pm2_5"].dropna()
    plt.hist(data, bins=30)
    plt.title("Histogram of PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_risk_bar_by_city(df, outpath):
    # Count risk flags per city
    if "risk_flag" not in df.columns:
        return
    pivot = df.groupby(["city", "risk_flag"]).size().unstack(fill_value=0)
    pivot.plot(kind="bar", stacked=False, figsize=(10, 6))
    plt.title("Risk Flags per City")
    plt.xlabel("City")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_hourly_pm25_trends(df, outpath):
    # Average pm2_5 by hour for each city (lines)
    if "pm2_5" not in df.columns:
        return
    df_hour = df.dropna(subset=["pm2_5", "hour"])
    avg_hour_city = df_hour.groupby(["hour", "city"])["pm2_5"].mean().reset_index()
    pivot = avg_hour_city.pivot(index="hour", columns="city", values="pm2_5")
    pivot.plot(figsize=(10, 6))
    plt.title("Hourly PM2.5 Trends by City (avg)")
    plt.xlabel("Hour of Day")
    plt.ylabel("Average PM2.5")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_severity_vs_pm25(df, outpath):
    if "severity_score" not in df.columns or "pm2_5" not in df.columns:
        return
    sub = df.dropna(subset=["severity_score", "pm2_5"])
    plt.figure(figsize=(8, 6))
    plt.scatter(sub["pm2_5"], sub["severity_score"])
    plt.title("Severity Score vs PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Severity Score")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

# -------------------------
# Main runner
# -------------------------
def run_analysis():
    logging.info("Starting ETL analysis job")

    df = fetch_air_quality_table()
    if df.empty:
        logging.warning("No rows fetched from Supabase - exiting")
        print("No data available in Supabase table.")
        return

    df = prepare_df(df)

    # Compute KPIs
    kpis = compute_kpis(df)
    logging.info("KPIs computed: %s", kpis)

    # Prepare summary_metrics.csv
    summary = {
        "key": [
            "city_highest_avg_pm2_5",
            "city_highest_avg_pm2_5_value",
            "city_highest_severity",
            "city_highest_severity_value",
            "worst_aqi_hour",
            "worst_aqi_hour_pm2_5"
        ],
        "value": [
            kpis.get("city_highest_avg_pm2_5"),
            kpis.get("city_highest_avg_pm2_5_value"),
            kpis.get("city_highest_severity"),
            kpis.get("city_highest_severity_value"),
            kpis.get("worst_aqi_hour"),
            kpis.get("worst_aqi_hour_pm2_5")
        ]
    }
    summary_df = pd.DataFrame(summary)
    # add risk percentages as separate rows
    risk_perc = kpis.get("risk_percentages", {})
    for k, v in risk_perc.items():
        new_row = pd.DataFrame([{"key": f"risk_pct_{k.replace(' ', '_')}", "value": v}])
        summary_df = pd.concat([summary_df, new_row], ignore_index=True)


    summary_path = os.path.join(OUTPUT_DIR, "summary_metrics.csv")
    summary_df.to_csv(summary_path, index=False)

    # City risk distribution CSV
    dist_df = city_risk_distribution(df)
    dist_path = os.path.join(OUTPUT_DIR, "city_risk_distribution.csv")
    dist_df.to_csv(dist_path, index=False)

    # Pollution trends CSV
    trends_df = pollution_trends(df)
    trends_path = os.path.join(OUTPUT_DIR, "pollution_trends.csv")
    trends_df.to_csv(trends_path, index=False)

    # Visualizations
    plot_pm25_histogram(df, os.path.join(OUTPUT_DIR, "pm25_histogram.png"))
    plot_risk_bar_by_city(df, os.path.join(OUTPUT_DIR, "risk_flags_per_city.png"))
    plot_hourly_pm25_trends(df, os.path.join(OUTPUT_DIR, "hourly_pm25_trends.png"))
    plot_severity_vs_pm25(df, os.path.join(OUTPUT_DIR, "severity_vs_pm25_scatter.png"))

    logging.info("Saved outputs to: %s", OUTPUT_DIR)

    # Print concise summary
    print("\n==== ETL ANALYSIS SUMMARY ====")
    print(f"Rows fetched: {len(df)}")
    print(f"Summary metrics saved to: {summary_path}")
    print(f"City risk distribution saved to: {dist_path}")
    print(f"Pollution trends saved to: {trends_path}")
    print("Plots saved to data/processed/*.png")
    print("KPIs:")
    for k, v in kpis.items():
        print(f" - {k}: {v}")

    logging.info("ETL analysis job completed")

if __name__ == "__main__":
    run_analysis()

 