import os
import json
import pandas as pd
from datetime import datetime
import logging

RAW_DIR = "data/raw/"
STAGED_DIR = "data/staged/"
OUTPUT_FILE = os.path.join(STAGED_DIR, "air_quality_transformed.csv")

os.makedirs(STAGED_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/transform.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------------------------------------
# Helper: AQI Category (based on PM2.5)
# -------------------------------------------------------
def categorize_aqi(pm2_5):
    if pd.isna(pm2_5):
        return None
    if pm2_5 <= 50:
        return "Good"
    elif pm2_5 <= 100:
        return "Moderate"
    elif pm2_5 <= 200:
        return "Unhealthy"
    elif pm2_5 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

# -------------------------------------------------------
# Helper: Compute Severity Score
# -------------------------------------------------------
def compute_severity(row):
    return (
        (row["pm2_5"] * 5)
        + (row["pm10"] * 3)
        + (row["nitrogen_dioxide"] * 4)
        + (row["sulphur_dioxide"] * 4)
        + (row["carbon_monoxide"] * 2)
        + (row["ozone"] * 3)
    )

# -------------------------------------------------------
# Helper: Risk Classification
# -------------------------------------------------------
def classify_risk(severity):
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"

# -------------------------------------------------------
# MAIN TRANSFORM FUNCTION
# -------------------------------------------------------
def run_transform():

    all_rows = []

    # -------------------------------------------------------
    # Read each raw JSON file
    # -------------------------------------------------------
    for file in os.listdir(RAW_DIR):

        if not file.endswith(".json"):
            continue

        filepath = os.path.join(RAW_DIR, file)

        try:
            with open(filepath, "r") as f:
                raw = json.load(f)

        except Exception as e:
            logging.error(f"Error reading {filepath}: {str(e)}")
            continue

        # Extract city name from file (since filename = city_raw_timestamp.json)
        city_name = file.split("_")[0]

        # -------------------------------------------------------
        # Detect API type:
        #   If "results" → OpenAQ API
        #   If "hourly" → Open-Meteo Hourly AQ API
        # -------------------------------------------------------

        if "results" in raw:       # OPENAQ API
            for station in raw["results"]:
                measurements = station.get("measurements", [])

                for m in measurements:
                    row = {
                        "city": city_name,
                        "time": m.get("lastUpdated"),
                        "pm10": None,
                        "pm2_5": None,
                        "carbon_monoxide": None,
                        "nitrogen_dioxide": None,
                        "sulphur_dioxide": None,
                        "ozone": None,
                        "uv_index": None
                    }

                    # Map OpenAQ pollutant fields
                    pollutant = m.get("parameter")
                    value = m.get("value")

                    if pollutant == "pm10":
                        row["pm10"] = value
                    if pollutant == "pm25" or pollutant == "pm2.5":
                        row["pm2_5"] = value
                    if pollutant == "co":
                        row["carbon_monoxide"] = value
                    if pollutant == "no2":
                        row["nitrogen_dioxide"] = value
                    if pollutant == "so2":
                        row["sulphur_dioxide"] = value
                    if pollutant == "o3":
                        row["ozone"] = value

                    all_rows.append(row)

        elif "hourly" in raw:      # OPEN-METEO API
            hourly = raw["hourly"]
            times = hourly.get("time", [])

            for i in range(len(times)):
                row = {
                    "city": city_name,
                    "time": times[i],
                    "pm10": hourly.get("pm10", [None])[i],
                    "pm2_5": hourly.get("pm2_5", [None])[i],
                    "carbon_monoxide": hourly.get("carbon_monoxide", [None])[i],
                    "nitrogen_dioxide": hourly.get("nitrogen_dioxide", [None])[i],
                    "sulphur_dioxide": hourly.get("sulphur_dioxide", [None])[i],
                    "ozone": hourly.get("ozone", [None])[i],
                    "uv_index": hourly.get("uv_index", [None])[i] if "uv_index" in hourly else None
                }
                all_rows.append(row)

        else:
            logging.warning(f"Unknown API structure in: {filepath}")
            continue

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)

    # Remove records where all pollutant values are missing
    pollutant_cols = [
        "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
        "sulphur_dioxide", "ozone"
    ]
    df = df.dropna(subset=pollutant_cols, how="all")

    # Convert numeric columns
    df[pollutant_cols] = df[pollutant_cols].apply(pd.to_numeric, errors="coerce")

    # Convert timestamp
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Feature Engineering
    df["aqi_category"] = df["pm2_5"].apply(categorize_aqi)
    df["severity_score"] = df.apply(compute_severity, axis=1)
    df["risk_flag"] = df["severity_score"].apply(classify_risk)

    # Hour-of-day feature
    df["hour"] = df["time"].dt.hour

    # Save CSV
    df.to_csv(OUTPUT_FILE, index=False)
    logging.info(f"Saved transformed CSV → {OUTPUT_FILE}")

    print("\n🎉 Transform Completed Successfully!")
    print(f"Saved file → {OUTPUT_FILE}")


# -------------------------------------------------------
if __name__ == "__main__":
    run_transform()
