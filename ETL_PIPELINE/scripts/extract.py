# import opendatasets as od
import os
import seaborn as sns
import pandas as pd
 
def extract_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # go up one level
    data_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(data_dir, exist_ok=True)
 
    df = pd.read_csv("C:\\Users\\arthi\\Downloads\\WA_Fn-UseC_-Telco-Customer-Churn.csv")
    raw_path = os.path.join(data_dir, "churn.csv")
    df.to_csv(raw_path, index=False)
 
    print(f"✅ Data extracted and saved at: {raw_path}")
    return raw_path
# dataset_url = "https://www.kaggle.com/blastchar/telco-customer-churn"

# print("Downloading dataset...")
# od.download(dataset_url, data_dir="data/raw")

# print("✔ Dataset downloaded.")

# # ---------------------------------------------------
# # 3. Load and save churn_raw.csv
# # ---------------------------------------------------
# # Kaggle dataset folder will look like: data/raw/telco-customer-churn/
# raw_path = "data/raw/telco-customer-churn/WA_Fn-UseC_-Telco-Customer-Churn.csv"

# df_raw = pd.read_csv(raw_path)

# # Save as churn_raw.csv in raw folder
# save_path = "data/raw/churn_raw.csv"
# df_raw.to_csv(save_path, index=False)

# print(f"✔ Raw dataset saved at: {save_path}")
 
if __name__ == "__main__":
    extract_data()