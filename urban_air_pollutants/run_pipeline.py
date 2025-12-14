import time
from extract import fetch_all_cities
from transform import run_transform
from load import load_to_supabase,create_table_if_not_exists
from etl_analysis import run_analysis
 
def run_full_pipeline():
    # 1) Extract
    raw_file = fetch_all_cities()
    print(f"Extracted {len(raw_file)} raw files")
    time.sleep(1)
    
 
    # 2) Transform
    staged_csv = run_transform()
    time.sleep(1)
    print(f"Transformed files to {staged_csv}")
 
    # 3) Load
    create_table_if_not_exists()
    load_to_supabase(staged_csv)
    time.sleep(1)
 
    # 4) Analysis
    print("Running analysis +Exploring charts")
    run_analysis()
    time.sleep(1)
 
if __name__ == "__main__":
    run_full_pipeline()