# import os
# import pandas as pd
 
# raw_path="data/raw/churn.csv"
# df=pd.read_csv(raw_path)
# cols=['tenure','MonthlyCharges','TotalCharges']
# for col in cols:
#     df['col']=df['col'].fillna(df['col'].median ,inplace=True)

# categorical_cols=df.select_dtypes(include=['object'].columns)
# for col1 in categorical_cols:
#     df['col1'].fillna("Unknown",inplace=True)

# df['tenure_group'] = pd.cut(
#     df['tenure'],
#     bins=[0, 12, 36, 60, float('inf')],
#     labels=['New', 'Regular', 'Loyal', 'Champion'],
#     include_lowest=True
# )

# # 2. monthly_charge_segment
# df['monthly_charge_segment'] = pd.cut(
#     df['MonthlyCharges'],
#     bins=[0, 30, 70, float('inf')],
#     labels=['Low', 'Medium', 'High'],
#     right=False
# )

# # 3. has_internet_service
# df['has_internet_service'] = df['InternetService'].map({
#     'DSL': 1,
#     'Fiber optic': 1,
#     'No': 0
# })

# # 4. is_multi_line_user
# df['is_multi_line_user'] = df['MultipleLines'].apply(
#     lambda x: 1 if x == "Yes" else 0
# )

# # 5. contract_type_code
# df['contract_type_code'] = df['Contract'].map({
#     'Month-to-month': 0,
#     'One year': 1,
#     'Two year': 2
# })


# columns_to_drop=['customerID','gender']
# df.drop(columns=columns_to_drop,inplace=True)

# output_path="data/staged/churn_transformed.csv"
# os.makedirs("data/staged",existed_ok=True)
# df.to_csv(output_path,index=False)
# print(f"Saved transformed file at :{output_path}")
import os
import pandas as pd
 
# Purpose: Clean and transform Titanic dataset
def transform_data(raw_path):
    # Ensure the path is relative to project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # go up one level
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
 
    df = pd.read_csv(raw_path)
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    cols=['tenure','MonthlyCharges','TotalCharges']
    for col in cols:
        df[col].fillna(df[col].median ,inplace=True)

    categorical_cols=df.select_dtypes(include=['object']).columns
    for col1 in categorical_cols:
        df[col1].fillna("Unknown",inplace=True)

    df['tenure_group'] = pd.cut(
        df['tenure'],
        bins=[0, 12, 36, 60, float('inf')],
        labels=['New', 'Regular', 'Loyal', 'Champion'],
        include_lowest=True
    )

# 2. monthly_charge_segment
    df['monthly_charge_segment'] = pd.cut(
        df['MonthlyCharges'],
        bins=[0, 30, 70, float('inf')],
        labels=['Low', 'Medium', 'High'],
        right=False
    )

# 3. has_internet_service
    df['has_internet_service'] = df['InternetService'].map({
    'DSL': 1,
    'Fiber optic': 1,
    'No': 0
    })

# 4. is_multi_line_user
    df['is_multi_line_user'] = df['MultipleLines'].apply(
        lambda x: 1 if x == "Yes" else 0
    )

# 5. contract_type_code
    df['contract_type_code'] = df['Contract'].map({
    'Month-to-month': 0,
    'One year': 1,
    'Two year': 2
    })


    columns_to_drop=['customerID','gender']
    df.drop(columns=columns_to_drop,inplace=True)

   
 
    # --- 4️⃣ Save transformed data ---
    staged_path = os.path.join(staged_dir, "churn_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path
 
 
if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_data(raw_path)
 
 