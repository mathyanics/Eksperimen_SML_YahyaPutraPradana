import pandas as pd
from datetime import datetime

def automate_preprocessing(customer_csv_path, transactions_csv_path, prod_cat_info_csv_path, output_csv_path):
    customer_df = pd.read_csv(customer_csv_path)
    transactions_df = pd.read_csv(transactions_csv_path)
    prod_cat_info_df = pd.read_csv(prod_cat_info_csv_path)

    # Merging dataframes
    customer_transactions = pd.merge(customer_df, transactions_df, left_on='customer_Id', right_on='cust_id', how='inner')
    full_df = pd.merge(customer_transactions, prod_cat_info_df, left_on='prod_cat_code', right_on='prod_cat_code', how='inner')

    # Handle missing values (example: fill with mode for categorical, mean/median for numerical)
    # For simplicity, let's drop rows with any missing values for now. A more robust approach would be imputation.
    full_df.dropna(inplace=True)

    # Convert 'DOB' and 'tran_date' to datetime objects
    full_df["DOB"] = pd.to_datetime(full_df["DOB"], errors="coerce", dayfirst=True)
    full_df["tran_date"] = pd.to_datetime(full_df["tran_date"], errors="coerce", dayfirst=True)
    # Feature Engineering: Calculate age and transaction age
    current_date = datetime.now()
    full_df['age'] = (current_date.year - full_df['DOB'].dt.year) - ((current_date.month < full_df['DOB'].dt.month) | ((current_date.month == full_df['DOB'].dt.month) & (current_date.day < full_df['DOB'].dt.day)))
    full_df['transaction_age'] = (current_date - full_df['tran_date']).dt.days

    # Encoding categorical variables (example: One-Hot Encoding for 'Gender', 'prod_cat', 'prod_subcat')
    full_df = pd.get_dummies(full_df, columns=['Gender', 'prod_cat', 'prod_subcat'], drop_first=True)

    # Drop original ID columns and other columns not needed for modeling
    full_df.drop(columns=['customer_Id', 'cust_id', 'prod_cat_code', 'prod_sub_cat_code', 'DOB', 'tran_date'], inplace=True)

    # Save the preprocessed data
    full_df.to_csv(output_csv_path, index=False)

if __name__ == '__main__':
    customer_csv = 'https://raw.githubusercontent.com/mathyanics/Eksperimen_SML_YahyaPutraPradana/refs/heads/main/salesdata_raw/Customer.csv'
    transactions_csv = 'https://raw.githubusercontent.com/mathyanics/Eksperimen_SML_YahyaPutraPradana/refs/heads/main/salesdata_raw/Transactions.csv'
    prod_cat_info_csv = '.https://raw.githubusercontent.com/mathyanics/Eksperimen_SML_YahyaPutraPradana/refs/heads/main/salesdata_raw/prod_cat_info.csv'
    output_preprocessed_csv = 'salesdata_preprocessing/preprocessed_data.csv'

    automate_preprocessing(customer_csv, transactions_csv, prod_cat_info_csv, output_preprocessed_csv)
    print(f"Preprocessing complete. Preprocessed data saved to {output_preprocessed_csv}")

