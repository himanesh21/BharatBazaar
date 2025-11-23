import requests
import os
from dotenv import load_dotenv
import pandas as pd
import time
from datetime import date, timedelta
from requests.exceptions import ConnectionError, Timeout
from urllib3.exceptions import ProtocolError
from tqdm import tqdm
import re
import json
import google.generativeai as genai
from sqlalchemy import create_engine

# Load API Keys
load_dotenv()
apikey = os.getenv('api-key')
gemini_key = os.getenv('gemini')

# Date generator
def generate_date_strings(start_d, start_m, start_y, end_d, end_m, end_y):
    start = date(start_y, start_m, start_d)
    end = date(end_y, end_m, end_d)
    while start <= end:
        yield start.strftime("%d/%m/%Y")
        start += timedelta(days=1)

def extract_base_commodity(name):
    return re.split(r'\s*\(', name)[0].strip().title()

def ETL_and_SQL_ingestion(df, engine):
    df = df.rename(columns={
        'State': 'state',
        'District': 'district',
        'Market': 'market',
        'Commodity': 'commodity',
        'Variety': 'variety',
        'Grade': 'grade',
        'Arrival_Date': 'arrival_date',
        'Min_Price': 'min_price',
        'Max_Price': 'max_price',
        'Modal_Price': 'modal_price'
    })

    df['arrival_date'] = pd.to_datetime(df['arrival_date'], errors='coerce', dayfirst=True)
    df.dropna(subset=['arrival_date'], inplace=True)
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    df = df[(df['min_price'] >= 0) & (df['max_price'] >= 0) & (df['modal_price'] >= 0)]

    # Remove outliers
    Q1, Q3 = df['modal_price'].quantile([0.25, 0.75])
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    df = df[(df['modal_price'] >= lower_bound) & (df['modal_price'] <= upper_bound)]
    df = df[(df['min_price'] >= lower_bound) & (df['min_price'] <= upper_bound)]
    df = df[(df['max_price'] >= lower_bound) & (df['max_price'] <= upper_bound)]

    df['commodity'] = df['commodity'].apply(extract_base_commodity)

    with open("commodity_mapping.json", "r") as f:
        mapped_names = json.load(f)

    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel("gemini-1.5-pro")

    def get_common_english_name(name):
        prompt = f"Translate the Indian agricultural commodity name '{name}' into its most common English name used in India. Do not include local names, Hindi, or brackets. Just return the common English name."
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            print(f"Error for {name}: {e}")
            return name

    for name in df['commodity'].unique():
        if name not in mapped_names:
            mapped_names[name] = get_common_english_name(name)
            time.sleep(1)

    with open("commodity_mapping.json", "w") as f:
        json.dump(mapped_names, f, indent=2)

    df['commodity'] = df['commodity'].map(mapped_names)

    for col in ['state', 'district', 'market', 'variety', 'grade']:
        df[col] = df[col].astype(str).str.strip().str.title()

    df['year'] = df['arrival_date'].dt.year
    df['month'] = df['arrival_date'].dt.month
    df['day'] = df['arrival_date'].dt.day
    df['weekday'] = df['arrival_date'].dt.day_name()

    df = df[(df['min_price'] <= df['modal_price']) & (df['modal_price'] <= df['max_price'])]

    # PostgreSQL engine only
    def upsert_and_get_ids(df_main, df_col, table_name, key_cols):
        df_col = df_col.drop_duplicates().copy()

        for col in key_cols:
            if df_col[col].dtype == 'object':
                df_col[col] = df_col[col].str.strip()

        with engine.begin() as conn:
            # Read existing data
            existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)

            # Ensure datetime consistency
            for col in key_cols:
                if col in df_main.columns and pd.api.types.is_datetime64_any_dtype(df_main[col]):
                    existing[col] = pd.to_datetime(existing[col], errors='coerce')

            for col in key_cols:
                if df_col[col].dtype == 'object' and col in existing.columns:
                    existing[col] = existing[col].astype(str).str.strip()
                elif pd.api.types.is_datetime64_any_dtype(df_col[col]):
                    existing[col] = pd.to_datetime(existing[col], errors='coerce')

            new = df_col.merge(existing, on=key_cols, how='left', indicator=True)
            insert_keys = new[new['_merge'] == 'left_only'][key_cols]
            insert = df_col.merge(insert_keys, on=key_cols, how='inner')

            if not insert.empty:
                insert.to_sql(table_name, con=conn, if_exists='append', index=False)

            # Reload updated table
            updated = pd.read_sql(f"SELECT * FROM {table_name}", conn)

            for col in key_cols:
                if pd.api.types.is_datetime64_any_dtype(df_main[col]):
                    updated[col] = pd.to_datetime(updated[col], errors='coerce')

            return df_main.merge(updated, on=key_cols, how='left')


    df = upsert_and_get_ids(df, df[['state']], "state_dim", ["state"])
    df = upsert_and_get_ids(df, df[['commodity', 'variety', 'grade']], "commodity_dim", ["commodity", "variety", "grade"])
    df = upsert_and_get_ids(df, df[['market', 'district', 'state_id']], "market_dim", ["market", "district", "state_id"])

    dim_date_df = df[['arrival_date']].drop_duplicates().copy()
    dim_date_df['day'] = dim_date_df['arrival_date'].dt.day
    dim_date_df['month'] = dim_date_df['arrival_date'].dt.month
    dim_date_df['year'] = dim_date_df['arrival_date'].dt.year
    dim_date_df['weekday'] = dim_date_df['arrival_date'].dt.day_name()
    df = upsert_and_get_ids(df, dim_date_df, "date_dim", ["arrival_date"])

    print("\n🔎 Null foreign keys in merged DataFrame:")
    for col in ['state_id', 'commodity_id', 'market_id', 'date_id']:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            print(f"{col}: {null_count} missing")

    fact_df = df[['date_id', 'market_id', 'commodity_id', 'min_price', 'max_price', 'modal_price']].dropna()
    fact_df.to_sql("fact_market_prices", engine, if_exists="append", index=False)

    print(f"\n✅ Inserted {len(fact_df)} rows into fact_market_prices")


# ---- Example Execution Block ----

engine = create_engine("postgresql+psycopg2://postgres:root123@localhost:5432/market_analysis", isolation_level="AUTOCOMMIT")

start_date = "01/01/2022"
end_date = "09/07/2025"
start_day, start_month, start_year = map(int, start_date.split("/"))
end_day, end_month, end_year = map(int, end_date.split("/"))

for date_str in tqdm(list(generate_date_strings(start_day, start_month, start_year, end_day, end_month, end_year)), desc="🤡 INGEsting by date"):
    safe_date = date_str.replace("/", "-")
    print(safe_date)
    df_path = f"testing/api_fetched_data/all_food_prices_{safe_date}.csv"
    if os.path.exists(df_path):
        df = pd.read_csv(df_path)
        ETL_and_SQL_ingestion(df, engine)
        print("started Ingestion")
    else:
        print(f"❌ Missing file: {df_path}")

