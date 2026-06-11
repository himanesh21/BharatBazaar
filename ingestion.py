import requests
import os
# pyrefly: ignore [missing-import]
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
from sqlalchemy import create_engine, text

# Load API Keys
load_dotenv()
apikey = os.getenv('api-key') or os.getenv('API_KEY')
gemini_key = os.getenv('gemini') or os.getenv('GEMINI_API_KEY')

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
    
    if df.empty:
        return
        
    target_arrival_date = df['arrival_date'].iloc[0].date()
    
    # Check if this date has already been ingested in fact table (idempotency check)
    with engine.begin() as conn:
        try:
            existing_date = pd.read_sql(
                text("SELECT date_id FROM date_dim WHERE arrival_date = :arrival_date"), 
                conn, 
                params={"arrival_date": target_arrival_date}
            )
            if not existing_date.empty:
                date_id_val = int(existing_date.iloc[0, 0])
                existing_fact = pd.read_sql(
                    text("SELECT 1 FROM fact_market_prices WHERE date_id = :date_id LIMIT 1"),
                    conn,
                    params={"date_id": date_id_val}
                )
                if not existing_fact.empty:
                    print(f"⏩ Date {target_arrival_date} already ingested. Skipping.")
                    return
        except Exception:
            # Table might not exist yet during initial setup check
            pass

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
    model = genai.GenerativeModel("gemini-1.5-flash")

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

    # Map raw state names to standardized map state names
    state_name_cleaner = {
        "Keralam": "Kerala",
        "Chattisgarh": "Chhattisgarh",
        "Nct Of Delhi": "Delhi",
        "Nct of Delhi": "Delhi",
        "Nct Of Delhi.": "Delhi",
        "Uttrakhand": "Uttarakhand",
        "Orissa": "Odisha",
        "Pondicherry": "Puducherry",
        "Dadra and Nagar Haveli": "Dādra and Nagar Haveli and Damān and Diu",
        "Daman and Diu": "Dādra and Nagar Haveli and Damān and Diu",
        "Dadra & Nagar Haveli": "Dādra and Nagar Haveli and Damān and Diu",
        "Dadra And Nagar Haveli And Daman And Diu": "Dādra and Nagar Haveli and Damān and Diu",
        "Jammu & Kashmir": "Jammu and Kashmir",
        "Jammu And Kashmir": "Jammu and Kashmir",
        "Andaman & Nicobar": "Andaman and Nicobar",
        "Andaman And Nicobar": "Andaman and Nicobar"
    }
    df['state'] = df['state'].map(state_name_cleaner).fillna(df['state'])

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
    # Deduplicate composite primary key (date_id, market_id, commodity_id) within the batch
    fact_df = fact_df.drop_duplicates(subset=['date_id', 'market_id', 'commodity_id'], keep='first')
    fact_df.to_sql("fact_market_prices", engine, if_exists="append", index=False)

    print(f"\n✅ Inserted {len(fact_df)} rows into fact_market_prices")


def prepopulate_state_dim(engine):
    state_mapping = {
        "Andaman and Nicobar": 0,
        "Telangana": 1,
        "Andhra Pradesh": 2,
        "Arunachal Pradesh": 3,
        "Assam": 4,
        "Bihar": 5,
        "Chandigarh": 6,
        "Chhattisgarh": 7,
        "Dādra and Nagar Haveli and Damān and Diu": 8,
        "Delhi": 9,
        "Goa": 10,
        "Gujarat": 11,
        "Haryana": 12,
        "Himachal Pradesh": 13,
        "Jharkhand": 14,
        "Karnataka": 15,
        "Kerala": 16,
        "Madhya Pradesh": 17,
        "Maharashtra": 18,
        "Manipur": 19,
        "Meghalaya": 20,
        "Mizoram": 21,
        "Nagaland": 22,
        "Odisha": 23,
        "Puducherry": 24,
        "Punjab": 25,
        "Rajasthan": 26,
        "Sikkim": 27,
        "Tamil Nadu": 28,
        "Tripura": 29,
        "Uttar Pradesh": 30,
        "Uttarakhand": 31,
        "West Bengal": 32,
        "Lakshadweep": 33,
        "Jammu and Kashmir": 34,
        "Ladakh": 35
    }
    
    df_states = pd.DataFrame([
        {"state_id": idx, "state": name}
        for name, idx in state_mapping.items()
    ])
    
    with engine.begin() as conn:
        try:
            existing = pd.read_sql("SELECT COUNT(*) FROM state_dim", conn)
            count = existing.iloc[0, 0]
        except Exception:
            count = 0
            
        if count != len(state_mapping):
            print("Populating state_dim with map-based state_ids...")
            conn.execute(text("TRUNCATE TABLE state_dim CASCADE"))
            df_states.to_sql("state_dim", con=conn, if_exists="append", index=False)
            # Reset sequence to 100 to avoid conflicting with manual 0-35 IDs
            conn.execute(text("SELECT setval(pg_get_serial_sequence('state_dim', 'state_id'), 100)"))
            print("Successfully populated state_dim!")


def create_tables_if_not_exist(engine):
    queries = [
        """
        CREATE TABLE IF NOT EXISTS state_dim (
            state_id INT PRIMARY KEY,
            state VARCHAR(255) UNIQUE NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS commodity_dim (
            commodity_id SERIAL PRIMARY KEY,
            commodity VARCHAR(255) NOT NULL,
            variety VARCHAR(255) NOT NULL,
            grade VARCHAR(255) NOT NULL,
            UNIQUE (commodity, variety, grade)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS market_dim (
            market_id SERIAL PRIMARY KEY,
            market VARCHAR(255) NOT NULL,
            district VARCHAR(255) NOT NULL,
            state_id INT REFERENCES state_dim(state_id) ON DELETE CASCADE,
            UNIQUE (market, district, state_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS date_dim (
            date_id SERIAL PRIMARY KEY,
            arrival_date DATE UNIQUE NOT NULL,
            day INT NOT NULL,
            month INT NOT NULL,
            year INT NOT NULL,
            weekday VARCHAR(50) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_market_prices (
            date_id INT REFERENCES date_dim(date_id) ON DELETE CASCADE,
            market_id INT REFERENCES market_dim(market_id) ON DELETE CASCADE,
            commodity_id INT REFERENCES commodity_dim(commodity_id) ON DELETE CASCADE,
            min_price NUMERIC(12, 2) NOT NULL,
            max_price NUMERIC(12, 2) NOT NULL,
            modal_price NUMERIC(12, 2) NOT NULL,
            PRIMARY KEY (date_id, market_id, commodity_id)
        );
        """
    ]
    with engine.begin() as conn:
        for q in queries:
            conn.execute(text(q))
    print("✨ Database tables verified/created successfully.")


# ---- Example Execution Block ----

if __name__ == "__main__":
    db_uri = os.getenv('DATABASE_URL')
    if db_uri and db_uri.startswith("postgres://"):
        # Render and Supabase sometimes give connection strings starting with postgres:// instead of postgresql://
        db_uri = db_uri.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_uri and not db_uri.startswith("postgresql+psycopg2://"):
        db_uri = db_uri.replace("postgresql://", "postgresql+psycopg2://", 1)

    if not db_uri:
        db_pass = os.getenv('DbPass', 'root123')
        db_uri = f"postgresql+psycopg2://postgres:{db_pass}@localhost:5432/market_analysis"

    engine = create_engine(db_uri, isolation_level="AUTOCOMMIT")

    # Ensure tables are setup before ingestion
    create_tables_if_not_exist(engine)

    # Prepopulate state_dim using our map indexes
    prepopulate_state_dim(engine)

    start_date = "10/06/2026"
    end_date = "10/06/2026"
    start_day, start_month, start_year = map(int, start_date.split("/"))
    end_day, end_month, end_year = map(int, end_date.split("/"))

    for date_str in tqdm(list(generate_date_strings(start_day, start_month, start_year, end_day, end_month, end_year)), desc="🤡 INGEsting by date"):
        safe_date = date_str.replace("/", "-")
        
        # Try deployment_data first, fallback to testing/api_fetched_data
        df_path = f"deployment_data/all_food_prices_{safe_date}.csv"
        if not os.path.exists(df_path):
            df_path = f"testing/api_fetched_data/all_food_prices_{safe_date}.csv"
            
        if os.path.exists(df_path):
            df = pd.read_csv(df_path)
            ETL_and_SQL_ingestion(df, engine)
            print(f"Ingested {safe_date}")
        else:
            print(f"❌ Missing file: {df_path}")


