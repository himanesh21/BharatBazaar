import os
import sys
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from xgboost import XGBRegressor
from tqdm import tqdm
from dotenv import load_dotenv
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor

# Features configuration
FEATURES = [
    'lag_1', 'lag_7', 'lag_14',
    'roll_mean_7', 'roll_mean_14', 'roll_std_7',
    'weekday_num', 'month'
]

def time_split(df, test_days=30):
    df = df.sort_values("arrival_date")
    split_date = df['arrival_date'].max() - pd.Timedelta(days=test_days)
    train = df[df['arrival_date'] <= split_date]
    test  = df[df['arrival_date'] > split_date]
    return train, test

def calculate_mape(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    mask = y_true != 0
    if not np.any(mask):
        return 0.0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

def calculate_rmse(y_true, y_pred):
    return np.sqrt(np.mean((np.array(y_true) - np.array(y_pred))**2))

def calculate_r2(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    if ss_tot == 0:
        return 0.0
    return 1.0 - (ss_res / ss_tot)

def add_features(df):
    df = df.copy()
    df['arrival_date'] = pd.to_datetime(df['arrival_date'])
    df['weekday_num'] = df['arrival_date'].dt.weekday

    df['lag_1']  = df['modal_price'].shift(1)
    df['lag_7']  = df['modal_price'].shift(7)
    df['lag_14'] = df['modal_price'].shift(14)

    df['roll_mean_7']  = df['modal_price'].rolling(7).mean()
    df['roll_mean_14'] = df['modal_price'].rolling(14).mean()
    df['roll_std_7']   = df['modal_price'].rolling(7).std()

    return df.dropna()

def train_single_series(db_uri, market_id, commodity_id, min_records, models_dir):
    """
    Worker function executed in child processes.
    Creates its own DB connection to avoid process-sharing.
    """
    # Create engine for this process
    engine = create_engine(db_uri)
    
    try:
        # 1. Load series data
        query = """
            SELECT d.arrival_date, d.month, f.modal_price
            FROM fact_market_prices f
            JOIN date_dim d ON f.date_id = d.date_id
            WHERE f.market_id = :market_id AND f.commodity_id = :commodity_id
            ORDER BY d.arrival_date
        """
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"market_id": market_id, "commodity_id": commodity_id})
            
        if len(df) < min_records:
            return None
            
        # 2. Add features
        df_feat = add_features(df)
        if len(df_feat) < 40:
            return None
            
        # 3. Train-test split
        train, test = time_split(df_feat, test_days=30)
        if len(test) < 10 or len(train) < 30:
            return None
            
        X_train, y_train = train[FEATURES], train['modal_price']
        X_test,  y_test  = test[FEATURES],  test['modal_price']
        
        # 4. Fit Model
        model = XGBRegressor(
            n_estimators=1000,        # Increased budget
            learning_rate=0.01,
            max_depth=5,
            reg_lambda=1.5,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=1,
            early_stopping_rounds=30  # Stop if validation score stops improving for 30 trees
        )
        
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
        
        # 5. Evaluate
        preds = model.predict(X_test)
        mape_val = calculate_mape(y_test, preds)
        rmse_val = calculate_rmse(y_test, preds)
        r2_val = calculate_r2(y_test, preds)
        
        # Clip metrics to protect DB constraints
        mape_clipped = float(np.clip(mape_val, 0.0, 999.99))
        rmse_clipped = float(np.clip(rmse_val, 0.0, 99999999.99))
        r2_clipped = float(np.clip(r2_val, -999.99, 1.0))
        
        # 6. Save model to JSON and compress using Gzip
        import gzip
        temp_path = os.path.join(models_dir, f"model_{market_id}_{commodity_id}.tmp")
        model_path = os.path.join(models_dir, f"model_{market_id}_{commodity_id}.json.gz")
        model.save_model(temp_path)
        with open(temp_path, 'rb') as f_in:
            with gzip.open(model_path, 'wb') as f_out:
                f_out.write(f_in.read())
        os.remove(temp_path)
        
        # 7. Package metrics and importances to return
        importances = model.feature_importances_
        feature_importance_list = [
            {"feature_name": name, "importance_score": float(score)}
            for name, score in zip(FEATURES, importances)
        ]
        
        return {
            "market_id": market_id,
            "commodity_id": commodity_id,
            "mape": mape_clipped,
            "rmse": rmse_clipped,
            "r2_score": r2_clipped,
            "features": feature_importance_list
        }
        
    except Exception as e:
        # Suppress output to prevent log flooding, but report failure
        return {"error": str(e), "market_id": market_id, "commodity_id": commodity_id}
    finally:
        engine.dispose()

def main():
    load_dotenv()
    db_uri = os.getenv('DATABASE_URL')
    if db_uri and db_uri.startswith("postgres://"):
        db_uri = db_uri.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_uri and not db_uri.startswith("postgresql+psycopg2://"):
        db_uri = db_uri.replace("postgresql://", "postgresql+psycopg2://", 1)

    if not db_uri:
        db_pass = os.getenv('DbPass', 'root123')
        db_uri = f"postgresql+psycopg2://postgres:{db_pass}@localhost:5432/market_analysis"
    
    # Target directory for model files
    models_dir = "backend/models"
    os.makedirs(models_dir, exist_ok=True)
    
    engine = create_engine(db_uri)
    
    # 1. Verify/Create tables if they do not exist
    print("Verifying database tables...")
    queries = {
        "model_evaluation": """
            CREATE TABLE IF NOT EXISTS model_evaluation (
                market_id INT REFERENCES market_dim(market_id) ON DELETE CASCADE,
                commodity_id INT REFERENCES commodity_dim(commodity_id) ON DELETE CASCADE,
                mape NUMERIC(5, 2),
                rmse NUMERIC(10, 2),
                r2_score NUMERIC(5, 2),
                PRIMARY KEY (market_id, commodity_id)
            );
        """,
        "feature_importance": """
            CREATE TABLE IF NOT EXISTS feature_importance (
                market_id INT REFERENCES market_dim(market_id) ON DELETE CASCADE,
                commodity_id INT REFERENCES commodity_dim(commodity_id) ON DELETE CASCADE,
                feature_name VARCHAR(50),
                importance_score NUMERIC(10, 5),
                PRIMARY KEY (market_id, commodity_id, feature_name)
            );
        """
    }
    with engine.begin() as conn:
        for t_name, sql in queries.items():
            conn.execute(text(sql))
    
    # 2. Get list of all series with >= 180 days of records
    print("Fetching active market-commodity series...")
    query_str = """
        SELECT f.market_id, f.commodity_id, COUNT(*) AS days
        FROM fact_market_prices f
        GROUP BY f.market_id, f.commodity_id
        HAVING COUNT(*) >= 180
        ORDER BY days DESC
    """
    with engine.connect() as conn:
        series_df = pd.read_sql(text(query_str), conn)
        
    total_series = len(series_df)
    print(f"Found {total_series} series eligible for training.")
    
    if total_series == 0:
        print("No eligible series found. Exiting.")
        sys.exit(0)
        
    # Determine number of worker processes
    max_workers = os.cpu_count() or 4
    print(f"Starting parallel training across {max_workers} worker processes...")
    
    success_count = 0
    error_count = 0
    
    # Batch collection of database updates to write in bulk
    eval_updates = []
    feat_updates = []
    
    # Setup progress bar
    pbar = tqdm(total=total_series, desc="Training models")
    
    # Submit tasks to process pool
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                train_single_series, 
                db_uri, 
                int(row['market_id']), 
                int(row['commodity_id']), 
                180, 
                models_dir
            ): (int(row['market_id']), int(row['commodity_id']))
            for _, row in series_df.iterrows()
        }
        
        printed_errors = 0
        for future in concurrent.futures.as_completed(futures):
            market_id, commodity_id = futures[future]
            pbar.update(1)
            
            try:
                res = future.result()
                if res is None:
                    continue
                if "error" in res:
                    error_count += 1
                    if printed_errors < 5:
                        print(f"\n⚠️ Training failed for market {market_id}, commodity {commodity_id}: {res['error']}")
                        printed_errors += 1
                    continue
                    
                success_count += 1
                
                # Append metrics for bulk upsert
                eval_updates.append({
                    "market_id": res["market_id"],
                    "commodity_id": res["commodity_id"],
                    "mape": res["mape"],
                    "rmse": res["rmse"],
                    "r2_score": res["r2_score"]
                })
                
                # Append feature importances
                for f_item in res["features"]:
                    feat_updates.append({
                        "market_id": res["market_id"],
                        "commodity_id": res["commodity_id"],
                        "feature_name": f_item["feature_name"],
                        "importance_score": f_item["importance_score"]
                    })
                    
                # Periodic bulk writing (every 250 series) to keep memory low and DB updated
                if len(eval_updates) >= 250:
                    write_bulk_updates(engine, eval_updates, feat_updates)
                    eval_updates = []
                    feat_updates = []
                    
            except Exception as e:
                error_count += 1
                
    # Write final residual updates
    if eval_updates:
        write_bulk_updates(engine, eval_updates, feat_updates)
        
    pbar.close()
    print(f"\n🎉 Parallel training pipeline completed!")
    print(f" - Successfully trained: {success_count}/{total_series} series")
    print(f" - Failed/skipped: {error_count}/{total_series} series")
    print(f" - Model files written to: {models_dir}")

    # S3-compatible Storage Upload Integration (Supabase Storage / Cloudflare R2 / AWS S3)
    s3_endpoint = os.getenv("S3_ENDPOINT_URL")
    s3_key_id = os.getenv("S3_ACCESS_KEY_ID")
    s3_secret = os.getenv("S3_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if s3_endpoint and s3_key_id and s3_secret and bucket_name:
        try:
            import boto3
            print("\n☁️ Uploading models to S3-compatible cloud bucket...")
            s3_client = boto3.client(
                's3',
                endpoint_url=s3_endpoint,
                aws_access_key_id=s3_key_id,
                aws_secret_access_key=s3_secret
            )
            
            model_files = [f for f in os.listdir(models_dir) if f.endswith(".json.gz")]
            upload_success = 0
            
            for file_name in tqdm(model_files, desc="Uploading to S3"):
                local_path = os.path.join(models_dir, file_name)
                try:
                    s3_client.upload_file(local_path, bucket_name, file_name)
                    upload_success += 1
                except Exception as e:
                    print(f"Failed to upload {file_name}: {e}")
                    
            print(f"✅ Successfully uploaded {upload_success}/{len(model_files)} models to cloud storage!")
        except Exception as e:
            print(f"❌ Error during cloud upload process: {e}")
    else:
        print("\nℹ️ S3 credentials (S3_ENDPOINT_URL, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY) or S3_BUCKET_NAME not set in environment.")
        print("Models are kept locally. Cloud upload skipped.")

def write_bulk_updates(engine, eval_list, feat_list):
    """
    Performs bulk upsert for performance.
    """
    with engine.begin() as conn:
        # Bulk Upsert Model Evaluations
        conn.execute(
            text("""
                INSERT INTO model_evaluation (market_id, commodity_id, mape, rmse, r2_score)
                VALUES (:market_id, :commodity_id, :mape, :rmse, :r2_score)
                ON CONFLICT (market_id, commodity_id) DO UPDATE SET
                    mape = EXCLUDED.mape,
                    rmse = EXCLUDED.rmse,
                    r2_score = EXCLUDED.r2_score
            """),
            eval_list
        )
        
        # Bulk Upsert Feature Importances
        conn.execute(
            text("""
                INSERT INTO feature_importance (market_id, commodity_id, feature_name, importance_score)
                VALUES (:market_id, :commodity_id, :feature_name, :importance_score)
                ON CONFLICT (market_id, commodity_id, feature_name) DO UPDATE SET
                    importance_score = EXCLUDED.importance_score
            """),
            feat_list
        )

if __name__ == "__main__":
    # Standard multiprocessing safety check
    main()
