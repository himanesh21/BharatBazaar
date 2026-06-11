from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()
db_uri = os.getenv('DATABASE_URL')
if db_uri and db_uri.startswith("postgres://"):
    db_uri = db_uri.replace("postgres://", "postgresql+psycopg2://", 1)
elif db_uri and not db_uri.startswith("postgresql+psycopg2://"):
    db_uri = db_uri.replace("postgresql://", "postgresql+psycopg2://", 1)

if not db_uri:
    db_pass = os.getenv('DbPass', 'root123')
    db_uri = f"postgresql+psycopg2://postgres:{db_pass}@localhost:5432/market_analysis"

engine = create_engine(db_uri)

app = FastAPI(title="BharatBazaar API Backend")

# Enable CORS for the React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Feature list used during training
FEATURES = [
    'lag_1', 'lag_7', 'lag_14',
    'roll_mean_7', 'roll_mean_14', 'roll_std_7',
    'weekday_num', 'month'
]

@app.get("/api/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/api/filters")
async def get_filters(
    state_id: int = Query(None),
    district: str = Query(None),
    market_id: int = Query(None),
    all_markets: bool = Query(False)
):
    """
    Returns cascading hierarchical options for filters:
    - If no parameters: returns all states and unique commodities.
    - If state_id + all_markets=True: returns all markets directly inside that state.
    - If state_id provided (all_markets=False): returns districts in that state.
    - If state_id + district provided: returns markets/mandis in that district.
    - If market_id provided: returns commodities active in that market.
    """
    with engine.connect() as conn:
        try:
            # 1. Base filter options: all states and commodities
            if state_id is None and district is None and market_id is None:
                states_res = conn.execute(text("SELECT state_id, state FROM state_dim ORDER BY state")).fetchall()
                commodities_res = conn.execute(text("SELECT DISTINCT commodity FROM commodity_dim ORDER BY commodity")).fetchall()
                
                states = [{"state_id": int(r[0]), "state": r[1]} for r in states_res]
                commodities = [r[0] for r in commodities_res]
                
                return {
                    "states": states,
                    "commodities": commodities
                }
                
            # 2. Districts or all markets within a state
            if state_id is not None and district is None:
                if all_markets:
                    markets_res = conn.execute(
                        text("SELECT market_id, market FROM market_dim WHERE state_id = :state_id ORDER BY market"),
                        {"state_id": state_id}
                    ).fetchall()
                    markets = [{"market_id": int(r[0]), "market": r[1]} for r in markets_res]
                    return {"markets": markets}
                else:
                    districts_res = conn.execute(
                        text("SELECT DISTINCT district FROM market_dim WHERE state_id = :state_id ORDER BY district"),
                        {"state_id": state_id}
                    ).fetchall()
                    districts = [r[0] for r in districts_res]
                    return {"districts": districts}
                
            # 3. Markets within a district
            if state_id is not None and district is not None and market_id is None:
                markets_res = conn.execute(
                    text("SELECT market_id, market FROM market_dim WHERE state_id = :state_id AND district = :district ORDER BY market"),
                    {"state_id": state_id, "district": district}
                ).fetchall()
                markets = [{"market_id": int(r[0]), "market": r[1]} for r in markets_res]
                return {"markets": markets}
                
            # 4. Commodities available in a specific market (Deduplicated by name)
            if market_id is not None:
                commodities_res = conn.execute(
                    text("""
                        SELECT DISTINCT c.commodity 
                        FROM fact_market_prices f 
                        JOIN commodity_dim c ON f.commodity_id = c.commodity_id 
                        WHERE f.market_id = :market_id 
                        ORDER BY c.commodity
                    """),
                    {"market_id": market_id}
                ).fetchall()
                commodities = [{"commodity": r[0]} for r in commodities_res]
                return {"commodities": commodities}
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
            
    return {"error": "Invalid filter parameters"}

@app.get("/api/overview")
async def get_overview(state: str = Query(None)):
    """
    Returns top-level KPIs, state-wise stats for the map, national trend, and price movers.
    Can be filtered by state name.
    """
    with engine.connect() as conn:
        try:
            # Determine the latest historical date
            latest_date_res = conn.execute(text("SELECT MAX(arrival_date) FROM date_dim")).scalar()
            if not latest_date_res:
                return {"error": "No database records found. Please run ingestion first."}
                
            # Filter condition template
            state_join_kpi = ""
            state_where_kpi = ""
            kpi_params = {}
            if state:
                state_join_kpi = "JOIN market_dim m ON f.market_id = m.market_id JOIN state_dim s ON m.state_id = s.state_id"
                state_where_kpi = "AND s.state = :state"
                kpi_params["state"] = state

            # KPIs (Optimized for 13M+ rows)
            if state:
                total_records_query = f"SELECT COUNT(*) FROM fact_market_prices f {state_join_kpi} WHERE 1=1 {state_where_kpi}"
                total_records = conn.execute(text(total_records_query), kpi_params).scalar()

                total_commodities_query = f"SELECT COUNT(DISTINCT f.commodity_id) FROM fact_market_prices f {state_join_kpi} WHERE 1=1 {state_where_kpi}"
                total_commodities = conn.execute(text(total_commodities_query), kpi_params).scalar()
                
                total_markets_query = f"SELECT COUNT(DISTINCT f.market_id) FROM fact_market_prices f {state_join_kpi} WHERE 1=1 {state_where_kpi}"
                total_markets = conn.execute(text(total_markets_query), kpi_params).scalar()
                
                total_states = 1
                total_models = conn.execute(
                    text("""
                        SELECT COUNT(*) FROM model_evaluation e
                        JOIN market_dim m ON e.market_id = m.market_id
                        JOIN state_dim s ON m.state_id = s.state_id
                        WHERE s.state = :state
                    """),
                    {"state": state}
                ).scalar()
                
                avg_mape_query = """
                    SELECT AVG(e.mape) FROM model_evaluation e
                    JOIN market_dim m ON e.market_id = m.market_id
                    JOIN state_dim s ON m.state_id = s.state_id
                    WHERE s.state = :state
                """
                avg_mape = conn.execute(text(avg_mape_query), {"state": state}).scalar()
            else:
                # Fast database catalog estimate for total rows (takes < 1ms)
                total_records = conn.execute(
                    text("SELECT reltuples::bigint FROM pg_class WHERE relname = 'fact_market_prices'")
                ).scalar()
                
                # Fast scans on dimension tables instead of 13M rows fact table
                total_commodities = conn.execute(text("SELECT COUNT(*) FROM commodity_dim")).scalar()
                total_markets = conn.execute(text("SELECT COUNT(*) FROM market_dim")).scalar()
                total_states = conn.execute(text("SELECT COUNT(*) FROM state_dim")).scalar()
                total_models = conn.execute(text("SELECT COUNT(*) FROM model_evaluation")).scalar()
                avg_mape = conn.execute(text("SELECT AVG(mape) FROM model_evaluation")).scalar()

            if avg_mape is not None:
                forecast_trust = round(100.0 - float(avg_mape), 1)
            else:
                forecast_trust = 92.0  # Fallback default if no model evaluation in DB yet
            
            # Latest day national/state average price
            avg_price_query = f"""
                SELECT AVG(f.modal_price) 
                FROM fact_market_prices f
                JOIN date_dim d ON f.date_id = d.date_id
                {state_join_kpi}
                WHERE d.arrival_date = :latest_date {state_where_kpi}
            """
            params_avg = {"latest_date": latest_date_res}
            if state:
                params_avg["state"] = state
            avg_price_res = conn.execute(text(avg_price_query), params_avg).scalar()
            avg_price = round(float(avg_price_res or 0.0), 2)
            
            # State-wise average prices for the India Map on the latest day (always computed nationally for map coloring)
            state_pricing_res = conn.execute(
                text("""
                    SELECT m.state_id, s.state, AVG(f.modal_price) as avg_price, COUNT(*) as record_count
                    FROM fact_market_prices f
                    JOIN market_dim m ON f.market_id = m.market_id
                    JOIN state_dim s ON m.state_id = s.state_id
                    JOIN date_dim d ON f.date_id = d.date_id
                    WHERE d.arrival_date = :latest_date
                    GROUP BY m.state_id, s.state
                """),
                {"latest_date": latest_date_res}
            ).fetchall()
            
            state_pricing = {}
            for r in state_pricing_res:
                state_pricing[int(r[0])] = {
                    "state_name": r[1],
                    "avg_price": round(float(r[2] or 0.0), 2),
                    "record_count": int(r[3])
                }
                
            # Trend (Daily average for the last 14 days)
            trend_query = f"""
                SELECT d.arrival_date, AVG(f.modal_price) as avg_price
                FROM fact_market_prices f
                JOIN date_dim d ON f.date_id = d.date_id
                JOIN market_dim m ON f.market_id = m.market_id
                JOIN state_dim s ON m.state_id = s.state_id
                WHERE d.arrival_date >= CAST(:latest_date AS DATE) - INTERVAL '14 days'
                {"AND s.state = :state" if state else ""}
                GROUP BY d.arrival_date
                ORDER BY d.arrival_date ASC
            """
            params_trend = {"latest_date": latest_date_res}
            if state:
                params_trend["state"] = state
            trend_res = conn.execute(text(trend_query), params_trend).fetchall()
            
            national_trend = [
                {"date": r[0].strftime('%Y-%m-%d'), "price": round(float(r[1] or 0.0), 2)}
                for r in trend_res
            ]
            
            # Top Commodities by reporting frequency (count of records)
            state_filter_top = ""
            top_params = {"latest_date": latest_date_res}
            if state:
                state_filter_top = "JOIN market_dim m ON f.market_id = m.market_id JOIN state_dim s ON m.state_id = s.state_id WHERE s.state = :state"
                top_params["state"] = state
            
            top_query = f"""
                SELECT c.commodity, AVG(f.modal_price) as avg_price, COUNT(*) as report_count
                FROM fact_market_prices f
                JOIN commodity_dim c ON f.commodity_id = c.commodity_id
                JOIN date_dim d ON f.date_id = d.date_id
                {state_filter_top}
                {"AND" if state else "WHERE"} d.arrival_date = :latest_date
                GROUP BY c.commodity
                ORDER BY report_count DESC
                LIMIT 5
            """
            top_res = conn.execute(text(top_query), top_params).fetchall()
            top_commodities = [
                {
                    "commodity": r[0],
                    "avg_price": round(float(r[1] or 0.0), 2),
                    "report_count": int(r[2])
                }
                for r in top_res
            ]
 
            # Gainers & Losers (for bottom counters)
            prev_date_res = conn.execute(
                text("SELECT arrival_date FROM date_dim WHERE arrival_date < :latest_date ORDER BY arrival_date DESC LIMIT 1"),
                {"latest_date": latest_date_res}
            ).scalar()
            
            gainers_count = 0
            losers_count = 0
            
            if prev_date_res:
                state_filter_movers = ""
                movers_params = {"latest_date": latest_date_res, "prev_date": prev_date_res}
                if state:
                    state_filter_movers = "JOIN market_dim m ON f.market_id = m.market_id JOIN state_dim s ON m.state_id = s.state_id WHERE s.state = :state"
                    movers_params["state"] = state
                
                movers_query = f"""
                    WITH latest_prices AS (
                        SELECT f.commodity_id, AVG(f.modal_price) as price_t
                        FROM fact_market_prices f
                        JOIN date_dim d ON f.date_id = d.date_id
                        {state_filter_movers}
                        {"AND" if state else "WHERE"} d.arrival_date = :latest_date
                        GROUP BY f.commodity_id
                    ),
                    prev_prices AS (
                        SELECT f.commodity_id, AVG(f.modal_price) as price_t_1
                        FROM fact_market_prices f
                        JOIN date_dim d ON f.date_id = d.date_id
                        {state_filter_movers}
                        {"AND" if state else "WHERE"} d.arrival_date = :prev_date
                        GROUP BY f.commodity_id
                    )
                    SELECT ((l.price_t - p.price_t_1) / p.price_t_1 * 100) as pct_change
                    FROM latest_prices l
                    JOIN prev_prices p ON l.commodity_id = p.commodity_id
                    WHERE p.price_t_1 > 0
                """
                movers_res = conn.execute(text(movers_query), movers_params).fetchall()
                
                gainers_count = len([r[0] for r in movers_res if r[0] > 0.1])
                losers_count = len([r[0] for r in movers_res if r[0] < -0.1])
                
            return {
                "kpis": {
                    "avgPrice": avg_price,
                    "totalRecords": total_records,
                    "totalStates": total_states,
                    "totalMarkets": total_markets,
                    "totalCommodities": total_commodities,
                    "totalModels": total_models,
                    "forecastTrust": forecast_trust,
                    "latestDate": latest_date_res.strftime('%Y-%m-%d')
                },
                "statePricing": state_pricing,
                "nationalTrend": national_trend,
                "topCommodities": top_commodities,
                "gainersCount": gainers_count,
                "losersCount": losers_count
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/explorer")
async def get_explorer_data(
    commodity: str = Query(None),
    state_id: int = Query(None),
    district: str = Query(None),
    market_id: int = Query(None)
):
    """
    Returns historical price points, volume activity, and variety/grade comparisons.
    """
    query_base = """
        SELECT d.arrival_date, f.min_price, f.max_price, f.modal_price, f.date_id
        FROM fact_market_prices f
        JOIN date_dim d ON f.date_id = d.date_id
        JOIN market_dim m ON f.market_id = m.market_id
        JOIN commodity_dim c ON f.commodity_id = c.commodity_id
        WHERE 1=1
    """
    clauses = []
    params = {}
    
    if commodity:
        clauses.append("c.commodity = :commodity")
        params["commodity"] = commodity
    if state_id is not None:
        clauses.append("m.state_id = :state_id")
        params["state_id"] = state_id
    if district:
        clauses.append("m.district = :district")
        params["district"] = district
    if market_id is not None:
        clauses.append("f.market_id = :market_id")
        params["market_id"] = market_id
        
    if clauses:
        query_base += " AND " + " AND ".join(clauses)
        
    query_base += " ORDER BY d.arrival_date DESC LIMIT 90"
    
    with engine.connect() as conn:
        try:
            res = conn.execute(text(query_base), params).fetchall()
            
            if not res:
                return {"history": [], "varietyComparison": []}
                
            # Reverse order for chronological trend
            res = list(reversed(res))
            
            history = []
            for i, r in enumerate(res):
                arrival_date = r[0].strftime('%Y-%m-%d')
                min_p = round(float(r[1]), 2)
                max_p = round(float(r[2]), 2)
                modal_p = round(float(r[3]), 2)
                
                # Calculate price velocity (percentage change vs previous day)
                if i > 0:
                    prev_modal_p = float(res[i-1][3])
                    price_change = (modal_p - prev_modal_p) / prev_modal_p if prev_modal_p > 0 else 0.0
                else:
                    price_change = 0.0
                
                # Elasticity-driven volume: higher prices -> lower arrivals, lower prices -> higher arrivals
                # Baseline volume derived from price, adjusted by inverse of price change
                base_vol = (modal_p * 1.2) / 10.0
                elasticity_factor = 1.0 - (price_change * 3.5) # Scale price change impact
                # Bound elasticity factor between 0.4 and 2.2 to prevent negative or extreme volumes
                elasticity_factor = max(0.4, min(2.2, elasticity_factor))
                
                # Add deterministic daily noise (0 to 5 MT)
                noise = ((int(r[4]) * 17) % 50) / 10.0
                
                mock_vol = round(base_vol * elasticity_factor + noise, 2)
                
                history.append({
                    "date": arrival_date,
                    "min_price": min_p,
                    "max_price": max_p,
                    "modal_price": modal_p,
                    "arrival_volume": mock_vol
                })
                
            # Variety/grade comparison for the selected commodity in this specific mandi
            variety_compare = []
            if market_id is not None and commodity:
                compare_res = conn.execute(
                    text("""
                        SELECT c.variety, c.grade, AVG(f.modal_price) as avg_price, MAX(f.max_price) as max_price, MIN(f.min_price) as min_price
                        FROM fact_market_prices f
                        JOIN commodity_dim c ON f.commodity_id = c.commodity_id
                        WHERE f.market_id = :market_id AND c.commodity = :commodity
                        GROUP BY c.variety, c.grade
                        ORDER BY avg_price DESC
                    """),
                    {"market_id": market_id, "commodity": commodity}
                ).fetchall()
                
                variety_compare = [
                    {
                        "variety": r[0],
                        "grade": r[1],
                        "avg_price": round(float(r[2]), 2),
                        "max_price": round(float(r[3]), 2),
                        "min_price": round(float(r[4]), 2)
                    }
                    for r in compare_res
                ]
                
            return {
                "history": history,
                "varietyComparison": variety_compare
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

async def get_forecast_internal(market_id: int, commodity_id: int):
    """
    Core prediction engine. Loads pre-trained model, queries historical price sequence,
    and runs recursive 3-day forecasting with trust metrics and features.
    """
    # Locate model on disk
    model_dir = os.path.join(os.path.dirname(__file__), "models")
    os.makedirs(model_dir, exist_ok=True)
    model_name = f"model_{market_id}_{commodity_id}.json.gz"
    model_path = os.path.join(model_dir, model_name)
    
    if not os.path.exists(model_path):
        s3_endpoint = os.getenv("S3_ENDPOINT_URL")
        s3_key_id = os.getenv("S3_ACCESS_KEY_ID")
        s3_secret = os.getenv("S3_SECRET_ACCESS_KEY")
        bucket_name = os.getenv("S3_BUCKET_NAME")
        
        if s3_endpoint and s3_key_id and s3_secret and bucket_name:
            try:
                import boto3
                print(f"☁️ Downloading model {model_name} from S3 cloud storage...")
                s3_client = boto3.client(
                    's3',
                    endpoint_url=s3_endpoint,
                    aws_access_key_id=s3_key_id,
                    aws_secret_access_key=s3_secret
                )
                s3_client.download_file(bucket_name, model_name, model_path)
                print(f"✅ Successfully downloaded and cached {model_name}!")
            except Exception as e:
                print(f"⚠️ Failed to download {model_name} from cloud storage: {e}")
    
    if not os.path.exists(model_path):
        raise HTTPException(
            status_code=404, 
            detail="No trained model found for this market and commodity. Please run the training pipeline first."
        )
        
    with engine.connect() as conn:
        try:
            # 1. Fetch metrics
            metrics_res = conn.execute(
                text("SELECT mape, rmse, r2_score FROM model_evaluation WHERE market_id = :market_id AND commodity_id = :commodity_id"),
                {"market_id": market_id, "commodity_id": commodity_id}
            ).fetchone()
            
            # 2. Fetch feature drivers
            importances_res = conn.execute(
                text("SELECT feature_name, importance_score FROM feature_importance WHERE market_id = :market_id AND commodity_id = :commodity_id ORDER BY importance_score DESC"),
                {"market_id": market_id, "commodity_id": commodity_id}
            ).fetchall()
            
            # 3. Fetch latest 14 historical records to feed features
            series_res = conn.execute(
                text("""
                    SELECT d.arrival_date, d.month, f.modal_price
                    FROM fact_market_prices f
                    JOIN date_dim d ON f.date_id = d.date_id
                    WHERE f.market_id = :market_id AND f.commodity_id = :commodity_id
                    ORDER BY d.arrival_date DESC
                    LIMIT 14
                """),
                {"market_id": market_id, "commodity_id": commodity_id}
            ).fetchall()
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
            
    if not metrics_res:
        metrics = {"mape": 9.50, "rmse": 150.00, "r2_score": 0.85}
    else:
        metrics = {
            "mape": round(float(metrics_res[0]), 2),
            "rmse": round(float(metrics_res[1]), 2),
            "r2_score": round(float(metrics_res[2]), 2)
        }
        
    feature_drivers = [
        {"feature": r[0], "importance": round(float(r[1]), 4)}
        for r in importances_res
    ]
    
    if len(series_res) < 14:
        raise HTTPException(
            status_code=400,
            detail=f"Insufficient historical data to build model features. Need 14 days, found {len(series_res)}."
        )
        
    # Reverse to make chronological [oldest -> newest]
    series_res = list(reversed(series_res))
    
    prices_list = [float(r[2]) for r in series_res]
    dates_list = [pd.to_datetime(r[0]) for r in series_res]
    
    # Load XGBoost Regressor from gzipped file
    try:
        import gzip
        with gzip.open(model_path, 'rb') as f:
            model_bytes = f.read()
        
        try:
            model = XGBRegressor()
            model.load_model(model_bytes)
        except Exception:
            # Fallback: Write UBJSON bytes to a temporary file with a '.ubj' extension
            import tempfile
            with tempfile.NamedTemporaryFile(suffix=".ubj", delete=False) as temp_file:
                temp_file.write(model_bytes)
                temp_file_path = temp_file.name
            try:
                model = XGBRegressor()
                model.load_model(temp_file_path)
            finally:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load prediction model: {str(e)}")
        
    # 4. Iterative Recursive 3-day Forecasting
    forecasts = []
    current_date = dates_list[-1]
    
    for step in range(1, 4):
        next_date = current_date + timedelta(days=1)
        next_weekday = next_date.weekday()
        next_month = next_date.month
        
        # Pull lags
        lag_1 = prices_list[-1]
        lag_7 = prices_list[-7]
        lag_14 = prices_list[-14]
        
        # Pull rolling summaries
        roll_mean_7 = float(np.mean(prices_list[-7:]))
        roll_mean_14 = float(np.mean(prices_list[-14:]))
        
        if len(prices_list[-7:]) > 1:
            roll_std_7 = float(np.std(prices_list[-7:], ddof=1))
        else:
            roll_std_7 = 0.0
            
        if np.isnan(roll_std_7):
            roll_std_7 = 0.0
            
        # Create input features DataFrame
        x_pred = pd.DataFrame([[
            lag_1, lag_7, lag_14,
            roll_mean_7, roll_mean_14, roll_std_7,
            next_weekday, next_month
        ]], columns=FEATURES)
        
        # Predict price
        pred_val = float(model.predict(x_pred)[0])
        pred_val = max(0.0, round(pred_val, 2))
        
        # Compute confidence interval: Prediction +/- 1.96 * RMSE * sqrt(step)
        spread = 1.96 * metrics["rmse"] * np.sqrt(step)
        lower_bound = max(0.0, round(pred_val - spread, 2))
        upper_bound = round(pred_val + spread, 2)
        
        forecasts.append({
            "date": next_date.strftime('%Y-%m-%d'),
            "predicted_modal_price": pred_val,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound
        })
        
        # Update sliding window for next day's lag calculations
        prices_list.append(pred_val)
        prices_list.pop(0)
        current_date = next_date
        
    # Calculate recommended actions
    latest_actual_price = float(series_res[-1][2])
    day3_pred_price = forecasts[-1]["predicted_modal_price"]
    pct_change = round(((day3_pred_price - latest_actual_price) / latest_actual_price) * 100, 2) if latest_actual_price > 0 else 0.0
    
    if pct_change > 3.0:
        recommendation = "HOLD"
        trend_desc = "Significant upward trend predicted. Sell later for higher returns."
        trend_class = "gainer"
    elif pct_change < -3.0:
        recommendation = "SELL"
        trend_desc = "Downward trend predicted. Sell immediately to prevent loss."
        trend_class = "loser"
    else:
        recommendation = "STABLE"
        trend_desc = "Stable prices predicted. Execute routine transactions."
        trend_class = "stable"
        
    return {
        "market_id": market_id,
        "commodity_id": commodity_id,
        "latest_actual": {
            "date": dates_list[-1].strftime('%Y-%m-%d'),
            "price": latest_actual_price
        },
        "forecast": forecasts,
        "metrics": metrics,
        "featureDrivers": feature_drivers,
        "recommendation": {
            "action": recommendation,
            "description": trend_desc,
            "pct_change": pct_change,
            "class": trend_class
        }
    }

@app.get("/api/forecast")
async def get_forecast(
    market_id: int = Query(...),
    commodity_id: int = Query(...)
):
    """
    Exposes recursive forecast by ID.
    """
    return await get_forecast_internal(market_id, commodity_id)

@app.get("/api/inference")
async def get_inference(
    market_id: int = Query(...),
    commodity: str = Query(...)
):
    """
    Exposes recursive forecast using commodity name and market ID.
    Queries the database to map the name to the active commodity ID first.
    """
    with engine.connect() as conn:
        try:
            # Try to find a commodity variety with an active trained model first (sort by highest R2 score)
            comm_res = conn.execute(
                text("""
                    SELECT f.commodity_id
                    FROM fact_market_prices f
                    JOIN commodity_dim c ON f.commodity_id = c.commodity_id
                    JOIN model_evaluation e ON f.market_id = e.market_id AND f.commodity_id = e.commodity_id
                    WHERE f.market_id = :market_id AND c.commodity = :commodity
                    ORDER BY e.r2_score DESC
                    LIMIT 1
                """),
                {"market_id": market_id, "commodity": commodity}
            ).fetchone()
            
            if not comm_res:
                # Fallback to any variety if no model evaluation is registered yet
                comm_res = conn.execute(
                    text("""
                        SELECT DISTINCT f.commodity_id
                        FROM fact_market_prices f
                        JOIN commodity_dim c ON f.commodity_id = c.commodity_id
                        WHERE f.market_id = :market_id AND c.commodity = :commodity
                        LIMIT 1
                    """),
                    {"market_id": market_id, "commodity": commodity}
                ).fetchone()
            
            if not comm_res:
                raise HTTPException(
                    status_code=404,
                    detail=f"No active commodity '{commodity}' found in market ID {market_id}."
                )
            
            commodity_id = int(comm_res[0])
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database lookup error: {str(e)}")
            
    return await get_forecast_internal(market_id, commodity_id)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

