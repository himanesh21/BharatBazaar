import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def add_indexes():
    # Load .env locally, then fallback to parent directory
    load_dotenv()
    if not os.getenv("DATABASE_URL"):
        load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
        
    db_uri = os.getenv("DATABASE_URL")
    if db_uri and db_uri.startswith("postgres://"):
        db_uri = db_uri.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_uri and not db_uri.startswith("postgresql+psycopg2://"):
        db_uri = db_uri.replace("postgresql://", "postgresql+psycopg2://", 1)

    if not db_uri:
        db_pass = os.getenv('DbPass', 'root123')
        db_uri = f"postgresql+psycopg2://postgres:{db_pass}@localhost:5432/market_analysis"
    
    print("Connecting to database...")
    engine = create_engine(db_uri, isolation_level="AUTOCOMMIT")
    
    # Queries to build indexes concurrently (or standard IF NOT EXISTS)
    index_queries = {
        "idx_fact_market": "CREATE INDEX IF NOT EXISTS idx_fact_market ON fact_market_prices (market_id);",
        "idx_fact_commodity": "CREATE INDEX IF NOT EXISTS idx_fact_commodity ON fact_market_prices (commodity_id);",
        "idx_fact_date": "CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_market_prices (date_id);",
        "idx_market_state": "CREATE INDEX IF NOT EXISTS idx_market_state ON market_dim (state_id);"
    }
    
    with engine.connect() as conn:
        for index_name, query in index_queries.items():
            print(f"Creating index '{index_name}' (this might take a minute on 13M rows)...")
            try:
                conn.execute(text(query))
                print(f"Index '{index_name}' is ready.")
            except Exception as e:
                print(f"Error creating index '{index_name}': {e}")
                
    print("\n✅ Database indexing complete! All analytical queries are now optimized.")

if __name__ == "__main__":
    add_indexes()
