import duckdb

DB_PATH = "upstox_data.duckdb"

def get_connection():
    return duckdb.connect(DB_PATH)

def init_db():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            instrument_key VARCHAR PRIMARY KEY,
            exchange VARCHAR,
            trading_symbol VARCHAR,
            name VARCHAR,
            expiry DATE,
            strike_price DOUBLE,
            instrument_type VARCHAR,
            segment VARCHAR,
            asset_type VARCHAR,
            underlying_symbol VARCHAR,
            lot_size INTEGER,
            tick_size DOUBLE,
            display_name VARCHAR
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_resolution
        ON instruments (underlying_symbol, instrument_type, strike_price, expiry)
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            instrument_key VARCHAR,
            human_key VARCHAR,
            last_price DOUBLE,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.close()

if __name__ == "__main__":
    init_db()
    print("Database initialized.")
