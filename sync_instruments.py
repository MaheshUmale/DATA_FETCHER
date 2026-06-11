import requests
import gzip
import io
import pandas as pd
import duckdb
from db import get_connection, init_db

INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

def sync():
    print(f"Downloading instruments from {INSTRUMENT_URL}...")
    response = requests.get(INSTRUMENT_URL)
    response.raise_for_status()

    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
        df = pd.read_json(f)

    print(f"Total instruments downloaded: {len(df)}")

    relevant_segments = ['NSE_FO', 'NSE_EQ', 'NSE_INDEX']
    df = df[df['segment'].isin(relevant_segments)].copy()
    df['expiry'] = pd.to_datetime(df['expiry'], unit='ms').dt.date
    df['strike_price'] = df['strike_price'].fillna(0.0)

    init_db()
    conn = get_connection()
    conn.execute("DELETE FROM instruments")
    conn.register('df_temp', df)
    conn.execute("""
        INSERT INTO instruments (
            instrument_key, exchange, trading_symbol, name, expiry,
            strike_price, instrument_type, segment, asset_type,
            underlying_symbol, lot_size, tick_size
        )
        SELECT
            instrument_key, exchange, trading_symbol, name, expiry,
            strike_price, instrument_type, segment, asset_type,
            underlying_symbol, lot_size, tick_size
        FROM df_temp
    """)

    count = conn.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
    print(f"Synchronized {count} instruments to DuckDB.")
    conn.close()

if __name__ == "__main__":
    sync()
