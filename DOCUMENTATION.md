# Upstox Proxy Server Documentation

This proxy server facilitates communication with the Upstox API using human-readable instrument keys instead of internal instrument tokens.

## Features

- **Bidirectional Key Translation**: Automatically converts human-readable keys (e.g., `NIFTY FUT`, `RELIANCE 25000 CE`) to Upstox instrument keys and back.
- **REST API Proxy**: Proxies Upstox REST APIs (LTP, Quotes, etc.) with caching and key translation.
- **WebSocket Streaming Proxy**: Proxies the Market Data Feed, allowing subscriptions using human-readable keys.
- **DuckDB Integration**: Efficiently stores instrument data for fast lookups and logs real-time ticks.
- **Caching**: 15-second caching for REST API requests to reduce API load.

## Human-Readable Key Formats

The proxy supports the following formats for instruments:

1.  **Futures**: `{UNDERLYING} FUT`
    - Example: `NIFTY FUT`, `BANKNIFTY FUT`, `RELIANCE FUT`
    - Resolves to the **nearest** expiry future contract.
2.  **Options**: `{UNDERLYING} {STRIKE} {CE/PE}`
    - Example: `NIFTY 25000 CE`, `BANKNIFTY 52000 PE`
    - Resolves to the **nearest** expiry option contract.
3.  **Equities/Indices**: `{SYMBOL}`
    - Example: `RELIANCE`, `TCS`, `NIFTY 50`, `NIFTY BANK`

## Endpoints

### REST API

The proxy mirrors the Upstox V2 API structure.

#### Get LTP
`GET /market-quote/ltp?instrument_key={HUMAN_KEYS}`
- `instrument_key`: Comma-separated human-readable keys.
- Example: `/market-quote/ltp?instrument_key=NIFTY%20FUT,RELIANCE`

#### Get Quotes
`GET /market-quote/quotes?instrument_key={HUMAN_KEYS}`

#### Generic Proxy
Any other Upstox V2 endpoint can be accessed through the proxy. If `instrument_key` is present in the query parameters, it will be translated.

### WebSocket API

#### Endpoint
`ws://localhost:8000/ws`

#### Subscribe
Send a JSON message to subscribe to instruments using human-readable keys.
```json
{
    "method": "sub",
    "data": {
        "instrumentKeys": ["NIFTY FUT", "RELIANCE 25000 CE"],
        "mode": "full"
    }
}
```

#### Receiving Data
The proxy will send back translated data where internal tokens are replaced or augmented with `human_key`.

## Data Storage (DuckDB)

The proxy uses `upstox_data.duckdb` to store:
- `instruments`: Master list of instruments (synced via `sync_instruments.py`).
- `ticks`: Real-time price updates received via WebSocket.

## Setup & Running

1.  **Environment**: Create a `.env` file with your `UPSTOX_ACCESS_TOKEN`.
2.  **Install Dependencies**: `pip install fastapi uvicorn duckdb upstox-python-sdk pandas requests python-dotenv websockets`
3.  **Sync Instruments**: `python sync_instruments.py` (Run this daily or whenever instrument masters change).
4.  **Run Server**: `python main.py`

The Swagger UI is available at `http://localhost:8000/docs` for testing.
