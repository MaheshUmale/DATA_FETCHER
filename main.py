import os
import time
import json
import asyncio
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
import requests
from dotenv import load_dotenv
import upstox_client
from db import get_connection
from translator import resolve_to_id, resolve_to_human

load_dotenv()

ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
BASE_URL = "https://api.upstox.com/v2"

app = FastAPI(title="Upstox Proxy Server")

@app.on_event("startup")
async def startup_event():
    app.loop = asyncio.get_running_loop()
    asyncio.create_task(stream_manager._process_ticks())

class Cache:
    def __init__(self, ttl=15):
        self.data = {}
        self.ttl = ttl
    def get(self, key):
        if key in self.data:
            entry = self.data[key]
            if time.time() - entry['timestamp'] < self.ttl:
                return entry['value']
            else:
                del self.data[key]
        return None
    def set(self, key, value):
        self.data[key] = {'value': value, 'timestamp': time.time()}

rest_cache = Cache(ttl=15)

def translate_keys_in_params(params: dict):
    if 'instrument_key' in params:
        keys = params['instrument_key'].split(',')
        resolved_keys = []
        for k in keys:
            ik = resolve_to_id(k)
            resolved_keys.append(ik if ik else k)
        params['instrument_key'] = ','.join(resolved_keys)
    return params

def translate_keys_in_response(data: Any):
    if isinstance(data, dict):
        new_data = {}
        for k, v in data.items():
            val = translate_keys_in_response(v)

            # Heuristic: Only attempt resolution for keys that look like instrument keys or known containers
            # Common instrument key patterns: SEGMENT|TOKEN or SEGMENT:SYMBOL
            if "|" in k or ":" in k or k.isupper():
                human_key = resolve_to_human(k)
            else:
                human_key = k

            if isinstance(val, dict):
                hk_from_val = None
                if 'instrument_token' in val:
                    hk_from_val = resolve_to_human(val['instrument_token'])
                    val['human_key'] = hk_from_val
                if 'instrument_key' in val:
                    hk_from_val = resolve_to_human(val['instrument_key'])
                    val['human_key'] = hk_from_val

                # If the key k didn't resolve to a nice human name but the token inside did, use it.
                if human_key == k and hk_from_val and hk_from_val != val.get('instrument_token') and hk_from_val != val.get('instrument_key'):
                    human_key = hk_from_val

            new_data[human_key] = val
        return new_data
    elif isinstance(data, list):
        return [translate_keys_in_response(i) for i in data]
    else:
        return data

class UpstoxStreamManager:
    def __init__(self):
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = ACCESS_TOKEN
        self.api_client = upstox_client.ApiClient(self.configuration)
        self.streamer = None
        self.active_keys = set()
        self.subscribers = set()
        self.lock = asyncio.Lock()
        self.tick_queue = asyncio.Queue()

    async def _process_ticks(self):
        """Background worker to write ticks to DuckDB in batches."""
        while True:
            ticks = []
            # Wait for at least one tick
            tick = await self.tick_queue.get()
            ticks.append(tick)

            # Collect more ticks if available immediately (batching)
            while not self.tick_queue.empty() and len(ticks) < 100:
                ticks.append(self.tick_queue.get_nowait())

            if ticks:
                conn = get_connection()
                try:
                    conn.executemany(
                        "INSERT INTO ticks (instrument_key, human_key, last_price) VALUES (?, ?, ?)",
                        ticks
                    )
                except Exception as e:
                    print(f"Error writing ticks to DuckDB: {e}")
                finally:
                    conn.close()

            for _ in range(len(ticks)):
                self.tick_queue.task_done()

    def on_message(self, message):
        loop = getattr(app, "loop", None)
        if not loop: return
        translated_message = translate_keys_in_response(message)
        # Queue ticks for background processing to DuckDB
        if message.get('type') == 'live_feed':
            feeds = message.get('feeds', {})
            for ik, feed in feeds.items():
                lp = None
                ff = feed.get('fullFeed', {})
                if 'indexFF' in ff: lp = ff['indexFF'].get('ltpc', {}).get('ltp')
                elif 'marketFF' in ff: lp = ff['marketFF'].get('ltpc', {}).get('ltp')
                if lp is not None:
                    hk = resolve_to_human(ik)
                    loop.call_soon_threadsafe(self.tick_queue.put_nowait, (ik, hk, lp))
        asyncio.run_coroutine_threadsafe(self.broadcast(translated_message), loop)

    async def broadcast(self, message):
        disconnected = []
        for ws in self.subscribers:
            try: await ws.send_json(message)
            except: disconnected.append(ws)
        for ws in disconnected: self.subscribers.remove(ws)

    async def update_subscriptions(self, new_keys: List[str]):
        async with self.lock:
            # Filter only truly new keys
            keys_to_sub = [k for k in new_keys if k not in self.active_keys]
            if not keys_to_sub:
                return

            self.active_keys.update(keys_to_sub)
            if self.streamer:
                if not self.streamer.is_connected():
                    self.streamer.connect()
                # Use the SDK's subscribe method for dynamic subscription
                self.streamer.subscribe(keys_to_sub, "full")
            else:
                self.streamer = upstox_client.MarketDataStreamerV3(self.api_client, list(self.active_keys), "full")
                self.streamer.on("message", self.on_message)
                self.streamer.connect()

stream_manager = UpstoxStreamManager()

@app.get("/market-quote/ltp")
async def get_ltp(instrument_key: str):
    cache_key = f"ltp:{instrument_key}"
    cached = rest_cache.get(cache_key)
    if cached: return cached
    params = translate_keys_in_params({"instrument_key": instrument_key})
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
    response = requests.get(f"{BASE_URL}/market-quote/ltp", params=params, headers=headers)
    if response.status_code != 200: raise HTTPException(status_code=response.status_code, detail=response.text)
    data = translate_keys_in_response(response.json())
    rest_cache.set(cache_key, data)
    return data

@app.get("/market-quote/quotes")
async def get_quotes(instrument_key: str):
    cache_key = f"quotes:{instrument_key}"
    cached = rest_cache.get(cache_key)
    if cached: return cached
    params = translate_keys_in_params({"instrument_key": instrument_key})
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
    response = requests.get(f"{BASE_URL}/market-quote/quotes", params=params, headers=headers)
    if response.status_code != 200: raise HTTPException(status_code=response.status_code, detail=response.text)
    data = translate_keys_in_response(response.json())
    rest_cache.set(cache_key, data)
    return data

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    stream_manager.subscribers.add(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            if message.get("method") == "sub":
                human_keys = message.get("data", {}).get("instrumentKeys", [])
                resolved_keys = [resolve_to_id(hk) for hk in human_keys if resolve_to_id(hk)]
                if resolved_keys: await stream_manager.update_subscriptions(resolved_keys)
    except WebSocketDisconnect:
        if websocket in stream_manager.subscribers: stream_manager.subscribers.remove(websocket)
    except Exception as e:
        if websocket in stream_manager.subscribers: stream_manager.subscribers.remove(websocket)

@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def catch_all(request: Request, path_name: str):
    method = request.method
    params = dict(request.query_params)
    cache_key = None
    if method == "GET":
        cache_key = f"{path_name}:{json.dumps(params, sort_keys=True)}"
        cached = rest_cache.get(cache_key)
        if cached: return cached
    upstox_params = translate_keys_in_params(params.copy())
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
    body = await request.body()
    response = requests.request(method=method, url=f"{BASE_URL}/{path_name}", params=upstox_params, headers=headers, data=body)
    try:
        data = translate_keys_in_response(response.json())
        if cache_key and response.status_code == 200: rest_cache.set(cache_key, data)
        return data
    except: return response.text

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
