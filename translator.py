import re
import functools
from db import get_connection

@functools.lru_cache(maxsize=1024)
def resolve_to_id(human_key: str):
    conn = get_connection()
    human_key = human_key.strip().upper()

    try:
        if human_key.endswith(" FUT"):
            underlying = human_key[:-4].strip()
            query = """
                SELECT instrument_key
                FROM instruments
                WHERE (name = ? OR underlying_symbol = ?)
                  AND instrument_type = 'FUT'
                  AND segment = 'NSE_FO'
                ORDER BY expiry ASC
                LIMIT 1
            """
            res = conn.execute(query, (underlying, underlying)).fetchone()
            if res: return res[0]

        match = re.match(r"(.+?)\s+(\d+(?:\.\d+)?)\s+(CE|PE)$", human_key)
        if match:
            underlying, strike, opt_type = match.groups()
            strike = float(strike)
            query = """
                SELECT instrument_key
                FROM instruments
                WHERE (name = ? OR underlying_symbol = ?)
                  AND instrument_type = ?
                  AND strike_price = ?
                  AND segment = 'NSE_FO'
                ORDER BY expiry ASC
                LIMIT 1
            """
            res = conn.execute(query, (underlying, underlying, opt_type, strike)).fetchone()
            if res: return res[0]

        query = """
            SELECT instrument_key
            FROM instruments
            WHERE trading_symbol = ? OR UPPER(name) = ? OR instrument_key = ?
            LIMIT 1
        """
        res = conn.execute(query, (human_key, human_key, human_key)).fetchone()
        if res: return res[0]

        return None
    finally:
        conn.close()

@functools.lru_cache(maxsize=1024)
def resolve_to_human(instrument_key: str):
    conn = get_connection()
    clean_key = instrument_key
    parts = []
    if ":" in instrument_key:
        parts = instrument_key.split(":")
        clean_key = parts[1]
    elif "|" in instrument_key:
        parts = instrument_key.split("|")
        clean_key = parts[1]

    try:
        alt_key = instrument_key.replace(":", "|") if ":" in instrument_key else instrument_key.replace("|", ":")
        query = """
            SELECT name, underlying_symbol, instrument_type, strike_price, trading_symbol, segment
            FROM instruments
            WHERE instrument_key = ? OR trading_symbol = ? OR instrument_key = ?
        """
        params = [instrument_key, clean_key, alt_key]
        if len(parts) == 2:
            query += " OR (segment = ? AND trading_symbol = ?)"
            params.extend([parts[0], parts[1]])
        query += " LIMIT 1"

        res = conn.execute(query, params).fetchone()
        if not res:
            return instrument_key

        name, underlying, inst_type, strike, trading_symbol, segment = res
        base = underlying if underlying else (trading_symbol if trading_symbol else name)

        if inst_type == 'FUT':
            return f"{base} FUT"
        elif inst_type in ['CE', 'PE']:
            strike_str = f"{strike:g}"
            return f"{base} {strike_str} {inst_type}"
        else:
            return trading_symbol if trading_symbol else name
    finally:
        conn.close()
