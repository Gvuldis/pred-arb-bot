"""
Market fetchers for Bodega vs Polymarket.
"""
import time
import requests
import logging
from typing import List, Tuple, Dict
from datetime import datetime

log = logging.getLogger(__name__)

#––– POLYMARKET FETCHER –––

def fetch_all_polymarket_clob_markets(
    max_retries: int = 4,
    backoff_factor: float = 0.5
) -> List[Dict]:
    """
    Retrieve all active Polymarket CLOB markets with retry/backoff.
    """
    base_url = "https://clob.polymarket.com/markets"
    results = []
    cursor = ""

    while True:
        url = base_url + (f"?next_cursor={cursor}" if cursor else "")
        for attempt in range(max_retries):
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code == 429:
                    sleep_time = backoff_factor * (2 ** attempt)
                    log.warning(f"Rate limited. Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                    continue
                resp.raise_for_status()
                data = resp.json()
                results.extend(data.get("data", []))
                cursor = data.get("next_cursor")
                break # Success, break retry loop
            except requests.exceptions.RequestException as e:
                log.error(f"Request to {url} failed on attempt {attempt+1}: {e}")
                if attempt == max_retries - 1:
                    raise  # Re-raise the exception if all retries fail
                time.sleep(backoff_factor * (2 ** attempt))
        else: # This else belongs to the for loop, executes if loop finishes without break
             log.error("All retries failed for Polymarket fetch.")
             break

        if not cursor or cursor == "LTE=":
            break
        time.sleep(0.2)
    return [m for m in results if m.get("active") and not m.get("closed")]

#––– BODEGA FETCHER –––

def fetch_bodega_v3_active_markets(api_url: str) -> List[Dict]:
    """
    Retrieve active Bodega V3 markets via POST /getMarketConfigs.
    """
    active_api_markets = []
    try:
        url = f"{api_url}/getMarketConfigs"
        resp = requests.post(url, json={}, timeout=10)
        resp.raise_for_status()
        configs = resp.json().get("marketConfigs", [])
        
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        for m in configs:
            if m.get("status") != "Active":
                continue
            dl = m.get("deadline")
            if not dl or not str(dl).isdigit() or int(dl) < now_ms:
                continue
            active_api_markets.append({
                "id": m["id"],
                "name": m["name"],
                "deadline": dl,
                "options": m.get("options", [])
            })
    except requests.exceptions.RequestException as e:
        log.warning(f"Could not fetch live Bodega markets: {e}. Returning empty list.")

    log.info(f"Loaded {len(active_api_markets)} active markets from API.")
    return active_api_markets
