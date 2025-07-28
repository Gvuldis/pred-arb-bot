import requests
import logging
import time
import json
from typing import List, Dict

log = logging.getLogger(__name__)

class BodegaClient:
    def __init__(self, api_url: str):
        self.api_url = api_url

    def fetch_markets(self, force_refresh: bool = False) -> List[Dict]:
        """
        Fetch all active Bodega V3 market configurations.
        The `force_refresh` parameter is ignored as there is no cache.
        """
        log.info("Fetching fresh Bodega markets from API.")
        url = f"{self.api_url}/getMarketConfigs"
        resp = requests.post(url, json={}, timeout=10)
        resp.raise_for_status()
        
        # --- ADD THESE LINES FOR DEBUGGING ---
        raw_data = resp.json()
        #log.info(f"DBG: Raw Bodega API response: {json.dumps(raw_data, indent=2)}")
        # --- END DEBUGGING LINES ---

        configs = raw_data.get("marketConfigs", [])
        
        now_ms = int(time.time() * 1000)
        active = []
        for m in configs:
            
            if m.get("status") != "Active":
                continue
            dl = m.get("deadline")
            if not dl or not str(dl).isdigit() or int(dl) < now_ms:
                continue

            # Standardize options to have 'side' and 'shares'.
            # Per user feedback, the 'shares' from this endpoint are incorrect.
            # The correct liquidity shares (q_yes, q_no) are fetched from /getPredictionInfo.
            # We set shares to 0 here to avoid confusion.
            opts = m.get("options", [])
            std_opts = []
            for o in opts:
                std_opts.append({
                    'side': o.get('side'),
                    'shares': 0
                })
            m['options'] = std_opts
            active.append(m)

        return active

    def fetch_market_config(self, market_id: str) -> Dict:
        """
        Retrieve a single Bodega market config by ID from the list of active markets.
        """
        # This will always fetch fresh data since fetch_markets has no cache
        for m in self.fetch_markets():
            if m.get("id") == market_id:
                return m
        raise ValueError(f"Market config not found for ID: {market_id}")

    def fetch_prices(self, market_id: str) -> Dict:
        """
        Fetch YES/NO volumes and prices for a given market ID via GET /getPredictionInfo?id=...
        Returns ADA-denominated prices & volumes. The 'volumes' are the liquidity shares.
        """
        url = f"{self.api_url}/getPredictionInfo"
        r = requests.get(url, params={"id": market_id}, timeout=10)
        r.raise_for_status()
        info = r.json().get("predictionInfo", {})

        # Convert Lovelace → ADA
        LP = 1_000_000
        yes_price_ada = info.get("prices", {}).get("yesPrice", 0) / LP
        no_price_ada  = info.get("prices", {}).get("noPrice",  0) / LP
        yes_vol_ada   = info.get("volumes", {}).get("yesVolume", 0)
        no_vol_ada    = info.get("volumes", {}).get("noVolume",  0)

        return {
            "yesPrice_ada": yes_price_ada,
            "noPrice_ada":  no_price_ada,
            "yesVolume_ada": yes_vol_ada,
            "noVolume_ada":  no_vol_ada
        }