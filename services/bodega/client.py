# services/bodega/client.py

import requests
from typing import List, Dict

class BodegaClient:
    def __init__(self, api_url: str):
        self.api_url = api_url
        self._all_markets = None

    def fetch_markets(self) -> List[Dict]:
        """
        Fetch all active Bodega V3 market configurations (cached).
        """
        if self._all_markets is None:
            url = f"{self.api_url}/getMarketConfigs"
            resp = requests.post(url, json={})
            resp.raise_for_status()
            configs = resp.json().get("marketConfigs", [])
            now_ms = int(__import__('time').time() * 1000)
            active = []
            for m in configs:
                if m.get("status") != "Active":
                    continue
                dl = m.get("deadline")
                if not dl or not str(dl).isdigit() or int(dl) < now_ms:
                    continue
                # Standardize options to have 'side' and 'shares'
                opts = m.get("options", [])
                std_opts = []
                for o in opts:
                    std_opts.append({
                        'side': o.get('side'),
                        'shares': o.get('shares', o.get('lpShares', 0))
                    })
                m['options'] = std_opts
                active.append(m)
            self._all_markets = active
        return self._all_markets

    def fetch_market_config(self, market_id: str) -> Dict:
        """
        Retrieve a single Bodega market config by ID.
        """
        for m in self.fetch_markets():
            if m.get("id") == market_id:
                return m
        raise ValueError(f"Market config not found for ID: {market_id}")

    def fetch_prices(self, market_id: str) -> Dict:
        """
        Fetch YES/NO volumes and prices for a given market ID via GET /getPredictionInfo?id=...
        Returns ADA-denominated prices & volumes.
        """
        url = f"{self.api_url}/getPredictionInfo"
        # Send as GET with ?id= instead of POST
        r = requests.get(url, params={"id": market_id})
        r.raise_for_status()
        info = r.json().get("predictionInfo", {})

        # Convert Lovelace → ADA
        LP = 1_000_000
        yes_price_ada = info.get("prices", {}).get("yesPrice", 0) / LP
        no_price_ada  = info.get("prices", {}).get("noPrice",  0) / LP
        yes_vol_ada   = info.get("volumes", {}).get("yesVolume", 0) / LP
        no_vol_ada    = info.get("volumes", {}).get("noVolume",  0) / LP

        return {
            "yesPrice_ada": yes_price_ada,
            "noPrice_ada":  no_price_ada,
            "yesVolume_ada": yes_vol_ada,
            "noVolume_ada":  no_vol_ada
        }


