# services/bodega/client.py

import requests
from datetime import datetime

class BodegaClient:
    def __init__(self, api_url:str):
        self.api_url = api_url

    def fetch_markets(self):
        """
        Returns list of dicts with keys:
          id, name, deadline, options
        """
        from matching.fuzzy import fetch_bodega_v3_active_markets
        return fetch_bodega_v3_active_markets()

    def fetch_prices(self, market_id:str):
        """
        Returns dict with yesPrice_ada, noPrice_ada for a single market
        """
        pred_url = f"{self.api_url}/getPredictionInfo?id={market_id}"
        r = requests.get(pred_url); r.raise_for_status()
        info = r.json().get("predictionInfo",{})
        yes = info.get("prices",{}).get("yesPrice")
        no  = info.get("prices",{}).get("noPrice")
        return {
            "yesPrice_ada": int(yes)/1e6 if yes else None,
            "noPrice_ada":  int(no )/1e6 if no  else None
        }
