# services/polymarket/client.py

import requests

class PolymarketClient:
    def __init__(self, api_url:str="https://clob.polymarket.com"):
        self.api_url = api_url

    def fetch_all_markets(self):
        from matching.fuzzy import fetch_all_polymarket_clob_markets
        return fetch_all_polymarket_clob_markets()

    def fetch_market(self, condition_id:str):
        url = f"{self.api_url}/markets/{condition_id}"
        r = requests.get(url); r.raise_for_status()
        m = r.json()
        tokens = m.get("tokens",[])
        yes = next((float(t["price"]) for t in tokens if t["outcome"].lower()=="yes"),0)
        no  = next((float(t["price"]) for t in tokens if t["outcome"].lower()=="no"),0)
        return {
            "condition_id": m["condition_id"],
            "question": m.get("question",""),
            "end_date_iso": m.get("end_date_iso",""),
            "best_yes_ask": yes,
            "best_no_ask": no,
            "orderbook_yes": [],
            "orderbook_no": []
        }
