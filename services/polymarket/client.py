# services/polymarket/client.py

import requests
from typing import List, Dict
from streamlit_app.db import load_polymarkets

class PolymarketClient:
    def __init__(self, api_url: str = "https://clob.polymarket.com"):
        self.api_url = api_url
        self._all_markets = None

    def fetch_all_markets(self) -> List[Dict]:
        """
        Retrieve all active Polymarket CLOB markets (cached after first fetch).
        """
        from matching.fuzzy import fetch_all_polymarket_clob_markets
        if self._all_markets is None:
            self._all_markets = fetch_all_polymarket_clob_markets()
        return self._all_markets

    def fetch_market(self, condition_id: str) -> Dict:
        """
        Fetch a single Polymarket market by condition_id and parse YES/NO prices.
        Returns a dict with keys: condition_id, question, best_yes_ask, best_no_ask.
        """
        url = f"{self.api_url}/markets/{condition_id}"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        # Extract token prices
        yes_price = None
        no_price = None
        for token in data.get('tokens', []):
            outcome = token.get('outcome')
            price = token.get('price')
            if outcome == 'Yes':
                yes_price = price
            elif outcome == 'No':
                no_price = price
        return {
            'condition_id': condition_id,
            'question': data.get('question'),
            'best_yes_ask': yes_price,
            'best_no_ask': no_price
        }

    def search_markets(self, query: str) -> List[Dict]:
        """
        Search for markets by filtering cached database records.
        Returns a list of dicts with 'condition_id', 'question'.
        """
        if not query:
            return []
        all_markets = load_polymarkets()
        q_low = query.lower()
        return [m for m in all_markets if q_low in m.get('question', '').lower()]
