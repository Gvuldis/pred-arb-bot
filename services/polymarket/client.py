import requests
import logging
from typing import List, Dict
from streamlit_app.db import load_polymarkets

log = logging.getLogger(__name__)

class PolymarketClient:
    def __init__(self, api_url: str = "https://clob.polymarket.com"):
        self.api_url = api_url

    def fetch_all_markets(self) -> List[Dict]:
        """
        Retrieve all active Polymarket CLOB markets by fetching from the API.
        """
        from matching.fuzzy import fetch_all_polymarket_clob_markets
        log.info("Fetching all Polymarket markets from API.")
        return fetch_all_polymarket_clob_markets()

    def fetch_market(self, condition_id: str) -> Dict:
        """
        Fetch a single Polymarket market by condition_id and parse YES/NO prices.
        Returns a dict with keys: condition_id, question, best_yes_ask, best_no_ask.
        """
        url = f"{self.api_url}/markets/{condition_id}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        
        # Prices are strings, need to be converted to float. Handle None.
        try:
            yes_price = float(data.get('best_yes_ask')) if data.get('best_yes_ask') else None
            no_price = float(data.get('best_no_ask')) if data.get('best_no_ask') else None
        except (ValueError, TypeError):
             yes_price = None
             no_price = None

        return {
            'condition_id': condition_id,
            'question': data.get('question'),
            'best_yes_ask': yes_price,
            'best_no_ask': no_price,
            'active': data.get('active', False),
            'closed': data.get('closed', True),
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