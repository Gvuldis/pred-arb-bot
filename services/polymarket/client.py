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
        Fetch a single Polymarket market by condition_id, finds the YES/NO token IDs,
        and then fetches their respective prices from the /price endpoint as requested.
        """
        # Step 1: Fetch market details to get token IDs
        market_url = f"{self.api_url}/markets/{condition_id}"
        try:
            market_resp = requests.get(market_url, timeout=10)
            market_resp.raise_for_status()
            market_data = market_resp.json()
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to fetch market data for {condition_id}: {e}")
            # Return a structure that won't crash downstream checks
            return {'condition_id': condition_id, 'question': 'FETCH_ERROR', 'price_yes': None, 'price_no': None, 'active': False, 'closed': True}

        # Step 2: Extract token IDs for "Yes" and "No" outcomes
        tokens = market_data.get("tokens", [])
        yes_token_id = None
        no_token_id = None
        for token in tokens:
            if token.get("outcome") == "Yes":
                yes_token_id = token.get("token_id")
            elif token.get("outcome") == "No":
                no_token_id = token.get("token_id")

        if not yes_token_id or not no_token_id:
            log.warning(f"Could not find YES/NO token IDs for market {condition_id}")
            return {
                'condition_id': condition_id,
                'question': market_data.get('question'),
                'price_yes': None,
                'price_no': None,
                'active': market_data.get('active', False),
                'closed': market_data.get('closed', True),
            }

        # Step 3: Fetch prices for each token ID, specifying the 'buy' side
        price_url = f"{self.api_url}/price"
        yes_price = None
        no_price = None

        try:
            # Fetch YES price
            if yes_token_id:
                params_yes = {"token_id": yes_token_id, "side": "buy"}
                yes_price_resp = requests.get(price_url, params=params_yes, timeout=10)
                yes_price_resp.raise_for_status()
                yes_price_data = yes_price_resp.json()
                yes_price = float(yes_price_data.get('price')) if yes_price_data.get('price') else None

            # Fetch NO price
            if no_token_id:
                params_no = {"token_id": no_token_id, "side": "buy"}
                no_price_resp = requests.get(price_url, params=params_no, timeout=10)
                no_price_resp.raise_for_status()
                no_price_data = no_price_resp.json()
                no_price = float(no_price_data.get('price')) if no_price_data.get('price') else None

        except (requests.exceptions.RequestException, ValueError, TypeError) as e:
            log.error(f"Failed to fetch or parse price for tokens in market {condition_id}: {e}")
            # Ensure prices are None if fetching fails, to prevent bad calculations
            yes_price = None
            no_price = None

        return {
            'condition_id': condition_id,
            'question': market_data.get('question'),
            'price_yes': yes_price,
            'price_no': no_price,
            'active': market_data.get('active', False),
            'closed': market_data.get('closed', True),
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