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
        Fetch a single Polymarket market by condition_id.
        This now fetches the full order book for both YES and NO outcomes
        and derives the best price from it.
        """
        # Step 1: Fetch market details to get token IDs
        market_url = f"{self.api_url}/markets/{condition_id}"
        try:
            market_resp = requests.get(market_url, timeout=10)
            market_resp.raise_for_status()
            market_data = market_resp.json()
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to fetch market data for {condition_id}: {e}")
            return {
                'condition_id': condition_id, 'question': 'FETCH_ERROR',
                'price_yes': None, 'price_no': None,
                'order_book_yes': [], 'order_book_no': [],
                'active': False, 'closed': True
            }

        # Step 2: Extract token IDs for "Yes" and "No" outcomes
        tokens = market_data.get("tokens", [])
        yes_token_id = next((token.get("token_id") for token in tokens if token.get("outcome") == "Yes"), None)
        no_token_id = next((token.get("token_id") for token in tokens if token.get("outcome") == "No"), None)

        order_book_yes = []
        order_book_no = []

        # Step 3: Fetch order books for each token ID using the /book endpoint
        order_book_url = f"{self.api_url}/book"
        try:
            # Fetch YES order book
            if yes_token_id:
                params_yes_book = {"token_id": yes_token_id}
                yes_book_resp = requests.get(order_book_url, params=params_yes_book, timeout=10)
                yes_book_resp.raise_for_status()
                # "asks" are what we can buy from
                yes_asks = yes_book_resp.json().get("asks", [])
                # Format: list of (price, size) tuples, sorted by price
                order_book_yes = sorted([(float(ask['price']), int(float(ask['size']))) for ask in yes_asks if float(ask['size']) > 0], key=lambda x: x[0])

            # Fetch NO order book
            if no_token_id:
                params_no_book = {"token_id": no_token_id}
                no_book_resp = requests.get(order_book_url, params=params_no_book, timeout=10)
                no_book_resp.raise_for_status()
                no_asks = no_book_resp.json().get("asks", [])
                order_book_no = sorted([(float(ask['price']), int(float(ask['size']))) for ask in no_asks if float(ask['size']) > 0], key=lambda x: x[0])

        except (requests.exceptions.RequestException, ValueError, TypeError) as e:
            log.error(f"Failed to fetch or parse order book for tokens in market {condition_id}: {e}")

        # Step 4: Derive best price from order book (lowest ask) for reference
        yes_price = order_book_yes[0][0] if order_book_yes else None
        no_price = order_book_no[0][0] if order_book_no else None

        return {
            'condition_id': condition_id,
            'question': market_data.get('question'),
            'price_yes': yes_price,
            'price_no': no_price,
            'order_book_yes': order_book_yes,
            'order_book_no': order_book_no,
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