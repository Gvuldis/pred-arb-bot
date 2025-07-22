import requests
import logging
from typing import List, Dict
from streamlit_app.db import load_polymarkets
import json
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
        This now fetches the full order book for both outcomes
        and derives the best price from it. It handles both traditional
        'Yes'/'No' markets and other binary markets (e.g., player vs player).
        """
        # Step 1: Fetch market details to get token IDs
        market_url = f"{self.api_url}/markets/{condition_id}"
        try:
            market_resp = requests.get(market_url, timeout=10)
            market_resp.raise_for_status()
            market_data = market_resp.json()
            log.info(f"DBG: Raw Polymarket API response: {json.dumps(market_data, indent=2)}")
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to fetch market data for {condition_id}: {e}")
            return {
                'condition_id': condition_id, 'question': 'FETCH_ERROR',
                'price_yes': None, 'price_no': None,
                'order_book_yes': [], 'order_book_no': [],
                'active': False, 'closed': True,
                'outcome_yes': 'Yes', 'outcome_no': 'No',
            }

        # Step 2: Identify the two outcome tokens.
        tokens = market_data.get("tokens", [])
        token_1 = None
        token_2 = None

        if len(tokens) == 2:
            # For binary markets, assign tokens.
            # Try to find "Yes" explicitly for standard markets.
            token_yes_candidate = next((t for t in tokens if t.get("outcome") == "Yes"), None)

            if token_yes_candidate:
                token_1 = token_yes_candidate
                # The other token is "No"
                token_2 = next((t for t in tokens if t.get("outcome") != "Yes"), None)
            else:
                # If no "Yes", assume the first is the "Yes"-equivalent outcome
                # and the second is the "No"-equivalent outcome.
                token_1 = tokens[0]
                token_2 = tokens[1]

        token_1_id = token_1.get("token_id") if token_1 else None
        token_2_id = token_2.get("token_id") if token_2 else None
        
        outcome_1_name = token_1.get("outcome") if token_1 else "Outcome 1"
        outcome_2_name = token_2.get("outcome") if token_2 else "Outcome 2"

        order_book_1 = []
        order_book_2 = []

        # Step 3: Fetch order books for each token ID using the /book endpoint
        order_book_url = f"{self.api_url}/book"
        try:
            # Fetch order book for the first outcome ("Yes" or equivalent)
            if token_1_id:
                params_book_1 = {"token_id": token_1_id}
                book_1_resp = requests.get(order_book_url, params=params_book_1, timeout=10)
                book_1_resp.raise_for_status()
                # "asks" are what we can buy from
                asks_1 = book_1_resp.json().get("asks", [])
                # Format: list of (price, size) tuples, sorted by price
                order_book_1 = sorted([(float(ask['price']), int(float(ask['size']))) for ask in asks_1 if float(ask['size']) > 0], key=lambda x: x[0])

            # Fetch order book for the second outcome ("No" or equivalent)
            if token_2_id:
                params_book_2 = {"token_id": token_2_id}
                book_2_resp = requests.get(order_book_url, params=params_book_2, timeout=10)
                book_2_resp.raise_for_status()
                asks_2 = book_2_resp.json().get("asks", [])
                order_book_2 = sorted([(float(ask['price']), int(float(ask['size']))) for ask in asks_2 if float(ask['size']) > 0], key=lambda x: x[0])

        except (requests.exceptions.RequestException, ValueError, TypeError) as e:
            log.error(f"Failed to fetch or parse order book for tokens in market {condition_id}: {e}")

        # Step 4: Derive best price from order book (lowest ask) for reference
        price_1 = order_book_1[0][0] if order_book_1 else None
        price_2 = order_book_2[0][0] if order_book_2 else None

        # The rest of the system expects 'yes' and 'no' keys. We will map outcome 1 to 'yes' and 2 to 'no'.
        return {
            'condition_id': condition_id,
            'question': market_data.get('question'),
            'price_yes': price_1,
            'price_no': price_2,
            'order_book_yes': order_book_1,
            'order_book_no': order_book_2,
            'outcome_yes': outcome_1_name, # To know what "yes" means
            'outcome_no': outcome_2_name,   # To know what "no" means
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