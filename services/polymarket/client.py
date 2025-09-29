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
        This now fetches the full order book (bids and asks) for both outcomes.
        """
        market_url = f"{self.api_url}/markets/{condition_id}"
        try:
            market_resp = requests.get(market_url, timeout=10)
            market_resp.raise_for_status()
            market_data = market_resp.json()
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to fetch market data for {condition_id}: {e}")
            return {'active': False, 'closed': True}

        tokens = market_data.get("tokens", [])
        token_1, token_2 = (tokens[0], tokens[1]) if len(tokens) == 2 else (None, None)
        if 'Yes' in [t.get('outcome') for t in tokens]:
             token_1 = next((t for t in tokens if t.get("outcome") == "Yes"), None)
             token_2 = next((t for t in tokens if t.get("outcome") != "Yes"), None)

        token_1_id_str = token_1.get("token_id") if token_1 else None
        token_2_id_str = token_2.get("token_id") if token_2 else None
        
        outcome_1_name = token_1.get("outcome") if token_1 else "Outcome 1"
        outcome_2_name = token_2.get("outcome") if token_2 else "Outcome 2"

        order_book_1_asks, order_book_1_bids = [], []
        order_book_2_asks, order_book_2_bids = [], []

        order_book_url = f"{self.api_url}/book"
        for i, token_id_str in enumerate([token_1_id_str, token_2_id_str]):
            if not token_id_str: continue
            try:
                # Get ASKS (for buying)
                asks_resp = requests.get(order_book_url, params={"token_id": token_id_str, "side": "sell"}, timeout=5)
                if asks_resp.status_code == 200:
                    asks = asks_resp.json().get("asks", [])
                    book_asks = sorted([(float(ask['price']), int(float(ask['size']))) for ask in asks if float(ask['size']) > 0], key=lambda x: x[0])
                    if i == 0: order_book_1_asks = book_asks
                    else: order_book_2_asks = book_asks

                # Get BIDS (for selling)
                bids_resp = requests.get(order_book_url, params={"token_id": token_id_str, "side": "buy"}, timeout=5)
                if bids_resp.status_code == 200:
                    bids = bids_resp.json().get("bids", [])
                    book_bids = sorted([(float(bid['price']), int(float(bid['size']))) for bid in bids if float(bid['size']) > 0], key=lambda x: x[0], reverse=True)
                    if i == 0: order_book_1_bids = book_bids
                    else: order_book_2_bids = book_bids
            
            except (requests.exceptions.RequestException, ValueError, TypeError) as e:
                log.error(f"Failed to fetch or parse order book for token {token_id_str}: {e}")

        price_1 = order_book_1_asks[0][0] if order_book_1_asks else None
        price_2 = order_book_2_asks[0][0] if order_book_2_asks else None

        return {
            'condition_id': condition_id,
            'question': market_data.get('question'),
            'price_yes': price_1,
            'price_no': price_2,
            'order_book_yes': order_book_1_asks, # For backward compatibility
            'order_book_no': order_book_2_asks,   # For backward compatibility
            'order_book_yes_asks': order_book_1_asks,
            'order_book_yes_bids': order_book_1_bids,
            'order_book_no_asks': order_book_2_asks,
            'order_book_no_bids': order_book_2_bids,
            'outcome_yes': outcome_1_name,
            'outcome_no': outcome_2_name,
            'token_id_yes': token_1_id_str,
            'token_id_no': token_2_id_str,
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
