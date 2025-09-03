# services/myriad/client.py
import requests
import logging
from typing import List, Dict, Optional

log = logging.getLogger(__name__)

class MyriadClient:
    def __init__(self, api_url: str):
        self.api_url = api_url

    def fetch_markets(self) -> List[Dict]:
        """Fetch all active Myriad markets."""
        log.info("Fetching fresh Myriad markets from API.")
        # This specific endpoint is for a particular "land" on Myriad, which is what we want.
        url = f"{self.api_url}/markets?network_id=274133&state=open&land_ids=myriad-szn2-usdc-v33"
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            markets = resp.json()
            # We only care about binary (2-outcome) markets for now
            return [m for m in markets if len(m.get("outcomes", [])) == 2]
        except requests.RequestException as e:
            log.error(f"Failed to fetch Myriad markets: {e}")
            return []

    def fetch_market_details(self, market_slug: str) -> Optional[Dict]:
        """Retrieve a single Myriad market by its slug."""
        url = f"{self.api_url}/markets/{market_slug}"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            log.error(f"Failed to fetch market details for slug {market_slug}: {e}")
            return None

    def fetch_prices(self, market_slug: str) -> Optional[Dict]:
        """
        Fetch prices and liquidity shares for a given market slug.
        Returns a dictionary with prices and shares for both outcomes.
        """
        market_data = self.fetch_market_details(market_slug)
        if not market_data or len(market_data.get("outcomes", [])) != 2:
            return None

        try:
            outcomes = market_data["outcomes"]
            outcome1 = next(o for o in outcomes if o['id'] == 0)
            outcome2 = next(o for o in outcomes if o['id'] == 1)

            return {
                "price1": outcome1.get("price"),
                "shares1": outcome1.get("shares_held"),
                "title1": outcome1.get("title"),
                "price2": outcome2.get("price"),
                "shares2": outcome2.get("shares_held"),
                "title2": outcome2.get("title"),
            }
        except (StopIteration, KeyError) as e:
            log.error(f"Error parsing prices for Myriad market {market_slug}: {e}")
            return None