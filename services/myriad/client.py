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
            
    def parse_realtime_prices(self, market_data: Dict) -> Optional[Dict]:
        """
        Parses market data to extract prices, shares, and titles for both outcomes.
        It prioritizes the most recent price from the 'price_charts' for outcome 1,
        and derives the price for outcome 0, ensuring data is as fresh and consistent as possible.
        """
        if not market_data or len(market_data.get("outcomes", [])) != 2:
            return None

        try:
            outcomes = market_data.get("outcomes", [])
            outcome0 = next(o for o in outcomes if o.get('id') == 0)
            outcome1 = next(o for o in outcomes if o.get('id') == 1)

            price1_realtime = None

            # Step 1: Attempt to get the most recent price for outcome 1 (usually "No") from its price chart.
            price_charts = outcome1.get("price_charts")
            if price_charts and isinstance(price_charts, list) and len(price_charts) > 0:
                prices_list = price_charts[0].get("prices")
                if prices_list and isinstance(prices_list, list) and len(prices_list) > 0:
                    last_price_point = prices_list[-1]
                    if "value" in last_price_point:
                        price1_realtime = float(last_price_point["value"])
                        log.info(f"Using real-time chart price for {market_data.get('slug')}: {price1_realtime}")

            # Step 2: If the price chart method fails, fall back to the main 'price' field.
            if price1_realtime is None:
                price1_fallback = outcome1.get("price")
                if price1_fallback is not None:
                    price1_realtime = float(price1_fallback)
                    log.warning(f"Falling back to main price field for {market_data.get('slug')}: {price1_realtime}")
            
            # Step 3: If we have a valid price for outcome 1, derive outcome 0's price. Otherwise, fail.
            if price1_realtime is None or not (0 <= price1_realtime <= 1):
                log.error(f"Could not determine a valid price for outcome 1 in market {market_data.get('slug')}")
                return None
            
            price0_derived = 1.0 - price1_realtime

            return {
                "price1": price0_derived,
                "shares1": outcome0.get("shares_held"),
                "title1": outcome0.get("title"),
                "price2": price1_realtime,
                "shares2": outcome1.get("shares_held"),
                "title2": outcome1.get("title"),
            }
        except (StopIteration, KeyError, IndexError, TypeError, ValueError) as e:
            log.error(f"Error parsing real-time prices for Myriad market {market_data.get('slug')}: {e}", exc_info=True)
            return None

    def fetch_prices(self, market_slug: str) -> Optional[Dict]:
        """
        DEPRECATED in favor of parse_realtime_prices.
        Fetch prices and liquidity shares for a given market slug.
        Returns a dictionary with prices and shares for both outcomes.
        """
        market_data = self.fetch_market_details(market_slug)
        return self.parse_realtime_prices(market_data)