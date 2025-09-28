import requests
import logging
import math
from typing import List, Dict, Optional
from web3.contract import Contract

log = logging.getLogger(__name__)

class MyriadClient:
    def __init__(self, api_url: str, myriad_contract: Optional[Contract]):
        self.api_url = api_url
        self.contract = myriad_contract

    def fetch_markets(self) -> List[Dict]:
        """Fetch all active Myriad markets."""
        log.info("Fetching fresh Myriad markets from API.")
        url = f"{self.api_url}/markets?network_id=274133&state=open&land_ids=myriad-szn2-usdc-v33"
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            markets = resp.json()
            return [m for m in markets if len(m.get("outcomes", [])) == 2]
        except requests.RequestException as e:
            # Re-raise the exception to be handled by the caller
            log.error(f"Failed to fetch Myriad markets: {e}")
            raise

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
        Parses market data, gets live on-chain prices, and re-calculates
        the AMM state (q0, q1) for accurate arbitrage calculations.
        """
        if not market_data or len(market_data.get("outcomes", [])) != 2:
            return None

        try:
            market_id = market_data.get('id')
            if not market_id:
                log.error(f"Market data for '{market_data.get('slug')}' is missing 'id'.")
                return None

            # --- Step 1: Fetch live on-chain prices ---
            if not self.contract:
                log.warning("Myriad contract not initialized in config. Cannot fetch on-chain prices. Aborting price parse.")
                return None
            
            log.info(f"Fetching on-chain price for Myriad market ID: {market_id}")
            price0_scaled = self.contract.functions.getMarketOutcomePrice(market_id, 0).call()
            price1_scaled = self.contract.functions.getMarketOutcomePrice(market_id, 1).call()
            
            price0_onchain = float(price0_scaled / 10**18)
            price1_onchain = float(price1_scaled / 10**18)
            
            # Basic validation
            if not (0 < price0_onchain < 1 and 0 < price1_onchain < 1 and abs(price0_onchain + price1_onchain - 1.0) < 0.01):
                 log.error(f"Invalid or non-summing on-chain prices for market {market_id}: p0={price0_onchain}, p1={price1_onchain}. Skipping.")
                 return None

            # --- Step 2: Get other parameters from API data ---
            outcomes = market_data.get("outcomes", [])
            outcome0_api = next(o for o in outcomes if o.get('id') == 0)
            outcome1_api = next(o for o in outcomes if o.get('id') == 1)

            q0_lag = outcome0_api.get("shares_held")
            q1_lag = outcome1_api.get("shares_held")
            b_param = market_data.get("liquidity")

            if None in [q0_lag, q1_lag, b_param] or b_param <= 0:
                log.error(f"Missing shares_held or liquidity from API for market {market_id}. Cannot recalculate shares.")
                return None

            # --- Step 3: Recalculate q0 and q1 using the on-chain price ---
            # We solve a system of two linear equations:
            # 1. q0 - q1 = b * log(p0/p1)    (derived from the LMSR price formula)
            # 2. q0 + q1 = q0_lag + q1_lag  (approximating total shares as constant)
            
            # Constraint 1: Difference of shares
            c1 = b_param * math.log(price0_onchain / price1_onchain) # This is q0 - q1
            
            # Constraint 2: Sum of shares
            c2 = q0_lag + q1_lag # This is approximately q0 + q1

            # Solve the system
            q0_recalc = (c1 + c2) / 2.0
            q1_recalc = (c2 - c1) / 2.0
            
            log.info(f"Recalculated shares for {market_data.get('slug')} (ID {market_id}): "
                     f"q0={q0_recalc:.2f}, q1={q1_recalc:.2f} (was q0={q0_lag:.2f}, q1={q1_lag:.2f})")

            return {
                # Use on-chain prices for opportunity detection.
                # 'price1'/'shares1' correspond to outcome 0, 'price2'/'shares2' to outcome 1.
                "price1": price0_onchain,
                "price2": price1_onchain,
                
                "shares1": q0_recalc,
                "shares2": q1_recalc,

                "title1": outcome0_api.get("title"),
                "title2": outcome1_api.get("title"),

                "liquidity": b_param,
            }
        except Exception as e:
            log.error(f"Error parsing on-chain prices for Myriad market {market_data.get('slug')}: {e}", exc_info=True)
            return None

    def fetch_prices(self, market_slug: str) -> Optional[Dict]:
        """
        DEPRECATED in favor of parse_realtime_prices.
        Fetch prices and liquidity shares for a given market slug.
        Returns a dictionary with prices and shares for both outcomes.
        """
        market_data = self.fetch_market_details(market_slug)
        return self.parse_realtime_prices(market_data)
