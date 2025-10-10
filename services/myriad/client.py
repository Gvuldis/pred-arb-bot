# services/myriad/client.py
import requests
import logging
import math
from typing import List, Dict, Optional
from web3.contract import Contract
from .model import compute_price as compute_lmsr_price

log = logging.getLogger(__name__)

class MyriadClient:
    def __init__(self, api_url: str, myriad_contract: Optional[Contract]):
        self.api_url = api_url
        self.contract = myriad_contract

    def fetch_markets(self) -> List[Dict]:
        """Fetch all active Myriad markets and their on-chain fees."""
        log.info("Fetching fresh Myriad markets from API.")
        url = f"{self.api_url}/markets?network_id=274133&state=open&land_ids=myriad-szn2-usdc-v33"
        try:
            # <<< FIX: Increased timeout from 15 to 25 >>>
            resp = requests.get(url, timeout=100)
            resp.raise_for_status()
            markets_api = resp.json()
            
            markets_with_fees = []
            for m in markets_api:
                if len(m.get("outcomes", [])) != 2:
                    continue
                
                market_id = m.get('id')
                if self.contract and market_id:
                    try:
                        # Fetch fee only once when refreshing market data
                        fee_scaled = self.contract.functions.getMarketFee(market_id).call()
                        m['fee'] = float(fee_scaled / 10**18)
                    except Exception as e:
                        log.error(f"Failed to fetch on-chain fee for {m.get('slug')}: {e}. Setting fee to None.")
                        m['fee'] = None
                else:
                    m['fee'] = None
                markets_with_fees.append(m)

            return markets_with_fees
        except requests.RequestException as e:
            log.error(f"Failed to fetch Myriad markets: {e}")
            raise

    def fetch_market_details(self, market_slug: str) -> Optional[Dict]:
        """Retrieve a single Myriad market by its slug, including its on-chain fee."""
        url = f"{self.api_url}/markets/{market_slug}"
        try:
            # <<< FIX: Increased timeout from 10 to 20 >>>
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            
            market_id = data.get('id')
            if self.contract and market_id:
                try:
                    fee_scaled = self.contract.functions.getMarketFee(market_id).call()
                    data['fee'] = float(fee_scaled / 10**18)
                except Exception as e:
                    log.error(f"Failed to fetch on-chain fee for {market_slug}: {e}. Setting fee to None.")
                    data['fee'] = None
            else:
                data['fee'] = None
            
            return data
        except requests.RequestException as e:
            log.error(f"Failed to fetch market details for slug {market_slug}: {e}")
            return None
            
    def parse_realtime_prices(self, market_data: Dict) -> Optional[Dict]:
        """
        Gets the live on-chain PRICE and creates a synthetic, but mathematically correct,
        set of share counts for use in cost calculations.
        """
        if not market_data or len(market_data.get("outcomes", [])) != 2:
            return None

        try:
            market_id = market_data.get('id')
            b_param = market_data.get("liquidity")

            if not market_id or not b_param or b_param <= 0:
                log.error(f"Market data for '{market_data.get('slug')}' is missing 'id' or 'liquidity'.")
                return None

            if not self.contract:
                log.warning("Myriad contract not initialized. Cannot fetch on-chain price.")
                return None
            
            # --- Step 1: Fetch the live on-chain PRICE (the ground truth for price) ---
            log.info(f"Fetching on-chain price for Myriad market ID: {market_id}")
            price0_scaled = self.contract.functions.getMarketOutcomePrice(market_id, 0).call()
            
            price0_live = float(price0_scaled / 10**18)
            price1_live = 1.0 - price0_live

            # --- Step 2: Derive the share DIFFERENCE from the live price ---
            # The cost of a trade only depends on the *difference* between shares (q0 - q1), not their absolute values.
            # We can calculate this difference directly from the price.
            # Formula: q0 - q1 = B * log(price0 / price1)
            if price0_live <= 0 or price1_live <= 0:
                log.error(f"Invalid on-chain price ({price0_live}) for market {market_id}. Cannot proceed.")
                return None
            share_difference = b_param * math.log(price0_live / price1_live)

            # --- Step 3: Create "synthetic" share counts for the calculation model ---
            # We create a pair of q0/q1 that has the correct difference. The simplest way is to set one to 0.
            # The resulting cost calculations will be mathematically identical to the real market state.
            q0_synthetic = share_difference
            q1_synthetic = 0.0
            
            log.info(f"Live price for {market_data.get('slug')} is {price0_live:.4f}. Using synthetic shares for calculation: q0={q0_synthetic:.2f}, q1={q1_synthetic:.2f}")

            # --- Step 4: Get outcome titles from cached API data ---
            outcomes = market_data.get("outcomes", [])
            outcome0_api = next(o for o in outcomes if o.get('id') == 0)
            outcome1_api = next(o for o in outcomes if o.get('id') == 1)

            return {
                # Return the live price and the synthetic shares.
                # The model will use these to calculate the correct trade cost.
                "price1": price0_live,
                "price2": price1_live,
                
                "shares1": q0_synthetic,
                "shares2": q1_synthetic,

                "title1": outcome0_api.get("title"),
                "title2": outcome1_api.get("title"),

                "liquidity": b_param,
            }
        except Exception as e:
            log.error(f"Error parsing on-chain price for Myriad market {market_data.get('slug')}: {e}", exc_info=True)
            return None