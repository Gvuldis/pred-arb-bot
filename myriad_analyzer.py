# myriad_analyzer.py
import requests
import math
import logging
from scipy.special import logsumexp
from typing import Dict, Any, List, Optional, Tuple

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Base API URL for Myriad/Polkamarkets
API_BASE_URL = "https://api-production.polkamarkets.com"

# The total fee for a buy transaction is 3% (1% market + 1% treasury + 1% distributor)
# Sell transactions appear to have 0% fee based on the API response.
FEE_RATE_BUY = 0.03


class MyriadMarketAnalyzer:
    """
    A tool to analyze Myriad/Polkamarkets AMM markets.

    This class fetches data for a specific market, infers its underlying LMSR
    parameters, and calculates trade costs and price impact. It is inspired
    by the AMM analysis tools used for Bodega markets.
    """

    def __init__(self, market_slug: str, api_base_url: str = API_BASE_URL):
        """
        Initializes the analyzer for a specific market slug.

        Args:
            market_slug: The unique slug for the market URL.
            api_base_url: The base URL for the Myriad/Polkamarkets API.
        """
        self.api_url = f"{api_base_url}/markets/{market_slug}"
        self.market_data: Optional[Dict[str, Any]] = None
        
        # State variables for the AMM pool
        self.q1: Optional[float] = None  # Liquidity shares for Outcome 1 (e.g., "Yes")
        self.q2: Optional[float] = None  # Liquidity shares for Outcome 2 (e.g., "No")
        self.price1: Optional[float] = None
        self.price2: Optional[float] = None
        self.outcome_titles: Dict[int, str] = {}
        
        # LMSR liquidity parameter from API
        self.b_param: Optional[float] = None

    def fetch_and_parse(self) -> bool:
        """
        Fetches market data from the API and parses it into a usable state.
        
        Returns:
            True if fetching and parsing were successful, False otherwise.
        """
        log.info(f"Fetching data from: {self.api_url}")
        try:
            response = requests.get(self.api_url, timeout=10)
            response.raise_for_status()
            self.market_data = response.json()
            log.info("Successfully fetched market data.")
            return self._parse_data()
        except requests.exceptions.RequestException as e:
            log.error(f"API request failed: {e}")
            return False
        except (ValueError, KeyError) as e:
            log.error(f"Failed to parse JSON response: {e}")
            return False

    def _parse_data(self) -> bool:
        """
        Extracts key information from the fetched market data.
        The liquidity pool shares for each outcome are in the 'shares_held' field.
        """
        if not self.market_data:
            log.error("Cannot parse, market_data has not been loaded.")
            return False

        if self.market_data.get('state') != 'open':
            title = self.market_data.get('title', 'Unknown')
            state = self.market_data.get('state', 'unknown')
            log.warning(f"Market '{title}' is not 'open' (current state: '{state}'). Calculations may not be relevant.")

        outcomes = self.market_data.get("outcomes", [])
        if len(outcomes) != 2:
            log.error(f"This tool only supports binary markets. Found {len(outcomes)} outcomes.")
            return False

        try:
            # Standard assumption: outcome with id=0 is "Yes", id=1 is "No".
            outcome1_data = next(o for o in outcomes if o['id'] == 0)
            outcome2_data = next(o for o in outcomes if o['id'] == 1)

            self.price1 = outcome1_data.get('price')
            self.outcome_titles[0] = outcome1_data.get('title', 'Outcome 1')

            self.price2 = outcome2_data.get('price')
            self.outcome_titles[1] = outcome2_data.get('title', 'Outcome 2')

            # The 'shares_held' field represents the number of shares in the liquidity pool for each outcome.
            self.q1 = outcome1_data.get('shares_held')
            self.q2 = outcome2_data.get('shares_held')

            if None in [self.q1, self.q2, self.price1, self.price2]:
                raise ValueError("A required field (shares_held, price) is missing in the API response.")

            log.info("Successfully parsed market state:")
            log.info(f"  - Q1 ({self.outcome_titles[0]}): {self.q1:.4f} (from shares_held)")
            log.info(f"  - Q2 ({self.outcome_titles[1]}): {self.q2:.4f} (from shares_held)")
            log.info(f"  - Price 1: {self.price1:.4f}")
            log.info(f"  - Price 2: {self.price2:.4f}")
            return True

        except (StopIteration, ValueError, KeyError) as e:
            log.error(f"Error while parsing outcomes: {e}")
            return False
            
    def load_b_parameter(self) -> bool:
        """
        Loads the B parameter (liquidity) directly from the market data.
        The correct approach is to use the static 'liquidity' key provided by the API.
        """
        if not self.market_data:
            log.error("Cannot load B parameter, market data not fetched.")
            return False
        
        liquidity = self.market_data.get('liquidity')
        if liquidity is not None and liquidity > 0:
            self.b_param = float(liquidity)
            log.info(f"Successfully loaded B parameter from API 'liquidity' key: {self.b_param:.4f}")
            return True
        else:
            log.error(f"Could not load B parameter. 'liquidity' key is missing, null, or zero in API response. Value: {liquidity}")
            self.b_param = None
            return False

    @staticmethod
    def _lmsr_cost(q1: float, q2: float, b: float) -> float:
        """Calculates the LMSR cost function C(q1, q2)."""
        if b <= 0:
            raise ValueError("B parameter must be positive.")
        return b * logsumexp([q1 / b, q2 / b])

    @staticmethod
    def _compute_price(q1: float, q2: float, b: float) -> Tuple[float, float]:
        """Computes the instantaneous prices of both outcomes."""
        if b <= 0:
            return 0.5, 0.5
        q1_b = q1 / b
        q2_b = q2 / b
        sum_exp = logsumexp([q1_b, q2_b])
        price1 = math.exp(q1_b - sum_exp)
        price2 = math.exp(q2_b - sum_exp)
        return price1, price2

    def calculate_trade_cost(self, outcome_index: int, shares_to_buy: float) -> Optional[Dict[str, Any]]:
        """
        Calculates the cost and price impact of buying a specific number of shares.

        Args:
            outcome_index (int): 0 for the first outcome (e.g., "Yes"), 1 for the second.
            shares_to_buy (float): The number of shares to simulate buying.

        Returns:
            A dictionary with trade details or None if calculation fails.
        """
        if self.b_param is None or self.q1 is None or self.q2 is None:
            log.error("Cannot calculate trade cost, B parameter is not set or shares are not loaded.")
            return None

        initial_cost = self._lmsr_cost(self.q1, self.q2, self.b_param)

        if outcome_index == 0:
            q1_final, q2_final = self.q1 + shares_to_buy, self.q2
            outcome_title = self.outcome_titles[0]
        elif outcome_index == 1:
            q1_final, q2_final = self.q1, self.q2 + shares_to_buy
            outcome_title = self.outcome_titles[1]
        else:
            log.error(f"Invalid outcome_index {outcome_index}. Must be 0 or 1.")
            return None
        
        final_cost = self._lmsr_cost(q1_final, q2_final, self.b_param)
        
        trade_cost = final_cost - initial_cost
        fee = trade_cost * FEE_RATE_BUY
        total_cost = trade_cost + fee
        
        new_price1, new_price2 = self._compute_price(q1_final, q2_final, self.b_param)
        
        return {
            "shares_bought": shares_to_buy,
            "outcome": outcome_title,
            "cost_before_fee": trade_cost,
            "fee": fee,
            "total_cost": total_cost,
            "avg_price_per_share": total_cost / shares_to_buy if shares_to_buy > 0 else 0,
            "new_price1": new_price1,
            "new_price2": new_price2,
        }

    def display_price_impact(self, outcome_index: int, amounts: List[float]):
        """Prints a table of price impacts for various trade sizes."""
        if self.b_param is None:
            log.error("Cannot display price impact, B parameter is not set.")
            return
            
        outcome_title = self.outcome_titles.get(outcome_index, f"Outcome {outcome_index}")
        print(f"\n--- Price Impact Analysis for Buying '{outcome_title}' Shares ---")
        header = f"{'Shares to Buy':<15} | {'Total Cost (USDC)':<20} | {'Avg Price':<12} | {'New P1 ({self.outcome_titles[0]})':<18} | {'New P2 ({self.outcome_titles[1]})':<18}"
        print(header)
        print("-" * len(header))
        
        for amount in amounts:
            result = self.calculate_trade_cost(outcome_index, amount)
            if result:
                print(
                    f"{result['shares_bought']:<15.2f} | "
                    f"${result['total_cost']:<19.4f} | "
                    f"{result['avg_price_per_share']:<12.4f} | "
                    f"{result['new_price1']:<18.4f} | "
                    f"{result['new_price2']:<18.4f}"
                )

# --- Main execution block for demonstration ---
if __name__ == "__main__":
    # The example market slug from the prompt
    market_slug_example = "vikings-vs-steelers"
    
    print(f"Analyzing Myriad Market: {market_slug_example}")
    analyzer = MyriadMarketAnalyzer(market_slug=market_slug_example)
    
    # Fetch, parse, and load parameters
    if analyzer.fetch_and_parse():
        if analyzer.load_b_parameter():
            # Sanity check: verify that our price calculation matches the API's price
            recalculated_p1, _ = analyzer._compute_price(analyzer.q1, analyzer.q2, analyzer.b_param)
            print("\n--- Parameter Verification ---")
            print(f"API Price ({analyzer.outcome_titles[0]}):      {analyzer.price1:.6f}")
            print(f"Recalculated Price:   {recalculated_p1:.6f}")
            print(f"Difference:           {abs(analyzer.price1 - recalculated_p1):.10f}\n")
            
            # Show price impact tables for buying each outcome
            amounts_to_check = [1, 10, 100, 500, 1000, 2000]
            analyzer.display_price_impact(outcome_index=0, amounts=amounts_to_check)
            analyzer.display_price_impact(outcome_index=1, amounts=amounts_to_check)
        else:
            log.error("Could not run analysis because B parameter could not be loaded.")
    else:
        log.error("Could not run analysis due to data fetching/parsing errors.")