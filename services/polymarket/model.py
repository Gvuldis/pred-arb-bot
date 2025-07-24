import math
import logging
from scipy.special import logsumexp
from typing import Optional, Tuple, Dict, Any, List

log = logging.getLogger(__name__)

# --- Helper Functions ---

def infer_b(q_yes: float, q_no: float, price_yes: float) -> float:
    """
    Infers the B parameter for a Bodega market from its current state.
    """
    log.info(f"DBG: Attempting to infer B with q_yes={q_yes}, q_no={q_no}, price_yes={price_yes}")
    if not (0.0 < price_yes < 1.0):
        log.warning(f"infer_b check failed: price_yes ({price_yes}) is not strictly between 0 and 1.")
        raise ValueError("price_yes must be strictly between 0 and 1.")

    diff = q_yes - q_no
    if diff == 0:
        log.warning("infer_b check failed: q_yes equals q_no, so B is indeterminate.")
        raise ValueError("q_yes equals q_no, B is indeterminate.")

    try:
        log_argument = price_yes / (1.0 - price_yes)
        denominator = math.log(log_argument)
        if denominator == 0:
            log.warning(f"infer_b check failed: Denominator is zero. This implies price is 0.5, but q_yes != q_no. (q_yes={q_yes}, q_no={q_no})")
            raise ValueError("Cannot divide by zero; price implies B is indeterminate but shares do not match.")
        return diff / denominator
    except ValueError as e:
        log.error(f"infer_b failed with a math domain error for price_yes={price_yes}. This is unexpected. Error: {e}", exc_info=True)
        raise

def compute_price(qy: float, qn: float, b: float) -> float:
    """Stable LMSR instantaneous price."""
    if b == 0: return 1.0 if qy > qn else 0.0 if qn > qy else 0.5
    qy_b, qn_b = qy / b, qn / b
    return math.exp(qy_b - logsumexp([qy_b, qn_b]))

def lmsr_cost(qy: float, qn: float, b: float) -> float:
    """LMSR cost function."""
    if b == 0: return max(qy, qn)
    return b * logsumexp([qy/b, qn/b])

def solve_x_for_price(q1: float, q2: float, p_tgt: float, b: float) -> Optional[float]:
    """Solve x so that compute_price(q1 + x, q2, b) == p_tgt."""
    if not (0 < p_tgt < 1): return None
    try: lr = math.log(p_tgt / (1 - p_tgt))
    except ValueError: return None
    return b * lr + q2 - q1

def consume_order_book(ob: List[Tuple[float, int]], qty: int) -> Tuple[int, float, float]:
    """Calculates the cost of buying a certain quantity from an order book."""
    bought = cost = 0.0
    if not ob or qty <= 0: return 0, 0.0, 0.0
    for price, avail in ob:
        take = min(qty - bought, avail)
        cost += take * price
        bought += take
        if bought >= qty: break
    avg_price = cost / bought if bought else 0.0
    return int(round(bought)), cost, avg_price

# --- Main function ---
def build_arbitrage_table(
    Q_YES: float, Q_NO: float,
    ORDER_BOOK_YES: List[Tuple[float, int]], ORDER_BOOK_NO: List[Tuple[float, int]],
    ADA_TO_USD: float, FEE_RATE: float, B: float,
) -> List[Dict[str, Any]]:
    """
    Calculates arbitrage metrics for a given market pair in both directions,
    regardless of profitability.
    """
    if ADA_TO_USD == 0:
        return []

    opportunities = []
    initial_cost_bod_ada = lmsr_cost(Q_YES, Q_NO, B)

    # --- Scenario 1: Buy YES on Bodega, hedge with NO on Polymarket ---
    if ORDER_BOOK_NO:
        p_bod_yes_start = compute_price(Q_YES, Q_NO, B)
        p_poly_no_best_ask = ORDER_BOOK_NO[0][0]
        implied_poly_yes_price = 1 - p_poly_no_best_ask

        x_opt_raw = solve_x_for_price(Q_YES, Q_NO, implied_poly_yes_price, B)
        
        # If there's a profitable opportunity, use the optimal size.
        # Otherwise, calculate for a hypothetical 1 share trade to show the negative profit.
        if x_opt_raw and x_opt_raw > 1:
            x_bod = int(round(x_opt_raw))
        else:
            x_bod = 1 # Use 1 share for non-profitable or tiny opportunities

        cost_bod_ada = lmsr_cost(Q_YES + x_bod, Q_NO, B) - initial_cost_bod_ada
        fee_bod_ada = cost_bod_ada * FEE_RATE
        p_bod_yes_end = compute_price(Q_YES + x_bod, Q_NO, B)
        
        poly_shares_to_buy = int(round(x_bod * ADA_TO_USD))
        filled_poly, cost_poly_usd, avg_poly_price = consume_order_book(ORDER_BOOK_NO, poly_shares_to_buy)
        
        cost_poly_ada = cost_poly_usd / ADA_TO_USD if ADA_TO_USD > 0 else 0
        comb_ada = cost_bod_ada + fee_bod_ada + cost_poly_ada
        comb_usd = comb_ada * ADA_TO_USD
        
        profit_ada = x_bod - comb_ada
        profit_usd = profit_ada * ADA_TO_USD
        fill_status = filled_poly >= poly_shares_to_buy
        
        opp = {
            "direction": "BUY_YES_BODEGA", "bodega_side": "YES", "polymarket_side": "NO",
            "p_start": p_bod_yes_start, "p_end": p_bod_yes_end,
            "bodega_shares": x_bod, "cost_bod_ada": cost_bod_ada, "fee_bod_ada": fee_bod_ada,
            "polymarket_shares": filled_poly, "cost_poly_usd": cost_poly_usd, "cost_poly_ada": cost_poly_ada,
            "avg_poly_price": avg_poly_price,
            "comb_ada": comb_ada, "comb_usd": comb_usd,
            "profit_ada": profit_ada, "profit_usd": profit_usd,
            "roi": profit_usd / comb_usd if comb_usd > 0 else 0,
            "fill": fill_status,
            "inferred_B": B, "ada_usd_rate": ADA_TO_USD,
        }
        opportunities.append(opp)

    # --- Scenario 2: Buy NO on Bodega, hedge with YES on Polymarket ---
    if ORDER_BOOK_YES:
        p_bod_no_start = 1 - compute_price(Q_YES, Q_NO, B)
        p_poly_yes_best_ask = ORDER_BOOK_YES[0][0]
        implied_poly_no_price = 1 - p_poly_yes_best_ask

        x_opt_raw = solve_x_for_price(Q_NO, Q_YES, implied_poly_no_price, B)

        if x_opt_raw and x_opt_raw > 1:
            x_bod = int(round(x_opt_raw))
        else:
            x_bod = 1

        cost_bod_ada = lmsr_cost(Q_YES, Q_NO + x_bod, B) - initial_cost_bod_ada
        fee_bod_ada = cost_bod_ada * FEE_RATE
        p_bod_no_end = 1 - compute_price(Q_YES, Q_NO + x_bod, B)
        
        poly_shares_to_buy = int(round(x_bod * ADA_TO_USD))
        filled_poly, cost_poly_usd, avg_poly_price = consume_order_book(ORDER_BOOK_YES, poly_shares_to_buy)
        
        cost_poly_ada = cost_poly_usd / ADA_TO_USD if ADA_TO_USD > 0 else 0
        comb_ada = cost_bod_ada + fee_bod_ada + cost_poly_ada
        comb_usd = comb_ada * ADA_TO_USD
        profit_ada = x_bod - comb_ada
        profit_usd = profit_ada * ADA_TO_USD
        fill_status = filled_poly >= poly_shares_to_buy
        
        opp = {
            "direction": "BUY_NO_BODEGA", "bodega_side": "NO", "polymarket_side": "YES",
            "p_start": p_bod_no_start, "p_end": p_bod_no_end,
            "bodega_shares": x_bod, "cost_bod_ada": cost_bod_ada, "fee_bod_ada": fee_bod_ada,
            "polymarket_shares": filled_poly, "cost_poly_usd": cost_poly_usd, "cost_poly_ada": cost_poly_ada,
            "avg_poly_price": avg_poly_price,
            "comb_ada": comb_ada, "comb_usd": comb_usd,
            "profit_ada": profit_ada, "profit_usd": profit_usd,
            "roi": profit_usd / comb_usd if comb_usd > 0 else 0,
            "fill": fill_status,
            "inferred_B": B, "ada_usd_rate": ADA_TO_USD,
        }
        opportunities.append(opp)

    return opportunities