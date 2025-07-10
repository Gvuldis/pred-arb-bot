import math
import logging
from scipy.special import logsumexp
from typing import Optional, Tuple, Dict, Any

log = logging.getLogger(__name__)

# --- LMSR helpers ---
def compute_price(qy: float, qn: float, b: float) -> float:
    """Stable LMSR instantaneous price."""
    qy_b, qn_b = qy / b, qn / b
    m = max(qy_b, qn_b)
    e1 = math.exp(qy_b - m)
    e2 = math.exp(qn_b - m)
    return e1 / (e1 + e2)

def lmsr_cost(qy: float, qn: float, b: float) -> float:
    """LMSR cost function."""
    if b == 0:
        return max(qy, qn)
    return b * logsumexp([qy/b, qn/b])

def solve_x_for_price(q1: float, q2: float, p_tgt: float, b: float) -> Optional[float]:
    """
    Solve x so that compute_price(q1 + x, q2, b) == p_tgt.
    x = b*log(p_tgt/(1-p_tgt)) + q2 - q1
    """
    if not (0 < p_tgt < 1):
        return None
    lr = math.log(p_tgt / (1 - p_tgt))
    return b * lr + q2 - q1

# --- Main function ---
def build_arbitrage_table(
    Q_YES: float,
    Q_NO: float,
    P_POLY_YES: float,
    P_POLY_NO: float,
    ADA_TO_USD: float,
    FEE_RATE: float,
    B: float,
) -> Tuple[Optional[int], Dict[str, Any], None]:
    """
    Finds the best arbitrage between Bodega (ADA) and Polymarket (ADA-priced).
    Shares on Polymarket are scaled by 1/ADA_TO_USD to equalize notional.
    Returns (optimal_ada_shares, summary_dict, None).
    """
    # Current Bodega prices
    p_bod_yes = compute_price(Q_YES, Q_NO, B)
    p_bod_no  = 1 - p_bod_yes

    best: Dict[str, Any] = {"profit_usd": -1e18}
    initial_cost = lmsr_cost(Q_YES, Q_NO, B)

    # Direction 1: buy YES at Bodega, NO at Polymarket
    p_tgt1 = P_POLY_YES
    x1 = solve_x_for_price(Q_YES, Q_NO, p_tgt1, B)
    if x1 and x1 > 0:
        x_bod = x1
        x_poly = x_bod / ADA_TO_USD  # scale shares
        cost_bod_ada = lmsr_cost(Q_YES + x_bod, Q_NO, B) - initial_cost
        fee_bod_ada  = cost_bod_ada * FEE_RATE
        cost_poly_ada= x_poly * P_POLY_NO
        combined_ada = cost_bod_ada + cost_poly_ada
        combined_usd = combined_ada * ADA_TO_USD
        profit_ada   = x_bod - combined_ada
        profit_usd   = profit_ada * ADA_TO_USD
        roi          = profit_usd / (combined_usd) if combined_usd else 0
        opp = {
            "direction": "BUY_YES_BODEGA",
            "bodega_shares": int(round(x_bod)),
            "polymarket_shares": int(round(x_poly)),
            "profit_usd": profit_usd,
            "roi": roi,
            "cost_ada": combined_ada,
            "cost_usd": combined_usd,
        }
        if profit_usd > best["profit_usd"]:
            best = opp

    # Direction 2: buy NO at Bodega, YES at Polymarket
    p_tgt2 = P_POLY_NO
    x2 = solve_x_for_price(Q_NO, Q_YES, p_tgt2, B)
    if x2 and x2 > 0:
        x_bod = x2
        x_poly = x_bod / ADA_TO_USD
        cost_bod_ada = lmsr_cost(Q_YES, Q_NO + x_bod, B) - initial_cost
        fee_bod_ada  = cost_bod_ada * FEE_RATE
        cost_poly_ada= x_poly * P_POLY_YES
        combined_ada = cost_bod_ada + cost_poly_ada
        combined_usd = combined_ada * ADA_TO_USD
        profit_ada   = x_bod - combined_ada
        profit_usd   = profit_ada * ADA_TO_USD
        roi          = profit_usd / combined_usd if combined_usd else 0
        opp = {
            "direction": "BUY_NO_BODEGA",
            "bodega_shares": int(round(x_bod)),
            "polymarket_shares": int(round(x_poly)),
            "profit_usd": profit_usd,
            "roi": roi,
            "cost_ada": combined_ada,
            "cost_usd": combined_usd,
        }
        if profit_usd > best["profit_usd"]:
            best = opp

    if best["profit_usd"] < 0:
        # no profitable opportunity
        return None, {"direction": "NONE", "profit_usd": 0.0, "roi": 0.0}, None

    x_star = best["bodega_shares"]
    return x_star, best, None