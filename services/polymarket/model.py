# services/polymarket/model.py
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
    # If price is exactly 0.5, log(1) is 0, which causes division by zero.
    # This should only happen if q_yes == q_no.
    if abs(price_yes - 0.5) < 1e-9:
        if abs(diff) > 1e-9:
            log.warning(f"infer_b check failed: Price is ~0.5, but shares are not equal (q_yes={q_yes}, q_no={q_no}). B is indeterminate.")
            raise ValueError("Price is 0.5 but shares are not equal, B is indeterminate.")
        else:
            # If price is 0.5 and shares are equal, B cannot be determined.
            log.warning("infer_b check failed: q_yes equals q_no and price is 0.5, so B is indeterminate.")
            raise ValueError("q_yes equals q_no and price is 0.5, B is indeterminate.")

    try:
        log_argument = price_yes / (1.0 - price_yes)
        denominator = math.log(log_argument)
        if denominator == 0:
            # This case is handled above, but as a safeguard:
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

def _calculate_trade_outcome(
    q1_bod: float, q2_bod: float, b: float,
    order_book_poly: List[Tuple[float, int]],
    ada_to_usd: float, fee_rate: float,
    initial_cost_bod_ada: float,
    target_bodega_price: float
) -> Optional[Dict[str, Any]]:
    """
    Calculates the outcome of a single arbitrage trade for a given target price.
    q1_bod is the quantity of the asset we are buying on Bodega.
    q2_bod is the quantity of the other asset on Bodega.
    """
    if not (0 < target_bodega_price < 1):
        return None

    x_bod_raw = solve_x_for_price(q1_bod, q2_bod, target_bodega_price, b)
    
    if not x_bod_raw or x_bod_raw <= 0:
        return None
        
    x_bod = int(round(x_bod_raw))
    if x_bod <= 0:
        return None

    cost_bod_ada = lmsr_cost(q1_bod + x_bod, q2_bod, b) - initial_cost_bod_ada
    fee_bod_ada = cost_bod_ada * fee_rate
    
    poly_shares_to_buy = int(round(x_bod * ada_to_usd))
    if poly_shares_to_buy <= 0:
        return None
        
    filled_poly, cost_poly_usd, avg_poly_price = consume_order_book(order_book_poly, poly_shares_to_buy)
    
    if filled_poly == 0: # Can't hedge at all
        return None

    cost_poly_ada = cost_poly_usd / ada_to_usd if ada_to_usd > 0 else 0
    comb_ada = cost_bod_ada + fee_bod_ada + cost_poly_ada
    comb_usd = comb_ada * ada_to_usd
    
    # Profit is based on the number of Bodega shares we intended to buy.
    # The `fill_status` will indicate if the hedge was incomplete.
    profit_ada = x_bod - comb_ada
    profit_usd = profit_ada * ada_to_usd
    
    fill_status = filled_poly >= poly_shares_to_buy

    return {
        "bodega_shares": x_bod,
        "cost_bod_ada": cost_bod_ada,
        "fee_bod_ada": fee_bod_ada,
        "polymarket_shares": filled_poly,
        "cost_poly_usd": cost_poly_usd,
        "cost_poly_ada": cost_poly_ada,
        "avg_poly_price": avg_poly_price,
        "comb_ada": comb_ada,
        "comb_usd": comb_usd,
        "profit_ada": profit_ada,
        "profit_usd": profit_usd,
        "roi": profit_usd / comb_usd if comb_usd > 0 else 0,
        "fill": fill_status,
        "p_end": compute_price(q1_bod + x_bod, q2_bod, b)
    }

def _calculate_trade_outcome_fixed_shares(
    q1_bod: float, q2_bod: float, b: float,
    order_book_poly: List[Tuple[float, int]],
    ada_to_usd: float, fee_rate: float,
    initial_cost_bod_ada: float,
    shares_to_buy_bodega: int
) -> Optional[Dict[str, Any]]:
    """
    Calculates the outcome of an arbitrage trade for a fixed number of shares.
    """
    x_bod = shares_to_buy_bodega
    if x_bod <= 0:
        return None

    cost_bod_ada = lmsr_cost(q1_bod + x_bod, q2_bod, b) - initial_cost_bod_ada
    fee_bod_ada = cost_bod_ada * fee_rate
    
    poly_shares_to_buy = int(round(x_bod * ada_to_usd))
    if poly_shares_to_buy <= 0:
        poly_shares_to_buy = 1 # At least try to buy 1 to show a valid hedge cost
        
    filled_poly, cost_poly_usd, avg_poly_price = consume_order_book(order_book_poly, poly_shares_to_buy)
    
    cost_poly_ada = cost_poly_usd / ada_to_usd if ada_to_usd > 0 else 0
    comb_ada = cost_bod_ada + fee_bod_ada + cost_poly_ada
    comb_usd = comb_ada * ada_to_usd
    
    profit_ada = x_bod - comb_ada
    profit_usd = profit_ada * ada_to_usd
    
    fill_status = filled_poly >= poly_shares_to_buy if poly_shares_to_buy > 0 else True

    return {
        "bodega_shares": x_bod,
        "cost_bod_ada": cost_bod_ada,
        "fee_bod_ada": fee_bod_ada,
        "polymarket_shares": filled_poly,
        "cost_poly_usd": cost_poly_usd,
        "cost_poly_ada": cost_poly_ada,
        "avg_poly_price": avg_poly_price,
        "comb_ada": comb_ada,
        "comb_usd": comb_usd,
        "profit_ada": profit_ada,
        "profit_usd": profit_usd,
        "roi": profit_usd / comb_usd if comb_usd > 0 else 0,
        "fill": fill_status,
        "p_end": compute_price(q1_bod + x_bod, q2_bod, b)
    }

# --- Main function ---
def build_arbitrage_table(
    Q_YES: float, Q_NO: float,
    ORDER_BOOK_YES: List[Tuple[float, int]], ORDER_BOOK_NO: List[Tuple[float, int]],
    ADA_TO_USD: float, FEE_RATE: float, B: float,
) -> List[Dict[str, Any]]:
    """
    Calculates arbitrage opportunities by testing various price targets.
    - If a profitable trade is found, it returns the most profitable one.
    - If no profitable trade is found, it calculates the profit/loss for a
      hypothetical 1-share trade to show the current market state.
    """
    if ADA_TO_USD == 0:
        return []

    all_opportunities = []
    initial_cost_bod_ada = lmsr_cost(Q_YES, Q_NO, B)
    
    price_adjustments = [i / 100.0 for i in range(0, 26)]

    # --- Scenario 1: Buy YES on Bodega, hedge with NO on Polymarket ---
    if ORDER_BOOK_NO:
        p_bod_yes_start = compute_price(Q_YES, Q_NO, B)
        p_poly_no_best_ask = ORDER_BOOK_NO[0][0]
        implied_poly_yes_price = 1 - p_poly_no_best_ask
        
        scenario_1_outcomes = []
        log.info(f"--- Analyzing BUY YES Bodega (vs Poly NO price of {p_poly_no_best_ask:.4f}, implied YES price {implied_poly_yes_price:.4f}) ---")
        
        for adj in price_adjustments:
            target_bodega_price = implied_poly_yes_price - adj
            outcome = _calculate_trade_outcome(
                q1_bod=Q_YES, q2_bod=Q_NO, b=B,
                order_book_poly=ORDER_BOOK_NO,
                ada_to_usd=ADA_TO_USD, fee_rate=FEE_RATE,
                initial_cost_bod_ada=initial_cost_bod_ada,
                target_bodega_price=target_bodega_price
            )
            if outcome:
                outcome['adjustment'] = adj
                scenario_1_outcomes.append(outcome)

        best_outcome = max(scenario_1_outcomes, key=lambda x: x['profit_usd']) if scenario_1_outcomes else None
        
        final_outcome = None
        analysis_details = []

        if best_outcome and best_outcome['profit_usd'] > 0:
            log.info(f"--> Best for BUY YES Bodega is at adjustment {best_outcome['adjustment']:.2f} with profit ${best_outcome['profit_usd']:.2f}")
            final_outcome = best_outcome
            analysis_details = scenario_1_outcomes
        else:
            log.info("No profitable arbitrage for BUY YES Bodega, calculating loss for 1 share.")
            one_share_outcome = _calculate_trade_outcome_fixed_shares(
                q1_bod=Q_YES, q2_bod=Q_NO, b=B,
                order_book_poly=ORDER_BOOK_NO,
                ada_to_usd=ADA_TO_USD, fee_rate=FEE_RATE,
                initial_cost_bod_ada=initial_cost_bod_ada,
                shares_to_buy_bodega=1
            )
            final_outcome = one_share_outcome
        
        if final_outcome:
            opp = {
                "direction": "BUY_YES_BODEGA", "bodega_side": "YES", "polymarket_side": "NO",
                "p_start": p_bod_yes_start,
                "inferred_B": B, "ada_usd_rate": ADA_TO_USD,
            }
            opp.update(final_outcome)
            opp['analysis_details'] = analysis_details
            all_opportunities.append(opp)

    # --- Scenario 2: Buy NO on Bodega, hedge with YES on Polymarket ---
    if ORDER_BOOK_YES:
        p_bod_no_start = 1 - compute_price(Q_YES, Q_NO, B)
        p_poly_yes_best_ask = ORDER_BOOK_YES[0][0]
        implied_poly_no_price = 1 - p_poly_yes_best_ask
        
        scenario_2_outcomes = []
        log.info(f"--- Analyzing BUY NO Bodega (vs Poly YES price of {p_poly_yes_best_ask:.4f}, implied NO price {implied_poly_no_price:.4f}) ---")

        for adj in price_adjustments:
            target_bodega_price = implied_poly_no_price - adj
            outcome = _calculate_trade_outcome(
                q1_bod=Q_NO, q2_bod=Q_YES, b=B,
                order_book_poly=ORDER_BOOK_YES,
                ada_to_usd=ADA_TO_USD, fee_rate=FEE_RATE,
                initial_cost_bod_ada=initial_cost_bod_ada,
                target_bodega_price=target_bodega_price
            )
            if outcome:
                outcome['adjustment'] = adj
                scenario_2_outcomes.append(outcome)

        best_outcome = max(scenario_2_outcomes, key=lambda x: x['profit_usd']) if scenario_2_outcomes else None
        
        final_outcome = None
        analysis_details = []

        if best_outcome and best_outcome['profit_usd'] > 0:
            log.info(f"--> Best for BUY NO Bodega is at adjustment {best_outcome['adjustment']:.2f} with profit ${best_outcome['profit_usd']:.2f}")
            final_outcome = best_outcome
            analysis_details = scenario_2_outcomes
        else:
            log.info("No profitable arbitrage for BUY NO Bodega, calculating loss for 1 share.")
            one_share_outcome = _calculate_trade_outcome_fixed_shares(
                q1_bod=Q_NO, q2_bod=Q_YES, b=B,
                order_book_poly=ORDER_BOOK_YES,
                ada_to_usd=ADA_TO_USD, fee_rate=FEE_RATE,
                initial_cost_bod_ada=initial_cost_bod_ada,
                shares_to_buy_bodega=1
            )
            final_outcome = one_share_outcome

        if final_outcome:
            opp = {
                "direction": "BUY_NO_BODEGA", "bodega_side": "NO", "polymarket_side": "YES",
                "p_start": p_bod_no_start,
                "inferred_B": B, "ada_usd_rate": ADA_TO_USD,
            }
            opp.update(final_outcome)
            opp['analysis_details'] = analysis_details
            all_opportunities.append(opp)

    return all_opportunities