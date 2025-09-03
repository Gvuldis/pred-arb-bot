# services/myriad/model.py
import math
import logging
from scipy.special import logsumexp
from typing import Optional, Tuple, Dict, Any, List

log = logging.getLogger(__name__)

def infer_b(q1: float, q2: float, price1: float) -> float:
    """
    Infers the B parameter (liquidity) of the Myriad LMSR market maker.
    """
    if not (0.0 < price1 < 1.0):
        raise ValueError("price1 must be strictly between 0 and 1.")

    if abs(price1 - 0.5) < 1e-9:
        if abs(q1 - q2) > 1e-9:
            raise ValueError("Price is ~0.5, but shares are not equal. B is indeterminate.")
        else:
            raise ValueError("Price is 0.5 and shares are equal, B is indeterminate.")
            
    price2 = 1.0 - price1
    if price2 <= 0:
        raise ValueError("Calculated price2 is invalid.")

    diff = q1 - q2
    log_ratio = math.log(price1 / price2)
    
    if abs(log_ratio) < 1e-9:
         raise ValueError("Log ratio is too close to zero, B is indeterminate.")
         
    return diff / log_ratio

def compute_price(q1: float, q2: float, b: float) -> Tuple[float, float]:
    """Stable LMSR instantaneous price calculation."""
    if b <= 0: return 0.5, 0.5
    q1_b, q2_b = q1 / b, q2 / b
    sum_exp = logsumexp([q1_b, q2_b])
    price1 = math.exp(q1_b - sum_exp)
    price2 = math.exp(q2_b - sum_exp)
    return price1, price2

def lmsr_cost(q1: float, q2: float, b: float) -> float:
    """LMSR cost function."""
    if b <= 0: raise ValueError("B parameter must be positive.")
    return b * logsumexp([q1/b, q2/b])

def solve_x_for_price(q1: float, q2: float, p_tgt: float, b: float) -> Optional[float]:
    """Solve for x such that compute_price(q1 + x, q2, b) == p_tgt."""
    if not (0 < p_tgt < 1): return None
    try:
        lr = math.log(p_tgt / (1 - p_tgt))
    except ValueError:
        return None
    return b * lr + q2 - q1

def consume_order_book(ob: List[Tuple[float, int]], qty: int) -> Tuple[int, float, float]:
    """Calculates the cost of buying a certain quantity from a Polymarket order book."""
    bought = cost = 0.0
    if not ob or qty <= 0: return 0, 0.0, 0.0
    for price, avail in ob:
        take = min(qty - bought, avail)
        cost += take * price
        bought += take
        if bought >= qty: break
    avg_price = cost / bought if bought else 0.0
    return int(round(bought)), cost, avg_price

def _calculate_trade_outcome_myriad(
    q1_myr: float, q2_myr: float, b: float,
    order_book_poly: List[Tuple[float, int]],
    fee_rate: float,
    initial_cost_myr_usd: float,
    target_myriad_price: float
) -> Optional[Dict[str, Any]]:
    """
    Calculates the outcome of a single arbitrage trade for a given target price.
    All calculations are in USDC.
    """
    if not (0 < target_myriad_price < 1):
        return None

    shares_to_buy_myr_raw = solve_x_for_price(q1_myr, q2_myr, target_myriad_price, b)
    if not shares_to_buy_myr_raw or shares_to_buy_myr_raw <= 0:
        return None
        
    shares_to_buy_myr = int(round(shares_to_buy_myr_raw))
    if shares_to_buy_myr <= 0:
        return None

    cost_myr_pre_fee = lmsr_cost(q1_myr + shares_to_buy_myr, q2_myr, b) - initial_cost_myr_usd
    fee_myr_usd = cost_myr_pre_fee * fee_rate
    total_cost_myr_usd = cost_myr_pre_fee + fee_myr_usd
    
    poly_shares_to_buy = shares_to_buy_myr
    filled_poly, cost_poly_usd, avg_poly_price = consume_order_book(order_book_poly, poly_shares_to_buy)
    
    if filled_poly == 0:
        return None

    total_cost_usd = total_cost_myr_usd + cost_poly_usd
    
    # Payout is simply the number of shares, as they are worth $1 on resolution
    payout_usd = shares_to_buy_myr
    profit_usd = payout_usd - total_cost_usd
    
    return {
        "myriad_shares": shares_to_buy_myr,
        "cost_myr_usd": total_cost_myr_usd,
        "fee_myr_usd": fee_myr_usd,
        "polymarket_shares": filled_poly,
        "cost_poly_usd": cost_poly_usd,
        "avg_poly_price": avg_poly_price,
        "total_cost_usd": total_cost_usd,
        "profit_usd": profit_usd,
        "roi": profit_usd / total_cost_usd if total_cost_usd > 0 else 0,
        "fill": filled_poly >= poly_shares_to_buy,
        "p_end": compute_price(q1_myr + shares_to_buy_myr, q2_myr, b)[0]
    }

def build_arbitrage_table_myriad(
    Q1_MYR: float, Q2_MYR: float,
    ORDER_BOOK_POLY_1: List[Tuple[float, int]], ORDER_BOOK_POLY_2: List[Tuple[float, int]],
    FEE_RATE: float, B: float,
) -> List[Dict[str, Any]]:
    all_opportunities = []
    initial_cost_myr_usd = lmsr_cost(Q1_MYR, Q2_MYR, B)
    price_adjustments = [i / 100.0 for i in range(0, 26)]

    # --- Scenario 1: Buy Outcome 1 on Myriad, hedge with Outcome 2 on Polymarket ---
    if ORDER_BOOK_POLY_2:
        p_myr_1_start = compute_price(Q1_MYR, Q2_MYR, B)[0]
        p_poly_2_best_ask = ORDER_BOOK_POLY_2[0][0]
        implied_poly_1_price = 1 - p_poly_2_best_ask
        
        scenario_1_outcomes = []
        for adj in price_adjustments:
            target_price = implied_poly_1_price - adj
            outcome = _calculate_trade_outcome_myriad(
                q1_myr=Q1_MYR, q2_myr=Q2_MYR, b=B,
                order_book_poly=ORDER_BOOK_POLY_2,
                fee_rate=FEE_RATE,
                initial_cost_myr_usd=initial_cost_myr_usd,
                target_myriad_price=target_price
            )
            if outcome:
                outcome['adjustment'] = adj
                scenario_1_outcomes.append(outcome)

        if scenario_1_outcomes:
            best_outcome = max(scenario_1_outcomes, key=lambda x: x['profit_usd'])
            if best_outcome['profit_usd'] > 0:
                opp = {"direction": "BUY_1_MYRIAD", "myriad_side": 1, "polymarket_side": 2, "p_start": p_myr_1_start, "inferred_B": B}
                opp.update(best_outcome)
                opp['analysis_details'] = scenario_1_outcomes
                all_opportunities.append(opp)

    # --- Scenario 2: Buy Outcome 2 on Myriad, hedge with Outcome 1 on Polymarket ---
    if ORDER_BOOK_POLY_1:
        p_myr_2_start = compute_price(Q1_MYR, Q2_MYR, B)[1]
        p_poly_1_best_ask = ORDER_BOOK_POLY_1[0][0]
        implied_poly_2_price = 1 - p_poly_1_best_ask
        
        scenario_2_outcomes = []
        for adj in price_adjustments:
            target_price = implied_poly_2_price - adj
            outcome = _calculate_trade_outcome_myriad(
                q1_myr=Q2_MYR, q2_myr=Q1_MYR, b=B, # Flipped Qs for calculation
                order_book_poly=ORDER_BOOK_POLY_1,
                fee_rate=FEE_RATE,
                initial_cost_myr_usd=initial_cost_myr_usd,
                target_myriad_price=target_price
            )
            if outcome:
                outcome['adjustment'] = adj
                scenario_2_outcomes.append(outcome)
        
        if scenario_2_outcomes:
            best_outcome = max(scenario_2_outcomes, key=lambda x: x['profit_usd'])
            if best_outcome['profit_usd'] > 0:
                opp = {"direction": "BUY_2_MYRIAD", "myriad_side": 2, "polymarket_side": 1, "p_start": p_myr_2_start, "inferred_B": B}
                opp.update(best_outcome)
                opp['analysis_details'] = scenario_2_outcomes
                all_opportunities.append(opp)
                
    return all_opportunities