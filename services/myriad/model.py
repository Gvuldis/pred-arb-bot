import math
import logging
from scipy.special import logsumexp
from typing import Optional, Tuple, Dict, Any, List

log = logging.getLogger(__name__)

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

def calculate_sell_revenue(q1_initial: float, q2_initial: float, b: float, shares_to_sell: float, fee_rate: float = 0.0) -> float:
    """Calculates the revenue from selling shares, including fees."""
    if shares_to_sell <= 0:
        return 0.0
        
    initial_pool_cost = lmsr_cost(q1_initial, q2_initial, b)
    final_pool_cost = lmsr_cost(q1_initial - shares_to_sell, q2_initial, b)
    revenue_pre_fee = initial_pool_cost - final_pool_cost
    return revenue_pre_fee * (1 - fee_rate)

def solve_shares_for_cost(
    q1_initial: float, q2_initial: float, b: float,
    max_cost: float, fee_rate: float,
    iterations: int = 30 # A binary search is very efficient
) -> float:
    """
    Calculates the maximum number of shares that can be bought for a given maximum cost.
    Uses a binary search algorithm to find the number of shares.
    """
    initial_pool_cost = lmsr_cost(q1_initial, q2_initial, b)
    
    # Define a helper function to calculate the total cost for a given number of shares
    def get_cost(shares_to_buy: float) -> float:
        if shares_to_buy <= 0:
            return 0.0
        cost_pre_fee = lmsr_cost(q1_initial + shares_to_buy, q2_initial, b) - initial_pool_cost
        return cost_pre_fee * (1 + fee_rate)

    # The price gives a hint for the search space. Max possible shares is very large.
    p1_start, _ = compute_price(q1_initial, q2_initial, b)
    avg_price_guess = (p1_start + 1.0) / 2.0
    high_guess = (max_cost / avg_price_guess) * 2 if avg_price_guess > 0 else max_cost * 4

    low_shares = 0.0
    high_shares = high_guess
    best_guess = 0.0

    # Binary search for the number of shares
    for _ in range(iterations):
        mid_shares = (low_shares + high_shares) / 2
        cost_at_mid = get_cost(mid_shares)

        if cost_at_mid <= max_cost:
            best_guess = mid_shares  # This is a valid number of shares
            low_shares = mid_shares
        else:
            high_shares = mid_shares
            
    return best_guess

def solve_x_for_price(q1: float, q2: float, p_tgt: float, b: float) -> Optional[float]:
    """Solve for x such that compute_price(q1 + x, q2, b) == p_tgt."""
    if not (0 < p_tgt < 1): return None
    try: lr = math.log(p_tgt / (1 - p_tgt))
    except ValueError: return None
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
    
    # --- FIXED PROFIT CALCULATION ---
    # Calculates profit based on the minimum guaranteed payout, assuming one leg must win.
    payout_myr_wins_usd = shares_to_buy_myr * 1.0
    payout_poly_wins_usd = filled_poly * 1.0
    profit_usd = min(payout_myr_wins_usd, payout_poly_wins_usd) - total_cost_usd
    # --- END FIX ---
    
    roi = profit_usd / total_cost_usd if total_cost_usd > 0 else 0
    score = profit_usd if profit_usd < 0 else roi * profit_usd
    
    return {
        "myriad_shares": shares_to_buy_myr,
        "cost_myr_usd": total_cost_myr_usd,
        "fee_myr_usd": fee_myr_usd,
        "polymarket_shares": filled_poly,
        "cost_poly_usd": cost_poly_usd,
        "avg_poly_price": avg_poly_price,
        "total_cost_usd": total_cost_usd,
        "profit_usd": profit_usd,
        "roi": roi,
        "score": score,
        "fill": filled_poly >= poly_shares_to_buy,
        "p_end": compute_price(q1_myr + shares_to_buy_myr, q2_myr, b)[0]
    }

def _calculate_trade_outcome_myriad_fixed_shares(
    q1_myr: float, q2_myr: float, b: float,
    order_book_poly: List[Tuple[float, int]],
    fee_rate: float,
    initial_cost_myr_usd: float,
    shares_to_buy_myriad: int
) -> Optional[Dict[str, Any]]:
    """
    Calculates the outcome of an arbitrage trade for a fixed number of shares.
    """
    if shares_to_buy_myriad <= 0:
        return None

    cost_myr_pre_fee = lmsr_cost(q1_myr + shares_to_buy_myriad, q2_myr, b) - initial_cost_myr_usd
    fee_myr_usd = cost_myr_pre_fee * fee_rate
    total_cost_myr_usd = cost_myr_pre_fee + fee_myr_usd
    
    poly_shares_to_buy = shares_to_buy_myriad
    filled_poly, cost_poly_usd, avg_poly_price = consume_order_book(order_book_poly, poly_shares_to_buy)
    
    total_cost_usd = total_cost_myr_usd + cost_poly_usd
    
    # --- FIXED PROFIT CALCULATION ---
    payout_myr_wins_usd = shares_to_buy_myriad * 1.0
    payout_poly_wins_usd = filled_poly * 1.0
    profit_usd = min(payout_myr_wins_usd, payout_poly_wins_usd) - total_cost_usd
    # --- END FIX ---
    
    roi = profit_usd / total_cost_usd if total_cost_usd > 0 else 0
    score = profit_usd if profit_usd < 0 else roi * profit_usd
    
    return {
        "myriad_shares": shares_to_buy_myriad,
        "cost_myr_usd": total_cost_myr_usd,
        "fee_myr_usd": fee_myr_usd,
        "polymarket_shares": filled_poly,
        "cost_poly_usd": cost_poly_usd,
        "avg_poly_price": avg_poly_price,
        "total_cost_usd": total_cost_usd,
        "profit_usd": profit_usd,
        "roi": roi,
        "score": score,
        "fill": filled_poly >= poly_shares_to_buy,
        "p_end": compute_price(q1_myr + shares_to_buy_myriad, q2_myr, b)[0]
    }

def _iterative_search(calculation_func, **kwargs) -> Optional[Dict]:
    """
    Performs a memory-efficient two-stage iterative search for the best trade outcome.
    """
    best_outcome = None
    
    def update_best(outcome):
        nonlocal best_outcome
        if outcome:
            if best_outcome is None or outcome.get('score', -1e9) > best_outcome.get('score', -1e9):
                best_outcome = outcome

    # Stage 1: Coarse search
    coarse_adjustments = [i / 100.0 for i in range(0, 51)] # 0.00 to 0.50, step 0.01
    for adj in coarse_adjustments:
        outcome = calculation_func(target_adjustment=adj, **kwargs)
        update_best(outcome)
            
    # Stage 2: Fine search
    if best_outcome and best_outcome.get('score', -1) > 0:
        best_adj_coarse = best_outcome['adjustment']
        fine_start = max(0, best_adj_coarse - 0.01)
        # Generate 21 steps of 0.001 around the best coarse adjustment
        fine_adjustments = [fine_start + i * 0.001 for i in range(21)]
        
        for adj in fine_adjustments:
            outcome = calculation_func(target_adjustment=adj, **kwargs)
            update_best(outcome)

    return best_outcome

def build_arbitrage_table_myriad(
    Q1_MYR: float, Q2_MYR: float,
    ORDER_BOOK_POLY_1: List[Tuple[float, int]], ORDER_BOOK_POLY_2: List[Tuple[float, int]],
    FEE_RATE: float, B: float, P1_MYR_REALTIME: float
) -> List[Dict[str, Any]]:
    """
    Calculates arbitrage opportunities using the real-time price for comparison.
    """
    all_opportunities = []
    initial_cost_myr_usd = lmsr_cost(Q1_MYR, Q2_MYR, B)

    # --- Scenario 1: Buy Outcome 1 on Myriad, hedge with Outcome 2 on Polymarket ---
    if ORDER_BOOK_POLY_2:
        p_myr_1_start = P1_MYR_REALTIME  # Use the real-time price for comparison
        p_poly_2_best_ask = ORDER_BOOK_POLY_2[0][0]
        implied_poly_1_price = 1 - p_poly_2_best_ask
        
        def calculate_scenario_1(target_adjustment, **kwargs):
            target_price = implied_poly_1_price - target_adjustment
            outcome = _calculate_trade_outcome_myriad(target_myriad_price=target_price, **kwargs)
            if outcome:
                outcome['adjustment'] = target_adjustment
            return outcome

        common_args = {
            'q1_myr': Q1_MYR, 'q2_myr': Q2_MYR, 'b': B,
            'order_book_poly': ORDER_BOOK_POLY_2, 'fee_rate': FEE_RATE,
            'initial_cost_myr_usd': initial_cost_myr_usd
        }
        
        best_outcome = _iterative_search(calculate_scenario_1, **common_args)
        
        final_outcome = None
        if best_outcome and best_outcome['profit_usd'] > 0:
            final_outcome = best_outcome
        else:
            final_outcome = _calculate_trade_outcome_myriad_fixed_shares(
                q1_myr=Q1_MYR, q2_myr=Q2_MYR, b=B,
                order_book_poly=ORDER_BOOK_POLY_2, fee_rate=FEE_RATE,
                initial_cost_myr_usd=initial_cost_myr_usd, shares_to_buy_myriad=1
            )
        
        if final_outcome:
            opp = {"direction": "BUY_1_MYRIAD", "myriad_side": 1, "polymarket_side": 2, "p_start": p_myr_1_start, "B": B}
            opp.update(final_outcome)
            all_opportunities.append(opp)

    # --- Scenario 2: Buy Outcome 2 on Myriad, hedge with Outcome 1 on Polymarket ---
    if ORDER_BOOK_POLY_1:
        p_myr_2_start = 1.0 - P1_MYR_REALTIME  # Use the real-time price for comparison
        p_poly_1_best_ask = ORDER_BOOK_POLY_1[0][0]
        implied_poly_2_price = 1 - p_poly_1_best_ask
        
        def calculate_scenario_2(target_adjustment, **kwargs):
            target_price = implied_poly_2_price - target_adjustment
            outcome = _calculate_trade_outcome_myriad(target_myriad_price=target_price, **kwargs)
            if outcome:
                outcome['adjustment'] = target_adjustment
            return outcome

        common_args_s2 = {
            'q1_myr': Q2_MYR, 'q2_myr': Q1_MYR, 'b': B, # Flipped Qs for calculation
            'order_book_poly': ORDER_BOOK_POLY_1, 'fee_rate': FEE_RATE,
            'initial_cost_myr_usd': initial_cost_myr_usd
        }

        best_outcome = _iterative_search(calculate_scenario_2, **common_args_s2)
        
        final_outcome = None
        if best_outcome and best_outcome['profit_usd'] > 0:
            final_outcome = best_outcome
        else:
            final_outcome = _calculate_trade_outcome_myriad_fixed_shares(
                q1_myr=Q2_MYR, q2_myr=Q1_MYR, b=B, # Flipped
                order_book_poly=ORDER_BOOK_POLY_1, fee_rate=FEE_RATE,
                initial_cost_myr_usd=initial_cost_myr_usd, shares_to_buy_myriad=1
            )

        if final_outcome:
            opp = {"direction": "BUY_2_MYRIAD", "myriad_side": 2, "polymarket_side": 1, "p_start": p_myr_2_start, "B": B}
            opp.update(final_outcome)
            all_opportunities.append(opp)
                
    return all_opportunities
