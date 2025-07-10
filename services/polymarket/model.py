import math
import numpy as np
import pandas as pd
from scipy.special import logsumexp
from typing import Tuple, Dict, Any
import logging

log = logging.getLogger(__name__)

def compute_price(q_yes: float, q_no: float, b: float) -> float:
    """LMSR instantaneous price: e^(q_yes/b) / (e^(q_yes/b) + e^(q_no/b))"""
    try:
        # Shift values to avoid overflow with large q values, which can happen in LMSR.
        q_yes_b = q_yes / b
        q_no_b = q_no / b
        max_q_b = max(q_yes_b, q_no_b)
        e_yes = math.exp(q_yes_b - max_q_b)
        e_no = math.exp(q_no_b - max_q_b)
        return e_yes / (e_yes + e_no)
    except (OverflowError, ZeroDivisionError):
        # Fallback for extreme values where one quantity is much larger.
        if q_yes > q_no: return 1.0
        if q_no > q_yes: return 0.0
        return 0.5

def lmsr_cost(q_yes: float, q_no: float, b: float) -> float:
    """LMSR cost function: b * logsumexp([q_yes/b, q_no/b])"""
    try:
        if b == 0: return max(q_yes, q_no)
        return b * logsumexp([q_yes / b, q_no / b])
    except (OverflowError, ZeroDivisionError):
        # If overflow, cost is essentially the larger of the two quantities.
        return max(q_yes, q_no)

def solve_x_for_price(q1: float, q2: float, p_target: float, b: float) -> float | None:
    """
    Solves for the number of shares 'x' to buy of outcome 1
    to move the instantaneous price to p_target.
    """
    log.info(f"  solve_x_for_price(q1={q1:.2f}, q2={q2:.2f}, p_target={p_target:.4f}, b={b})")
    if not (0 < p_target < 1):
        log.info(f"  -> p_target {p_target:.4f} is not between 0 and 1. Cannot solve.")
        return None
    try:
        # Derived formula: x = b * log(p_target / (1-p_target)) + q2 - q1
        if p_target <= 1e-9 or p_target >= 1.0 - 1e-9:
            log.info(f"  -> p_target {p_target:.4f} is too close to 0 or 1. Cannot solve.")
            return None

        log_price_ratio = math.log(p_target / (1.0 - p_target))
        x = b * log_price_ratio + q2 - q1
        log.info(f"  -> log_price_ratio={log_price_ratio:.4f}, calculated x={x:.4f}")

        if x > 1e-6:
            log.info(f"  -> x is positive, returning {x:.4f}")
            return x
        else:
            log.info(f"  -> x is not positive ({x:.4f}), returning None.")
            return None
    except (ValueError, OverflowError) as e:
        log.error(f"  -> solve_x_for_price encountered an error: {e}", exc_info=True)
        return None

def build_arbitrage_table(
    Q_YES: float,
    Q_NO: float,
    P_POLY_YES: float,
    P_POLY_NO: float,
    ADA_TO_USD: float,
    FEE_RATE: float,
    B: float,
) -> Tuple[float | None, Dict[str, Any], None]:
    """
    Checks for arbitrage opportunities by comparing Bodega (ADA) and Polymarket (USD).
    This involves creating a risk-free portfolio by buying opposite outcomes on each market,
    balancing the trade sizes based on the ADA/USD exchange rate.

    Returns:
    - Optimal Bodega shares to trade (or None if no opportunity).
    - A summary dictionary with the best opportunity (even if negative), profit, ROI, and trade details.
    - None (previously a DataFrame, now unused).
    """
    log.info("--- Starting Arbitrage Calculation ---")
    log.info(f"INPUTS: Q_YES={Q_YES:.2f}, Q_NO={Q_NO:.2f}, B={B}")
    log.info(f"        P_POLY_YES={P_POLY_YES}, P_POLY_NO={P_POLY_NO}")
    log.info(f"        ADA_TO_USD={ADA_TO_USD}, FEE_RATE={FEE_RATE}")
    
    initial_bodega_price = compute_price(Q_YES, Q_NO, B)
    log.info(f"Initial Bodega Price (YES): {initial_bodega_price:.4f}")


    if not all([P_POLY_YES, P_POLY_NO, ADA_TO_USD, B > 0]):
        log.warning("One or more essential inputs are missing or invalid. Aborting calculation.")
        return None, {"profit_usd": 0, "roi": 0, "direction": "N/A"}, None

    best_opp = {"profit_usd": -float('inf')}
    initial_cost = lmsr_cost(Q_YES, Q_NO, B)

    # --- DIRECTION 1: Buy YES on Bodega, Buy NO on Polymarket ---
    log.info("[DIR 1] Evaluating: BUY YES on Bodega, BUY NO on Polymarket")
    # We buy on Bodega until its price matches Polymarket's, accounting for fees.
    # The target price on Bodega should be less than Poly's due to fees.
    p_target_bodega_yes = P_POLY_YES / (1.0 + FEE_RATE)
    log.info(f"[DIR 1] Target Bodega YES price (P_POLY_YES / (1+fee)): {p_target_bodega_yes:.4f}")
    
    x_bodega = solve_x_for_price(Q_YES, Q_NO, p_target_bodega_yes, B)

    if x_bodega:
        log.info(f"[DIR 1] Solved for Bodega trade size (x_bodega): {x_bodega:.4f} YES shares")
        x_poly = x_bodega * ADA_TO_USD
        log.info(f"[DIR 1] Corresponding Polymarket trade size (x_poly): {x_poly:.4f} NO shares")
        
        cost_bodega_ada = lmsr_cost(Q_YES + x_bodega, Q_NO, B) - initial_cost
        log.info(f"[DIR 1] Cost on Bodega (ADA): {cost_bodega_ada:.4f}")
        
        cost_bodega_usd = cost_bodega_ada * (1.0 + FEE_RATE) * ADA_TO_USD
        log.info(f"[DIR 1] Cost on Bodega (USD, with fees & fx): {cost_bodega_usd:.4f}")
        
        cost_poly_usd = x_poly * P_POLY_NO
        log.info(f"[DIR 1] Cost on Polymarket (USD): {cost_poly_usd:.4f}")
        
        total_cost = cost_bodega_usd + cost_poly_usd
        log.info(f"[DIR 1] Total Cost (USD): {total_cost:.4f}")
        
        # Payout is guaranteed if trades are placed simultaneously
        payout = x_poly 
        log.info(f"[DIR 1] Guaranteed Payout (USD): {payout:.4f}")

        profit = payout - total_cost
        log.info(f"[DIR 1] PROFIT (USD): {profit:.4f}")

        if total_cost > 0:
            roi = profit / total_cost
            log.info(f"[DIR 1] ROI: {roi*100:.2f}%")
            opp1 = {
                "direction": "BUY_YES_BODEGA", "profit_usd": profit, "roi": roi,
                "bodega_shares": x_bodega, "bodega_side": "YES",
                "polymarket_shares": x_poly, "polymarket_side": "NO",
                "cost_usd": total_cost, "payout_usd": payout,
            }
            if opp1["profit_usd"] > best_opp["profit_usd"]:
                best_opp = opp1
                log.info("[DIR 1] >> This is currently the best opportunity found. <<")
    else:
        log.info("[DIR 1] No viable trade size found.")

    # --- DIRECTION 2: Buy NO on Bodega, Buy YES on Polymarket ---
    log.info("[DIR 2] Evaluating: BUY NO on Bodega, BUY YES on Polymarket")
    p_target_bodega_no = P_POLY_NO / (1.0 + FEE_RATE)
    log.info(f"[DIR 2] Target Bodega NO price (P_POLY_NO / (1+fee)): {p_target_bodega_no:.4f}")
    
    x_bodega = solve_x_for_price(Q_NO, Q_YES, p_target_bodega_no, B)

    if x_bodega:
        log.info(f"[DIR 2] Solved for Bodega trade size (x_bodega): {x_bodega:.4f} NO shares")
        x_poly = x_bodega * ADA_TO_USD
        log.info(f"[DIR 2] Corresponding Polymarket trade size (x_poly): {x_poly:.4f} YES shares")
        
        cost_bodega_ada = lmsr_cost(Q_YES, Q_NO + x_bodega, B) - initial_cost
        log.info(f"[DIR 2] Cost on Bodega (ADA): {cost_bodega_ada:.4f}")
        
        cost_bodega_usd = cost_bodega_ada * (1.0 + FEE_RATE) * ADA_TO_USD
        log.info(f"[DIR 2] Cost on Bodega (USD, with fees & fx): {cost_bodega_usd:.4f}")
        
        cost_poly_usd = x_poly * P_POLY_YES
        log.info(f"[DIR 2] Cost on Polymarket (USD): {cost_poly_usd:.4f}")
        
        total_cost = cost_bodega_usd + cost_poly_usd
        log.info(f"[DIR 2] Total Cost (USD): {total_cost:.4f}")

        payout = x_poly
        log.info(f"[DIR 2] Guaranteed Payout (USD): {payout:.4f}")

        profit = payout - total_cost
        log.info(f"[DIR 2] PROFIT (USD): {profit:.4f}")

        if total_cost > 0:
            roi = profit / total_cost
            log.info(f"[DIR 2] ROI: {roi*100:.2f}%")
            opp2 = {
                "direction": "BUY_NO_BODEGA", "profit_usd": profit, "roi": roi,
                "bodega_shares": x_bodega, "bodega_side": "NO",
                "polymarket_shares": x_poly, "polymarket_side": "YES",
                "cost_usd": total_cost, "payout_usd": payout,
            }
            if opp2["profit_usd"] > best_opp["profit_usd"]:
                best_opp = opp2
                log.info("[DIR 2] >> This is currently the best opportunity found. <<")
    else:
        log.info("[DIR 2] No viable trade size found.")
    
    log.info("--- Finished Arbitrage Calculation ---")
    if best_opp["profit_usd"] == -float('inf'):
        log.info("RESULT: No viable opportunity found in either direction.")
        return None, {"profit_usd": 0, "roi": 0, "direction": "N/A"}, None
    
    log.info(f"RESULT: Best opportunity found: {best_opp}")
    x_star = best_opp.get("bodega_shares")
    return x_star, best_opp, None