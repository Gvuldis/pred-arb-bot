import math
import numpy as np
import pandas as pd
from scipy.special import logsumexp
from typing import Tuple, Dict

def compute_price(q_yes: float, q_no: float, b: float) -> float:
    """
    LMSR instantaneous price: e^(q_yes/b) / (e^(q_yes/b) + e^(q_no/b))
    """
    e_yes = math.exp(q_yes / b)
    e_no  = math.exp(q_no  / b)
    return e_yes / (e_yes + e_no)


def lmsr_cost(q_yes: float, q_no: float, b: float) -> float:
    """
    LMSR cost function: b * logsumexp([q_yes/b, q_no/b])
    """
    return b * logsumexp([q_yes / b, q_no / b])


def lmsr_cost_vector(q_yes: float, q_no: float, x_vals: np.ndarray, b: float) -> np.ndarray:
    """
    Vectorized cost for buying x_vals YES shares.
    """
    arr = np.vstack([ (q_yes + x_vals) / b, np.full_like(x_vals, q_no / b) ])
    return b * logsumexp(arr, axis=0)


def optimal_x_with_fee(
    q_yes: float,
    q_no: float,
    p_ext: float,
    fee: float,
    b: float
) -> float:
    """
    Compute optimal x* given external price p_ext and fee.
    Returns None if no valid x*.
    """
    p_eff = p_ext / (1 + fee)
    if p_eff <= 0 or p_eff >= 1:
        return None
    e_yes = math.exp(q_yes / b)
    e_no  = math.exp(q_no  / b)
    y = (p_eff * e_no) / ((1 - p_eff) * e_yes)
    if y <= 0:
        return None
    return b * math.log(y)


def build_arbitrage_table(
    Q_YES: float,
    Q_NO: float,
    P_POLY_YES: float,
    ADA_TO_USD: float,
    FEE_RATE: float,
    B: float,
    x_max: float = None,
    num_steps: int = 50
) -> Tuple[float, Dict[str, float], pd.DataFrame]:
    """
    Build arbitrage payoff table and summary.

    Returns:
      x_star: optimal YES shares to buy
      summary: dict with profit_usd, cost_usd, payout_usd, roi
      df: pandas DataFrame with columns [x, cost, payout, profit, roi]
    """
    # Determine x_max if not provided (e.g., up to no. of shares)
    if x_max is None:
        x_max = Q_NO * 2  # arbitrary cap

    # Candidate x values from small to x_max
    x_vals = np.linspace(0, x_max, num_steps)

    # Compute cost without fee
    cost_no_fee = lmsr_cost_vector(Q_YES, Q_NO, x_vals, B) - lmsr_cost(Q_YES, Q_NO, B)
    cost_with_fee = cost_no_fee * (1 + FEE_RATE)

    # Payout if YES: x * 1 ADA
    payout_ada = x_vals

    # Convert all to USD
    cost_usd = cost_with_fee * ADA_TO_USD
    payout_usd = payout_ada * ADA_TO_USD

    # Profit and ROI for taking YES side arbitrage
    profit_usd = payout_usd - cost_usd
    roi = np.where(cost_usd > 0, profit_usd / cost_usd, 0)

    # Optimal x* from closed-form
    x_star = optimal_x_with_fee(Q_YES, Q_NO, P_POLY_YES, FEE_RATE, B)

    # Summary for optimal
    if x_star is not None:
        cost_star = (lmsr_cost(Q_YES + x_star, Q_NO, B) - lmsr_cost(Q_YES, Q_NO, B)) * (1 + FEE_RATE) * ADA_TO_USD
        payout_star = x_star * ADA_TO_USD
        profit_star = payout_star - cost_star
        roi_star = profit_star / cost_star if cost_star > 0 else 0
    else:
        cost_star = payout_star = profit_star = roi_star = 0

    summary = {
        "cost_usd": float(cost_star),
        "payout_usd": float(payout_star),
        "profit_usd": float(profit_star),
        "roi": float(roi_star),
        "x_star": float(x_star) if x_star is not None else None
    }

    # Build DataFrame
    df = pd.DataFrame({
        "x": x_vals,
        "cost_usd": cost_usd,
        "payout_usd": payout_usd,
        "profit_usd": profit_usd,
        "roi": roi
    })

    return x_star, summary, df
