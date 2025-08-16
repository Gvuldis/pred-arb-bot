# track_record/portfolio_summary.py
import streamlit as st
import logging
from . import database
from .clients import CoinGeckoClient

log = logging.getLogger(__name__)

@st.cache_resource
def get_cg_client():
    return CoinGeckoClient()

@st.cache_data
def get_cached_ada_price():
    """Fetches and caches the ADA price for the session."""
    log.info("Fetching fresh ADA price from API for summary...")
    return get_cg_client().get_live_ada_price()

def get_portfolio_summary(poly_cash_usd: float, bodega_cash_ada: float) -> dict:
    """
    Calculates the complete portfolio summary.
    This can be called from any Streamlit page.
    """
    live_ada_price = get_cached_ada_price()
    if live_ada_price == 0.0:
        log.error("Could not fetch live ADA price for summary calculation.")

    open_positions_raw = database.get_all_positions_with_transactions()
    
    total_worst_case_payout_from_positions = 0

    for pos_raw in open_positions_raw:
        net_bodega_shares = sum(t['token_amount'] if t['trade_type'] == 'BUY' else -t['token_amount'] for t in pos_raw['bodega_trades'])
        net_poly_shares = sum(t['token_amount'] if t['action'] == 'Buy' else -t['token_amount'] for t in pos_raw['poly_trades'])
        
        payout_if_bodega_wins = (net_bodega_shares * live_ada_price * 0.98) # Applying 2% fee
        payout_if_poly_wins = net_poly_shares
        
        worst_case_payout = min(payout_if_bodega_wins, payout_if_poly_wins)
        total_worst_case_payout_from_positions += worst_case_payout
        
    total_cash_value_usd = poly_cash_usd + (bodega_cash_ada * live_ada_price)
    total_portfolio_value = total_cash_value_usd + total_worst_case_payout_from_positions

    return {
        "live_ada_price": live_ada_price,
        "total_cash_usd": total_cash_value_usd,
        "position_value_usd": total_worst_case_payout_from_positions,
        "total_portfolio_value_usd": total_portfolio_value
    }