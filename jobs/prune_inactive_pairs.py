# jobs/prune_inactive_pairs.py
import logging
from requests.exceptions import RequestException
from config import b_client, m_client, p_client, log
from streamlit_app.db import (
    load_manual_pairs, delete_manual_pair,
    load_manual_pairs_myriad, delete_manual_pair_myriad
)

def prune_inactive_bodega_pairs():
    """
    Iterates through all manually saved Bodega pairs and removes them if either market
    is no longer active.
    """
    log.info("Starting job to prune inactive Bodega matched pairs...")
    manual_pairs = load_manual_pairs()
    pruned_count = 0

    if not manual_pairs:
        log.info("No manual Bodega pairs to check.")
        return

    for b_id, p_id, _, _ in manual_pairs: # Adjusted to handle new tuple format
        try:
            # 1. Check Bodega market. `fetch_market_config` raises ValueError if not found.
            try:
                b_client.fetch_market_config(b_id)
            except ValueError:
                log.info(f"Pruning Bodega pair ({b_id}, {p_id}): Bodega market no longer active.")
                delete_manual_pair(b_id, p_id)
                pruned_count += 1
                continue

            # 2. Check Polymarket market.
            try:
                poly_market = p_client.fetch_market(p_id)
                if not poly_market.get('active') or poly_market.get('closed'):
                    log.info(f"Pruning Bodega pair ({b_id}, {p_id}): Polymarket market no longer active.")
                    delete_manual_pair(b_id, p_id)
                    pruned_count += 1
                    continue
            except RequestException:
                log.info(f"Pruning Bodega pair ({b_id}, {p_id}): Polymarket market not found (404).")
                delete_manual_pair(b_id, p_id)
                pruned_count += 1
                continue
        except Exception as e:
            log.error(f"Error checking Bodega pair ({b_id}, {p_id}) for pruning: {e}", exc_info=True)

    log.info(f"Bodega pruning job complete. Removed {pruned_count} inactive pairs.")

def prune_inactive_myriad_pairs():
    """
    Iterates through all manually saved Myriad pairs and removes them if either market
    is no longer active.
    """
    log.info("Starting job to prune inactive Myriad matched pairs...")
    manual_pairs = load_manual_pairs_myriad()
    pruned_count = 0

    if not manual_pairs:
        log.info("No manual Myriad pairs to check.")
        return

    for m_slug, p_id, _, _ in manual_pairs:
        try:
            # 1. Check Myriad market state
            myriad_market = m_client.fetch_market_details(m_slug)
            if not myriad_market or myriad_market.get('state') != 'open':
                log.info(f"Pruning Myriad pair ({m_slug}, {p_id}): Myriad market no longer open.")
                delete_manual_pair_myriad(m_slug, p_id)
                pruned_count += 1
                continue

            # 2. Check Polymarket market state
            poly_market = p_client.fetch_market(p_id)
            if not poly_market.get('active') or poly_market.get('closed'):
                log.info(f"Pruning Myriad pair ({m_slug}, {p_id}): Polymarket market no longer active.")
                delete_manual_pair_myriad(m_slug, p_id)
                pruned_count += 1
                continue
        except Exception as e:
            log.error(f"Error checking Myriad pair ({m_slug}, {p_id}) for pruning: {e}", exc_info=True)

    log.info(f"Myriad pruning job complete. Removed {pruned_count} inactive pairs.")

def prune_all_inactive_pairs():
    """Runs both pruning functions."""
    prune_inactive_bodega_pairs()
    prune_inactive_myriad_pairs()