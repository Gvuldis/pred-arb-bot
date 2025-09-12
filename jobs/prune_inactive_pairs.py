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

    # --- OPTIMIZATION: Fetch all active Bodega markets once ---
    try:
        all_bodega_markets = b_client.fetch_markets()
        active_bodega_ids = {m['id'] for m in all_bodega_markets}
        log.info(f"Pruner fetched {len(active_bodega_ids)} active Bodega markets.")
    except Exception as e:
        log.error(f"Pruner failed to fetch Bodega markets: {e}. Aborting.")
        return

    for b_id, p_id, _, _, _ in manual_pairs:
        try:
            # 1. Check Bodega market using pre-fetched list
            if b_id not in active_bodega_ids:
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

    # --- OPTIMIZATION: Fetch all active Myriad markets once ---
    try:
        all_myriad_markets = m_client.fetch_markets()
        myriad_market_map = {m['slug']: m for m in all_myriad_markets}
        log.info(f"Pruner fetched {len(myriad_market_map)} active Myriad markets.")
    except Exception as e:
        log.error(f"Pruner failed to fetch Myriad markets: {e}. Aborting.")
        return

    for m_slug, p_id, _, _, _, _ in manual_pairs: # Handle new is_autotrade_safe column
        try:
            # 1. Check Myriad market state using pre-fetched data
            myriad_market = myriad_market_map.get(m_slug)
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