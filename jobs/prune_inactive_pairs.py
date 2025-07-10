import logging
from requests.exceptions import RequestException
from config import b_client, p_client, log
from streamlit_app.db import load_manual_pairs, delete_manual_pair

def prune_inactive_pairs():
    """
    Iterates through all manually saved pairs and removes them if either market
    is no longer active.
    """
    log.info("Starting job to prune inactive matched pairs...")
    manual_pairs = load_manual_pairs()
    pruned_count = 0

    if not manual_pairs:
        log.info("No manual pairs to check.")
        return

    for b_id, p_id in manual_pairs:
        try:
            # 1. Check Bodega market. `fetch_market_config` raises ValueError if not found
            # in the active list from the API.
            try:
                b_client.fetch_market_config(b_id)
            except ValueError:
                log.info(f"Pruning pair ({b_id}, {p_id}): Bodega market no longer active.")
                delete_manual_pair(b_id, p_id)
                pruned_count += 1
                continue

            # 2. Check Polymarket market.
            try:
                poly_market = p_client.fetch_market(p_id)
                if not poly_market.get('active') or poly_market.get('closed'):
                    log.info(f"Pruning pair ({b_id}, {p_id}): Polymarket market no longer active.")
                    delete_manual_pair(b_id, p_id)
                    pruned_count += 1
                    continue
            except RequestException:
                # If the market fetch fails (e.g., 404), it's no longer active.
                log.info(f"Pruning pair ({b_id}, {p_id}): Polymarket market not found (404).")
                delete_manual_pair(b_id, p_id)
                pruned_count += 1
                continue

        except Exception as e:
            log.error(f"Error checking pair ({b_id}, {p_id}) for pruning: {e}", exc_info=True)

    log.info(f"Pruning job complete. Removed {pruned_count} inactive pairs.")