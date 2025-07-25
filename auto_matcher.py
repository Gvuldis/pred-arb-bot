import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from config import b_client, p_client, fx_client, notifier, FEE_RATE, log
from jobs.fetch_new_bodega import fetch_and_notify_new_bodega
from jobs.prune_inactive_pairs import prune_inactive_pairs
from streamlit_app.db import (
    load_manual_pairs,
    add_suggested_match,
    load_bodega_markets,
    load_polymarkets,
    save_polymarkets,
    load_probability_watches,
    delete_probability_watch,
)
from matching.fuzzy import (
    fetch_all_polymarket_clob_markets,
    fuzzy_match_markets,
)
from services.polymarket.model import build_arbitrage_table, infer_b


def fetch_and_save_polymarkets():
    """
    Fetches all Polymarket markets and saves them to the database.
    This job ensures the local database is kept up-to-date with active markets.
    """
    log.info("Starting job to fetch and save Polymarket markets...")
    try:
        fresh_markets = fetch_all_polymarket_clob_markets()
        if fresh_markets:
            existing_ids = {m["condition_id"] for m in load_polymarkets()}
            new_markets = [
                m for m in fresh_markets if m.get("condition_id") not in existing_ids
            ]
            save_polymarkets(fresh_markets)
            log.info(
                f"Saved/updated {len(fresh_markets)} Polymarket markets in the database."
            )
            if new_markets:
                log.info(f"Found {len(new_markets)} new Polymarket markets.")
            else:
                log.info("No new Polymarket markets found.")
        else:
            log.info("No active Polymarket markets found from API.")
    except Exception as e:
        log.error(f"Failed to fetch and save Polymarket markets: {e}", exc_info=True)


def run_auto_match():
    """
    Loads markets from the database, finds potential matches using
    fuzzy string matching, and records them for manual review.
    """
    log.info("Starting periodic auto-match job")
    try:
        bodes = load_bodega_markets()
        polys = load_polymarkets()
        log.info(
            f"Loaded {len(bodes)} Bodega markets and {len(polys)} Polymarket markets from DB for matching."
        )
        matches, ignored_count = fuzzy_match_markets(bodes, polys)
        log.info(f"Auto-match found {len(matches)} matches, {ignored_count} ignored")
        if not notifier:
            log.warning("Discord notifier not available. Skipping notifications.")
            return
        if matches:
            notifier.notify_auto_match(len(matches), ignored_count)
            for b, p, score in matches:
                pair = f"{b['name']} <-> {p['question']}"
                notifier.send(f"👉 Suggested Match: {pair} (score: {score:.1f})")
                add_suggested_match(b["id"], p["condition_id"], score)
    except Exception as e:
        log.error(f"Auto-match job failed: {e}", exc_info=True)


def run_arb_check():
    """
    Checks for arbitrage opportunities in manually confirmed pairs.
    """
    log.info("Starting periodic arbitrage-check job")
    try:
        ada_usd = fx_client.get_ada_usd()
        manual_pairs = load_manual_pairs()
        opportunities = []

        log.info(f"Found {len(manual_pairs)} manual pairs to check.")
        for b_id, p_id, is_flipped in manual_pairs:
            try:
                log.info(f"--- Checking Pair: Bodega ID={b_id}, Polymarket ID={p_id} ---")
                
                pool = b_client.fetch_market_config(b_id)
                p_data = p_client.fetch_market(p_id)

                if not p_data.get('active') or p_data.get('closed'):
                    log.warning(f"Skipping pair ({b_id}, {p_id}) because Polymarket market is not active.")
                    continue

                bodega_prediction_info = b_client.fetch_prices(b_id)

                # Get original order books and outcome names
                order_book_yes = p_data.get('order_book_yes')
                order_book_no = p_data.get('order_book_no')
                poly_outcome_name_yes = p_data.get('outcome_yes', 'YES')
                poly_outcome_name_no = p_data.get('outcome_no', 'NO')

                # Swap them if the pair is marked as flipped
                if is_flipped:
                    log.info(f"Pair ({b_id}, {p_id}) is flipped. Swapping Polymarket outcomes for check.")
                    order_book_yes, order_book_no = order_book_no, order_book_yes
                    poly_outcome_name_yes, poly_outcome_name_no = poly_outcome_name_no, poly_outcome_name_yes

                if not all([pool, p_data, bodega_prediction_info, order_book_yes, order_book_no]):
                    log.warning(f"Skipping pair ({b_id}, {p_id}) due to missing data (e.g., empty order books).")
                    continue

                Q_YES = bodega_prediction_info.get("yesVolume_ada", 0)
                Q_NO = bodega_prediction_info.get("noVolume_ada", 0)
                p_bod_yes = bodega_prediction_info.get("yesPrice_ada")

                if not p_bod_yes:
                    log.warning(f"Skipping pair ({b_id}, {p_id}): Could not fetch live Bodega YES price.")
                    continue
                
                try:
                    inferred_B = infer_b(Q_YES, Q_NO, p_bod_yes)
                    log.info(f"Inferred B for market {b_id}: {inferred_B:.2f}")
                except ValueError as e:
                    log.warning(f"Skipping pair ({b_id}, {p_id}): Could not infer B parameter. Reason: {e}")
                    continue

                pair_opportunities = build_arbitrage_table(
                    Q_YES=Q_YES,
                    Q_NO=Q_NO,
                    ORDER_BOOK_YES=order_book_yes,
                    ORDER_BOOK_NO=order_book_no,
                    ADA_TO_USD=ada_usd,
                    FEE_RATE=FEE_RATE,
                    B=inferred_B
                )
                
                if not pair_opportunities:
                    log.info(f"No arbitrage opportunity for pair ({b_id}, {p_id}).")
                else:
                    for summary in pair_opportunities:
                        # Relabel the polymarket_side in the summary for correct logging and notifications
                        logical_poly_side = summary['polymarket_side']
                        if logical_poly_side == 'YES':
                            summary['polymarket_side'] = poly_outcome_name_yes
                        else: # 'NO'
                            summary['polymarket_side'] = poly_outcome_name_no

                        log.info(f"ARBITRAGE CHECK SUMMARY for ({b_id}, {p_id}): {summary}")
                        profit = summary.get("profit_usd", 0)
                        roi = summary.get("roi", 0)

                        if profit > 20 and roi > 0.015:
                            log.info(f"!!!!!! PROFITABLE ARBITRAGE FOUND for pair ({b_id}, {p_id}) !!!!!!")
                            pair_desc = f"{pool['name']} <-> {p_data['question']}"
                            opportunities.append((pair_desc, summary, b_id, p_id))

            except Exception as e:
                log.error(f"Arbitrage check for pair ({b_id}, {p_id}) failed: {e}", exc_info=True)

        if notifier and opportunities:
            for pair, summary, b_id, p_id in opportunities:
                notifier.notify_arb_opportunity(pair, summary, b_id, p_id, b_client.api_url)
        
        if not opportunities:
            log.info("No arbitrage opportunities meeting profit/ROI threshold found this cycle.")

    except Exception as e:
        log.error(f"Arbitrage check job failed entirely: {e}", exc_info=True)

def run_prob_watch_check():
    """
    Checks Bodega markets against manually set expected probabilities and sends alerts on deviation.
    """
    log.info("Starting periodic probability watch check job")
    try:
        watches = load_probability_watches()
        if not watches:
            log.info("No probability watches configured. Skipping check.")
            return

        log.info(f"Found {len(watches)} probability watches to check.")
        for watch in watches:
            b_id = watch['bodega_id']
            try:
                expected_prob = watch['expected_probability']
                threshold = watch['deviation_threshold']
                
                # Fetch live market data
                market_config = b_client.fetch_market_config(b_id)
                market_name = market_config.get('name', f"ID: {b_id}")
                
                prices = b_client.fetch_prices(b_id)
                live_prob = prices.get('yesPrice_ada')

                if live_prob is None:
                    log.warning(f"Could not get live probability for watch on market {b_id}. Skipping.")
                    continue

                deviation = abs(live_prob - expected_prob)

                log.info(f"Watch Check ({b_id}): Expected={expected_prob:.3f}, Live={live_prob:.3f}, Deviation={deviation:.3f}, Threshold={threshold:.3f}")

                if deviation >= threshold:
                    log.warning(f"!!! DEVIATION ALERT for market {b_id} !!!")
                    if notifier:
                        notifier.notify_probability_deviation(
                            market_name=market_name,
                            bodega_id=b_id,
                            bodega_api_base=b_client.api_url,
                            expected_prob=expected_prob,
                            live_prob=live_prob,
                            deviation=deviation
                        )

            except ValueError:
                # This can happen if fetch_market_config fails because the market is no longer active.
                log.warning(f"Market {b_id} for probability watch is inactive. Pruning watch.")
                delete_probability_watch(b_id)
            except Exception as e:
                log.error(f"Probability watch check for Bodega ID {b_id} failed: {e}", exc_info=True)

    except Exception as e:
        log.error(f"Probability watch job failed entirely: {e}", exc_info=True)

if __name__ == "__main__":
    sched = BlockingScheduler(timezone="Europe/Amsterdam")

    log.info("Running initial jobs on startup...")
    fetch_and_notify_new_bodega()
    fetch_and_save_polymarkets()
    run_auto_match()
    run_arb_check()
    run_prob_watch_check()
    prune_inactive_pairs()
    log.info("Initial jobs complete.")

    # Schedule recurring jobs
    sched.add_job(fetch_and_notify_new_bodega, "cron", minute="*/15") # Check for new markets every 15 mins
    sched.add_job(fetch_and_save_polymarkets, "cron", minute="*/15")
    sched.add_job(prune_inactive_pairs, "cron", hour="*") # Prune once an hour
    
    # User-defined schedule
    sched.add_job(run_auto_match, "interval", minutes=30)
    sched.add_job(run_arb_check, "interval", minutes=3)
    sched.add_job(run_prob_watch_check, "interval", minutes=3)

    log.info(
        "Scheduler started — arb-check & prob-watch every 3min, auto-match every 30min."
    )
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")