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
    load_manual_bodega_markets,
    save_polymarkets,
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
        db_bodes = load_bodega_markets()
        manual_bodes = load_manual_bodega_markets()
        bodes_map = {m["id"]: m for m in db_bodes}
        for m in manual_bodes:
            if m["id"] not in bodes_map:
                bodes_map[m["id"]] = m
        bodes = list(bodes_map.values())
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
        for b_id, p_id in manual_pairs:
            try:
                log.info(f"--- Checking Pair: Bodega ID={b_id}, Polymarket ID={p_id} ---")
                pool = b_client.fetch_market_config(b_id)
                p_data = p_client.fetch_market(p_id)
                bodega_prediction_info = b_client.fetch_prices(b_id) # Fetches from /getPredictionInfo
                
                order_book_yes = p_data.get('order_book_yes')
                order_book_no = p_data.get('order_book_no')

                if not all([pool, p_data, bodega_prediction_info, order_book_yes, order_book_no]):
                    log.warning(f"Skipping pair ({b_id}, {p_id}) due to missing data (e.g., empty order books, prediction info).")
                    continue

                # Get liquidity shares (q_yes, q_no) from /getPredictionInfo.
                Q_YES = bodega_prediction_info.get("yesVolume_ada", 0)
                Q_NO = bodega_prediction_info.get("noVolume_ada", 0)
                log.info(f"DBG: Extracted shares for {b_id} from getPredictionInfo: Q_YES={Q_YES}, Q_NO={Q_NO}")

                # --- DYNAMIC B CALCULATION ---
                p_bod_yes = bodega_prediction_info.get("yesPrice_ada")
                log.info(f"DBG: Fetched Bodega price for {b_id}: p_bod_yes={p_bod_yes}")

                if not p_bod_yes:
                    log.warning(f"Skipping pair ({b_id}, {p_id}): Could not fetch live Bodega YES price.")
                    continue
                
                try:
                    inferred_B = infer_b(Q_YES, Q_NO, p_bod_yes)
                    log.info(f"Inferred B for market {b_id}: {inferred_B:.2f}")
                except ValueError as e:
                    log.warning(f"Skipping pair ({b_id}, {p_id}): Could not infer B parameter. Reason: {e}")
                    continue
                # --- END DYNAMIC B CALCULATION ---

                _, summary, _ = build_arbitrage_table(
                    Q_YES=Q_YES,
                    Q_NO=Q_NO,
                    ORDER_BOOK_YES=order_book_yes,
                    ORDER_BOOK_NO=order_book_no,
                    ADA_TO_USD=ada_usd,
                    FEE_RATE=FEE_RATE,
                    B=inferred_B
                )
                
                if summary and summary.get("direction") not in ("N/A", "NONE"):
                    log.info(f"ARBITRAGE CHECK SUMMARY for ({b_id}, {p_id}): {summary}")
                else:
                    reason = summary.get('reason', 'N/A') if summary else 'N/A'
                    log.info(f"No arbitrage opportunity for pair ({b_id}, {p_id}). Reason: {reason}")

                profit = summary.get("profit_usd", 0) if summary else 0
                roi = summary.get("roi", 0) if summary else 0

                if profit > 0 and roi > 0.015:
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

if __name__ == "__main__":
    sched = BlockingScheduler(timezone="Europe/Amsterdam")

    log.info("Running initial jobs on startup...")
    fetch_and_notify_new_bodega()
    fetch_and_save_polymarkets()
    run_auto_match()
    run_arb_check()
    prune_inactive_pairs()
    log.info("Initial jobs complete.")

    sched.add_job(fetch_and_notify_new_bodega, "cron", minute=2)
    sched.add_job(fetch_and_save_polymarkets, "cron", minute=4)
    sched.add_job(run_auto_match, "cron", minute=0)
    sched.add_job(run_arb_check, "interval", minutes=5)
    sched.add_job(prune_inactive_pairs, "cron", minute=10)

    log.info(
        "Scheduler started — fetch jobs hourly, auto-match hourly, arbitrage-check every 5min, prune hourly"
    )
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")