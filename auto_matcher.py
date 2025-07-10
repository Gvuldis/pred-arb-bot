import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from config import b_client, p_client, fx_client, notifier, FEE_RATE, B, log
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
from services.polymarket.model import build_arbitrage_table


def fetch_and_save_polymarkets():
    """
    Fetches all Polymarket markets and saves them to the database.
    This job ensures the local database is kept up-to-date with active markets.
    """
    log.info("Starting job to fetch and save Polymarket markets...")
    try:
        # Fetch fresh markets from the API
        fresh_markets = fetch_all_polymarket_clob_markets()

        if fresh_markets:
            # Get existing market IDs to see what's new
            existing_ids = {m["condition_id"] for m in load_polymarkets()}
            new_markets = [
                m for m in fresh_markets if m.get("condition_id") not in existing_ids
            ]

            # Save all fresh markets to the DB (this will update existing ones)
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
        # Load markets from the database
        db_bodes = load_bodega_markets()
        manual_bodes = load_manual_bodega_markets()  # Include manual test markets

        # Combine, ensuring no duplicates (live markets take precedence)
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

        # Notify if any matches
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
                # Fetch market data
                pool = b_client.fetch_market_config(b_id)
                p_data = p_client.fetch_market(p_id)
                
                if not all([pool, p_data, p_data.get('best_yes_ask'), p_data.get('best_no_ask')]):
                    log.warning(f"Skipping pair ({b_id}, {p_id}) due to missing or incomplete market data.")
                    log.debug(f"Pool data: {pool}")
                    log.debug(f"Polymarket data: {p_data}")
                    continue

                opts = pool.get("options", [])
                Q_YES = next((o["shares"] for o in opts if o["side"] == "YES"), 0)
                Q_NO = next((o["shares"] for o in opts if o["side"] == "NO"), 0)

                # Compute arbitrage opportunity
                _, summary, _ = build_arbitrage_table(
                    Q_YES=Q_YES,
                    Q_NO=Q_NO,
                    P_POLY_YES=p_data["best_yes_ask"],
                    P_POLY_NO=p_data["best_no_ask"],
                    ADA_TO_USD=ada_usd,
                    FEE_RATE=FEE_RATE,
                    B=B
                )
                
                # Log the summary for every pair checked to see near-misses and details
                if summary and summary.get("direction") != "N/A":
                    log.info(f"ARBITRAGE CHECK SUMMARY for ({b_id}, {p_id}): {summary}")
                else:
                    log.info(f"No arbitrage opportunity could be calculated for pair ({b_id}, {p_id}).")

                profit = summary.get("profit_usd", 0)
                roi = summary.get("roi", 0)

                if profit > 0 and roi > 0.015:
                    log.info(f"!!!!!! PROFITABLE ARBITRAGE FOUND for pair ({b_id}, {p_id}) !!!!!!")
                    pair_desc = f"{pool['name']} <-> {p_data['question']}"
                    opportunities.append((pair_desc, summary))

            except Exception as e:
                log.error(f"Arbitrage check for pair ({b_id}, {p_id}) failed: {e}", exc_info=True)

        # Notify if we found any opportunities
        if notifier and opportunities:
            for pair, summary in opportunities:
                notifier.notify_arb_opportunity(pair, summary)
        
        if not opportunities:
            log.info("No arbitrage opportunities meeting profit/ROI threshold found this cycle.")

    except Exception as e:
        log.error(f"Arbitrage check job failed entirely: {e}", exc_info=True)

if __name__ == "__main__":
    sched = BlockingScheduler(timezone="Europe/Amsterdam")

    # Run jobs on startup
    log.info("Running initial jobs on startup...")
    fetch_and_notify_new_bodega()
    fetch_and_save_polymarkets()
    run_auto_match()
    run_arb_check()
    prune_inactive_pairs()
    log.info("Initial jobs complete.")

    # Fetch jobs hourly
    sched.add_job(fetch_and_notify_new_bodega, "cron", minute=2)
    sched.add_job(fetch_and_save_polymarkets, "cron", minute=4)
    # Hourly auto-match at minute 0 (runs on data from previous hour's fetch)
    sched.add_job(run_auto_match, "cron", minute=0)
    # Arbitrage check every 5 minutes
    sched.add_job(run_arb_check, "interval", minutes=5)
    # Prune inactive pairs hourly at minute 10
    sched.add_job(prune_inactive_pairs, "cron", minute=10)

    log.info(
        "Scheduler started — fetch jobs hourly, auto-match hourly, arbitrage-check every 5min, prune hourly"
    )
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")