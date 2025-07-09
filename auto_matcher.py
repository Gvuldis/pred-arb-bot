# auto_matcher.py

import os
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from notifications.discord import DiscordNotifier
from jobs.fetch_new_bodega import fetch_and_notify_new_bodega
from services.bodega.client import BodegaClient
from services.polymarket.client import PolymarketClient
from streamlit_app.db import load_manual_pairs
from streamlit_app.db import add_suggested_match
from matching.fuzzy import (
    fetch_all_polymarket_clob_markets,
    fetch_bodega_v3_active_markets,
    fuzzy_match_markets
)


# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Config & clients
BODEGA_API = os.getenv("BODEGA_API", "https://testnet.bodegamarket.io/api")
POLY_API   = os.getenv("POLY_API", "https://clob.polymarket.com")
WEBHOOK    = "https://discord.com/api/webhooks/1255893289136160869/ZwX3Qo1JsF_fBD0kdmI8-xaEyvah9TnAV_R7dIHIKdBAwpEvj6VgmP3YcOa7j8zpyAPN"

b_client  = BodegaClient(BODEGA_API)
p_client  = PolymarketClient(POLY_API)
notifier  = DiscordNotifier(WEBHOOK)

def run_auto_match():
    log.info("Starting periodic auto-match job")
    bodes = fetch_bodega_v3_active_markets(BODEGA_API)
    polys = fetch_all_polymarket_clob_markets()

    matches, ignored = fuzzy_match_markets(bodes, polys)
    log.info(f"Auto-match found {len(matches)} matches, {len(ignored)} ignored")

    # Notify if any matches
    if matches:
        notifier.notify_auto_match(len(matches), len(ignored))
        # Optionally, detail each match
        for b, p, score in matches:
            # 1) send Discord notification
            pair = f"{b['name']} ↔ {p['question']}"
            notifier.send(f"👉 Suggested Match: {pair} (score: {score:.1f})")
            # 2) record in suggested_matches for review
            add_suggested_match(b["id"], p["condition_id"], score)



def run_arb_check():
    log.info("Starting periodic arbitrage‐check job")
    ada_usd = FXClient(os.getenv("COIN_API", "https://api.coingecko.com/api/v3/simple/price?ids=cardano&vs_currencies=usd")).get_ada_usd()
    manual_pairs = load_manual_pairs()
    opportunities = []

    from services.polymarket.model import build_arbitrage_table

    for b_id, p_id in manual_pairs:
        try:
            # Fetch on‐chain pool sizes
            pool = b_client.fetch_market_config(b_id)
            opts = pool["options"]
            Q_YES = next(o["shares"] for o in opts if o["side"] == "YES")
            Q_NO  = next(o["shares"] for o in opts if o["side"] == "NO")

            # Fetch prices
            prices_b = b_client.fetch_prices(b_id)
            p_data   = p_client.fetch_market(p_id)
            P_POLY_YES = p_data["best_yes_ask"]

            # Compute x*, summary
            x_star, summary, _ = build_arbitrage_table(
                Q_YES=Q_YES,
                Q_NO=Q_NO,
                P_POLY_YES=P_POLY_YES,
                ADA_TO_USD=ada_usd,
                FEE_RATE=0.02,
                B=3000
            )

            profit = summary["profit_usd"]
            roi    = summary["roi"]

            if profit > 0 and roi > 0.015:
                pair_desc = f"{pool['name']} ↔ {p_data['question']}"
                opportunities.append((pair_desc, x_star, profit, roi))

        except Exception as e:
            log.warning("Arb check failed ")

    # Notify if we found any
    if opportunities:
        for pair, x_star, profit, roi in opportunities:
            notifier.notify_arb_opportunity(pair, x_star, profit, roi)
    else:
        log.info("No arbitrage opportunities found this cycle")

if __name__ == "__main__":
    from services.bodega.client import BodegaClient
    from services.polymarket.client import PolymarketClient
    from services.fx.client import FXClient
    from streamlit_app.db import load_manual_pairs
    from notifications.discord import DiscordNotifier

    sched = BlockingScheduler(timezone="Europe/Amsterdam")

    # Hourly auto-match at minute 0
    sched.add_job(run_auto_match, "cron", minute=0)
    sched.add_job(fetch_and_notify_new_bodega, "cron", minute=2)
    # Arbitrage check every 5 minutes
    sched.add_job(run_arb_check, "interval", minutes=5)

    log.info("Scheduler started — auto-match hourly, arbitrage-check every 5min")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")
