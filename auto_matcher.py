# auto_matcher.py
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from config import b_client, m_client, p_client, fx_client, notifier, FEE_RATE_BODEGA, FEE_RATE_MYRIAD_BUY, log
from jobs.fetch_new_bodega import fetch_and_notify_new_bodega
from jobs.fetch_new_myriad import fetch_and_notify_new_myriad
from jobs.prune_inactive_pairs import prune_all_inactive_pairs
from streamlit_app.db import (
    load_manual_pairs, load_manual_pairs_myriad,
    load_bodega_markets, load_polymarkets, save_polymarkets,
    load_probability_watches, delete_probability_watch,
    get_config_value, load_myriad_markets, save_myriad_markets
)
from matching.fuzzy import fetch_all_polymarket_clob_markets, fuzzy_match_markets
from services.polymarket.model import build_arbitrage_table, infer_b
from services.myriad.model import build_arbitrage_table_myriad
import services.myriad.model as myriad_model

def fetch_and_save_markets():
    """Fetches and saves markets for all platforms."""
    log.info("Starting job to fetch and save all platform markets...")
    try:
        # Polymarket
        fresh_poly_markets = fetch_all_polymarket_clob_markets()
        if fresh_poly_markets:
            save_polymarkets(fresh_poly_markets)
            log.info(f"Saved/updated {len(fresh_poly_markets)} Polymarket markets.")
        else:
            log.info("No active Polymarket markets found from API.")
        
        # Myriad
        fresh_myriad_markets = m_client.fetch_markets()
        if fresh_myriad_markets:
            save_myriad_markets(fresh_myriad_markets)
            log.info(f"Saved/updated {len(fresh_myriad_markets)} Myriad markets.")
        else:
            log.info("No active Myriad markets found from API.")
            
    except Exception as e:
        log.error(f"Failed to fetch and save all markets: {e}", exc_info=True)


def run_bodega_arb_check():
    """Checks for arbitrage opportunities in Bodega-Polymarket pairs."""
    log.info("--- Starting BODEGA arbitrage-check job ---")
    try:
        ada_usd = fx_client.get_ada_usd()
        manual_pairs = load_manual_pairs()
        opportunities = []

        log.info(f"Found {len(manual_pairs)} manual Bodega pairs to check.")
        for b_id, p_id, is_flipped, profit_threshold in manual_pairs:
            try:
                log.info(f"--- Checking Bodega Pair: ID={b_id}, Poly ID={p_id} ---")
                
                pool = b_client.fetch_market_config(b_id)
                p_data = p_client.fetch_market(p_id)

                if not p_data.get('active') or p_data.get('closed'):
                    log.warning(f"Skipping pair ({b_id}, {p_id}) because Polymarket market is not active.")
                    continue

                bodega_prediction_info = b_client.fetch_prices(b_id)
                order_book_yes, order_book_no = p_data.get('order_book_yes'), p_data.get('order_book_no')
                poly_outcome_name_yes, poly_outcome_name_no = p_data.get('outcome_yes', 'YES'), p_data.get('outcome_no', 'NO')

                if is_flipped:
                    order_book_yes, order_book_no = order_book_no, order_book_yes
                    poly_outcome_name_yes, poly_outcome_name_no = poly_outcome_name_no, poly_outcome_name_yes

                if not all([pool, p_data, bodega_prediction_info, order_book_yes, order_book_no]):
                    log.warning(f"Skipping pair ({b_id}, {p_id}) due to missing data.")
                    continue

                Q_YES, Q_NO = bodega_prediction_info.get("yesVolume_ada", 0), bodega_prediction_info.get("noVolume_ada", 0)
                p_bod_yes = bodega_prediction_info.get("yesPrice_ada")
                if p_bod_yes is None: continue

                inferred_B = infer_b(Q_YES, Q_NO, p_bod_yes)
                pair_opportunities = build_arbitrage_table(Q_YES, Q_NO, order_book_yes, order_book_no, ada_usd, FEE_RATE_BODEGA, inferred_B)
                
                for summary in pair_opportunities:
                    if summary.get("profit_usd", 0) > 25 and summary.get("roi", 0) > 0.05:
                        summary['polymarket_side'] = poly_outcome_name_yes if summary['polymarket_side'] == 'YES' else poly_outcome_name_no
                        pair_desc = f"{pool['name']} <-> {p_data['question']}"
                        opportunities.append((pair_desc, summary, b_id, p_id))

            except Exception as e:
                log.error(f"Bodega arb check for pair ({b_id}, {p_id}) failed: {e}", exc_info=True)

        if notifier and opportunities:
            for pair, summary, b_id, p_id in opportunities:
                notifier.notify_arb_opportunity(pair, summary, b_id, p_id, b_client.api_url)
        
        log.info(f"Bodega arb check finished. Found {len(opportunities)} opportunities.")

    except Exception as e:
        log.error(f"Bodega arbitrage check job failed entirely: {e}", exc_info=True)

def run_myriad_arb_check():
    """Checks for arbitrage opportunities in Myriad-Polymarket pairs."""
    log.info("--- Starting MYRIAD arbitrage-check job ---")
    try:
        manual_pairs = load_manual_pairs_myriad()
        opportunities = []

        log.info(f"Found {len(manual_pairs)} manual Myriad pairs to check.")
        for m_slug, p_id, is_flipped, profit_threshold in manual_pairs:
            try:
                log.info(f"--- Checking Myriad Pair: Slug={m_slug}, Poly ID={p_id} ---")

                m_data = m_client.fetch_market_details(m_slug)
                p_data = p_client.fetch_market(p_id)

                if not all([m_data, p_data]) or m_data.get('state') != 'open' or not p_data.get('active') or p_data.get('closed'):
                    log.warning(f"Skipping pair ({m_slug}, {p_id}) due to inactive/missing market data.")
                    continue
                
                m_prices = m_client.fetch_prices(m_slug)
                if not m_prices: continue
                
                Q1, Q2 = m_prices['shares1'], m_prices['shares2']
                P1 = m_prices['price1']
                
                inferred_B = myriad_model.infer_b(Q1, Q2, P1)
                
                order_book_poly_1, order_book_poly_2 = p_data.get('order_book_yes'), p_data.get('order_book_no')
                
                if is_flipped:
                    order_book_poly_1, order_book_poly_2 = order_book_poly_2, order_book_poly_1
                
                pair_opportunities = build_arbitrage_table_myriad(Q1, Q2, order_book_poly_1, order_book_poly_2, FEE_RATE_MYRIAD_BUY, inferred_B)

                for summary in pair_opportunities:
                    if summary.get("profit_usd", 0) > 25 and summary.get("roi", 0) > 0.05:
                        summary['myriad_side_title'] = m_prices['title1'] if summary['myriad_side'] == 1 else m_prices['title2']
                        summary['polymarket_side_title'] = p_data['outcome_yes'] if summary['polymarket_side'] == 1 else p_data['outcome_no']
                        pair_desc = f"{m_data['title']} <-> {p_data['question']}"
                        opportunities.append((pair_desc, summary, m_slug, p_id))

            except Exception as e:
                log.error(f"Myriad arb check for pair ({m_slug}, {p_id}) failed: {e}", exc_info=True)

        if notifier and opportunities:
            for pair, summary, m_slug, p_id in opportunities:
                notifier.notify_arb_opportunity_myriad(pair, summary, m_slug, p_id)
        
        log.info(f"Myriad arb check finished. Found {len(opportunities)} opportunities.")

    except Exception as e:
        log.error(f"Myriad arbitrage check job failed entirely: {e}", exc_info=True)


def run_all_arb_checks():
    run_bodega_arb_check()
    run_myriad_arb_check()

def run_prob_watch_check():
    """Checks Bodega markets against manually set expected probabilities."""
    log.info("Starting periodic probability watch check job")
    try:
        watches = load_probability_watches()
        if not watches:
            log.info("No probability watches configured. Skipping check.")
            return

        for watch in watches:
            b_id = watch['bodega_id']
            try:
                prices = b_client.fetch_prices(b_id)
                live_prob = prices.get('yesPrice_ada')
                if live_prob is None: continue

                deviation = abs(live_prob - watch['expected_probability'])
                if deviation >= watch['deviation_threshold']:
                    market_config = b_client.fetch_market_config(b_id)
                    if notifier:
                        notifier.notify_probability_deviation(
                            market_name=market_config.get('name', f"ID: {b_id}"), bodega_id=b_id,
                            bodega_api_base=b_client.api_url, expected_prob=watch['expected_probability'],
                            live_prob=live_prob, deviation=deviation
                        )
            except ValueError:
                log.warning(f"Market {b_id} for probability watch is inactive. Pruning watch.")
                delete_probability_watch(b_id)
            except Exception as e:
                log.error(f"Prob watch for Bodega ID {b_id} failed: {e}", exc_info=True)
    except Exception as e:
        log.error(f"Probability watch job failed entirely: {e}", exc_info=True)

def update_schedules(scheduler):
    """Checks for config changes from the DB and reschedules jobs accordingly."""
    log.info("Checking for schedule updates...")
    try:
        arb_check_interval_seconds = int(get_config_value('arb_check_interval_seconds', '180'))
        arb_job = scheduler.get_job('arb_check_job')
        if arb_job and int(arb_job.trigger.interval.total_seconds()) != arb_check_interval_seconds:
            log.warning(f"Rescheduling arbitrage check from {arb_job.trigger.interval.total_seconds()}s to {arb_check_interval_seconds}s.")
            scheduler.reschedule_job('arb_check_job', trigger='interval', seconds=arb_check_interval_seconds)
    except Exception as e:
        log.error(f"Failed to update schedules: {e}", exc_info=True)

if __name__ == "__main__":
    sched = BlockingScheduler(timezone="UTC")

    log.info("Running initial jobs on startup...")
    fetch_and_notify_new_bodega()
    fetch_and_notify_new_myriad()
    fetch_and_save_markets()
    run_all_arb_checks()
    run_prob_watch_check()
    prune_all_inactive_pairs()
    log.info("Initial jobs complete.")

    initial_arb_interval_seconds = int(get_config_value('arb_check_interval_seconds', '180'))

    sched.add_job(fetch_and_notify_new_bodega, "cron", minute="*/15")
    sched.add_job(fetch_and_notify_new_myriad, "cron", minute="*/15")
    sched.add_job(fetch_and_save_markets, "cron", minute="*/15")
    sched.add_job(prune_all_inactive_pairs, "cron", hour="*")
    sched.add_job(run_all_arb_checks, "interval", seconds=initial_arb_interval_seconds, id="arb_check_job")
    sched.add_job(run_prob_watch_check, "interval", minutes=3, id="prob_watch_job")
    sched.add_job(update_schedules, "interval", seconds=15, args=[sched])

    log.info(f"Scheduler started. Arb-check interval: {initial_arb_interval_seconds}s.")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")