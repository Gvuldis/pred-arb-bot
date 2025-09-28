# auto_matcher.py
import logging
import uuid
import json
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, timezone
from config import b_client, m_client, p_client, fx_client, notifier, FEE_RATE_BODEGA, FEE_RATE_MYRIAD_BUY, log
from jobs.fetch_new_bodega import fetch_and_notify_new_bodega
from jobs.fetch_new_myriad import fetch_and_notify_new_myriad
from jobs.prune_inactive_pairs import prune_all_inactive_pairs
from streamlit_app.db import (
    load_manual_pairs, load_manual_pairs_myriad,
    save_polymarkets,
    load_probability_watches, delete_probability_watch,
    get_config_value, save_myriad_markets,
    add_arb_opportunity
)
from matching.fuzzy import fetch_all_polymarket_clob_markets
from services.polymarket.model import build_arbitrage_table, infer_b
from services.myriad.model import build_arbitrage_table_myriad
import services.myriad.model as myriad_model

def calculate_apy(roi: float, end_date_ms: int) -> float:
    """Calculates APY given ROI and an end date timestamp in milliseconds."""
    if not end_date_ms or roi <= 0:
        return 0.0

    now_utc = datetime.now(timezone.utc)
    end_date_utc = datetime.fromtimestamp(end_date_ms / 1000, tz=timezone.utc)
    
    time_to_expiry = end_date_utc - now_utc
    days_to_expiry = time_to_expiry.total_seconds() / (24 * 3600)

    if days_to_expiry <= 0.01: # Avoid division by zero or huge APYs for near-expiry
        return 0.0
    
    apy = (roi / days_to_expiry) * 365
    return apy

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

        if not manual_pairs:
            log.info("No manual Bodega pairs to check. Skipping.")
            return

        # --- OPTIMIZATION: Fetch all market configs once ---
        try:
            all_bodega_markets = b_client.fetch_markets()
            bodega_market_map = {m['id']: m for m in all_bodega_markets}
            log.info(f"Fetched {len(bodega_market_map)} active Bodega market configs.")
        except Exception as e:
            log.error(f"Failed to fetch Bodega market configs: {e}. Aborting Bodega arb check.")
            return

        log.info(f"Found {len(manual_pairs)} manual Bodega pairs to check.")
        for b_id, p_id, is_flipped, profit_threshold, end_date_override in manual_pairs:
            try:
                log.info(f"--- Checking Bodega Pair: ID={b_id}, Poly ID={p_id} ---")
                
                pool = bodega_market_map.get(b_id)
                if not pool:
                    log.warning(f"Skipping pair ({b_id}, {p_id}) because Bodega market config was not found.")
                    continue
                
                p_data = p_client.fetch_market(p_id)

                if not p_data.get('active') or p_data.get('closed'):
                    log.warning(f"Skipping pair ({b_id}, {p_id}) because Polymarket market is not active.")
                    continue
                
                market_end_date_ms = pool.get('deadline')
                final_end_date_ms = end_date_override if end_date_override else market_end_date_ms

                bodega_prediction_info = b_client.fetch_prices(b_id)
                order_book_yes, order_book_no = p_data.get('order_book_yes'), p_data.get('order_book_no')
                poly_outcome_name_yes, poly_outcome_name_no = p_data.get('outcome_yes', 'YES'), p_data.get('outcome_no', 'NO')

                if is_flipped:
                    order_book_yes, order_book_no = order_book_no, order_book_yes
                    poly_outcome_name_yes, poly_outcome_name_no = poly_outcome_name_no, poly_outcome_name_yes

                if not all([p_data, bodega_prediction_info, order_book_yes, order_book_no]):
                    log.warning(f"Skipping pair ({b_id}, {p_id}) due to missing data.")
                    continue

                Q_YES, Q_NO = bodega_prediction_info.get("yesVolume_ada", 0), bodega_prediction_info.get("noVolume_ada", 0)
                p_bod_yes = bodega_prediction_info.get("yesPrice_ada")
                if p_bod_yes is None: continue

                inferred_B = infer_b(Q_YES, Q_NO, p_bod_yes)
                pair_opportunities = build_arbitrage_table(Q_YES, Q_NO, order_book_yes, order_book_no, ada_usd, FEE_RATE_BODEGA, inferred_B)
                
                for summary in pair_opportunities:
                    summary['apy'] = calculate_apy(summary.get('roi', 0), final_end_date_ms)
                    
                    if summary.get("profit_usd", 0) > profit_threshold and \
                       summary.get("roi", 0) > 0.05 and \
                       summary.get("apy", 0) >= 0.5:
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

        if not manual_pairs:
            log.info("No manual Myriad pairs to check. Skipping.")
            return

        log.info(f"Found {len(manual_pairs)} manual Myriad pairs to check.")
        for m_slug, p_id, is_flipped, profit_threshold, end_date_override, is_autotrade_safe in manual_pairs:
            try:
                log.info(f"--- Checking Myriad Pair: Slug={m_slug}, Poly ID={p_id}, Flipped={is_flipped}, Autotradeable={is_autotrade_safe} ---")

                m_data = m_client.fetch_market_details(m_slug)
                p_data = p_client.fetch_market(p_id)

                if not all([m_data, p_data]) or m_data.get('state') != 'open' or not p_data.get('active') or p_data.get('closed'):
                    log.warning(f"Skipping pair ({m_slug}, {p_id}) due to inactive/missing market data.")
                    continue
                
                market_expiry_utc = m_data.get("expires_at")
                final_end_date_ms = None
                if end_date_override:
                    final_end_date_ms = end_date_override
                elif market_expiry_utc:
                    try:
                        dt_object = datetime.fromisoformat(market_expiry_utc.replace('Z', '+00:00'))
                        final_end_date_ms = int(dt_object.timestamp() * 1000)
                    except (ValueError, TypeError):
                        log.warning(f"Could not parse Myriad end date: {market_expiry_utc}")

                m_prices = m_client.parse_realtime_prices(m_data)
                if not m_prices:
                    log.warning(f"Could not parse real-time prices for Myriad market {m_slug}, skipping.")
                    continue

                if m_prices['price1'] is None or m_prices['shares1'] is None or m_prices.get('price1_for_b') is None:
                    log.warning(f"Skipping pair for {m_slug} due to missing price/share/price_for_b data in market object.")
                    continue
                
                Q1, Q2 = m_prices['shares1'], m_prices['shares2']
                
                P1_for_b = m_prices['price1_for_b']
                inferred_B = myriad_model.infer_b(Q1, Q2, P1_for_b)
                
                order_book_poly_1, order_book_poly_2 = p_data.get('order_book_yes'), p_data.get('order_book_no')
                
                if is_flipped:
                    order_book_poly_1, order_book_poly_2 = order_book_poly_2, order_book_poly_1
                
                pair_opportunities = build_arbitrage_table_myriad(
                    Q1, Q2, order_book_poly_1, order_book_poly_2, 
                    FEE_RATE_MYRIAD_BUY, inferred_B,
                    P1_MYR_REALTIME=m_prices['price1']
                )

                for summary in pair_opportunities:
                    summary['apy'] = calculate_apy(summary.get('roi', 0), final_end_date_ms)
                    
                    if is_flipped:
                        current_poly_side = summary['polymarket_side']
                        summary['polymarket_side'] = 2 if current_poly_side == 1 else 1

                    if summary.get("profit_usd", 0) > profit_threshold and \
                       summary.get("roi", 0) > 0.05 and \
                       summary.get("apy", 0) >= 5:
                        summary['myriad_side_title'] = m_prices['title1'] if summary['myriad_side'] == 1 else m_prices['title2']
                        summary['polymarket_side_title'] = p_data['outcome_yes'] if summary['polymarket_side'] == 1 else p_data['outcome_no']
                        pair_desc = f"{m_data['title']} <-> {p_data['question']}"
                        opportunities.append((pair_desc, summary, m_slug, p_id))

                        if is_autotrade_safe:
                            try:
                                polymarket_token_id_buy = None
                                polymarket_limit_price = None
                                if summary['polymarket_side'] == 1 and p_data.get('order_book_yes'):
                                    polymarket_token_id_buy = p_data.get('token_id_yes')
                                    polymarket_limit_price = p_data['order_book_yes'][0][0]
                                elif summary['polymarket_side'] == 2 and p_data.get('order_book_no'):
                                    polymarket_token_id_buy = p_data.get('token_id_no')
                                    polymarket_limit_price = p_data['order_book_no'][0][0]

                                if not polymarket_token_id_buy or not polymarket_limit_price:
                                    log.warning(f"Could not determine Polymarket token ID or limit price for autotrade on {m_slug}. Skipping queue.")
                                    continue

                                opportunity_message = {
                                    "opportunity_id": str(uuid.uuid4()),
                                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                                    "platform": "Myriad",
                                    "market_identifiers": {
                                        "myriad_slug": m_slug,
                                        "myriad_market_id": m_data.get('id'),
                                        "polymarket_condition_id": p_id,
                                        "polymarket_token_id_buy": polymarket_token_id_buy,
                                        "is_flipped": bool(is_flipped)
                                    },
                                    "market_details": {
                                        "myriad_title": m_data.get('title'),
                                        "polymarket_question": p_data.get('question'),
                                        "market_expiry_utc": market_expiry_utc
                                    },
                                    "trade_plan": {
                                        "direction": summary.get('direction'),
                                        "myriad_side_to_buy": summary.get('myriad_side'),
                                        "polymarket_side_to_buy": summary.get('polymarket_side'),
                                        "myriad_shares_to_buy": summary.get('myriad_shares'),
                                        "estimated_myriad_cost_usd": summary.get('cost_myr_usd'),
                                        "polymarket_shares_to_buy": summary.get('polymarket_shares'),
                                        "polymarket_limit_price": polymarket_limit_price,
                                        "estimated_polymarket_cost_usd": summary.get('cost_poly_usd')
                                    },
                                    "profitability_metrics": {
                                        "estimated_profit_usd": summary.get('profit_usd'),
                                        "roi": summary.get('roi'),
                                        "apy": summary.get('apy')
                                    },
                                    "amm_parameters": {
                                        "myriad_q1": Q1,
                                        "myriad_q2": Q2,
                                        "myriad_inferred_b": inferred_B
                                    }
                                }
                                add_arb_opportunity(opportunity_message)
                            except Exception as e:
                                log.error(f"Failed to build and queue autotrade opportunity for {m_slug}: {e}", exc_info=True)
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