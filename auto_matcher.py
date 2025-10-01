# auto_matcher.py
import logging
import uuid
import json
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, timezone
import requests
import math

from config import b_client, m_client, p_client, fx_client, notifier, FEE_RATE_BODEGA, FEE_RATE_MYRIAD_BUY, log, myriad_account, myriad_contract, POLYMARKET_PROXY_ADDRESS
from jobs.fetch_new_bodega import fetch_and_notify_new_bodega
from jobs.fetch_new_myriad import fetch_and_notify_new_myriad
from jobs.prune_inactive_pairs import prune_all_inactive_pairs
from streamlit_app.db import (
    load_manual_pairs, load_manual_pairs_myriad,
    save_polymarkets,
    load_probability_watches, delete_probability_watch,
    get_config_value, save_myriad_markets,
    add_arb_opportunity, get_market_cooldown, update_market_cooldown
)
from matching.fuzzy import fetch_all_polymarket_clob_markets
from services.polymarket.model import build_arbitrage_table, infer_b
from services.myriad.model import build_arbitrage_table_myriad, calculate_sell_revenue, consume_order_book
import services.myriad.model as myriad_model

# --- POSITION TRACKING HELPERS ---
def get_myriad_positions(myriad_market_map: dict) -> dict:
    """ 
    Fetches current Myriad positions for all manually matched markets.
    OPTIMIZED: This function no longer makes API calls. It uses the pre-fetched
    market data map to find market IDs.
    """
    if not myriad_account or not myriad_contract:
        log.warning("Myriad account or contract not available for position check.")
        return {}
    
    positions = {}
    manual_pairs = load_manual_pairs_myriad()
    
    # Create a simple lookup map of slug -> market_id from the pre-fetched data.
    slug_to_id_map = {slug: data.get('id') for slug, data in myriad_market_map.items() if data.get('id')}

    market_ids_to_check = {slug_to_id_map.get(pair[0]) for pair in manual_pairs if slug_to_id_map.get(pair[0])}

    for market_id in market_ids_to_check:
        try:
            _liquidity, outcomes = myriad_contract.functions.getUserMarketShares(market_id, myriad_account.address).call()
            # Shares are scaled by 1e6
            shares_outcome_0 = outcomes[0] / 1e6
            shares_outcome_1 = outcomes[1] / 1e6
            if shares_outcome_0 > 1 or shares_outcome_1 > 1: # Threshold to ignore dust
                positions[market_id] = {0: shares_outcome_0, 1: shares_outcome_1}
        except Exception as e:
            log.error(f"Failed to get Myriad shares for market {market_id}: {e}")
    return positions


def get_poly_positions() -> dict:
    """ Fetches current Polymarket positions from the data API. """
    if not POLYMARKET_PROXY_ADDRESS:
        log.warning("Polymarket proxy address not available for position check.")
        return {}
    
    positions = {}
    try:
        url = "https://data-api.polymarket.com/positions"
        params = {"user": POLYMARKET_PROXY_ADDRESS, "sizeThreshold": 1}
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        
        for pos in response.json():
            positions[pos['conditionId']] = positions.get(pos['conditionId'], {})
            positions[pos['conditionId']][pos['outcome']] = float(pos['size'])
            
    except Exception as e:
        log.error(f"Failed to fetch Polymarket positions: {e}")
    return positions


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
    log.info("--- Starting MYRIAD arbitrage-check job (with SELL check) ---")
    try:
        manual_pairs = load_manual_pairs_myriad()
        opportunities = []

        if not manual_pairs:
            log.info("No manual Myriad pairs to check. Skipping.")
            return

        # --- OPTIMIZATION: Pre-fetch all Myriad market data once to reduce API calls ---
        try:
            all_myriad_markets = m_client.fetch_markets()
            myriad_market_map = {m['slug']: m for m in all_myriad_markets}
            log.info(f"Pre-fetched {len(myriad_market_map)} Myriad market configs for arb check.")
        except Exception as e:
            log.error(f"Failed to pre-fetch Myriad markets for arb check: {e}. Aborting.")
            return

        # Fetch current positions once, using the pre-fetched market data to avoid new API calls.
        myriad_positions = get_myriad_positions(myriad_market_map)
        poly_positions = get_poly_positions()
        log.info(f"Found {len(myriad_positions)} Myriad market positions and {len(poly_positions)} Polymarket market positions.")


        log.info(f"Found {len(manual_pairs)} manual Myriad pairs to check.")
        for m_slug, p_id, is_flipped, profit_threshold, end_date_override, is_autotrade_safe in manual_pairs:
            try:
                # Use pre-fetched data instead of making a new API call
                m_data = myriad_market_map.get(m_slug)

                # ==========================================================
                # 1. EARLY EXIT (SELL) CHECK
                # ==========================================================
                if not m_data or m_data.get('state') != 'open':
                    log.info(f"Myriad market {m_slug} is not open, skipping sell check.")
                    continue
                
                myriad_market_id = m_data.get('id')
                myr_pos = myriad_positions.get(myriad_market_id, {})
                poly_pos = poly_positions.get(p_id, {})

                if myr_pos and poly_pos:
                    log.info(f"Positions found for pair ({m_slug}, {p_id}). Checking for early exit.")
                    p_data_sell = p_client.fetch_market(p_id)
                    
                    myr_s0, myr_s1 = myr_pos.get(0, 0), myr_pos.get(1, 0)
                    poly_s_yes, poly_s_no = poly_pos.get(p_data_sell['outcome_yes'], 0), poly_pos.get(p_data_sell['outcome_no'], 0)
                    
                    paired_position = None
                    if is_flipped:
                        if myr_s0 > 0 and poly_s_yes > 0: paired_position = {'myr_outcome': 0, 'myr_shares': myr_s0, 'poly_outcome_name': p_data_sell['outcome_yes'], 'poly_shares': poly_s_yes, 'poly_token': p_data_sell['token_id_yes'], 'poly_book': p_data_sell['order_book_yes_bids']}
                        if myr_s1 > 0 and poly_s_no > 0: paired_position = {'myr_outcome': 1, 'myr_shares': myr_s1, 'poly_outcome_name': p_data_sell['outcome_no'], 'poly_shares': poly_s_no, 'poly_token': p_data_sell['token_id_no'], 'poly_book': p_data_sell['order_book_no_bids']}
                    else:
                        if myr_s0 > 0 and poly_s_no > 0: paired_position = {'myr_outcome': 0, 'myr_shares': myr_s0, 'poly_outcome_name': p_data_sell['outcome_no'], 'poly_shares': poly_s_no, 'poly_token': p_data_sell['token_id_no'], 'poly_book': p_data_sell['order_book_no_bids']}
                        if myr_s1 > 0 and poly_s_yes > 0: paired_position = {'myr_outcome': 1, 'myr_shares': myr_s1, 'poly_outcome_name': p_data_sell['outcome_yes'], 'poly_shares': poly_s_yes, 'poly_token': p_data_sell['token_id_yes'], 'poly_book': p_data_sell['order_book_yes_bids']}

                    if paired_position:
                        min_shares = min(paired_position['myr_shares'], paired_position['poly_shares'])
                        shares_to_sell = min(min_shares, 10.0)
                        
                        m_prices = m_client.parse_realtime_prices(m_data)
                        if not m_prices: continue # Skip if prices can't be parsed
                        
                        q1, q2, b = m_prices['shares1'], m_prices['shares2'], m_prices['liquidity']
                        
                        myr_revenue = calculate_sell_revenue(q1, q2, b, shares_to_sell) if paired_position['myr_outcome'] == 0 else calculate_sell_revenue(q2, q1, b, shares_to_sell)
                        
                        _f, poly_revenue, _p = consume_order_book(paired_position['poly_book'], shares_to_sell)
                        total_revenue = myr_revenue + poly_revenue
                        
                        log.info(f"[SELL CHECK] Pair ({m_slug}, {p_id}): min_shares={min_shares:.2f}, shares_to_sell={shares_to_sell:.2f}, Myriad revenue=${myr_revenue:.2f}, Poly revenue=${poly_revenue:.2f}, Total=${total_revenue:.2f}")

                        if total_revenue > (shares_to_sell * 1.03) and total_revenue > min_shares:
                            log.warning(f"Found profitable early exit for {m_slug}! Total revenue for {shares_to_sell} shares is ${total_revenue:.2f}.")
                            sell_opp = {
                                "type": "sell", "opportunity_id": str(uuid.uuid4()), "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                                "market_identifiers": {"myriad_slug": m_slug, "myriad_market_id": myriad_market_id, "polymarket_condition_id": p_id, "polymarket_token_id_sell": paired_position['poly_token']},
                                "market_details": {"myriad_title": m_data.get('title')},
                                "trade_plan": {"myriad_outcome_id_sell": paired_position['myr_outcome'], "myriad_shares_to_sell": shares_to_sell, "myriad_min_usd_receive": myr_revenue * 0.99, "polymarket_shares_to_sell": shares_to_sell, "polymarket_limit_price": paired_position['poly_book'][0][0] if paired_position['poly_book'] else 0.01},
                                "profitability_metrics": {"estimated_profit_usd": total_revenue - shares_to_sell}
                            }
                            add_arb_opportunity(sell_opp)
                            update_market_cooldown(f"myriad_{m_slug}_sell", datetime.now(timezone.utc).isoformat())

                # ==========================================================
                # 2. ARBITRAGE (BUY) CHECK
                # ==========================================================
                log.info(f"--- Checking Myriad Pair: Slug={m_slug}, Poly ID={p_id}, Flipped={is_flipped}, Autotradeable={is_autotrade_safe} ---")
                
                p_data = p_client.fetch_market(p_id)

                if not p_data.get('active') or p_data.get('closed'):
                    log.warning(f"Skipping BUY check for pair ({m_slug}, {p_id}) because Polymarket market is not active.")
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

                if m_prices.get('price1') is None or m_prices.get('shares1') is None:
                    log.warning(f"Skipping pair for {m_slug} due to missing price/share data in market object.")
                    continue
                
                Q1, Q2 = m_prices['shares1'], m_prices['shares2']
                
                B_param = m_data.get('liquidity')
                if not B_param or B_param <= 0:
                    log.warning(f"Skipping pair for {m_slug} due to invalid or missing 'liquidity' parameter: {B_param}")
                    continue
                
                order_book_poly_1, order_book_poly_2 = p_data.get('order_book_yes'), p_data.get('order_book_no')
                
                if is_flipped:
                    order_book_poly_1, order_book_poly_2 = order_book_poly_2, order_book_poly_1
                
                pair_opportunities = build_arbitrage_table_myriad(Q1, Q2, order_book_poly_1, order_book_poly_2, FEE_RATE_MYRIAD_BUY, B_param, P1_MYR_REALTIME=m_prices['price1'])

                for summary in pair_opportunities:
                    summary['apy'] = calculate_apy(summary.get('roi', 0), final_end_date_ms)
                    
                    if is_flipped:
                        summary['polymarket_side'] = 2 if summary['polymarket_side'] == 1 else 1

                    if summary.get("profit_usd", 0) > profit_threshold and summary.get("roi", 0) > 0.05 and summary.get("apy", 0) >= 5:
                        summary['myriad_current_price'] = m_prices['price1'] if summary['myriad_side'] == 1 else m_prices['price2']
                        summary['poly_current_price'] = (order_book_poly_1[0][0] if summary['polymarket_side'] == 1 and order_book_poly_1 else (order_book_poly_2[0][0] if summary['polymarket_side'] == 2 and order_book_poly_2 else None))
                        summary['myriad_side_title'] = m_prices['title1'] if summary['myriad_side'] == 1 else m_prices['title2']
                        summary['polymarket_side_title'] = p_data['outcome_yes'] if summary['polymarket_side'] == 1 else p_data['outcome_no']
                        pair_desc = f"{m_data['title']} <-> {p_data['question']}"
                        opportunities.append((pair_desc, summary, m_slug, p_id))

                        if is_autotrade_safe:
                            try:
                                polymarket_token_id_buy = (p_data.get('token_id_yes') if summary['polymarket_side'] == 1 and p_data.get('order_book_yes') else (p_data.get('token_id_no') if summary['polymarket_side'] == 2 and p_data.get('order_book_no') else None))
                                polymarket_limit_price = (p_data['order_book_yes'][0][0] if summary['polymarket_side'] == 1 and p_data.get('order_book_yes') else (p_data['order_book_no'][0][0] if summary['polymarket_side'] == 2 and p_data.get('order_book_no') else None))

                                if not polymarket_token_id_buy or not polymarket_limit_price:
                                    log.warning(f"Could not determine Polymarket token ID or limit price for autotrade on {m_slug}. Skipping queue.")
                                    continue

                                opportunity_message = {
                                    "type": "buy", "opportunity_id": str(uuid.uuid4()), "timestamp_utc": datetime.now(timezone.utc).isoformat(), "platform": "Myriad",
                                    "market_identifiers": {"myriad_slug": m_slug, "myriad_market_id": m_data.get('id'), "polymarket_condition_id": p_id, "polymarket_token_id_buy": polymarket_token_id_buy, "is_flipped": bool(is_flipped)},
                                    "market_details": {"myriad_title": m_data.get('title'), "polymarket_question": p_data.get('question'), "market_expiry_utc": market_expiry_utc},
                                    "trade_plan": {"direction": summary.get('direction'), "myriad_side_to_buy": summary.get('myriad_side'), "polymarket_side_to_buy": summary.get('polymarket_side'), "myriad_shares_to_buy": summary.get('myriad_shares'), "estimated_myriad_cost_usd": summary.get('cost_myr_usd'), "polymarket_shares_to_buy": summary.get('polymarket_shares'), "polymarket_limit_price": polymarket_limit_price, "estimated_polymarket_cost_usd": summary.get('cost_poly_usd')},
                                    "profitability_metrics": {"estimated_profit_usd": summary.get('profit_usd'), "roi": summary.get('roi'), "apy": summary.get('apy')},
                                    "amm_parameters": {"myriad_q1": Q1, "myriad_q2": Q2, "myriad_liquidity": B_param}
                                }
                                add_arb_opportunity(opportunity_message)
                            except Exception as e:
                                log.error(f"Failed to build and queue autotrade opportunity for {m_slug}: {e}", exc_info=True)
            except Exception as e:
                log.error(f"Myriad arb check for pair ({m_slug}, {p_id}) failed: {e}", exc_info=True)

        if notifier and opportunities:
            for pair, summary, m_slug, p_id in opportunities:
                notifier.notify_arb_opportunity_myriad(pair, summary, m_slug, p_id)
        
        log.info(f"Myriad arb check finished. Found {len(opportunities)} BUY opportunities.")

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