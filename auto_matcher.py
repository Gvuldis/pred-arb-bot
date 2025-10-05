import logging
import uuid
import json
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, timezone, timedelta
import requests
import math
import time

from config import b_client, m_client, p_client, fx_client, notifier, FEE_RATE_BODEGA, myriad_account, myriad_contract, POLYMARKET_PROXY_ADDRESS
from jobs.fetch_new_bodega import fetch_and_notify_new_bodega
from jobs.fetch_new_myriad import fetch_and_notify_new_myriad
from jobs.prune_inactive_pairs import prune_all_inactive_pairs
from streamlit_app.db import (
    load_manual_pairs, load_manual_pairs_myriad,
    save_polymarkets,
    load_probability_watches, delete_probability_watch,
    get_config_value, save_myriad_markets, load_myriad_markets,
    add_arb_opportunity, get_market_cooldown, update_market_cooldown
)
from matching.fuzzy import fetch_all_polymarket_clob_markets
from services.polymarket.model import build_arbitrage_table, infer_b
from services.myriad.model import build_arbitrage_table_myriad, calculate_sell_revenue, consume_order_book
import services.myriad.model as myriad_model

# Get a logger for this specific module
log = logging.getLogger(__name__)

# --- CACHING (OPTIMIZATION) ---
_myriad_positions_cache = {}
_poly_positions_cache = {}
_ada_usd_price_cache = 0.85  # Initialize with a reasonable fallback
_myriad_positions_last_updated = 0
_poly_positions_last_updated = 0
_ada_usd_price_last_updated = 0
POSITION_CACHE_TTL_SECONDS = 60 # Update portfolio positions every 60 seconds
FX_CACHE_TTL_SECONDS = 60       # Update ADA price every 60 seconds

# --- HELPER FUNCTIONS ---
def get_cached_ada_usd() -> float:
    """Fetches ADA/USD price from the client, with caching."""
    global _ada_usd_price_cache, _ada_usd_price_last_updated
    now = time.time()
    if now - _ada_usd_price_last_updated > FX_CACHE_TTL_SECONDS:
        log.info("ADA/USD price cache expired. Fetching fresh data from FX client.")
        _ada_usd_price_cache = fx_client.get_ada_usd()
        _ada_usd_price_last_updated = now
    else:
        log.info(f"Using cached ADA/USD price: ${_ada_usd_price_cache}")
    return _ada_usd_price_cache

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
    try:
        end_date_ms = int(end_date_ms)
    except (ValueError, TypeError):
        end_date_ms = 0

    if not end_date_ms or roi <= 0:
        return 0.0

    now_utc = datetime.now(timezone.utc)
    end_date_utc = datetime.fromtimestamp(end_date_ms / 1000, tz=timezone.utc)
    
    time_to_expiry = end_date_utc - now_utc
    days_to_expiry = time_to_expiry.total_seconds() / (24 * 3600)

    if days_to_expiry <= 0.01:
        return 0.0
    
    apy = (roi / days_to_expiry) * 365
    return apy

def fetch_and_save_markets():
    """Fetches and saves markets for all platforms, including on-chain Myriad fees."""
    log.info("Starting job to fetch and save all platform markets...")
    try:
        # Polymarket
        fresh_poly_markets = fetch_all_polymarket_clob_markets()
        if fresh_poly_markets:
            save_polymarkets(fresh_poly_markets)
            log.info(f"Saved/updated {len(fresh_poly_markets)} Polymarket markets.")
        else:
            log.info("No active Polymarket markets found from API.")
        
        # Myriad (this now includes fetching on-chain fees)
        fresh_myriad_markets = m_client.fetch_markets()
        if fresh_myriad_markets:
            save_myriad_markets(fresh_myriad_markets)
            log.info(f"Saved/updated {len(fresh_myriad_markets)} Myriad markets (with fees).")
        else:
            log.info("No active Myriad markets found from API.")
            
    except Exception as e:
        log.error(f"Failed to fetch and save all markets: {e}", exc_info=True)


def run_bodega_arb_check(pairs_to_check: list):
    """Checks for arbitrage opportunities in a given list of Bodega-Polymarket pairs."""
    log.info(f"--- Running BODEGA arbitrage-check job for {len(pairs_to_check)} pairs ---")
    try:
        ada_usd = get_cached_ada_usd() # Use cached price
        opportunities = []

        if not pairs_to_check:
            log.info("No Bodega pairs in this segment to check. Skipping.")
            return

        try:
            all_bodega_markets = b_client.fetch_markets()
            bodega_market_map = {m['id']: m for m in all_bodega_markets}
            log.info(f"Fetched {len(bodega_market_map)} active Bodega market configs for segment check.")
        except Exception as e:
            log.error(f"Failed to fetch Bodega market configs: {e}. Aborting Bodega arb check for this segment.")
            return

        for b_id, p_id, is_flipped, profit_threshold, end_date_override in pairs_to_check:
            try:
                profit_threshold = float(profit_threshold)
                
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
                       summary.get("roi", 0) > 0.02 and \
                       summary.get("apy", 0) >= 5:
                        summary['polymarket_side'] = poly_outcome_name_yes if summary['polymarket_side'] == 'YES' else poly_outcome_name_no
                        pair_desc = f"{pool['name']} <-> {p_data['question']}"
                        opportunities.append((pair_desc, summary, b_id, p_id))

            except Exception as e:
                log.error(f"Bodega arb check for pair ({b_id}, {p_id}) failed: {e}", exc_info=True)

        if notifier and opportunities:
            for pair, summary, b_id, p_id in opportunities:
                notifier.notify_arb_opportunity(pair, summary, b_id, p_id, b_client.api_url)
        
        log.info(f"Bodega arb check for segment finished. Found {len(opportunities)} opportunities.")

    except Exception as e:
        log.error(f"Bodega arbitrage check job for segment failed entirely: {e}", exc_info=True)

def run_myriad_arb_check(pairs_to_check: list):
    """Checks for arbitrage opportunities in a given list of Myriad-Polymarket pairs."""
    global _myriad_positions_cache, _poly_positions_cache, _myriad_positions_last_updated, _poly_positions_last_updated

    log.info(f"--- Running MYRIAD arbitrage-check job for {len(pairs_to_check)} pairs ---")
    try:
        opportunities = []

        if not pairs_to_check:
            log.info("No Myriad pairs in this segment to check. Skipping.")
            return

        all_myriad_markets_db = load_myriad_markets()
        if not all_myriad_markets_db:
            log.warning("Myriad markets not found in local DB. Run the fetch job or wait for it to run.")
            return
        myriad_market_map_raw = {m['slug']: m for m in all_myriad_markets_db}
        log.info(f"Loaded {len(myriad_market_map_raw)} Myriad markets from DB cache for arb check.")
        
        now = time.time()
        if now - _myriad_positions_last_updated > POSITION_CACHE_TTL_SECONDS:
            log.info("Myriad positions cache expired. Fetching fresh data.")
            myriad_market_map_simple = {m['slug']: {'id': m['id']} for m in all_myriad_markets_db}
            _myriad_positions_cache = get_myriad_positions(myriad_market_map_simple)
            _myriad_positions_last_updated = now
        else:
            log.info("Using cached Myriad positions.")
        myriad_positions = _myriad_positions_cache

        if now - _poly_positions_last_updated > POSITION_CACHE_TTL_SECONDS:
            log.info("Polymarket positions cache expired. Fetching fresh data.")
            _poly_positions_cache = get_poly_positions()
            _poly_positions_last_updated = now
        else:
            log.info("Using cached Polymarket positions.")
        poly_positions = _poly_positions_cache
        
        log.info(f"Found {len(myriad_positions)} Myriad market positions and {len(poly_positions)} Polymarket market positions.")

        for m_slug, p_id, is_flipped, profit_threshold, end_date_override, is_autotrade_safe in pairs_to_check:
            try:
                profit_threshold = float(profit_threshold)

                m_data_raw = myriad_market_map_raw.get(m_slug)
                if not m_data_raw or not m_data_raw['full_data_json']:
                    log.warning(f"Market data for '{m_slug}' not found or incomplete in DB cache. Skipping.")
                    continue
                
                m_data = json.loads(m_data_raw['full_data_json'])

                if m_data.get('state') != 'open':
                    log.info(f"Myriad market {m_slug} is not 'open', skipping all checks for this pair.")
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
                        shares_to_sell = min_shares - 0.5
                        
                        if shares_to_sell < 1.0:
                            continue

                        m_prices = m_client.parse_realtime_prices(m_data)
                        if not m_prices:
                             log.warning(f"Could not parse real-time prices for Myriad SELL check on {m_slug}, skipping.")
                             continue
                        
                        q1, q2, b = m_prices['shares1'], m_prices['shares2'], m_prices['liquidity']
                        
                        myr_revenue = calculate_sell_revenue(q1, q2, b, shares_to_sell) if paired_position['myr_outcome'] == 0 else calculate_sell_revenue(q2, q1, b, shares_to_sell)
                        
                        _f, poly_revenue, _p = consume_order_book(paired_position['poly_book'], shares_to_sell)
                        total_revenue = myr_revenue + poly_revenue
                        
                        log.info(f"[SELL CHECK] Pair ({m_slug}, {p_id}): min_shares={min_shares:.2f}, shares_to_sell={shares_to_sell:.2f}, Myriad revenue=${myr_revenue:.2f}, Poly revenue=${poly_revenue:.2f}, Total=${total_revenue:.2f}")

                        if total_revenue > (shares_to_sell * 1.015):
                            log.warning(f"Found profitable early exit for {m_slug}! Total revenue for {shares_to_sell} shares is ${total_revenue:.2f}.")
                            sell_opp = {
                                "type": "sell", "opportunity_id": str(uuid.uuid4()), "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                                "market_identifiers": {"myriad_slug": m_slug, "myriad_market_id": myriad_market_id, "polymarket_condition_id": p_id, "polymarket_token_id_sell": paired_position['poly_token']},
                                "market_details": {"myriad_title": m_data.get('title')},
                                "trade_plan": {"myriad_outcome_id_sell": paired_position['myr_outcome'], "myriad_shares_to_sell": shares_to_sell, "myriad_min_usd_receive": myr_revenue * 0.99, "polymarket_shares_to_sell": shares_to_sell, "polymarket_limit_price": paired_position['poly_book'][0][0] if paired_position['poly_book'] else 0.01},
                                "profitability_metrics": {"estimated_profit_usd": total_revenue - shares_to_sell},
                                "amm_parameters": {"myriad_q1": q1, "myriad_q2": q2, "myriad_liquidity": b}
                            }
                            add_arb_opportunity(sell_opp)

                log.info(f"--- Checking Myriad Pair: Slug={m_slug}, Poly ID={p_id} ---")
                
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
                
                market_fee = m_data.get('fee')
                if market_fee is None:
                    log.warning(f"Fee not found in DB for Myriad market {m_slug}, skipping.")
                    continue

                m_prices = m_client.parse_realtime_prices(m_data)
                if not m_prices:
                    log.warning(f"Could not parse real-time prices for Myriad market {m_slug}, skipping.")
                    continue
                
                poly_price_yes = p_data['order_book_yes'][0][0] if p_data.get('order_book_yes') else None
                poly_price_no = p_data['order_book_no'][0][0] if p_data.get('order_book_no') else None
                
                myr_p0_title = m_prices['title1']
                myr_p1_title = m_prices['title2']
                poly_p_yes_title = p_data['outcome_yes']
                poly_p_no_title = p_data['outcome_no']

                if is_flipped:
                    log.info(f"PRICES (Flipped): Myriad '{myr_p0_title}' @ {m_prices['price1']:.4f} vs Poly '{poly_p_no_title}' @ {poly_price_no if poly_price_no else 'N/A'}")
                    log.info(f"PRICES (Flipped): Myriad '{myr_p1_title}' @ {m_prices['price2']:.4f} vs Poly '{poly_p_yes_title}' @ {poly_price_yes if poly_price_yes else 'N/A'}")
                else:
                    log.info(f"PRICES: Myriad '{myr_p0_title}' @ {m_prices['price1']:.4f} vs Poly '{poly_p_yes_title}' @ {poly_price_yes if poly_price_yes else 'N/A'}")
                    log.info(f"PRICES: Myriad '{myr_p1_title}' @ {m_prices['price2']:.4f} vs Poly '{poly_p_no_title}' @ {poly_price_no if poly_price_no else 'N/A'}")

                Q1, Q2 = m_prices['shares1'], m_prices['shares2']
                
                B_param = m_data.get('liquidity')
                if not B_param or B_param <= 0:
                    log.warning(f"Skipping pair for {m_slug} due to invalid or missing 'liquidity' parameter: {B_param}")
                    continue
                
                order_book_poly_1, order_book_poly_2 = p_data.get('order_book_yes'), p_data.get('order_book_no')
                
                if is_flipped:
                    order_book_poly_1, order_book_poly_2 = order_book_poly_2, order_book_poly_1
                
                pair_opportunities = build_arbitrage_table_myriad(Q1, Q2, order_book_poly_1, order_book_poly_2, market_fee, B_param, P1_MYR_REALTIME=m_prices['price1'])

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
                                    "market_details": {"myriad_title": m_data.get('title'), "polymarket_question": p_data.get('question'), "market_expiry_utc": market_expiry_utc, "market_fee": market_fee},
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
        
        log.info(f"Myriad arb check for segment finished. Found {len(opportunities)} BUY opportunities.")

    except Exception as e:
        log.error(f"Myriad arbitrage check job for segment failed entirely: {e}", exc_info=True)


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

def setup_market_check_jobs(scheduler, platform: str):
    """
    Dynamically creates two tiers of jobs: high-priority and normal-priority.
    """
    platform_lower = platform.lower()
    log.info(f"Setting up new scheduler jobs for {platform.upper()}...")

    for job in scheduler.get_jobs():
        if job.id.startswith(f"{platform_lower}_arb_check_job_"):
            scheduler.remove_job(job.id)

    if platform_lower == 'myriad':
        all_pairs = load_manual_pairs_myriad()
        market_data = {m['slug']: m for m in load_myriad_markets()}
        check_function = run_myriad_arb_check
    elif platform_lower == 'bodega':
        all_pairs = load_manual_pairs()
        market_data = {m['id']: m for m in b_client.fetch_markets(force_refresh=True)}
        check_function = run_bodega_arb_check
    else:
        return

    if not all_pairs:
        log.info(f"No manual pairs found for {platform.upper()}. No jobs scheduled.")
        return

    hp_threshold_hours = float(get_config_value(f'{platform_lower}_high_priority_threshold_hours', '10'))
    priority_cutoff_time = datetime.now(timezone.utc) + timedelta(hours=hp_threshold_hours)
    
    high_priority_pairs = []
    normal_priority_pairs = []

    for pair in all_pairs:
        market_key = pair[0]
        data = market_data.get(market_key)
        if not data: continue

        expires_at_str = data.get('expires_at') if platform_lower == 'myriad' else None
        deadline_ms = data.get('deadline') if platform_lower == 'bodega' else None
        market_end_time = None
        try:
            if expires_at_str: market_end_time = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
            elif deadline_ms: market_end_time = datetime.fromtimestamp(deadline_ms / 1000, tz=timezone.utc)
        except (ValueError, TypeError): pass

        if market_end_time and market_end_time < priority_cutoff_time:
            high_priority_pairs.append(pair)
        else:
            normal_priority_pairs.append(pair)
    
    log.info(f"[{platform.upper()}] Prioritization complete. High-priority: {len(high_priority_pairs)}, Normal-priority: {len(normal_priority_pairs)}.")

    tiers = {'high_priority': high_priority_pairs, 'normal_priority': normal_priority_pairs}

    for tier_name, pairs in tiers.items():
        if not pairs: continue
        
        is_hp = tier_name == 'high_priority'
        segments = int(get_config_value(f'{platform_lower}_{tier_name}_segments', '3' if is_hp else '1'))
        interval = int(get_config_value(f'{platform_lower}_{tier_name}_interval_seconds', '15' if is_hp else '90'))
        
        # --- SAFETY CHECK: Allow 5-second interval ---
        if segments <= 0 or interval < 5:
            log.error(f"Invalid config for {platform.upper()} {tier_name}. Skipping.")
            continue

        pair_segments = [pairs[i::segments] for i in range(segments)]
        stagger_delay = interval / segments

        for i, segment_list in enumerate(pair_segments):
            if not segment_list: continue
            job_id = f"{platform_lower}_arb_check_job_{tier_name}_segment_{i}"
            scheduler.add_job(
                check_function, "interval", seconds=interval, id=job_id, args=[segment_list],
                next_run_time=datetime.now(timezone.utc) + timedelta(seconds=i * stagger_delay),
                misfire_grace_time=30
            )
            log.info(f"Scheduled job '{job_id}' ({len(segment_list)} pairs) to run every {interval}s.")

def reschedule_all_jobs(scheduler):
    """Periodically re-evaluates market priorities and reschedules all jobs."""
    log.info("--- Periodic Reschedule Triggered ---")
    setup_market_check_jobs(scheduler, "myriad")
    setup_market_check_jobs(scheduler, "bodega")
    log.info("--- Periodic Reschedule Complete ---")


if __name__ == "__main__":
    sched = BlockingScheduler(timezone="UTC")

    log.info("Running initial jobs on startup...")
    fetch_and_notify_new_bodega()
    fetch_and_notify_new_myriad()
    fetch_and_save_markets()
    run_prob_watch_check()
    prune_all_inactive_pairs()
    log.info("Initial jobs complete.")

    sched.add_job(fetch_and_notify_new_bodega, "cron", minute="*/15")
    sched.add_job(fetch_and_notify_new_myriad, "cron", minute="*/15")
    sched.add_job(fetch_and_save_markets, "cron", minute="*/15")
    sched.add_job(prune_all_inactive_pairs, "cron", hour="*")
    sched.add_job(run_prob_watch_check, "interval", minutes=3, id="prob_watch_job")
    
    reschedule_all_jobs(sched)
    sched.add_job(reschedule_all_jobs, "interval", minutes=5, args=[sched])

    log.info(f"Scheduler started with dynamic, two-tiered configuration.")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")
