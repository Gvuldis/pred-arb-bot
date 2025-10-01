import os
import math
import logging
import json
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from decimal import Decimal, ROUND_DOWN
import threading
import requests

# --- Web3 and Clob Client Imports ---
from web3 import Web3
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

# --- Local Project Imports ---
from config import m_client, p_client, notifier, log, FEE_RATE_MYRIAD_BUY, myriad_account
import streamlit_app.db as db
import services.myriad.model as myriad_model

# ==============================================================================
# 1. CONFIGURATION AND SETUP
# ==============================================================================

# --- Load Environment Variables ---
load_dotenv()

# --- Trader Configuration ---
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "DRY_RUN")
LIMITED_LIVE_CAP_USD = float(os.getenv("LIMITED_LIVE_CAP_USD", "10.0"))
MIN_PROFIT_USD = float(os.getenv("MIN_PROFIT_USD", "5.00"))
MIN_ROI = float(os.getenv("MIN_ROI", "0.05"))
MIN_APY = float(os.getenv("MIN_APY", "5"))

# --- Safety Parameters ---
MIN_ETH_BALANCE = 0.0003
MARKET_EXPIRY_BUFFER_MINUTES = 10
TRADE_COOLDOWN_MINUTES = 1
CAPITAL_SAFETY_BUFFER_USD = 10.0 # New safety buffer

# --- On-Chain Configuration ---
ABSTRACT_RPC_URL = os.getenv("ABSTRACT_RPC_URL")
POLY_PVT_KEY = os.getenv("POLYMARKET_PRIVATE_KEY")
POLY_PROXY_ADDRESS = os.getenv("POLYMARKET_PROXY_ADDRESS")
MYRIAD_PVT_KEY = os.getenv("MYRIAD_PRIVATE_KEY")

if not all([ABSTRACT_RPC_URL, POLY_PVT_KEY, MYRIAD_PVT_KEY, POLY_PROXY_ADDRESS]):
    raise ValueError(
        "Required environment variables are missing. "
        "Check .env file for: ABSTRACT_RPC_URL, POLYMARKET_PRIVATE_KEY, MYRIAD_PRIVATE_KEY, POLYMARKET_PROXY_ADDRESS"
    )

# --- Contract Addresses & ABIs (Myriad ONLY) ---
ABSTRACT_USDC_ADDRESS = "0x84a71ccd554cc1b02749b35d22f684cc8ec987e1"
MYRIAD_MARKET_ADDRESS = "0x3e0f5F8F5FB043aBFA475C0308417Bf72c463289"
ERC20_ABI = json.loads('[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"},{"name":"_spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]')
MYRIAD_MARKET_ABI = json.loads('[{"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"},{"internalType":"uint256","name":"outcomeId","type":"uint256"},{"internalType":"uint256","name":"minOutcomeSharesToBuy","type":"uint256"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"buy","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"},{"internalType":"uint256","name":"outcomeId","type":"uint256"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"maxOutcomeSharesToSell","type":"uint256"}],"name":"sell","outputs":[],"stateMutability":"nonpayable","type":"function"}]')

# --- Client Initialization ---
# Myriad (requires Web3)
w3_abs = Web3(Web3.HTTPProvider(ABSTRACT_RPC_URL))

TWO_DP   = Decimal("0.01")
FOUR_DP  = Decimal("0.0001")
DEFAULT_TICK = Decimal("0.01")
DEFAULT_STEP = Decimal("0.0001")

def _decimals_from_tick(tick: Decimal) -> int:
    return max(0, -tick.as_tuple().exponent)

def normalize_buy_args(price: float, size: float,
                       tick: Decimal = DEFAULT_TICK,
                       step: Decimal = DEFAULT_STEP):
    P = Decimal(str(price))
    S = Decimal(str(size))
    P = ((P // tick) * tick).quantize(tick, rounding=ROUND_DOWN)
    S = ((S // step) * step).quantize(FOUR_DP, rounding=ROUND_DOWN)
    p = _decimals_from_tick(tick)
    price_units = int((P * (10 ** p)).to_integral_value(rounding=ROUND_DOWN))
    shares_units = int((S * 10_000).to_integral_value(rounding=ROUND_DOWN))
    modulus = 10 ** (p + 4 - 2)
    need_multiple = modulus // math.gcd(price_units, modulus)
    shares_units_adj = (shares_units // need_multiple) * need_multiple
    if shares_units_adj == 0 or price_units == 0:
        return 0.0, 0.0, 0.0
    S_adj = Decimal(shares_units_adj) / Decimal(10_000)
    maker = (P * S_adj).quantize(TWO_DP, rounding=ROUND_DOWN)
    S_adj = S_adj.quantize(FOUR_DP, rounding=ROUND_DOWN)
    return float(P), float(S_adj), float(maker)

# Polymarket (py-clob-client)
clob_client = ClobClient(
    host="https://clob.polymarket.com",
    key=POLY_PVT_KEY,
    chain_id=137,
    funder=POLY_PROXY_ADDRESS,
    signature_type=2
)
clob_client.set_api_creds(clob_client.create_or_derive_api_creds())

log.info(f"Unified Executor initialized. EXECUTION_MODE: {EXECUTION_MODE}")
log.info(f"Using Polymarket proxy address: {POLY_PROXY_ADDRESS}")
log.info(f"Using Myriad/Abstract address: {myriad_account.address}")

# ==============================================================================
# 2. ON-CHAIN INTERACTION & POST-TRADE VERIFICATION
# ==============================================================================

def find_myriad_trade_details(market_id: int, expected_cost: float, myriad_address: str, trade_id: str, market_title: str):
    """
    Polls the Myriad market feed API to find trade details. Runs in a thread.
    """
    log.info(f"[{trade_id}] Starting Myriad trade lookup for market {market_id}...")
    api_url = f"https://api-production.polkamarkets.com/markets/{market_id}/feed?network_id=274133"
    myriad_address_lower = myriad_address.lower()
    
    for i in range(15):
        log.info(f"[{trade_id}] Attempt {i+1}/15 to fetch Myriad trade details...")
        try:
            response = requests.get(api_url, timeout=15)
            response.raise_for_status()
            
            # FIX: The API response can be a list directly, or a dictionary containing a 'data' key.
            # This handles both cases to prevent the AttributeError.
            json_response = response.json()
            if isinstance(json_response, list):
                feed_data = json_response
            elif isinstance(json_response, dict):
                feed_data = json_response.get("data", [])
            else:
                log.warning(f"[{trade_id}] Myriad feed API returned an unexpected type: {type(json_response)}")
                feed_data = []

            for tx in feed_data:
                tx_address_lower = tx.get("user_address", "").lower()
                tx_action = tx.get("action")
                tx_value = tx.get("value", 0.0)
                
                if (tx_action == "buy" and 
                    tx_address_lower == myriad_address_lower and
                    abs(tx_value - expected_cost) / expected_cost < 0.10):
                    
                    log.info(f"[{trade_id}] Found matching Myriad trade in API feed!")
                    
                    trade_details = {
                        "executed_myriad_shares": tx.get("shares"),
                        "executed_myriad_cost_usd": tx_value,
                        "myriad_api_lookup_status": "SUCCESS"
                    }
                    db.update_trade_log_myriad_details(trade_id, trade_details)
                    if notifier:
                        notifier.notify_myriad_trade_details_found(
                            market_title, trade_id, tx.get("shares"), tx_value
                        )
                    return
        except requests.RequestException as e:
            log.error(f"[{trade_id}] API error during Myriad trade lookup: {e}")
        except Exception as e:
            log.error(f"[{trade_id}] Unexpected error during Myriad trade lookup: {e}", exc_info=True)
            
        time.sleep(30)

    log.critical(f"[{trade_id}] FAILED to find Myriad trade details after 15 attempts for market {market_id}.")
    db.update_trade_log_myriad_status(trade_id, "FAILED")
    if notifier:
        notifier.notify_critical_alert(
            "Manual Check Required: Myriad Trade Lookup Failed",
            f"Could not find Myriad trade details for trade ID `{trade_id}` on market `{market_title}` ({market_id}). "
            f"Please manually verify the trade. Expected cost was ~${expected_cost:.2f}."
        )

# --- POLYGON (POLYMARKET) FUNCTIONS ---
w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
ERC20_ABI_POLY = [{"constant": True, "inputs": [{"name": "_owner","type": "address"}],"name": "balanceOf", "outputs": [{"name": "balance","type": "uint256"}],"type": "function"}]

def get_polygon_usdc_balance() -> float:
    log.info(f"[POLY] Checking Polygon USDC balance for {POLY_PROXY_ADDRESS}...")
    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_CONTRACT), abi=ERC20_ABI_POLY)
    balance = usdc.functions.balanceOf(Web3.to_checksum_address(POLY_PROXY_ADDRESS)).call() / 1e6
    log.info(f"[POLY] Found Polygon USDC balance: {balance:.4f} USDC")
    return float(balance)

def execute_polymarket_buy(token_id: str, price: float, size: float) -> dict:
    px, sz, usd = normalize_buy_args(price, size)
    if sz <= 0 or px <= 0:
        log.warning(f"[POLY] Normalized to zero. price={price}, size={size} -> px={px}, sz={sz}")
        return {'success': True, 'response': {'status': 'skipped_zero_size', 'success': True, 'takingAmount': '0'}}
    log.info(f"[POLY] Executing BUY FAK. Token: {token_id} | price={px:.2f}, size={sz:.4f}, dollars={usd:.2f}")
    try:
        order_args = OrderArgs(price=px, size=sz, side=BUY, token_id=token_id)
        signed_order = clob_client.create_order(order_args)
        resp = clob_client.post_order(signed_order, OrderType.FAK)
        log.info(f"[POLY] FAK Order response: {resp}")
        return {'success': resp.get('success', False), 'response': resp}
    except Exception as e:
        log.error(f"[POLY] FAK Order execution failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e), 'response': {}}

def execute_polymarket_sell(token_id: str, price: float, size: float) -> dict:
    """Executes a FAK sell order on Polymarket."""
    # Price for a sell order is the minimum price we'll accept. To sell into the bid book,
    # we can set the price to the best bid to ensure it fills at that price or better.
    log.info(f"[POLY] Executing SELL FAK. Token: {token_id} | price={price:.2f}, size={size:.4f}")
    try:
        order_args = OrderArgs(price=price, size=size, side=SELL, token_id=token_id)
        signed_order = clob_client.create_order(order_args)
        resp = clob_client.post_order(signed_order, OrderType.FAK)
        log.info(f"[POLY] FAK Sell Order response: {resp}")
        return {'success': resp.get('success', False), 'response': resp}
    except Exception as e:
        log.error(f"[POLY] FAK Sell Order execution failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e), 'response': {}}

def unwind_polymarket_position(token_id: str, size: float) -> dict:
    log.warning(f"[PANIC][POLY] Attempting to unwind by SELLING {size:.2f} of token {token_id} at price 0.01")
    final_size = float(f"{size:.2f}")
    if final_size <= 0:
        log.error(f"[PANIC][POLY] Unwind failed: size {size} became zero after formatting.")
        return {'success': False, 'error': 'Size became zero'}
    try:
        order_args = OrderArgs(price=0.01, size=final_size, side=SELL, token_id=token_id)
        signed_order = clob_client.create_order(order_args)
        resp = clob_client.post_order(signed_order, OrderType.FAK)
        log.info(f"[PANIC][POLY] Unwind FAK sell order placed. Response: {resp}")
        return {'success': True, 'response': resp}
    except Exception as e:
        log.error(f"[PANIC][POLY] Failed to unwind position: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

# --- ABSTRACT (MYRIAD) FUNCTIONS ---
def get_abstract_usdc_balance() -> float:
    log.info("[MYRIAD] Checking Abstract USDC balance...")
    contract = w3_abs.eth.contract(address=Web3.to_checksum_address(ABSTRACT_USDC_ADDRESS), abi=ERC20_ABI)
    balance = float(contract.functions.balanceOf(myriad_account.address).call() / 10**6)
    log.info(f"[MYRIAD] Found Abstract USDC balance: {balance:.4f} USDC")
    return balance
    
def get_abstract_eth_balance() -> float:
    log.info("[MYRIAD] Checking Abstract ETH balance...")
    balance = float(w3_abs.from_wei(w3_abs.eth.get_balance(myriad_account.address), 'ether'))
    log.info(f"[MYRIAD] Found Abstract ETH balance: {balance:.6f} ETH")
    return balance

def execute_myriad_buy(market_id: int, outcome_id: int, usdc_amount: float) -> dict:
    log.info(f"[MYRIAD] Executing BUY. Market: {market_id}, Outcome: {outcome_id}, Amount: {usdc_amount:.4f} USDC")
    try:
        usdc_contract = w3_abs.eth.contract(address=Web3.to_checksum_address(ABSTRACT_USDC_ADDRESS), abi=ERC20_ABI)
        market_contract = w3_abs.eth.contract(address=Web3.to_checksum_address(MYRIAD_MARKET_ADDRESS), abi=MYRIAD_MARKET_ABI)
        amount_wei = int(usdc_amount * (10**6))
        log.info(f"[MYRIAD] Calculated amount in wei: {amount_wei}")
        allowance = usdc_contract.functions.allowance(myriad_account.address, market_contract.address).call()
        if allowance < amount_wei:
            log.info("[MYRIAD] Approving USDC spending...")
            nonce = w3_abs.eth.get_transaction_count(myriad_account.address)
            gas_price = w3_abs.eth.gas_price
            approve_tx = usdc_contract.functions.approve(market_contract.address, amount_wei).build_transaction({'from': myriad_account.address, 'nonce': nonce, 'gasPrice': gas_price})
            signed_approve = w3_abs.eth.account.sign_transaction(approve_tx, private_key=MYRIAD_PVT_KEY)
            approve_hash = w3_abs.eth.send_raw_transaction(signed_approve.raw_transaction)
            w3_abs.eth.wait_for_transaction_receipt(approve_hash, timeout=120)
            log.info(f"[MYRIAD] Approval successful. Tx Hash: {approve_hash.hex()}")
        log.info("[MYRIAD] Proceeding with buy transaction...")
        nonce = w3_abs.eth.get_transaction_count(myriad_account.address)
        gas_price = w3_abs.eth.gas_price
        buy_tx = market_contract.functions.buy(market_id, outcome_id, 1, amount_wei).build_transaction({'from': myriad_account.address, 'nonce': nonce, 'gasPrice': gas_price})
        signed_buy = w3_abs.eth.account.sign_transaction(buy_tx, private_key=MYRIAD_PVT_KEY)
        tx_hash = w3_abs.eth.send_raw_transaction(signed_buy.raw_transaction)
        log.info(f"[MYRIAD] Buy transaction sent. Tx Hash: {tx_hash.hex()}")
        receipt = w3_abs.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        if receipt['status'] != 1: return {'success': False, 'error': 'Transaction reverted', 'tx_hash': tx_hash.hex()}
        return {'success': True, 'tx_hash': tx_hash.hex()}
    except Exception as e:
        log.error(f"[MYRIAD] Buy execution failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

def execute_myriad_sell(market_id: int, outcome_id: int, shares_to_sell: float, min_usdc_receive: float) -> dict:
    """Executes a sell order on Myriad."""
    log.info(f"[MYRIAD] Executing SELL. Market: {market_id}, Outcome: {outcome_id}, Shares: {shares_to_sell:.4f}, Min USDC: {min_usdc_receive:.4f}")
    try:
        market_contract = w3_abs.eth.contract(address=Web3.to_checksum_address(MYRIAD_MARKET_ADDRESS), abi=MYRIAD_MARKET_ABI)
        # Both shares and USDC are scaled by 1e6 on Myriad's contract
        shares_wei = int(shares_to_sell * (10**6))
        usdc_wei = int(min_usdc_receive * (10**6))

        log.info(f"[MYRIAD] Building sell transaction with shares_wei={shares_wei}, usdc_wei={usdc_wei}")
        nonce = w3_abs.eth.get_transaction_count(myriad_account.address)
        gas_price = w3_abs.eth.gas_price
        
        # sell(marketId, outcomeId, minUsdcToReceive, maxSharesToSell)
        sell_tx = market_contract.functions.sell(market_id, outcome_id, usdc_wei, shares_wei).build_transaction({
            'from': myriad_account.address, 
            'nonce': nonce, 
            'gasPrice': gas_price
        })

        signed_sell = w3_abs.eth.account.sign_transaction(sell_tx, private_key=MYRIAD_PVT_KEY)
        tx_hash = w3_abs.eth.send_raw_transaction(signed_sell.raw_transaction)
        log.info(f"[MYRIAD] Sell transaction sent. Tx Hash: {tx_hash.hex()}")
        receipt = w3_abs.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        if receipt['status'] != 1:
            return {'success': False, 'error': 'Transaction reverted', 'tx_hash': tx_hash.hex()}
        
        return {'success': True, 'tx_hash': tx_hash.hex()}
    except Exception as e:
        log.error(f"[MYRIAD] Sell execution failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

# ==============================================================================
# 3. CORE ARBITRAGE LOGIC
# ==============================================================================
def process_sell_opportunity(opp: dict):
    """Processes an opportunity to sell an existing position for early profit."""
    trade_id, myriad_slug, poly_id = opp['opportunity_id'], opp['market_identifiers']['myriad_slug'], opp['market_identifiers']['polymarket_condition_id']
    market_title = opp['market_details']['myriad_title']
    log.info(f"--- Processing SELL opportunity {trade_id} for '{market_title}' ---")
    log.info(f"Full sell opportunity details: {json.dumps(opp, indent=2)}")

    trade_log = {'trade_id': trade_id, 'attempt_timestamp_utc': datetime.now(timezone.utc).isoformat(), 'myriad_slug': myriad_slug, 'polymarket_condition_id': poly_id, 'log_details': opp}
    market_key = f"myriad_{myriad_slug}_sell" # Use a separate cooldown key for sells

    try:
        # STEP 1: PRE-FLIGHT CHECKS
        log.info("--- Performing pre-flight checks for SELL ---")
        if get_abstract_eth_balance() < MIN_ETH_BALANCE: raise ValueError(f"Insufficient gas on Myriad for sell.")
        last_trade_ts = db.get_market_cooldown(market_key)
        if last_trade_ts and datetime.now(timezone.utc) < (datetime.fromisoformat(last_trade_ts) + timedelta(minutes=TRADE_COOLDOWN_MINUTES)): raise ValueError(f"Market is on sell cooldown.")

        # Re-verify profitability before executing
        m_data = m_client.fetch_market_details(myriad_slug)
        p_data = p_client.fetch_market(poly_id)
        if not m_data or m_data.get('state') != 'open' or not p_data or not p_data.get('active'):
            raise ValueError("One of the markets is no longer active.")
        
        # This re-verification is complex, for now we trust the opportunity from the matcher
        # as it runs very frequently. A full re-calc would need position data again.
        log.info("✅ All Pre-flight checks for SELL passed.")

        # STEP 2: EXECUTE SELLS
        db.update_market_cooldown(market_key, datetime.now(timezone.utc).isoformat())
        plan = opp['trade_plan']

        # LEG 1: POLYMARKET SELL
        log.info(f"--- Executing Leg 1 (Polymarket SELL) ---")
        if EXECUTION_MODE == "DRY_RUN":
            poly_result = {'success': True, 'response': {'success': True, 'takingAmount': str(plan['polymarket_shares_to_sell'])}}
        else:
            poly_result = execute_polymarket_sell(
                opp['market_identifiers']['polymarket_token_id_sell'],
                plan['polymarket_limit_price'],
                plan['polymarket_shares_to_sell']
            )
        if not poly_result.get('success'):
            raise RuntimeError(f"Failed Leg 1 (Poly SELL): {poly_result.get('error') or poly_result.get('response', {}).get('errorMsg')}")
        log.info(f"✅ Leg 1 (Poly SELL) SUCCESS.")

        # LEG 2: MYRIAD SELL
        log.info(f"--- Executing Leg 2 (Myriad SELL) ---")
        if EXECUTION_MODE == "DRY_RUN":
            myriad_result = {'success': True, 'tx_hash': 'dry_run_hash_sell'}
        else:
            myriad_result = execute_myriad_sell(
                opp['market_identifiers']['myriad_market_id'],
                plan['myriad_outcome_id_sell'],
                plan['myriad_shares_to_sell'],
                plan['myriad_min_usd_receive']
            )
        if not myriad_result.get('success'):
            # This is a critical failure - we sold on Poly but not Myriad.
            # We are now unhedged. A real implementation might try to buy back. For now, CRITICAL ALERT.
            raise RuntimeError(f"Failed Leg 2 (Myriad SELL): {myriad_result.get('error')}")

        log.info("✅ Both SELL legs executed successfully!")
        trade_log.update({
            'status': 'SUCCESS_SELL', 
            'status_message': 'Both sell legs executed.', 
            'poly_tx_hash': json.dumps(poly_result.get('response', {})),
            'myriad_tx_hash': myriad_result.get('tx_hash'),
            'final_profit_usd': opp['profitability_metrics']['estimated_profit_usd']
        })
        db.log_trade_attempt(trade_log)

        if EXECUTION_MODE != "DRY_RUN" and notifier:
            notifier.notify_autotrade_success(market_title, trade_log['final_profit_usd'], plan['polymarket_shares_to_sell'], 0, 0, trade_type="SELL")
    except (ValueError, RuntimeError) as e:
        log.error(f"SELL trade failed for {trade_id}: {e}")
        # <<< FIX: REMOVED THE LINE BELOW >>>
        # db.update_market_cooldown(market_key, datetime.now(timezone.utc).isoformat())
        if notifier: notifier.notify_autotrade_failure(market_title, str(e), "FAIL_SELL")
        if 'Leg 2' in str(e): # Critical failure after selling on one leg
            log.critical(f"!!!!!! SELL PANIC MODE TRIGGERED FOR {trade_id} !!!!!!")
            if notifier: notifier.notify_autotrade_panic(market_title, str(e), trade_type="SELL")


def process_opportunity(opp: dict):
    trade_id, myriad_slug, poly_id, token_id, market_title = opp['opportunity_id'], opp['market_identifiers']['myriad_slug'], opp['market_identifiers']['polymarket_condition_id'], opp['market_identifiers']['polymarket_token_id_buy'], opp['market_details']['myriad_title']
    log.info(f"--- Processing opportunity {trade_id} for '{market_title}' ---")
    log.info(f"Full opportunity details: {json.dumps(opp, indent=2)}")
    
    trade_log = {'trade_id': trade_id, 'attempt_timestamp_utc': datetime.now(timezone.utc).isoformat(), 'myriad_slug': myriad_slug, 'polymarket_condition_id': poly_id, 'log_details': opp}
    market_key = f"myriad_{myriad_slug}"

    try:
        # STEP 1: PRE-FLIGHT CHECKS
        log.info("--- Performing pre-flight checks ---")
        pair_info = next((p for p in db.load_manual_pairs_myriad() if p[0] == myriad_slug and p[1] == poly_id), None)
        if not pair_info or not pair_info[5]: raise ValueError(f"Autotrade check failed.")
        m_market_details = m_client.fetch_market_details(myriad_slug)
        if m_market_details.get('state') != 'open': raise ValueError(f"Myriad market is not 'open'.")
        p_data = p_client.fetch_market(poly_id)
        if not p_data.get('active') or p_data.get('closed'): raise ValueError(f"Polymarket market is not active/is closed.")
        expiry_dt = datetime.fromisoformat(opp['market_details']['market_expiry_utc'].replace('Z', '+00:00'))
        if datetime.now(timezone.utc) > (expiry_dt - timedelta(minutes=MARKET_EXPIRY_BUFFER_MINUTES)): raise ValueError(f"Market expires too soon.")
        last_trade_ts = db.get_market_cooldown(market_key)
        if last_trade_ts and datetime.now(timezone.utc) < (datetime.fromisoformat(last_trade_ts) + timedelta(minutes=TRADE_COOLDOWN_MINUTES)): raise ValueError(f"Market is on cooldown.")
        if get_abstract_eth_balance() < MIN_ETH_BALANCE: raise ValueError(f"Insufficient gas on Myriad.")
            
        trade_plan = opp['trade_plan']
        if EXECUTION_MODE == "LIMITED_LIVE" and trade_plan['estimated_polymarket_cost_usd'] > LIMITED_LIVE_CAP_USD:
            scaling_factor = LIMITED_LIVE_CAP_USD / trade_plan['estimated_polymarket_cost_usd']
            trade_plan['polymarket_shares_to_buy'] *= scaling_factor
            trade_plan['myriad_shares_to_buy'] *= scaling_factor
        
        amm = opp['amm_parameters']
        myriad_b = amm['myriad_liquidity']
        initial_cost = myriad_model.lmsr_cost(amm['myriad_q1'], amm['myriad_q2'], myriad_b)
        trade_plan['estimated_polymarket_cost_usd'] = trade_plan['polymarket_shares_to_buy'] * trade_plan['polymarket_limit_price']
        q1_f_est, q2_f_est = (amm['myriad_q1'] + trade_plan['myriad_shares_to_buy'], amm['myriad_q2']) if trade_plan['myriad_side_to_buy'] == 1 else (amm['myriad_q1'], amm['myriad_q2'] + trade_plan['myriad_shares_to_buy'])
        trade_plan['estimated_myriad_cost_usd'] = (myriad_model.lmsr_cost(q1_f_est, q2_f_est, myriad_b) - initial_cost) * (1 + FEE_RATE_MYRIAD_BUY)
        opp['trade_plan'] = trade_plan
        log.info(f"Initial Full Trade Plan: Buy {trade_plan['polymarket_shares_to_buy']:.2f} Poly for ~${trade_plan['estimated_polymarket_cost_usd']:.4f}. Buy {trade_plan['myriad_shares_to_buy']:.2f} Myriad for ~${trade_plan['estimated_myriad_cost_usd']:.4f}")
            
        myriad_usdc_balance = get_abstract_usdc_balance()
        poly_usdc_balance = get_polygon_usdc_balance()
        if myriad_usdc_balance < trade_plan['estimated_myriad_cost_usd'] or poly_usdc_balance < trade_plan['estimated_polymarket_cost_usd']:
            log.warning("Insufficient capital for full trade. Calculating smaller trade...")
            available_myriad_capital = max(0, myriad_usdc_balance - CAPITAL_SAFETY_BUFFER_USD)
            available_poly_capital = max(0, poly_usdc_balance - CAPITAL_SAFETY_BUFFER_USD)
            q1_i_myr, q2_i_myr = (amm['myriad_q1'], amm['myriad_q2']) if trade_plan['myriad_side_to_buy'] == 1 else (amm['myriad_q2'], amm['myriad_q1'])
            max_shares_myriad = myriad_model.solve_shares_for_cost(q1_i_myr, q2_i_myr, myriad_b, available_myriad_capital, FEE_RATE_MYRIAD_BUY)
            max_shares_poly = (available_poly_capital / trade_plan['polymarket_limit_price']) if trade_plan['polymarket_limit_price'] > 0 else 0
            resized_shares = math.floor(min(max_shares_myriad, max_shares_poly))
            if resized_shares < 1: raise ValueError(f"Capital-constrained calculation resulted in < 1 share.")
            trade_plan.update({'myriad_shares_to_buy': resized_shares, 'polymarket_shares_to_buy': resized_shares})
            trade_plan['estimated_polymarket_cost_usd'] = resized_shares * trade_plan['polymarket_limit_price']
            q1_f_res, q2_f_res = (amm['myriad_q1'] + resized_shares, amm['myriad_q2']) if trade_plan['myriad_side_to_buy'] == 1 else (amm['myriad_q1'], amm['myriad_q2'] + resized_shares)
            trade_plan['estimated_myriad_cost_usd'] = (myriad_model.lmsr_cost(q1_f_res, q2_f_res, myriad_b) - initial_cost) * (1 + FEE_RATE_MYRIAD_BUY)
            if (resized_shares - (trade_plan['estimated_myriad_cost_usd'] + trade_plan['estimated_polymarket_cost_usd'])) < MIN_PROFIT_USD:
                 raise ValueError(f"Resized trade profit is below minimum.")
            log.info(f"REVISED Plan: Buy {resized_shares} shares on both platforms.")
            opp['trade_plan'] = trade_plan
        
        if poly_usdc_balance < trade_plan['estimated_polymarket_cost_usd']: raise ValueError(f"Insufficient USDC on Polygon.")
        if myriad_usdc_balance < trade_plan['estimated_myriad_cost_usd']: raise ValueError(f"Insufficient USDC on Myriad.")
        log.info("✅ All Pre-flight checks passed.")
        trade_log.update({'planned_poly_shares': trade_plan['polymarket_shares_to_buy'], 'planned_myriad_shares': trade_plan['myriad_shares_to_buy']})

        # STEP 2: LEG 1 EXECUTION (POLYMARKET)
        log.info("--- Executing Leg 1 (Polymarket) ---")
        db.update_market_cooldown(market_key, datetime.now(timezone.utc).isoformat())

        existing_trade_ids = {t['id'] for t in clob_client.get_trades()} if EXECUTION_MODE != "DRY_RUN" else set()
        
        if EXECUTION_MODE == "DRY_RUN":
            poly_result = {'success': True, 'response': {'success': True, 'takingAmount': str(trade_plan['polymarket_shares_to_buy'])}}
        else:
            poly_result = execute_polymarket_buy(opp['market_identifiers']['polymarket_token_id_buy'], trade_plan['polymarket_limit_price'], trade_plan['polymarket_shares_to_buy'])
        
        if not poly_result.get('success'): raise RuntimeError(f"Failed Leg 1 (Poly): {poly_result.get('error') or poly_result.get('response', {}).get('errorMsg')}")
        
        fak_response = poly_result.get('response', {})
        executed_poly_shares, executed_poly_cost_usd = 0.0, 0.0
        trade_info_json = json.dumps(fak_response)
        order_id = fak_response.get('orderID')

        if EXECUTION_MODE != "DRY_RUN" and order_id:
            log.info(f"[POLY] Order {order_id} submitted. Waiting 5s to find trade details...")
            time.sleep(5)
            all_my_trades_after = clob_client.get_trades()
            db.save_poly_trades(all_my_trades_after)
            new_trade = next((t for t in all_my_trades_after if t['id'] not in existing_trade_ids and t.get('taker_order_id') == order_id), None)
            if new_trade:
                log.info(f"[POLY] Found new trade: {new_trade['id']}")
                for mo in new_trade.get('maker_orders', []):
                    executed_poly_shares += float(mo.get('matched_amount', '0'))
                    executed_poly_cost_usd += float(mo.get('matched_amount', '0')) * float(mo.get('price', '0'))
                trade_info_json = json.dumps(new_trade)
            else: log.error(f"[POLY] CRITICAL: Could not find trade details for order {order_id}.")
        else:
            executed_poly_shares = float(fak_response.get('takingAmount', '0'))
            executed_poly_cost_usd = executed_poly_shares * trade_plan['polymarket_limit_price']
            
        if executed_poly_shares <= 0: raise RuntimeError("Leg 1 (Poly) executed but no shares acquired.")
        log.info(f"✅ Leg 1 SUCCESS: Acquired {executed_poly_shares:.4f} shares for ${executed_poly_cost_usd:.4f} on Polymarket.")
        trade_log.update({'executed_poly_shares': executed_poly_shares, 'executed_poly_cost_usd': executed_poly_cost_usd, 'poly_tx_hash': trade_info_json})

        # STEP 3: LEG 2 EXECUTION (MYRIAD)
        log.info("--- Executing Leg 2 (Myriad) ---")
        q1_f_final, q2_f_final = (amm['myriad_q1'] + executed_poly_shares, amm['myriad_q2']) if trade_plan['myriad_side_to_buy'] == 1 else (amm['myriad_q1'], amm['myriad_q2'] + executed_poly_shares)
        final_myriad_cost = (myriad_model.lmsr_cost(q1_f_final, q2_f_final, myriad_b) - initial_cost) * (1 + FEE_RATE_MYRIAD_BUY)
        if get_abstract_usdc_balance() < final_myriad_cost: raise RuntimeError(f"Insufficient capital for Leg 2.")

        if EXECUTION_MODE == "DRY_RUN":
            myriad_result = {'success': True, 'tx_hash': 'dry_run_hash'}
        else:
            myriad_result = execute_myriad_buy(opp['market_identifiers']['myriad_market_id'], trade_plan['myriad_side_to_buy'] - 1, final_myriad_cost)
        
        if not myriad_result.get('success'): raise RuntimeError(f"Failed Leg 2 (Myriad): {myriad_result.get('error')}")

        log.info("✅ Both legs executed successfully!")
        trade_log.update({'status': 'SUCCESS', 'status_message': 'Both legs executed. Awaiting Myriad API confirmation.', 'myriad_tx_hash': myriad_result.get('tx_hash'), 'final_profit_usd': opp['profitability_metrics']['estimated_profit_usd']})
        db.log_trade_attempt(trade_log)

        threading.Thread(target=find_myriad_trade_details, args=(opp['market_identifiers']['myriad_market_id'], final_myriad_cost, myriad_account.address, trade_id, market_title)).start()

        if EXECUTION_MODE != "DRY_RUN":
            notifier.notify_autotrade_success(market_title, trade_log['final_profit_usd'], executed_poly_shares, executed_poly_cost_usd, final_myriad_cost)
        else:
            notifier.notify_autotrade_dry_run(market_title, trade_log['final_profit_usd'])

    except (ValueError, RuntimeError) as e:
        log.error(f"Trade failed for {trade_id}: {e}")
        # <<< FIX: REMOVED THE LINE BELOW >>>
        # db.update_market_cooldown(market_key, datetime.now(timezone.utc).isoformat())
        status = 'FAIL_PREFLIGHT' if 'Leg 1' not in str(e) and 'Leg 2' not in str(e) else 'FAIL_LEG1_EXECUTION' if 'Leg 1' in str(e) else 'FAIL_LEG2_EXECUTION'
        
        # Only log and notify for actual execution failures, not pre-flight checks
        if status != 'FAIL_PREFLIGHT':
            trade_log.update({'status': status, 'status_message': str(e)})
            db.log_trade_attempt(trade_log)
            if notifier: notifier.notify_autotrade_failure(market_title, str(e), status)
            
            if status == 'FAIL_LEG2_EXECUTION' and trade_log.get('executed_poly_shares', 0) > 0:
                log.critical(f"!!!!!! PANIC MODE TRIGGERED FOR {trade_id} !!!!!!")
                if notifier: notifier.notify_autotrade_panic(market_title, str(e))
                unwind_result = unwind_polymarket_position(opp['market_identifiers']['polymarket_token_id_buy'], trade_log['executed_poly_shares']) if EXECUTION_MODE != "DRY_RUN" else {'success': True}
                trade_log.update({'status': 'SUCCESS_RECONCILED' if unwind_result.get('success') else 'FAIL_RECONCILED'})
                db.log_trade_attempt(trade_log)
        else:
            log.info(f"Pre-flight check failed for {trade_id}, not logging to DB. Reason: {e}")

    except Exception as e:
        log.critical(f"An unexpected error occurred processing {trade_id}: {e}", exc_info=True)
        trade_log.update({'status': 'FAIL_UNEXPECTED', 'status_message': str(e)})
        db.log_trade_attempt(trade_log)

# ==============================================================================
# 4. MAIN SERVICE LOOP
# ==============================================================================
def main_loop():
    log.info(f"--- Unified Arb Executor started in {EXECUTION_MODE} mode ---")
    while True:
        try:
            opportunity = db.pop_arb_opportunity()
            if opportunity:
                if opportunity.get('type') == 'sell':
                    process_sell_opportunity(opportunity)
                else: # Default to buy
                    process_opportunity(opportunity)
            else:
                time.sleep(5)
        except Exception as e:
            log.error(f"Error in main loop: {e}", exc_info=True)
            time.sleep(30)

if __name__ == "__main__":
    db.init_db()
    main_loop()
