import os
import math
import logging
import json
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from decimal import Decimal, ROUND_DOWN

# --- Web3 and Clob Client Imports ---
from web3 import Web3
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

# --- Local Project Imports ---
from config import m_client, p_client, notifier, log, FEE_RATE_MYRIAD_BUY
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
MIN_ROI = float(os.getenv("MIN_ROI", "0.025"))
MIN_APY = float(os.getenv("MIN_APY", "0.5"))

# --- Safety Parameters ---
MIN_ETH_BALANCE = 0.0003
MARKET_EXPIRY_BUFFER_MINUTES = 5
TRADE_COOLDOWN_MINUTES = 1
# LEG1_TIMEOUT_SECONDS = 60 # No longer needed with FAK orders

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
MYRIAD_MARKET_ABI = json.loads('[{"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"},{"internalType":"uint256","name":"outcomeId","type":"uint256"},{"internalType":"uint256","name":"minOutcomeSharesToBuy","type":"uint256"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"buy","outputs":[],"stateMutability":"nonpayable","type":"function"}]')

# --- Client Initialization ---
# Myriad (requires Web3)
w3_abs = Web3(Web3.HTTPProvider(ABSTRACT_RPC_URL))
myriad_account = w3_abs.eth.account.from_key(MYRIAD_PVT_KEY)

TWO_DP   = Decimal("0.01")
FOUR_DP  = Decimal("0.0001")
DEFAULT_TICK = Decimal("0.01")
DEFAULT_STEP = Decimal("0.0001")

def _decimals_from_tick(tick: Decimal) -> int:
    # e.g., 0.01 -> 2, 0.001 -> 3
    return max(0, -tick.as_tuple().exponent)

def normalize_buy_args(price: float, size: float,
                       tick: Decimal = DEFAULT_TICK,
                       step: Decimal = DEFAULT_STEP):
    """
    Make makerAmount (USDC) end in exactly 2 decimals by choosing a shares
    quantity whose scaled integer satisfies divisibility:
      Let p = decimals of price tick (2 or 3 typically), shares scale = 1e4.
      We need (price_units * shares_units) divisible by 10^(p+4-2).
    """
    P = Decimal(str(price))
    S = Decimal(str(size))

    # 1) snap to tick/step
    P = ((P // tick) * tick).quantize(tick, rounding=ROUND_DOWN)
    S = ((S // step) * step).quantize(FOUR_DP, rounding=ROUND_DOWN)

    # 2) integer math to enforce EXACT cents
    p = _decimals_from_tick(tick)          # 2 if tick=0.01, 3 if 0.001
    price_units = int((P * (10 ** p)).to_integral_value(rounding=ROUND_DOWN))
    shares_units = int((S * 10_000).to_integral_value(rounding=ROUND_DOWN))

    modulus = 10 ** (p + 4 - 2)           # divisor needed for cents
    need_multiple = modulus // math.gcd(price_units, modulus)

    # round shares_units DOWN to nearest valid multiple
    shares_units_adj = (shares_units // need_multiple) * need_multiple
    if shares_units_adj == 0 or price_units == 0:
        return 0.0, 0.0, 0.0

    S_adj = Decimal(shares_units_adj) / Decimal(10_000)

    # 3) compute dollars (will already be cents-exact), keep as 2 dp
    maker = (P * S_adj).quantize(TWO_DP, rounding=ROUND_DOWN)

    # 4) final safety caps
    S_adj = S_adj.quantize(FOUR_DP, rounding=ROUND_DOWN)

    return float(P), float(S_adj), float(maker)

# Polymarket (py-clob-client)
clob_client = ClobClient(
    host="https://clob.polymarket.com",
    key=POLY_PVT_KEY,
    chain_id=137,
    funder=POLY_PROXY_ADDRESS, # Use the proxy address to trade on behalf of your web account
    signature_type=2 # Use 1 for Email/Magic Link accounts
)
clob_client.set_api_creds(clob_client.create_or_derive_api_creds())

log.info(f"Unified Executor initialized. EXECUTION_MODE: {EXECUTION_MODE}")
log.info(f"Using Polymarket proxy address: {POLY_PROXY_ADDRESS}")
log.info(f"Using Myriad/Abstract address: {myriad_account.address}")

# ==============================================================================
# 2. ON-CHAIN INTERACTION FUNCTIONS
# ==============================================================================

# --- POLYGON (POLYMARKET) FUNCTIONS ---
# Polygon RPC
w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))

# USDC contract (Polygon)
USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
ERC20_ABI_POLY = [
    {"constant": True, "inputs": [{"name": "_owner","type": "address"}],
     "name": "balanceOf", "outputs": [{"name": "balance","type": "uint256"}],
     "type": "function"}
]


def get_polygon_usdc_balance() -> float:
    """
    Gets the available USDC collateral balance for the Polymarket Proxy Account.
    """
    log.info(f"[POLY] Checking Polygon USDC balance for {POLY_PROXY_ADDRESS}...")
    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_CONTRACT), abi=ERC20_ABI_POLY)
    balance = usdc.functions.balanceOf(Web3.to_checksum_address(POLY_PROXY_ADDRESS)).call() / 1e6
    log.info(f"[POLY] Found Polygon USDC balance: {balance:.4f} USDC")
    return float(balance)

def execute_polymarket_buy(token_id: str, price: float, size: float) -> dict:
    """Executes a Polymarket BUY using a Fill-And-Kill (FAK) order type."""
    # Normalize to satisfy maker(USDC)=2dp and shares<=4dp
    px, sz, usd = normalize_buy_args(price, size)
    if sz <= 0 or px <= 0:
        log.warning(f"[POLY] Normalized to zero. price={price}, size={size} -> px={px}, sz={sz}")
        return {'success': True, 'executed_shares': 0, 'tx_hash': 'SKIPPED_ZERO_SIZE'}

    log.info(f"[POLY] Executing BUY FAK. Token: {token_id} | "
         f"price={px:.2f}, size={sz:.4f}, dollars={usd:.2f}")

    try:
        order_args = OrderArgs(price=px, size=sz, side=BUY, token_id=token_id)

        signed_order = clob_client.create_order(order_args)
        
        # Use Fill-And-Kill (FAK), which executes immediately against the book or is cancelled.
        resp = clob_client.post_order(signed_order, OrderType.FAK)
        log.info(f"[POLY] FAK Order response: {resp}")

        # The response from a FAK order includes the amount filled.
        # --- FIX 2: Corrected key from 'taking_amount' to 'takingAmount' to match API response. ---
        executed_shares = float(resp.get('takingAmount', 0.0))
        
        if executed_shares > 0:
            log.info(f"[POLY] FAK order successfully filled for {executed_shares} shares.")
        else:
            log.warning("[POLY] FAK order was not filled (or was filled for 0 shares).")

        return {'success': True, 'executed_shares': executed_shares, 'tx_hash': str(resp)}

    except Exception as e:
        log.error(f"[POLY] FAK Order execution failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

def unwind_polymarket_position(token_id: str, size: float) -> dict:
    """Attempts to unwind a position by placing an aggressive FAK sell order."""
    log.warning(f"[PANIC][POLY] Attempting to unwind by SELLING {size:.2f} of token {token_id} at price 0.01")
    
    # --- FIX: Format size to 2 decimal places to ensure the unwind order is valid. ---
    final_size = float(f"{size:.2f}")
    if final_size <= 0:
        log.error(f"[PANIC][POLY] Unwind failed: size {size} became zero after formatting.")
        return {'success': False, 'error': 'Size became zero'}

    try:
        # Sell at a very low price to ensure it's taken (market sell)
        order_args = OrderArgs(price=0.01, size=final_size, side=SELL, token_id=token_id)
        signed_order = clob_client.create_order(order_args)
        resp = clob_client.post_order(signed_order, OrderType.FAK) # Use FAK to avoid hanging orders
        log.info(f"[PANIC][POLY] Unwind FAK sell order placed. Response: {resp}")
        return {'success': True, 'response': resp}
    except Exception as e:
        log.error(f"[PANIC][POLY] Failed to unwind position: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

# --- ABSTRACT (MYRIAD) FUNCTIONS ---
def get_abstract_usdc_balance() -> float:
    log.info("[MYRIAD] Checking Abstract USDC balance...")
    contract = w3_abs.eth.contract(address=Web3.to_checksum_address(ABSTRACT_USDC_ADDRESS), abi=ERC20_ABI)
    balance_wei = contract.functions.balanceOf(myriad_account.address).call()
    balance = float(balance_wei / 10**6)
    log.info(f"[MYRIAD] Found Abstract USDC balance: {balance:.4f} USDC")
    return balance
    
def get_abstract_eth_balance() -> float:
    log.info("[MYRIAD] Checking Abstract ETH balance...")
    balance_wei = w3_abs.eth.get_balance(myriad_account.address)
    balance = float(w3_abs.from_wei(balance_wei, 'ether'))
    log.info(f"[MYRIAD] Found Abstract ETH balance: {balance:.6f} ETH")
    return balance

def execute_myriad_buy(market_id: int, outcome_id: int, usdc_amount: float) -> dict:
    """Executes a Myriad BUY transaction with robust gas estimation."""
    log.info(f"[MYRIAD] Executing BUY. Market: {market_id}, Outcome: {outcome_id}, Amount: {usdc_amount:.4f} USDC")
    try:
        usdc_contract = w3_abs.eth.contract(address=Web3.to_checksum_address(ABSTRACT_USDC_ADDRESS), abi=ERC20_ABI)
        market_contract = w3_abs.eth.contract(address=Web3.to_checksum_address(MYRIAD_MARKET_ADDRESS), abi=MYRIAD_MARKET_ABI)
        amount_wei = int(usdc_amount * (10**6))
        log.info(f"[MYRIAD] Calculated amount in wei: {amount_wei}")
        
        allowance = usdc_contract.functions.allowance(myriad_account.address, market_contract.address).call()
        log.info(f"[MYRIAD] Current USDC allowance: {allowance}. Required: {amount_wei}")
        if allowance < amount_wei:
            log.info("[MYRIAD] Approving USDC spending...")
            nonce = w3_abs.eth.get_transaction_count(myriad_account.address)
            gas_price = w3_abs.eth.gas_price
            tx_options = {'from': myriad_account.address, 'nonce': nonce, 'gasPrice': gas_price}
            
            approve_function = usdc_contract.functions.approve(market_contract.address, amount_wei)
            estimated_gas = approve_function.estimate_gas(tx_options)
            tx_options['gas'] = int(estimated_gas * 1.2) # Add 20% buffer
            
            log.info(f"[MYRIAD] Building approval tx with nonce: {nonce}, gasPrice: {gas_price}, gas: {tx_options['gas']}")
            approve_tx = approve_function.build_transaction(tx_options)
            
            signed_approve = w3_abs.eth.account.sign_transaction(approve_tx, private_key=MYRIAD_PVT_KEY)
            approve_hash = w3_abs.eth.send_raw_transaction(signed_approve.raw_transaction)
            w3_abs.eth.wait_for_transaction_receipt(approve_hash, timeout=120)
            log.info(f"[MYRIAD] Approval successful. Tx Hash: {approve_hash.hex()}")

        log.info("[MYRIAD] Proceeding with buy transaction...")
        nonce = w3_abs.eth.get_transaction_count(myriad_account.address)
        gas_price = w3_abs.eth.gas_price
        tx_options = {'from': myriad_account.address, 'nonce': nonce, 'gasPrice': gas_price}
        
        buy_function = market_contract.functions.buy(market_id, outcome_id, 1, amount_wei) # min_shares_to_buy = 1
        estimated_gas_buy = buy_function.estimate_gas(tx_options)
        tx_options['gas'] = int(estimated_gas_buy * 1.2) # Add 20% buffer

        log.info(f"[MYRIAD] Building buy tx with nonce: {nonce}, gasPrice: {gas_price}, gas: {tx_options['gas']}")
        buy_tx = buy_function.build_transaction(tx_options)

        signed_buy = w3_abs.eth.account.sign_transaction(buy_tx, private_key=MYRIAD_PVT_KEY)
        tx_hash = w3_abs.eth.send_raw_transaction(signed_buy.raw_transaction)
        log.info(f"[MYRIAD] Buy transaction sent. Tx Hash: {tx_hash.hex()}")
        receipt = w3_abs.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        
        log.info(f"[MYRIAD] Transaction receipt received. Status: {receipt['status']}")
        if receipt['status'] != 1: return {'success': False, 'error': 'Transaction reverted', 'tx_hash': tx_hash.hex()}
        
        # This is a rough estimate; the actual shares are determined by the AMM's curve.
        est_shares = usdc_amount / 0.5 
        log.info(f"[MYRIAD] Buy successful. Estimated shares received: >{est_shares:.2f}")
        return {'success': True, 'executed_shares': est_shares, 'tx_hash': tx_hash.hex()}
    except Exception as e:
        log.error(f"[MYRIAD] Buy execution failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

# ==============================================================================
# 3. CORE ARBITRAGE LOGIC
# ==============================================================================
def process_opportunity(opp: dict):
    trade_id, myriad_slug, poly_id, token_id, market_title = opp['opportunity_id'], opp['market_identifiers']['myriad_slug'], opp['market_identifiers']['polymarket_condition_id'], opp['market_identifiers']['polymarket_token_id_buy'], opp['market_details']['myriad_title']
    log.info(f"--- Processing opportunity {trade_id} for '{market_title}' ---")
    log.info(f"Full opportunity details: {json.dumps(opp, indent=2)}")
    
    trade_log = {'trade_id': trade_id, 'attempt_timestamp_utc': datetime.now(timezone.utc).isoformat(), 'myriad_slug': myriad_slug, 'polymarket_condition_id': poly_id, 'log_details': opp}

    try:
        # STEP 1: PRE-FLIGHT CHECKS
        log.info("--- Performing pre-flight checks ---")
        pair_info = next((p for p in db.load_manual_pairs_myriad() if p[0] == myriad_slug and p[1] == poly_id), None)
        if not pair_info or not pair_info[5]:
            autotrade_status = "Not enabled" if pair_info else "Pair not found"
            raise ValueError(f"Autotrade check failed. Status: {autotrade_status}")
        log.info(f"✅ Autotrade enabled for pair ({myriad_slug}, {poly_id}).")

        m_market_details = m_client.fetch_market_details(myriad_slug)
        m_state = m_market_details.get('state')
        if m_state != 'open': raise ValueError(f"Myriad market is not 'open'. Current state: '{m_state}'.")
        log.info(f"✅ Myriad market state is '{m_state}'.")

        p_data = p_client.fetch_market(poly_id)
        if not p_data.get('active') or p_data.get('closed'): raise ValueError(f"Polymarket market is not active/is closed.")
        log.info(f"✅ Polymarket market is active and not closed.")
        
        expiry_dt_str = opp['market_details']['market_expiry_utc']
        expiry_dt = datetime.fromisoformat(expiry_dt_str.replace('Z', '+00:00'))
        buffer_time = expiry_dt - timedelta(minutes=MARKET_EXPIRY_BUFFER_MINUTES)
        now_utc = datetime.now(timezone.utc)
        if now_utc > buffer_time: raise ValueError(f"Market expires within {MARKET_EXPIRY_BUFFER_MINUTES} mins. Current time: {now_utc}, Expiry buffer: {buffer_time}")
        log.info(f"✅ Market expiry check passed. Now: {now_utc.isoformat()}, Buffer time: {buffer_time.isoformat()}")
        
        market_key = f"myriad_{myriad_slug}"
        last_trade_ts = db.get_market_cooldown(market_key)
        if last_trade_ts:
            last_trade_dt = datetime.fromisoformat(last_trade_ts)
            cooldown_until = last_trade_dt + timedelta(minutes=TRADE_COOLDOWN_MINUTES)
            if now_utc < cooldown_until: raise ValueError(f"Market is on cooldown until {cooldown_until.isoformat()}.")
        log.info(f"✅ Market cooldown check passed. Last trade: {last_trade_ts or 'None'}")
        
        log.info("Performing capital checks...")
        eth_balance = get_abstract_eth_balance()
        if eth_balance < MIN_ETH_BALANCE: raise ValueError(f"Insufficient gas on Myriad. Have: {eth_balance:.6f} ETH, Need: {MIN_ETH_BALANCE:.6f} ETH.")
        log.info(f"✅ Myriad ETH balance sufficient.")
            
        trade_plan = opp['trade_plan']
        
        if EXECUTION_MODE == "LIMITED_LIVE" and trade_plan['estimated_polymarket_cost_usd'] > LIMITED_LIVE_CAP_USD:
            log.warning(f"LIMITED LIVE: Capping trade cost from ${trade_plan['estimated_polymarket_cost_usd']:.2f} to ${LIMITED_LIVE_CAP_USD}.")
            scaling_factor = LIMITED_LIVE_CAP_USD / trade_plan['estimated_polymarket_cost_usd']
            trade_plan['polymarket_shares_to_buy'] *= scaling_factor
            trade_plan['myriad_shares_to_buy'] *= scaling_factor
        
        # Recalculate final costs based on potentially scaled shares
        trade_plan['estimated_polymarket_cost_usd'] = trade_plan['polymarket_shares_to_buy'] * trade_plan['polymarket_limit_price']
        amm = opp['amm_parameters']
        initial_cost = myriad_model.lmsr_cost(amm['myriad_q1'], amm['myriad_q2'], amm['myriad_inferred_b'])
        q1_f, q2_f = (amm['myriad_q1'] + trade_plan['myriad_shares_to_buy'], amm['myriad_q2']) if trade_plan['myriad_side_to_buy'] == 1 else (amm['myriad_q1'], amm['myriad_q2'] + trade_plan['myriad_shares_to_buy'])
        trade_plan['estimated_myriad_cost_usd'] = (myriad_model.lmsr_cost(q1_f, q2_f, amm['myriad_inferred_b']) - initial_cost) * (1 + FEE_RATE_MYRIAD_BUY)
        opp['trade_plan'] = trade_plan
        log.info(f"Final Trade Plan: Buy {trade_plan['polymarket_shares_to_buy']:.2f} on Poly for ~${trade_plan['estimated_polymarket_cost_usd']:.4f}. Buy {trade_plan['myriad_shares_to_buy']:.2f} on Myriad for ~${trade_plan['estimated_myriad_cost_usd']:.4f}")
            
        poly_usdc_balance = get_polygon_usdc_balance()
        if poly_usdc_balance < trade_plan['estimated_polymarket_cost_usd']: raise ValueError(f"Insufficient USDC on Polygon. Have: ${poly_usdc_balance:.2f}, Need: ${trade_plan['estimated_polymarket_cost_usd']:.2f}")
        log.info(f"✅ Polygon USDC balance sufficient.")
        
        myriad_usdc_balance = get_abstract_usdc_balance()
        if myriad_usdc_balance < trade_plan['estimated_myriad_cost_usd']: raise ValueError(f"Insufficient USDC on Myriad. Have: ${myriad_usdc_balance:.2f}, Need: ${trade_plan['estimated_myriad_cost_usd']:.2f}")
        log.info(f"✅ Myriad USDC balance sufficient.")

        log.info("✅ All Pre-flight checks passed.")
        trade_log.update({'planned_poly_shares': trade_plan['polymarket_shares_to_buy'], 'planned_myriad_shares': trade_plan['myriad_shares_to_buy']})

        # STEP 2: LEG 1 EXECUTION (POLYMARKET)
        log.info("--- Executing Leg 1 (Polymarket) ---")
        db.update_market_cooldown(market_key, datetime.now(timezone.utc).isoformat())
        if EXECUTION_MODE == "DRY_RUN":
            log.warning("[DRY RUN] Simulating Polymarket BUY.")
            poly_result = {'success': True, 'executed_shares': trade_plan['polymarket_shares_to_buy']}
        else:
            poly_result = execute_polymarket_buy(opp['market_identifiers']['polymarket_token_id_buy'], trade_plan['polymarket_limit_price'], trade_plan['polymarket_shares_to_buy'])
        
        if not poly_result.get('success'): raise RuntimeError(f"Failed Leg 1 (Poly): {poly_result.get('error')}")
        executed_poly_shares = poly_result.get('executed_shares', 0.0)
        if executed_poly_shares <= 0: raise RuntimeError("Leg 1 (Poly) executed but no shares acquired.")
        log.info(f"✅ Leg 1 SUCCESS: Acquired {executed_poly_shares} shares on Polymarket.")
        trade_log.update({'executed_poly_shares': executed_poly_shares, 'poly_tx_hash': poly_result.get('tx_hash')})

        # STEP 3: LEG 2 EXECUTION (MYRIAD)
        log.info("--- Executing Leg 2 (Myriad) ---")
        # Recalculate Myriad cost based on actual shares filled on Polymarket
        q1_f_final, q2_f_final = (amm['myriad_q1'] + executed_poly_shares, amm['myriad_q2']) if trade_plan['myriad_side_to_buy'] == 1 else (amm['myriad_q1'], amm['myriad_q2'] + executed_poly_shares)
        final_myriad_cost = (myriad_model.lmsr_cost(q1_f_final, q2_f_final, amm['myriad_inferred_b']) - initial_cost) * (1 + FEE_RATE_MYRIAD_BUY)
        log.info(f"Recalculated Myriad cost for {executed_poly_shares} shares: ${final_myriad_cost:.4f}")

        myriad_usdc_balance = get_abstract_usdc_balance() # Re-check balance just in case
        if myriad_usdc_balance < final_myriad_cost: raise RuntimeError(f"Insufficient capital for Leg 2. Have: ${myriad_usdc_balance:.2f}, Need: ${final_myriad_cost:.2f}")
        log.info(f"✅ Myriad USDC balance sufficient for final cost.")
        
        if EXECUTION_MODE == "DRY_RUN":
            log.warning("[DRY RUN] Simulating Myriad BUY.")
            myriad_result = {'success': True, 'executed_shares': executed_poly_shares}
        else:
            myriad_result = execute_myriad_buy(opp['market_identifiers']['myriad_market_id'], trade_plan['myriad_side_to_buy'] - 1, final_myriad_cost)
        
        if not myriad_result.get('success'): raise RuntimeError(f"Failed Leg 2 (Myriad): {myriad_result.get('error')}")

        log.info("✅ Both legs executed successfully!")
        trade_log.update({'status': 'SUCCESS', 'status_message': 'Both legs executed.', 'executed_myriad_shares': myriad_result.get('executed_shares'), 'myriad_tx_hash': myriad_result.get('tx_hash'), 'final_profit_usd': opp['profitability_metrics']['estimated_profit_usd']})
        db.log_trade_attempt(trade_log)
        if EXECUTION_MODE != "DRY_RUN": notifier.notify_autotrade_success(market_title, trade_log['final_profit_usd'], executed_poly_shares)
        else: notifier.notify_autotrade_dry_run(market_title, trade_log['final_profit_usd'])

    except (ValueError, RuntimeError) as e:
        log.error(f"Trade failed for {trade_id}: {e}")
        status = 'FAIL_PREFLIGHT'
        if 'Leg 1' in str(e): status = 'FAIL_LEG1_EXECUTION'
        if 'Leg 2' in str(e): status = 'FAIL_LEG2_EXECUTION'
        trade_log.update({'status': status, 'status_message': str(e)})
        db.log_trade_attempt(trade_log)
        if notifier: notifier.notify_autotrade_failure(market_title, str(e), status)
        if status == 'FAIL_LEG2_EXECUTION' and trade_log.get('executed_poly_shares', 0) > 0:
            log.critical(f"!!!!!! PANIC MODE TRIGGERED FOR {trade_id} !!!!!!")
            if notifier: notifier.notify_autotrade_panic(market_title, str(e))
            if EXECUTION_MODE != "DRY_RUN":
                log.info("Attempting to unwind Polymarket position...")
                unwind_result = unwind_polymarket_position(opp['market_identifiers']['polymarket_token_id_buy'], trade_log['executed_poly_shares'])
            else:
                log.warning("[DRY RUN] Simulating panic unwind.")
                unwind_result = {'success': True}
            
            log_status = 'SUCCESS_RECONCILED' if unwind_result.get('success') else 'FAIL_RECONCILED'
            log.info(f"Panic unwind status: {log_status}")
            trade_log.update({'status': log_status})
            db.log_trade_attempt(trade_log)
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
                process_opportunity(opportunity)
            else:
               time.sleep(5)
        except Exception as e:
            log.error(f"Error in main loop: {e}", exc_info=True)
            time.sleep(30)

if __name__ == "__main__":
    db.init_db()
    main_loop()
