# sell_test.py
import os
import json
import logging
from dotenv import load_dotenv
from web3 import Web3

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)
load_dotenv()

# --- Configuration (Copied from arb_executor.py) ---
ABSTRACT_RPC_URL = os.getenv("ABSTRACT_RPC_URL")
MYRIAD_PVT_KEY = os.getenv("MYRIAD_PRIVATE_KEY")

if not all([ABSTRACT_RPC_URL, MYRIAD_PVT_KEY]):
    raise ValueError("Missing ABSTRACT_RPC_URL or MYRIAD_PRIVATE_KEY in your .env file")

# --- Contract Addresses & ABIs ---
ABSTRACT_USDC_ADDRESS = "0x84a71ccd554cc1b02749b35d22f684cc8ec987e1"
MYRIAD_MARKET_ADDRESS = "0x3e0f5F8F5FB043aBFA475C0308417Bf72c463289"
ERC20_ABI = json.loads('[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]')
# A simplified ABI for what we need to test
MYRIAD_MARKET_ABI = json.loads('[{"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"},{"internalType":"uint256","name":"outcomeId","type":"uint256"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"maxOutcomeSharesToSell","type":"uint256"}],"name":"sell","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"},{"internalType":"address","name":"user","type":"address"}],"name":"getUserMarketShares","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256[]","name":"outcomes","type":"uint256[]"}],"stateMutability":"view","type":"function"}]')

# --- Web3 Initialization ---
w3 = Web3(Web3.HTTPProvider(ABSTRACT_RPC_URL))
account = w3.eth.account.from_key(MYRIAD_PVT_KEY)
log.info(f"Using account: {account.address}")

# --- Contract Instances ---
market_contract = w3.eth.contract(address=Web3.to_checksum_address(MYRIAD_MARKET_ADDRESS), abi=MYRIAD_MARKET_ABI)
usdc_contract = w3.eth.contract(address=Web3.to_checksum_address(ABSTRACT_USDC_ADDRESS), abi=ERC20_ABI)

# --- Helper Functions to Get Balances ---
def get_share_balance(market_id, outcome_id):
    """Gets the on-chain share balance for a specific market and outcome."""
    try:
        _liquidity, outcomes = market_contract.functions.getUserMarketShares(market_id, account.address).call()
        if outcome_id < len(outcomes):
            return outcomes[outcome_id] / 1e6  # Shares are scaled by 1e6
        return 0.0
    except Exception as e:
        log.error(f"Failed to get share balance: {e}")
        return 0.0

def get_usdc_balance():
    """Gets the on-chain USDC balance."""
    try:
        balance_wei = usdc_contract.functions.balanceOf(account.address).call()
        return balance_wei / 1e6  # USDC is scaled by 1e6
    except Exception as e:
        log.error(f"Failed to get USDC balance: {e}")
        return 0.0

# --- Main Test Function ---
def run_sell_test():
    # --- TEST PARAMETERS ---
    market_id = 235
    outcome_id = 0
    shares_to_sell = 10.0  # The "How Many" shares
    min_usdc_receive = 2.0 # The "Safety Check" value

    log.info("--- TEST SCENARIO ---")
    log.info(f"Market ID: {market_id}, Outcome ID: {outcome_id}")
    log.info(f"Attempting to sell: {shares_to_sell} shares")
    log.info(f"Setting MINIMUM USDC to receive at: ${min_usdc_receive}")
    log.info("Hypothesis: The transaction will sell the full 10 shares and return the market value, which should be > $2.")
    log.info("---------------------\n")

    # 1. Get balances BEFORE the trade
    log.info("--- BEFORE THE SELL ---")
    initial_shares = get_share_balance(market_id, outcome_id)
    initial_usdc = get_usdc_balance()
    log.info(f"Initial Share Balance: {initial_shares:.4f}")
    log.info(f"Initial USDC Balance:  ${initial_usdc:.4f}")

    if initial_shares < shares_to_sell:
        log.error(f"Cannot run test. You only have {initial_shares} shares, but you need at least {shares_to_sell}.")
        return

    # 2. Prepare and execute the transaction
    log.info("\n--- EXECUTING THE SELL ---")
    try:
        # Convert to wei format (scaled by 1e6)
        shares_wei = int(shares_to_sell * 1e6)
        usdc_wei = int(min_usdc_receive * 1e6)
        
        log.info(f"Building transaction with maxOutcomeSharesToSell={shares_wei} and value={usdc_wei}...")
        
        nonce = w3.eth.get_transaction_count(account.address)
        gas_price = w3.eth.gas_price

        # The function signature is: sell(marketId, outcomeId, minUsdcToReceive, maxSharesToSell)
        # In the ABI, minUsdcToReceive is named 'value'.
        sell_tx = market_contract.functions.sell(
            market_id,
            outcome_id,
            usdc_wei, # This corresponds to 'value' in your screenshot
            shares_wei  # This corresponds to 'maxOutcomeSharesToSell'
        ).build_transaction({
            'from': account.address,
            'nonce': nonce,
            'gasPrice': gas_price
        })

        signed_tx = w3.eth.account.sign_transaction(sell_tx, private_key=MYRIAD_PVT_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        log.info(f"Transaction sent! Hash: {tx_hash.hex()}")
        log.info("Waiting for transaction to be mined...")

        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        if receipt['status'] == 1:
            log.info("✅ Transaction successful!")
        else:
            log.error("❌ Transaction FAILED (reverted on-chain).")
            return

    except Exception as e:
        log.error(f"An error occurred during transaction: {e}", exc_info=True)
        return

    # 3. Get balances AFTER the trade
    log.info("\n--- AFTER THE SELL ---")
    final_shares = get_share_balance(market_id, outcome_id)
    final_usdc = get_usdc_balance()
    log.info(f"Final Share Balance: {final_shares:.4f}")
    log.info(f"Final USDC Balance:  ${final_usdc:.4f}")

    # 4. Analyze the results
    log.info("\n--- RESULTS ---")
    shares_sold = initial_shares - final_shares
    usdc_gained = final_usdc - initial_usdc

    log.info(f"Shares sold: {shares_sold:.4f}")
    log.info(f"USDC gained: ${usdc_gained:.4f}")

    if abs(shares_sold - shares_to_sell) < 0.0001:
        log.info("\n✅ HYPOTHESIS CONFIRMED: The full amount of shares was sold.")
    else:
        log.error("\n❌ HYPOTHESIS FAILED: The sold amount does not match the intended amount.")

    if usdc_gained > min_usdc_receive:
        log.info("✅ HYPOTHESIS CONFIRMED: The USDC received was the market value, not the minimum.")
    else:
        log.warning("⚠️ The USDC received was very close to the minimum. This is unlikely but possible in a very low-liquidity market.")

if __name__ == "__main__":
    run_sell_test()