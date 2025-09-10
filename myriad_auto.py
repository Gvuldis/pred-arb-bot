# myriad_prediction_trader.py
import os
import json
from web3 import Web3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
PRIVATE_KEY = os.getenv("MYRIAD_PRIVATE_KEY")
if not PRIVATE_KEY:
    raise ValueError("Please set the MYRIAD_PRIVATE_KEY environment variable.")

# Your EOA signer address, derived from your private key.
# This is the wallet that needs ETH for gas AND the USDC to trade with.
account = Web3().eth.account.from_key(PRIVATE_KEY)
MY_ADDRESS = account.address

# Network and Contract Details
ABSTRACT_RPC_URL = "https://api.mainnet.abs.xyz"
MARKET_CONTRACT_ADDRESS = "0x3e0f5F8F5FB043aBFA475C0308417Bf72c463289"
USDC_CONTRACT_ADDRESS = "0x84a71ccd554cc1b02749b35d22f684cc8ec987e1" # Bridged USDC (USDC.e)

# ABI for the prediction market, now including sell and claim functions.
MARKET_ABI = json.loads("""
[
    {"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"},{"internalType":"uint256","name":"outcomeId","type":"uint256"},{"internalType":"uint256","name":"minOutcomeSharesToBuy","type":"uint256"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"buy","outputs":[],"stateMutability":"nonpayable","type":"function"},
    {"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"},{"internalType":"uint256","name":"outcomeId","type":"uint256"},{"internalType":"uint256","name":"sharesToSell","type":"uint256"},{"internalType":"uint256","name":"minUsdcToReceive","type":"uint256"}],"name":"sell","outputs":[],"stateMutability":"nonpayable","type":"function"},
    {"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"}],"name":"claimWinnings","outputs":[],"stateMutability":"nonpayable","type":"function"}
]
""")

# Standard ERC20 ABI for `approve` and `allowance`
USDC_ABI = json.loads("""
[
    {"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},
    {"constant":true,"inputs":[{"name":"_owner","type":"address"},{"name":"_spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}
]
""")

# ==============================================================================
# 2. WEB3 SETUP
# ==============================================================================
print(f"Attempting to connect to the Abstract network node at {ABSTRACT_RPC_URL}...")
w3 = Web3(Web3.HTTPProvider(ABSTRACT_RPC_URL))

if not w3.is_connected():
    raise ConnectionError(f"Failed to connect to the Abstract node at {ABSTRACT_RPC_URL}")

print(f"✅ Successfully connected to the Abstract network. Chain ID: {w3.eth.chain_id}")
print(f"Using EOA signer address: {MY_ADDRESS}")

market_contract = w3.eth.contract(address=Web3.to_checksum_address(MARKET_CONTRACT_ADDRESS), abi=MARKET_ABI)
usdc_contract = w3.eth.contract(address=Web3.to_checksum_address(USDC_CONTRACT_ADDRESS), abi=USDC_ABI)

# ==============================================================================
# 3. TRANSACTION FUNCTIONS
# ==============================================================================

def execute_transaction(function_call, transaction_options):
    """Helper function to estimate gas, build, sign, and send a transaction."""
    try:
        estimated_gas = function_call.estimate_gas(transaction_options)
        transaction_options['gas'] = int(estimated_gas * 1.2)
        print(f"Gas estimated successfully. Limit set to: {transaction_options['gas']}")

        tx = function_call.build_transaction(transaction_options)
        signed_tx = w3.eth.account.sign_transaction(tx, private_key=PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        print(f"Transaction sent! Hash: {w3.to_hex(tx_hash)}")
        print("Waiting for receipt...")

        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        if receipt['status'] == 1:
            print(f"✅ Transaction successful! Block: {receipt['blockNumber']}")
        else:
            print(f"❌ Transaction failed! See receipt: {receipt}")
        return receipt
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def approve_usdc_spending(amount_in_usdc: float):
    """Approves the Market Contract to spend a specific amount of your USDC."""
    print(f"\n--- Preparing to Approve USDC ---")
    spender_address = Web3.to_checksum_address(MARKET_CONTRACT_ADDRESS)
    amount_wei = int(amount_in_usdc * (10**6))

    current_allowance = usdc_contract.functions.allowance(MY_ADDRESS, spender_address).call()
    if current_allowance >= amount_wei:
        print(f"Approval already sufficient.")
        return True

    print(f"Approving market contract to spend {amount_in_usdc} USDC...")
    try:
        tx_options = {'from': MY_ADDRESS, 'nonce': w3.eth.get_transaction_count(MY_ADDRESS), 'gasPrice': w3.eth.gas_price}
        approve_function = usdc_contract.functions.approve(spender_address, amount_wei)
        
        estimated_gas = approve_function.estimate_gas(tx_options)
        tx_options['gas'] = int(estimated_gas * 1.2)
        
        tx = approve_function.build_transaction(tx_options)
        signed_tx = w3.eth.account.sign_transaction(tx, private_key=PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        print(f"Approval transaction sent. Hash: {w3.to_hex(tx_hash)}")
        
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt['status'] == 1:
            print("✅ Approval successful!")
            return True
        else:
            print("❌ Approval failed!")
            return False
    except Exception as e:
        print(f"An error occurred during approval:\n{e}")
        return False

def action_buy_prediction(market_id: int, outcome_id: int, usdc_amount: float):
    """Action: Executes a buy on a prediction market after ensuring approval."""
    print(f"\n--- ACTION: BUYING PREDICTION ---")
    
    if not approve_usdc_spending(usdc_amount):
        print("Stopping transaction because approval failed.")
        return

    print(f"\nProceeding to buy on market {market_id} for outcome {outcome_id} with {usdc_amount} USDC...")
    try:
        value_wei = int(usdc_amount * (10**6))
        min_shares_to_buy = 1
        tx_options = {'from': MY_ADDRESS, 'nonce': w3.eth.get_transaction_count(MY_ADDRESS), 'gasPrice': w3.eth.gas_price}
        buy_function = market_contract.functions.buy(market_id, outcome_id, min_shares_to_buy, value_wei)
        execute_transaction(buy_function, tx_options)
    except Exception as e:
        print(f"An error occurred during the buy transaction: {e}")

def action_sell_prediction(market_id: int, outcome_id: int, shares_to_sell: float):
    """Action: Executes a sell of outcome shares on a prediction market."""
    print(f"\n--- ACTION: SELLING PREDICTION ---")
    print(f"Attempting to sell {shares_to_sell} shares on market {market_id} for outcome {outcome_id}...")
    
    # NOTE: Before you can sell, you may need to approve the Market Contract
    # to spend your outcome share tokens. This is a separate, one-time transaction
    # per market. If this sell function fails, that is the most likely reason.

    try:
        # Prediction market shares typically have 18 decimals.
        shares_wei = int(shares_to_sell * (10**18))
        
        # Slippage protection: accept any amount of USDC greater than 1 wei.
        min_usdc_to_receive = 1

        tx_options = {'from': MY_ADDRESS, 'nonce': w3.eth.get_transaction_count(MY_ADDRESS), 'gasPrice': w3.eth.gas_price}
        sell_function = market_contract.functions.sell(market_id, outcome_id, shares_wei, min_usdc_to_receive)
        execute_transaction(sell_function, tx_options)
    except Exception as e:
        print(f"An error occurred during the sell transaction: {e}")

def action_claim_winnings(market_id: int):
    """Action: Claims winnings from a resolved market."""
    print(f"\n--- ACTION: CLAIMING WINNINGS ---")
    print(f"Attempting to claim winnings from market {market_id}...")

    try:
        tx_options = {'from': MY_ADDRESS, 'nonce': w3.eth.get_transaction_count(MY_ADDRESS), 'gasPrice': w3.eth.gas_price}
        claim_function = market_contract.functions.claimWinnings(market_id)
        execute_transaction(claim_function, tx_options)
    except Exception as e:
        print(f"An error occurred during the claim transaction: {e}")

# ==============================================================================
# 4. MAIN EXECUTION
# ==============================================================================
if __name__ == "__main__":
    # --- Instructions ---
    # 1. Fill in the parameters for the action you want to perform.
    # 2. Uncomment ONLY ONE of the action calls below to execute it.
    
    # --- Parameters for your trade ---
    market_to_trade = 247            # <-- !!! REPLACE with the Market ID from Myriad !!!
    outcome_id = 1                   # <-- 1 for YES, 0 for NO
    usdc_amount = 3.0                # <-- The amount of USDC you want to spend
    #shares_amount = 10.5             # <-- The number of shares you want to sell

    # --- CHOOSE YOUR ACTION (UNCOMMENT ONE LINE) ---

    # 1. BUY SHARES in a prediction
    action_buy_prediction(market_id=market_to_trade, outcome_id=outcome_id, usdc_amount=usdc_amount)

    # 2. SELL SHARES in a prediction
    # action_sell_prediction(market_id=market_to_trade, outcome_id=outcome_id, shares_to_sell=shares_amount)

    # 3. CLAIM WINNINGS from a resolved market
    # action_claim_winnings(market_id=market_to_trade)

    print("\nScript finished.")