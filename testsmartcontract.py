# test_direct_onchain_price.py
import os
import logging
import json
from dotenv import load_dotenv
from web3 import Web3

# --- 1. Setup ---
# Configure logging to see the detailed output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Load environment variables from your .env file
load_dotenv()

# --- 2. Configuration ---
# Get the RPC URL from your environment variables
ABSTRACT_RPC_URL = os.getenv("ABSTRACT_RPC_URL", "https://api.mainnet.abs.xyz")

# --- Smart Contract Details ---
# This is the address of the Myriad Market contract on the Abstract chain
MYRIAD_MARKET_ADDRESS = "0x3e0f5F8F5FB043aBFA475C0308417Bf72c463289"
# This is the minimal ABI needed to call the getMarketOutcomePrice function
MYRIAD_MARKET_ABI = json.loads('[{"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"},{"internalType":"uint256","name":"outcomeId","type":"uint256"}],"name":"getMarketOutcomePrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]')

# --- Market to Test ---
# IMPORTANT: Change this to a currently active Myriad market if the one below is closed.
# You can find the market ID in the URL of a market on the Myriad website.
TEST_MARKET_SLUG = "btc-above-105k-throughout-september"
TEST_MARKET_ID = 289 # The ID for the market slug above

# --- 3. Main Test Function ---
def test_direct_price_fetch():
    """
    Tests the ability to fetch a market price directly from the Myriad smart contract.
    """
    log.info("--- Starting Direct On-Chain Price Fetch Demo ---")

    if not ABSTRACT_RPC_URL:
        log.error("FATAL: ABSTRACT_RPC_URL is not set in your .env file. The test cannot run.")
        return

    try:
        # Initialize a connection to the Abstract blockchain
        w3 = Web3(Web3.HTTPProvider(ABSTRACT_RPC_URL))
        if not w3.is_connected():
            log.error(f"Failed to connect to the RPC URL: {ABSTRACT_RPC_URL}")
            return

        log.info(f"Successfully connected to RPC endpoint.")

        # Create a contract object to interact with
        myriad_contract = w3.eth.contract(
            address=Web3.to_checksum_address(MYRIAD_MARKET_ADDRESS),
            abi=MYRIAD_MARKET_ABI
        )
        log.info(f"Successfully created contract instance for market: {TEST_MARKET_SLUG} (ID: {TEST_MARKET_ID})")

        # --- Call the Smart Contract ---
        log.info("Querying contract for Outcome 0 price...")
        price0_scaled = myriad_contract.functions.getMarketOutcomePrice(TEST_MARKET_ID, 0).call()
        
        price0 = float(price0_scaled / 10**18)

        log.info("Querying contract for Outcome 1 price...")
        price1_scaled = myriad_contract.functions.getMarketOutcomePrice(TEST_MARKET_ID, 1).call()
        
        price1 = float(price1_scaled / 10**18)

        # --- 4. Display Results ---
        print("\n" + "="*50)
        print("          ON-CHAIN PRICE RESULTS          ")
        print("="*50)
        print(f"Market: {TEST_MARKET_SLUG} (ID: {TEST_MARKET_ID})")
        print(f"\nOutcome 0 ('Yes') Price: {price0:.4f}")
        print(f"Outcome 1 ('No')  Price: {price1:.4f}")
        print(f"Sum of Prices:         {(price0 + price1):.4f}")
        print("\n" + "="*50)
        log.info("--- Demo Finished Successfully ---")

    except Exception as e:
        log.error(f"An error occurred during the test: {e}", exc_info=True)
        log.info("--- Demo Finished with Errors ---")


# --- 5. Run the script ---
if __name__ == "__main__":
    test_direct_price_fetch()