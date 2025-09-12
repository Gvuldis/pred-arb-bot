from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL # Import SELL for selling orders too
import os # To securely load environment variables

# --- Configuration ---
# It's recommended to load sensitive information from environment variables
# rather than hardcoding them directly in your script.

# host: str = "https://clob.polymarket.com" # Mainnet
host: str = "https://clob.polymarket.com" # Testnet (if available, check Polymarket docs)

# IMPORTANT: Replace with your actual private key. DO NOT SHARE THIS!
# For security, consider loading this from an environment variable:
# key: str = os.environ.get("POLYMARKET_PRIVATE_KEY")
key: str = os.getenv("POLYMARKET_PRIVATE_KEY")# <--- YOUR PRIVATE KEY HERE

chain_id: int = 137  # Polygon Mainnet
# Polymarket Proxy Address if you login with Email/Magic or Browser Wallet.
# key: str = os.environ.get("POLYMARKET_PROXY_ADDRESS")
POLYMARKET_PROXY_ADDRESS: str = os.getenv("POLYMARKET_PROXY_ADDRESS") # <--- YOUR POLYMARKET PROXY ADDRESS HERE (if applicable)

# --- Market Specifics ---
# Replace with the actual Token ID for the market you want to trade.
# You can find this using the Polymarket Markets API:
# https://docs.polymarket.com/developers/gamma-markets-api/get-markets
TARGET_TOKEN_ID: str = "98472312499561003106951400651225799090327453571676668952160833455446414876451" # <--- TARGET TOKEN ID HERE (e.g., '0x...')

# --- Order Parameters ---
# For buying 5 'YES' tokens at a price of 0.50 USDC each (or whatever the token represents)
ORDER_PRICE: float = 0.50 # Price per token (e.g., 0.50 for 50 cents)
ORDER_SIZE: float = 5.0  # Number of tokens to buy/sell
ORDER_SIDE: str = BUY # Or SELL for selling tokens

# --- Client Initialization ---
# Select one of the following initialization options based on your login method.
# Comment out the unused lines.

# 1. Initialization for a Polymarket Proxy associated with an Email/Magic account:
# client = ClobClient(host, key=key, chain_id=chain_id, signature_type=1, funder=POLYMARKET_PROXY_ADDRESS)

# 2. Initialization for a Polymarket Proxy associated with a Browser Wallet (Metamask, Coinbase Wallet, etc):
# client = ClobClient(host, key=key, chain_id=chain_id, signature_type=2, funder=POLYMARKET_PROXY_ADDRESS)

# 3. Initialization for a client that trades directly from an EOA (Externally Owned Account - your wallet's private key directly):
client = ClobClient(host, key=key, chain_id=chain_id)


# --- Set API Credentials ---
# This step is essential for authenticating your requests with the CLOB.
try:
    client.set_api_creds(client.create_or_derive_api_creds())
    print("API credentials set successfully.")
except Exception as e:
    print(f"Error setting API credentials: {e}")
    print("Please ensure your private key and Polymarket Proxy Address (if applicable) are correct.")
    exit() # Exit if we can't set credentials

# --- Create and Sign Order ---
print(f"Preparing to place a {ORDER_SIDE} order for {ORDER_SIZE} tokens at {ORDER_PRICE}...")

order_args = OrderArgs(
    price=ORDER_PRICE,
    size=ORDER_SIZE,
    side=ORDER_SIDE,
    token_id=TARGET_TOKEN_ID,
)

try:
    signed_order = client.create_order(order_args)
    print("Order signed successfully.")
except Exception as e:
    print(f"Error signing order: {e}")
    print("Check your order parameters (price, size, token_id) and client initialization.")
    exit()

# --- Post Order (Good-Till-Cancelled) ---
# This will place a limit order that remains active until filled or cancelled.
print("Posting GTC order to Polymarket...")
try:
    resp = client.post_order(signed_order, OrderType.GTC)
    print("\n--- Order Response ---")
    print(resp)
    print("\nOrder placed successfully!")
except Exception as e:
    print(f"Error posting order: {e}")
    print("Ensure you have sufficient funds and the market is active.")

# You can add more logic here to check order status, cancel orders, etc.
# Refer to the py-clob-client documentation for more functions:
# https://github.com/Polymarket/py-clob-client 