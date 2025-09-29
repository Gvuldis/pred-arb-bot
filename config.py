import os
import logging
from dotenv import load_dotenv
import json
from web3 import Web3

from services.bodega.client import BodegaClient
from services.polymarket.client import PolymarketClient
from services.myriad.client import MyriadClient
from services.fx.client import FXClient
from notifications.discord import DiscordNotifier

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- API and Configuration Constants ---
BODEGA_API = os.getenv("BODEGA_API")
POLY_API = os.getenv("POLY_API")
MYRIAD_API = "https://api-production.polkamarkets.com"
COIN_API = os.getenv("COIN_API")
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
ABSTRACT_RPC_URL = os.getenv("ABSTRACT_RPC_URL")
POLYMARKET_PROXY_ADDRESS = os.getenv("POLYMARKET_PROXY_ADDRESS")
MYRIAD_PRIVATE_KEY = os.getenv("MYRIAD_PRIVATE_KEY")

# Fee constants for arbitrage calculation
FEE_RATE_BODEGA = 0.02  # 4% total fee on Bodega (2% market + 2% protocol)
FEE_RATE_MYRIAD_BUY = 0.03 # 3% total fee on Myriad buys

# --- Web3 and Myriad Contract Setup ---
myriad_contract = None
myriad_account = None

if not ABSTRACT_RPC_URL:
    log.warning("ABSTRACT_RPC_URL is not set in .env. Myriad on-chain interactions will be disabled.")
else:
    try:
        w3_abs = Web3(Web3.HTTPProvider(ABSTRACT_RPC_URL))
        if not w3_abs.is_connected():
            raise ConnectionError("Failed to connect to Abstract RPC")
        
        if MYRIAD_PRIVATE_KEY:
            myriad_account = w3_abs.eth.account.from_key(MYRIAD_PRIVATE_KEY)
            log.info(f"Initialized Myriad account: {myriad_account.address}")
        else:
            log.warning("MYRIAD_PRIVATE_KEY not set. Myriad wallet interactions will be disabled.")
        
        MYRIAD_MARKET_ADDRESS = "0x3e0f5F8F5FB043aBFA475C0308417Bf72c463289"
        MYRIAD_MARKET_ABI = json.loads('[{"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"},{"internalType":"uint256","name":"outcomeId","type":"uint256"}],"name":"getMarketOutcomePrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"marketId","type":"uint256"},{"internalType":"address","name":"user","type":"address"}],"name":"getUserMarketShares","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256[]","name":"outcomes","type":"uint256[]"}],"stateMutability":"view","type":"function"}]')
        
        myriad_contract = w3_abs.eth.contract(
            address=Web3.to_checksum_address(MYRIAD_MARKET_ADDRESS),
            abi=MYRIAD_MARKET_ABI
        )
        log.info("Successfully connected to Abstract RPC and initialized Myriad contract.")
    except Exception as e:
        log.error(f"Failed to initialize Web3 for Abstract, disabling on-chain prices: {e}")
        myriad_contract = None
        myriad_account = None


# --- Singleton Clients ---
# Initializing clients here makes them act as singletons for the application's lifetime.
log.info("Initializing API clients...")
b_client = BodegaClient(BODEGA_API)
p_client = PolymarketClient(POLY_API)
m_client = MyriadClient(MYRIAD_API, myriad_contract)
fx_client = FXClient(COIN_API)

if not WEBHOOK_URL:
    log.warning("DISCORD_WEBHOOK_URL is not set. Discord notifications will be disabled.")
    notifier = None
else:
    notifier = DiscordNotifier(WEBHOOK_URL)
log.info("API clients initialized.")
