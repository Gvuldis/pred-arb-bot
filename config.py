import os
import logging
from dotenv import load_dotenv
from services.bodega.client import BodegaClient
from services.polymarket.client import PolymarketClient
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
COIN_API = os.getenv("COIN_API")
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# LMSR constants for arbitrage calculation
FEE_RATE = 0.04    # 2% fee on Bodega trades
         # LMSR liquidity parameter

# --- Singleton Clients ---
# Initializing clients here makes them act as singletons for the application's lifetime.
log.info("Initializing API clients...")
b_client = BodegaClient(BODEGA_API)
p_client = PolymarketClient(POLY_API)
fx_client = FXClient(COIN_API)

if not WEBHOOK_URL:
    log.warning("DISCORD_WEBHOOK_URL is not set. Discord notifications will be disabled.")
    notifier = None
else:
    notifier = DiscordNotifier(WEBHOOK_URL)
log.info("API clients initialized.")