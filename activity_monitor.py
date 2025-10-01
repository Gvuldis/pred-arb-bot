# activity_monitor.py
import os
import requests
import time
import logging
from collections import deque
from notifications.discord import DiscordNotifier

# --- Configuration ---
# The API endpoint for recent Bodega trades
BODEGA_ACTIVITY_URL = "https://v3.bodegamarket.io/api/stats/getRecentActivity"

# The threshold for what you consider a "large" trade
LARGE_TRADE_THRESHOLD_SHARES = 500

# Your Discord webhook URL, loaded from the .env file
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# How often to check the API, in seconds
POLL_INTERVAL_SECONDS = 20

# --- MEMORY LEAK FIX: Capped Recent Hashes ---
# We will only store the last 2000 transaction hashes. This is more than enough
# to prevent duplicate alerts from recent activity and completely stops memory growth.
MAX_SEEN_HASHES = 2000

# --- Initialization ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)
notifier = DiscordNotifier(DISCORD_WEBHOOK_URL)

# --- Main Monitoring Function ---
def monitor_bodega_activity():
    """
    Continuously polls the Bodega activity API and sends a Discord alert
    for new, large buy orders.
    """
    if not all([DISCORD_WEBHOOK_URL, notifier]):
        log.error("Missing DISCORD_WEBHOOK_URL. Exiting.")
        return

    # A deque with a max length acts as a "rolling" set. When it's full,
    # adding a new item automatically discards the oldest one. This is the fix.
    seen_tx_hashes = deque(maxlen=MAX_SEEN_HASHES)

    log.info(f"Starting Bodega activity monitor. Alerting on trades > {LARGE_TRADE_THRESHOLD_SHARES} shares.")
    log.info(f"Memory management is active: keeping the last {MAX_SEEN_HASHES} transaction hashes.")

    while True:
        try:
            response = requests.get(BODEGA_ACTIVITY_URL, timeout=10)
            response.raise_for_status()
            activity_data = response.json().get("data", [])

            # We iterate through the list in reverse to process oldest-to-newest
            for trade in reversed(activity_data):
                tx_hash = trade.get("txHash")
                action = trade.get("action")
                amount = trade.get("amount", 0)

                # Skip if we've seen it, it's not a buy, or it's too small
                if not tx_hash or tx_hash in seen_tx_hashes:
                    continue
                
                if action != "Buy Position":
                    continue

                if amount >= LARGE_TRADE_THRESHOLD_SHARES:
                    log.warning(f"!!! LARGE TRADE DETECTED: {amount} shares on market {trade.get('id')} !!!")
                    
                    # Construct the alert message for Discord
                    market_id = trade.get('id')
                    side = trade.get('side')
                    
                    # Create a clickable link to the transaction on Cardanoscan
                    tx_url = f"https://cardanoscan.io/transaction/{tx_hash}"

                    message = (
                        f"📈 **Large Bodega Trade Alert** 📈\n\n"
                        f"A trade of **{amount} {side} shares** was just confirmed.\n\n"
                        f"**Market ID:** `{market_id}`\n"
                        f"**Transaction:** <{tx_url}>\n\n"
                        f"This might impact market prices."
                    )
                    
                    notifier.send(message)

                # Add the hash to our deque. If it's full, the oldest hash is dropped.
                seen_tx_hashes.append(tx_hash)

        except requests.exceptions.RequestException as e:
            log.error(f"Error connecting to Bodega API: {e}")
        except Exception as e:
            log.error(f"An unexpected error occurred: {e}", exc_info=True)

        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    monitor_bodega_activity()