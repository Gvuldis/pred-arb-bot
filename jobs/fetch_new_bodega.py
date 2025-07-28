import logging
from datetime import datetime
from config import b_client, notifier, BODEGA_API
from streamlit_app.db import (
    load_bodega_markets,
    save_bodega_markets,
    add_new_bodega_market
)

log = logging.getLogger(__name__)

def fetch_and_notify_new_bodega():
    log.info("Starting job to fetch new Bodega markets...")
    try:
        # 1) IDs already in main snapshot
        # BUGFIX: The dicts from load_bodega_markets have the key 'id', not 'market_id'.
        existing_ids = {m["id"] for m in load_bodega_markets()}

        # 2) Fetch fresh
        fresh_markets = b_client.fetch_markets(force_refresh=True)

        new_markets_found = []
        # 3) Detect brand-new markets
        for m in fresh_markets:
            if m["id"] not in existing_ids:
                add_new_bodega_market({
                    "id":       m["id"],
                    "name":     m["name"],
                    "deadline": m["deadline"]
                })
                new_markets_found.append(m)

        # 4) Notify about all new markets at once, if any
        if notifier and new_markets_found:
            log.info(f"Found {len(new_markets_found)} new Bodega markets. Notifying...")
            message_parts = ["@everyone 🆕 **New Bodega Markets Detected**"]
            for m in new_markets_found:
                # human-readable deadline
                ts = datetime.utcfromtimestamp(m["deadline"] / 1000).strftime("%Y-%m-%d %H:%M UTC")
                market_url = f"{BODEGA_API.replace('/api', '')}/marketDetails?id={m['id']}"
                message_parts.append(
                    f"\n- **{m['name']}**\n  Deadline: {ts}\n  <{market_url}>"
                )
            # Discord has a 2000 character limit per message
            full_message = "\n".join(message_parts)
            if len(full_message) > 2000:
                # Truncate if too long
                full_message = full_message[:1900] + "\n... (message truncated)"
            notifier.send(full_message)
        elif new_markets_found:
            log.warning("New markets found, but Discord notifier is not configured.")
        else:
            log.info("No new Bodega markets found.")

        # 5) Persist fresh snapshot so we won’t re-notify these next time
        if fresh_markets:
            save_bodega_markets(fresh_markets)
            log.info(f"Saved/updated {len(fresh_markets)} Bodega markets in the database.")
            
    except Exception as e:
        log.error(f"Failed to fetch and notify new Bodega markets: {e}", exc_info=True)