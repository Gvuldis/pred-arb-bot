# jobs/fetch_new_myriad.py
import logging
from datetime import datetime
from config import m_client, notifier
from streamlit_app.db import (
    load_myriad_markets,
    save_myriad_markets,
    add_new_myriad_market
)

log = logging.getLogger(__name__)

def fetch_and_notify_new_myriad():
    log.info("Starting job to fetch new Myriad markets...")
    try:
        # 1) Get IDs already in main snapshot
        existing_ids = {m["id"] for m in load_myriad_markets()}

        # 2) Fetch fresh markets from the API
        fresh_markets = m_client.fetch_markets()
        if not fresh_markets:
            log.info("No active Myriad markets found from API.")
            return

        new_markets_found = []
        # 3) Detect brand-new markets
        for m in fresh_markets:
            market_id = m.get("id")
            if market_id and market_id not in existing_ids:
                add_new_myriad_market({
                    "id": market_id,
                    "slug": m.get("slug"),
                    "name": m.get("title"),
                    "expires_at": m.get("expires_at")
                })
                new_markets_found.append(m)

        # 4) Notify about all new markets at once, if any
        if notifier and new_markets_found:
            log.info(f"Found {len(new_markets_found)} new Myriad markets. Notifying...")
            message_parts = ["@everyone 🆕 **New Myriad Markets Detected**"]
            for m in new_markets_found:
                expires_at_str = m.get("expires_at", "N/A")
                try:
                    # Format the date string for better readability
                    dt_object = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
                    ts = dt_object.strftime("%Y-%m-%d %H:%M UTC")
                except (ValueError, TypeError):
                    ts = expires_at_str
                
                market_url = f"https://app.myriad.social/markets/{m.get('slug')}"
                message_parts.append(
                    f"\n- **{m.get('title')}**\n  Expires: {ts}\n  <{market_url}>"
                )
            
            full_message = "\n".join(message_parts)
            if len(full_message) > 2000:
                full_message = full_message[:1900] + "\n... (message truncated)"
            notifier.send(full_message)
        elif new_markets_found:
            log.warning("New Myriad markets found, but Discord notifier is not configured.")
        else:
            log.info("No new Myriad markets found.")

        # 5) Persist fresh snapshot
        save_myriad_markets(fresh_markets)
        log.info(f"Saved/updated {len(fresh_markets)} Myriad markets in the database.")
            
    except Exception as e:
        log.error(f"Failed to fetch and notify new Myriad markets: {e}", exc_info=True)