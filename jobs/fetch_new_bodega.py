# jobs/fetch_new_bodega.py

from services.bodega.client import BodegaClient              # :contentReference[oaicite:6]{index=6}
from streamlit_app.db import (
    load_bodega_markets,
    save_bodega_markets,
    add_new_bodega_market
)
from notifications.discord import DiscordNotifier, webhook_url
import os
from datetime import datetime

BODEGA_API = os.getenv("BODEGA_API", "https://testnet.bodegamarket.io/api")
b_client   = BodegaClient(BODEGA_API)
notifier = DiscordNotifier(webhook_url)
def fetch_and_notify_new_bodega():
    # 1) IDs already in main snapshot
    existing_ids = {m["market_id"] for m in load_bodega_markets()}

    # 2) Fetch fresh
    fresh = b_client.fetch_markets()

    # 3) Detect brand-new markets
    for m in fresh:
        if m["id"] not in existing_ids:
            add_new_bodega_market({
                "id":       m["id"],
                "name":     m["name"],
                "deadline": m["deadline"]
            })
            # human-readable deadline
            ts = datetime.utcfromtimestamp(m["deadline"] / 1000).strftime("%Y-%m-%d %H:%M UTC")
            msg = (
                "🆕 **New Bodega Market**\n"
                f"**{m['name']}**\n"
                f"Deadline: {ts}\n"
                f"<{BODEGA_API}/markets/{m['id']}>"
            )
            notifier.send(msg)

    # 4) Persist fresh snapshot so we won’t re-notify these next time
    save_bodega_markets([
        {"id": m["id"], "name": m["name"], "deadline": m["deadline"]}
        for m in fresh
    ])
