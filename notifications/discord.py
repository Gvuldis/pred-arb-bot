import requests
import os
from datetime import datetime

webhook_url="https://discord.com/api/webhooks/1255893289136160869/ZwX3Qo1JsF_fBD0kdmI8-xaEyvah9TnAV_R7dIHIKdBAwpEvj6VgmP3YcOa7j8zpyAPN"

class DiscordNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send(self, content: str):
        """
        Send a raw message payload to the Discord webhook.
        """
        if not self.webhook_url:
            return
        try:
            requests.post(self.webhook_url, json={"content": content})
        except Exception:
            pass

    def notify_manual_pair(self, bodega_id: str, poly_id: str):
        """
        Notify when a manual pair is added.
        """
        content = (
            f"**Manual Pair Added**\n"
            f"Bodega ID: `{bodega_id}`\n"
            f"Polymarket ID: `{poly_id}`"
        )
        self.send(content)

    def notify_auto_match(self, matches_count: int, ignored_count: int):
        """
        Notify upon auto-match completion.
        """
        content = (
            f"🔄 **Auto-Match Completed**\n"
            f"Matches found: {matches_count}\n"
            f"Ignored: {ignored_count}"
        )
        self.send(content)

    def notify_arb_opportunity(
        self,
        pair: str,
        x_star: float,
        profit_usd: float,
        roi: float
    ):
        """
        Notify for each arbitrage opportunity.
        """
        content = (
            f"🚀 **Arbitrage Opportunity**\n"
            f"Pair: {pair}\n"
            f"Optimal YES shares (x*): {x_star:.4f}\n"
            f"Profit (USD): ${profit_usd:.4f}\n"
            f"ROI: {roi*100:.2f}%"
        )
        self.send(content)
