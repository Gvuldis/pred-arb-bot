import requests
import logging

log = logging.getLogger(__name__)

class DiscordNotifier:
    def __init__(self, webhook_url: str):
        if not webhook_url or not webhook_url.startswith("https://discord.com/api/webhooks/"):
            log.warning("Invalid or missing Discord webhook URL.")
            self.webhook_url = None
        else:
            self.webhook_url = webhook_url

    def send(self, content: str):
        """
        Send a raw message payload to the Discord webhook.
        """
        if not self.webhook_url:
            return
        try:
            response = requests.post(self.webhook_url, json={"content": content}, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to send Discord notification: {e}")
        except Exception as e:
            log.error(f"An unexpected error occurred while sending Discord notification: {e}")

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

    def notify_arb_opportunity(self, pair: str, summary: dict):
        """
        Notify for each arbitrage opportunity using detailed summary.
        """
        if not summary or summary.get("profit_usd", 0) <= 0:
            log.warning(f"Skipping arb notification for '{pair}' due to invalid summary.")
            return

        profit_usd = summary["profit_usd"]
        roi = summary["roi"]
        direction = summary.get("direction", "N/A").replace("_", " ").title()
        bodega_shares = summary.get("bodega_shares", 0)
        bodega_side = summary.get("bodega_side", "?")
        poly_shares = summary.get("polymarket_shares", 0)
        poly_side = summary.get("polymarket_side", "?")

        content = (
            f"🚀 **Arbitrage Opportunity**\n"
            f"Pair: {pair}\n"
            f"Direction: **{direction}**\n\n"
            f"**Trades:**\n"
            f" - **Bodega**: Buy `{bodega_shares:.2f} {bodega_side}` shares\n"
            f" - **Polymarket**: Buy `{poly_shares:.2f} {poly_side}` shares\n\n"
            f"Profit (USD): **${profit_usd:.4f}**\n"
            f"ROI: **{roi*100:.2f}%**"
        )
        self.send(content)