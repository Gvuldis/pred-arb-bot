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
            payload = {
                "content": content,
                "allowed_mentions": {"parse": ["everyone"]}
            }
            response = requests.post(self.webhook_url, json=payload, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to send Discord notification: {e}")
        except Exception as e:
            log.error(f"An unexpected error occurred while sending Discord notification: {e}")

    def notify_manual_pair(self, bodega_id: str, poly_id: str):
        """Notify when a manual pair is added."""
        content = (f"**Manual Pair Added**\n"
                   f"Bodega ID: `{bodega_id}`\n"
                   f"Polymarket ID: `{poly_id}`")
        self.send(content)

    def notify_auto_match(self, matches_count: int, ignored_count: int):
        """Notify upon auto-match completion."""
        content = (f"🔄 **Auto-Match Completed**\n"
                   f"Matches found: {matches_count}\n"
                   f"Ignored: {ignored_count}")
        self.send(content)

    def notify_arb_opportunity(self, pair: str, summary: dict, b_id: str, p_id: str, bodega_api_base: str):
        """
        Notify for each arbitrage opportunity with a detailed execution and payout plan.
        """
        if not summary or summary.get("profit_usd", 0) <= 0:
            log.warning(f"Skipping arb notification for '{pair}' due to invalid summary.")
            return

        # --- Extract and calculate all necessary financial figures ---
        profit_usd = summary.get("profit_usd", 0)
        profit_ada = summary.get("profit_ada", 0)
        roi = summary.get("roi", 0)
        ada_usd_rate = summary.get("ada_usd_rate", 0)
        inferred_B = summary.get("inferred_B", 0)
        
        # Bodega figures
        bodega_shares = summary.get("bodega_shares", 0)
        bodega_side = summary.get("bodega_side", "?")
        cost_bod_ada = summary.get("cost_bod_ada", 0)
        fee_bod_ada = summary.get("fee_bod_ada", 0)
        total_cost_bod_ada = cost_bod_ada + fee_bod_ada
        total_cost_bod_usd = total_cost_bod_ada * ada_usd_rate

        # Polymarket figures
        poly_shares = summary.get("polymarket_shares", 0)
        poly_side = summary.get("polymarket_side", "?")
        cost_poly_usd = summary.get("cost_poly_usd", 0)
        cost_poly_ada = cost_poly_usd / ada_usd_rate if ada_usd_rate > 0 else 0
        
        # Payout calculations
        payout_bodega_win_usd = bodega_shares * ada_usd_rate
        payout_poly_win_usd = poly_shares  # Polymarket shares are 1:1 with USDC

        # --- Construct URLs ---
        bodega_url = f"{bodega_api_base.replace('/api', '')}/marketDetails?id={b_id}"

        # --- Build the rich notification message ---
        content = (
            f"@everyone\n"
            f"🚀 **Arbitrage Opportunity Detected** 🚀\n\n"
            f"**Pair:** {pair}\n"
            f"**Profit:** `${profit_usd:.2f} USD` (`₳{profit_ada:.2f}`) | **ROI:** `{roi*100:.2f}%`\n\n"
            f"----------------------------------------\n"
            f"**Execution Plan:**\n"
            f"**1. Bodega Trade (Execute First!)**\n"
            f"   - **Action:** Buy `{bodega_shares}` **{bodega_side}** shares.\n"
            f"   - **Link:** <{bodega_url}>\n\n"
            f"**2. Polymarket Hedge**\n"
            f"   - **Action:** Buy `{poly_shares}` **{poly_side}** shares.\n"
            f"----------------------------------------\n"
            f"**Cost & Payout Analysis (USD):**\n"
            f"  - **Spent on Bodega:** `${total_cost_bod_usd:.2f}` (`₳{total_cost_bod_ada:.2f}`)\n"
            f"  - **Spent on Polymarket:** `${cost_poly_usd:.2f}` (`₳{cost_poly_ada:.2f}`)\n\n"
            f"  - **Total spent:** `${cost_poly_usd+total_cost_bod_usd:.2f}` (`₳{cost_poly_ada+total_cost_bod_ada:.2f}`)\n\n"
            f"  - **Payout if Bodega wins:** `${payout_bodega_win_usd:.2f}`\n"
            f"  - **Payout if Polymarket wins:** `${payout_poly_win_usd:.2f}`\n"
            f"----------------------------------------\n\n"
            f"*Parameters Used: Inferred B=`{inferred_B:.2f}`, ADA/USD=`${ada_usd_rate:.4f}`*"
        )
        self.send(content)