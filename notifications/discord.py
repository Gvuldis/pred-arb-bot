# notifications/discord.py
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

    def notify_manual_pair(self, platform: str, platform_id: str, poly_id: str):
        """Notify when a manual pair is added."""
        content = (f"**Manual Pair Added ({platform.upper()})**\n"
                   f"{platform.capitalize()} ID: `{platform_id}`\n"
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
        Notify for each Bodega arbitrage opportunity with a detailed execution and payout plan.
        """
        if not summary or summary.get("profit_usd", 0) <= 0:
            log.warning(f"Skipping Bodega arb notification for '{pair}' due to invalid summary.")
            return
        
        profit_usd, profit_ada, roi = summary.get("profit_usd", 0), summary.get("profit_ada", 0), summary.get("roi", 0)
        ada_usd_rate, inferred_B = summary.get("ada_usd_rate", 0), summary.get("inferred_B", 0)
        bodega_shares, bodega_side = summary.get("bodega_shares", 0), summary.get("bodega_side", "?")
        total_cost_bod_ada = summary.get("cost_bod_ada", 0) + summary.get("fee_bod_ada", 0)
        total_cost_bod_usd = total_cost_bod_ada * ada_usd_rate
        poly_shares, poly_side = summary.get("polymarket_shares", 0), summary.get("polymarket_side", "?")
        cost_poly_usd = summary.get("cost_poly_usd", 0)
        payout_bodega_win_usd = bodega_shares * ada_usd_rate
        payout_poly_win_usd = poly_shares

        bodega_url = f"{bodega_api_base.replace('/api', '')}/marketDetails?id={b_id}"

        content = (
            f"@everyone\n"
            f"🚀 **BODEGA Arbitrage Opportunity** 🚀\n\n"
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
            f"  - **Spent on Bodega:** `${total_cost_bod_usd:.2f}`\n"
            f"  - **Spent on Polymarket:** `${cost_poly_usd:.2f}`\n"
            f"  - **Total spent:** `${cost_poly_usd+total_cost_bod_usd:.2f}`\n\n"
            f"  - **Payout if Bodega wins:** `${payout_bodega_win_usd:.2f}`\n"
            f"  - **Payout if Polymarket wins:** `${payout_poly_win_usd:.2f}`\n"
            f"----------------------------------------\n\n"
            f"*Parameters: Inferred B=`{inferred_B:.2f}`, ADA/USD=`${ada_usd_rate:.4f}`*"
        )
        self.send(content)

    def notify_arb_opportunity_myriad(self, pair: str, summary: dict, m_slug: str, p_id: str):
        """Notify for each Myriad arbitrage opportunity."""
        if not summary or summary.get("profit_usd", 0) <= 0:
            log.warning(f"Skipping Myriad arb notification for '{pair}' due to invalid summary.")
            return

        profit_usd, roi = summary.get("profit_usd", 0), summary.get("roi", 0)
        inferred_B = summary.get("inferred_B", 0)
        myriad_shares, myriad_side = summary.get("myriad_shares", 0), summary.get("myriad_side_title", "?")
        total_cost_myr_usd = summary.get("cost_myr_usd", 0)
        poly_shares, poly_side = summary.get("polymarket_shares", 0), summary.get("polymarket_side_title", "?")
        cost_poly_usd = summary.get("cost_poly_usd", 0)
        
        # On Myriad, shares resolve to $1
        payout_myriad_win_usd = myriad_shares
        payout_poly_win_usd = poly_shares

        myriad_url = f"https://app.myriad.social/markets/{m_slug}"

        content = (
            f"@everyone\n"
            f"🚀 **MYRIAD Arbitrage Opportunity** 🚀\n\n"
            f"**Pair:** {pair}\n"
            f"**Profit:** `${profit_usd:.2f} USD` | **ROI:** `{roi*100:.2f}%`\n\n"
            f"----------------------------------------\n"
            f"**Execution Plan:**\n"
            f"**1. Myriad Trade (Execute First!)**\n"
            f"   - **Action:** Buy `{myriad_shares}` **{myriad_side}** shares.\n"
            f"   - **Link:** <{myriad_url}>\n\n"
            f"**2. Polymarket Hedge**\n"
            f"   - **Action:** Buy `{poly_shares}` **{poly_side}** shares.\n"
            f"----------------------------------------\n"
            f"**Cost & Payout Analysis (USD):**\n"
            f"  - **Spent on Myriad:** `${total_cost_myr_usd:.2f}`\n"
            f"  - **Spent on Polymarket:** `${cost_poly_usd:.2f}`\n"
            f"  - **Total spent:** `${cost_poly_usd + total_cost_myr_usd:.2f}`\n\n"
            f"  - **Payout if Myriad wins:** `${payout_myriad_win_usd:.2f}`\n"
            f"  - **Payout if Polymarket wins:** `${payout_poly_win_usd:.2f}`\n"
            f"----------------------------------------\n\n"
            f"*Parameters Used: Inferred B=`{inferred_B:.2f}`*"
        )
        self.send(content)

    def notify_probability_deviation(self, market_name: str, bodega_id: str, bodega_api_base: str, expected_prob: float, live_prob: float, deviation: float):
        """Notify when a Bodega market deviates from its expected probability."""
        bodega_url = f"{bodega_api_base.replace('/api', '')}/marketDetails?id={bodega_id}"

        content = (
            f"🎯 **Probability Deviation Alert** 🎯\n\n"
            f"A significant deviation was detected for a watched market.\n\n"
            f"**Market:** {market_name}\n"
            f"**Bodega ID:** `{bodega_id}`\n"
            f"**Link:** <{bodega_url}>\n\n"
            f"**Expected Probability:** `{expected_prob:.3f}` ({expected_prob*100:.1f}%)\n"
            f"**Current Probability:** `{live_prob:.3f}` ({live_prob*100:.1f}%)\n"
            f"**Deviation:** `{deviation:.3f}` ({deviation*100:.1f}%)\n\n"
            f"This could indicate a new opportunity or market shift."
        )
        self.send(content)