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

    def notify_arb_opportunity(self, pair: str, summary: dict, b_id: str, p_id: str, bodega_api_base: str):
        """
        Notify for each Bodega arbitrage opportunity with a detailed execution and payout plan.
        """
        if not summary or summary.get("profit_usd", 0) <= 0:
            log.warning(f"Skipping Bodega arb notification for '{pair}' due to invalid summary.")
            return
        
        profit_usd, profit_ada, roi, apy = summary.get("profit_usd", 0), summary.get("profit_ada", 0), summary.get("roi", 0), summary.get("apy", 0)
        ada_usd_rate, inferred_B = summary.get("ada_usd_rate", 0), summary.get("inferred_B", 0)
        bodega_shares, bodega_side = summary.get("bodega_shares", 0), summary.get("bodega_side", "?")
        total_cost_bod_ada = summary.get("cost_bod_ada", 0) + summary.get("fee_bod_ada", 0)
        total_cost_bod_usd = total_cost_bod_ada * ada_usd_rate
        poly_shares, poly_side = summary.get("polymarket_shares", 0), summary.get("polymarket_side", "?")
        cost_poly_usd = summary.get("cost_poly_usd", 0)
        payout_bodega_win_usd = bodega_shares * ada_usd_rate
        payout_poly_win_usd = poly_shares

        bodega_url = f"{bodega_api_base.replace('/api', '')}/marketDetails?id={b_id}"
        poly_url = f"https://polymarket.com/event/{p_id}"

        content = (
            f"@everyone\n"
            f"🚀 **BODEGA Arbitrage Opportunity** 🚀\n\n"
            f"**Pair:** {pair}\n"
            f"**Profit:** `${profit_usd:.2f} USD` (`₳{profit_ada:.2f}`) | **ROI:** `{roi*100:.2f}%` | **APY:** `{apy*100:.2f}%`\n\n"
            f"----------------------------------------\n"
            f"**Execution Plan:**\n"
            f"**1. Bodega Trade (Execute First!)**\n"
            f"   - **Action:** Buy `{bodega_shares}` **{bodega_side}** shares.\n"
            f"   - **Link:** <{bodega_url}>\n\n"
            f"**2. Polymarket Hedge**\n"
            f"   - **Action:** Buy `{poly_shares}` **{poly_side}** shares.\n"
            f"   - **Link:** <{poly_url}>\n"
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

        profit_usd, roi, apy = summary.get("profit_usd", 0), summary.get("roi", 0), summary.get("apy", 0)
        liquidity_param = summary.get("B", 0)
        myriad_shares, myriad_side = summary.get("myriad_shares", 0), summary.get("myriad_side_title", "?")
        myriad_price = summary.get('myriad_current_price')
        total_cost_myr_usd = summary.get("cost_myr_usd", 0)
        poly_shares, poly_side = summary.get("polymarket_shares", 0), summary.get("polymarket_side_title", "?")
        poly_price = summary.get('poly_current_price')
        cost_poly_usd = summary.get("cost_poly_usd", 0)
        
        payout_myriad_win_usd = myriad_shares
        payout_poly_win_usd = poly_shares

        myriad_url = f"https://app.myriad.social/markets/{m_slug}"
        poly_url = f"https://polymarket.com/event/{p_id}"

        # Format price strings, handle None case
        myriad_price_str = f"**Current Price:** `${myriad_price:.4f}`\n   - " if myriad_price is not None else ""
        poly_price_str = f"**Current Price:** `${poly_price:.4f}`\n   - " if poly_price is not None else ""

        content = (
            f"@everyone\n"
            f"🚀 **MYRIAD Arbitrage Opportunity** 🚀\n\n"
            f"**Pair:** {pair}\n"
            f"**Profit:** `${profit_usd:.2f} USD` | **ROI:** `{roi*100:.2f}%` | **APY:** `{apy*100:.2f}%`\n\n"
            f"----------------------------------------\n"
            f"**Execution Plan:**\n"
            f"**1. Myriad Trade (Execute First!)**\n"
            f"   - **Action:** Buy `{myriad_shares}` **{myriad_side}** shares.\n"
            f"   - {myriad_price_str}**Link:** <{myriad_url}>\n\n"
            f"**2. Polymarket Hedge**\n"
            f"   - **Action:** Buy `{poly_shares}` **{poly_side}** shares.\n"
            f"   - {poly_price_str}**Link:** <{poly_url}>\n"
            f"----------------------------------------\n"
            f"**Cost & Payout Analysis (USD):**\n"
            f"  - **Spent on Myriad:** `${total_cost_myr_usd:.2f}`\n"
            f"  - **Spent on Polymarket:** `${cost_poly_usd:.2f}`\n"
            f"  - **Total spent:** `${cost_poly_usd + total_cost_myr_usd:.2f}`\n\n"
            f"  - **Payout if Myriad wins:** `${payout_myriad_win_usd:.2f}`\n"
            f"  - **Payout if Polymarket wins:** `${payout_poly_win_usd:.2f}`\n"
            f"----------------------------------------\n\n"
            f"*Parameters Used: Liquidity (B)=`{liquidity_param:.2f}`*"
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

    # --- ARB-EXECUTOR NOTIFICATIONS ---
    def notify_autotrade_success(self, market_title: str, profit: float, poly_shares: float, poly_cost: float, myriad_cost_est: float):
        content = (
            f"✅ @everyone **AUTOMATED ARB EXECUTED**\n\n"
            f"**Market**: `{market_title}`\n"
            f"**Est. Profit**: `${profit:.2f}`\n\n"
            f"**Polymarket Leg:**\n"
            f"- Bought `{poly_shares:.4f}` shares for `${poly_cost:.2f}`\n\n"
            f"**Myriad Leg:**\n"
            f"- Sent `${myriad_cost_est:.2f}` to buy shares.\n\n"
            f"*Myriad trade details will be fetched from the API shortly...*"
        )
        self.send(content)

    def notify_myriad_trade_details_found(self, market_title: str, trade_id: str, myriad_shares: float, myriad_cost: float):
        content = (
            f"ℹ️ **Myriad Trade Details Confirmed**\n\n"
            f"**Market**: `{market_title}`\n"
            f"**Trade ID**: `{trade_id}`\n"
            f"**Myriad Leg Confirmed:**\n"
            f"- Bought `{myriad_shares:.4f}` shares for `${myriad_cost:.2f}`"
        )
        self.send(content)

    def notify_autotrade_failure(self, market_title: str, reason: str, status: str):
        content = f"❌ **AUTOMATED ARB FAILED**: {market_title}.\n**Status**: `{status}`\n**Reason**: {reason}"
        self.send(content)
        
    def notify_autotrade_panic(self, market_title: str, error_msg: str):
        content = (f"🚨 @everyone **CRITICAL FAILURE: HEDGE FAILED** on {market_title}.\n"
                   f"Succeeded on Polymarket, failed on Myriad. **ATTEMPTING TO UNWIND POSITION.**\n"
                   f"MANUAL INTERVENTION MAY BE REQUIRED.\n"
                   f"Error: {error_msg}")
        self.send(content)

    def notify_autotrade_dry_run(self, market_title: str, profit: float):
        content = f"DRY RUN: Would have executed arb on **{market_title}** for **${profit:.2f}** profit."
        self.send(content)

    def notify_critical_alert(self, title: str, message: str):
        content = (
            f"🚨 @everyone **CRITICAL ALERT: {title}** 🚨\n\n"
            f"{message}"
        )
        self.send(content)