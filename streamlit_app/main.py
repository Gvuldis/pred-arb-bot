# streamlit_app/main.py

import sys, pathlib
# Ensure the project root is on Python’s import path
ROOT = pathlib.Path(__file__).parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import os
import pandas as pd
import streamlit as st

from services.bodega.client import BodegaClient
from services.polymarket.client import PolymarketClient
from services.fx.client import FXClient
from notifications.discord import DiscordNotifier
from services.polymarket.model import build_arbitrage_table

# Database helpers
from streamlit_app.db import (
    init_db,
    save_bodega_markets,
    save_polymarkets,
    save_manual_pair,
    load_manual_pairs,
    load_new_bodega_markets,
    remove_new_bodega_market,
    ignore_bodega_market,
    load_suggested_matches,
    remove_suggested_match
)
# Matching logic
from matching.fuzzy import (
    fetch_all_polymarket_clob_markets,
    fetch_bodega_v3_active_markets,
    fuzzy_match_markets
)

# Initialize database
init_db()

# —–– CONFIG & CLIENTS —––––––––––––––––––––––––
BODEGA_API = "https://testnet.bodegamarket.io/api"
POLY_API   = "https://clob.polymarket.com"
COIN_API   = "https://api.coingecko.com/api/v3/simple/price?ids=cardano&vs_currencies=usd"
WEBHOOK    = "https://discord.com/api/webhooks/1255893289136160869/ZwX3Qo1JsF_fBD0kdmI8-xaEyvah9TnAV_R7dIHIKdBAwpEvj6VgmP3YcOa7j8zpyAPN"


b_client  = BodegaClient(BODEGA_API)
p_client  = PolymarketClient(POLY_API)
fx_client = FXClient(COIN_API)
notifier  = DiscordNotifier(WEBHOOK)

# —–– CACHING —–––––––––––––––––––––––––––––––––
@st.cache_data(ttl=300)
def get_all_bodegas():
    markets = fetch_bodega_v3_active_markets(BODEGA_API)
    save_bodega_markets(markets)
    return markets

@st.cache_data(ttl=300)
def get_all_polymarkets():
    markets = fetch_all_polymarket_clob_markets()
    save_polymarkets(markets)
    return markets

# —–– UI —––––––––––––––––––––––––––––––––––––––
st.set_page_config(layout="wide")
st.title("🌉 Arb-Bot Dashboard")

# Manual addition
st.subheader("➕ Add Manual Pair")
col1, col2, col3 = st.columns([3,3,2])
with col1:
    bid = st.text_input("Bodega ID")
with col2:
    search = st.text_input("Search Polymarket")
    pm_results = p_client.search_markets(search) if search else []
    options = {f'{m["question"]} ({m["condition_id"]})': m["condition_id"] for m in pm_results}
    pid_label = st.selectbox("Pick Polymarket market", [""] + list(options.keys()))
    pid = options.get(pid_label, "")
with col3:
    if st.button("Add Pair"):
        if bid and pid:
            save_manual_pair(bid, pid)
            st.success("Pair added to DB!")
        else:
            st.warning("Please enter both Bodega ID and select a Polymarket market.")

# Show existing manual pairs
manual_pairs = load_manual_pairs()
if manual_pairs:
    st.markdown("**Saved Manual Pairs:**")
    for b_id, p_id in manual_pairs:
    # Bodega link (testnet)
        b_url = f"https://testnet.bodegamarket.io/marketDetails?id={b_id}"
    # Polymarket link (main site)

        st.markdown(
            f"• [Bodega {b_id}]({b_url})  ↔  [Polymarket {p_id}]",
            unsafe_allow_html=True
        )
st.markdown("---")
# —–– Pending New Bodega Markets —–––––––––––––––––––––
st.subheader("🆕 Pending New Bodega Markets")
pending = load_new_bodega_markets()
if not pending:
    st.info("No new Bodega markets awaiting processing.")
else:
    for m in pending:
        st.markdown(f"**{m['market_name']}**  –  Deadline: <t:{m['deadline']}:f>", unsafe_allow_html=True)
        cols = st.columns([3,2,2])
        # Input for Polymarket ID
        with cols[0]:
            poly_input = st.text_input(
                "Polymarket ID", key=f"polyid_{m['market_id']}"
            )
        # Match button
        with cols[1]:
            if st.button("Match", key=f"match_{m['market_id']}"):
                if poly_input:
                    save_manual_pair(m["market_id"], poly_input)
                    remove_new_bodega_market(m["market_id"])
                    st.success(f"Matched Bodega {m['market_name']} ↔ {poly_input}")
                    st.experimental_rerun()
                else:
                    st.error("Enter a Polymarket condition ID before matching.")
        # Ignore button
        with cols[2]:
            if st.button("Ignore", key=f"ignore_{m['market_id']}"):
                ignore_bodega_market(m["market_id"])
                st.warning(f"Ignored Bodega {m['market_name']}")
                st.experimental_rerun()

st.markdown("---")

# Auto-match section
st.subheader("🔄 Auto-Match Markets")
if st.button("Run Auto-Match"):
    bodes = get_all_bodegas()
    polys = get_all_polymarkets()
    matches, ignored = fuzzy_match_markets(bodes, polys)
    st.success(f"Auto-match done: {len(matches)} matches, {len(ignored)} ignored.")
    notifier.notify_auto_match(len(matches), len(ignored))


st.subheader("🔍 Suggested Matches")
suggested = load_suggested_matches()
if not suggested:
    st.info("No fuzzy-match suggestions at this time.")
else:
    for s in suggested:
        # fetch display names from main tables
        b = next((m for m in get_all_bodegas() if m["id"] == s["bodega_id"]), {})
        p = next((m for m in get_all_polymarkets() if m["condition_id"] == s["poly_id"]), {})
        score = s["score"]

        st.markdown(f"**Bodega:** {b.get('name','?')}\n\n**Polymarket:** {p.get('question','?')}\n\nScore: {score:.1f}")
        cols = st.columns([2,2])
        with cols[0]:
            if st.button("Approve", key=f"approve_{s['bodega_id']}"):
                save_manual_pair(s["bodega_id"], s["poly_id"])
                remove_suggested_match(s["bodega_id"], s["poly_id"])
                st.success("✅ Match approved")
                st.experimental_rerun()
        with cols[1]:
            if st.button("Decline", key=f"decline_{s['bodega_id']}"):
                remove_suggested_match(s["bodega_id"], s["poly_id"])
                st.warning("🚫 Match declined")
                st.experimental_rerun()
st.markdown("---")
# 🚀 Check Arbitrage
st.subheader("🚀 Check Arbitrage")
if st.button("Check Arbitrage"):
    ada_usd      = fx_client.get_ada_usd()
    manual_pairs = load_manual_pairs()
    summaries    = []

    from services.polymarket.model import build_arbitrage_table

    for b_id, p_id in manual_pairs:
        # — Fetch Bodega pool config & prices —
        pool     = b_client.fetch_market_config(b_id)
        opts     = pool['options']
        Q_YES = next((o['shares'] for o in opts if o.get('side') == "YES"), 0)
        Q_NO  = next((o['shares'] for o in opts if o.get('side') == "NO"),  0)
        prices_b = b_client.fetch_prices(b_id)
        b_yes_ada = prices_b['yesPrice_ada']
        b_no_ada  = prices_b['noPrice_ada']
        b_yes_usd = b_yes_ada * ada_usd
        b_no_usd  = b_no_ada  * ada_usd

        # — Fetch Polymarket prices —
        try:
            poly = p_client.fetch_market(p_id)
        except Exception:
            continue
        p_yes_usd = poly['best_yes_ask']
        p_no_usd  = poly['best_no_ask']
        question  = poly['question']

        # — Compute arbitrage (x*, profit, roi) —
        x_star, summary, df_table = build_arbitrage_table(
            Q_YES=Q_YES,
            Q_NO =Q_NO,
            P_POLY_YES=p_yes_usd,
            ADA_TO_USD=ada_usd,
            FEE_RATE=0.02,
            B=3000,
        )
        profit = summary['profit_usd']
        roi    = summary['roi']

        # — Only show if profitable —
        if profit > -1000000 and roi > -1000.015:
            notifier.notify_arb_opportunity(question, x_star, profit, roi)

            summaries.append({
                "Pair":            f"{pool['name']} ↔ {question}",
                "Bodega Yes (USD)": f"${b_yes_usd:.4f}",
                "Bodega No  (USD)": f"${b_no_usd:.4f}",
                "Poly Yes   (USD)": f"${p_yes_usd:.4f}",
                "Poly No    (USD)": f"${p_no_usd:.4f}",
                "x*":              f"{x_star:.2f}" if x_star else "N/A",
                "Profit (USD)":    f"${profit:.4f}",
                "ROI":             f"{roi*100:.2f}%"
            })

        # optional: show full payoff table
        if st.checkbox(f"Details for {b_id}", key=b_id):
            st.dataframe(df_table)

    # render results
    if summaries:
        st.table(pd.DataFrame(summaries))
    else:
        st.info("No positive arbitrage opportunities found.")
