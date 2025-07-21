import sys, pathlib, time
# Ensure the project root is on Python’s import path
ROOT = pathlib.Path(__file__).parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st
import logging

from config import b_client, p_client, fx_client, notifier, BODEGA_API, FEE_RATE, log
from services.polymarket.model import build_arbitrage_table, infer_b

# Database helpers
from streamlit_app.db import (
    init_db,
    save_bodega_markets,
    save_polymarkets,
    save_manual_pair,
    load_manual_pairs,
    delete_manual_pair,
    load_new_bodega_markets,
    remove_new_bodega_market,
    ignore_bodega_market,
    add_suggested_match,
    load_suggested_matches,
    remove_suggested_match,
    add_manual_bodega_market,
    load_manual_bodega_markets,
    delete_manual_bodega_market
)
# Matching logic
from matching.fuzzy import (
    fetch_all_polymarket_clob_markets,
    fetch_bodega_v3_active_markets,
    fuzzy_match_markets
)

# Initialize database
init_db()

# —–– CACHING —–––––––––––––––––––––––––––––––––
@st.cache_data(ttl=300)
def get_all_bodegas():
    markets = fetch_bodega_v3_active_markets(BODEGA_API)
    real_markets = [m for m in markets if not m['id'].startswith("TEST-")]
    if real_markets:
        save_bodega_markets(real_markets)
    return markets

@st.cache_data(ttl=300)
def get_all_polymarkets():
    markets = fetch_all_polymarket_clob_markets()
    if markets:
        save_polymarkets(markets)
    return markets

# —–– UI —––––––––––––––––––––––––––––––––––––––
st.set_page_config(layout="wide")
st.title("🌉 Arb-Bot Dashboard")

# Manual addition
st.subheader("➕ Add Manual Pair")
col1, col2, col3 = st.columns([3,3,1])
with col1:
    bid = st.text_input("Bodega ID", key="manual_pair_bodega_id")
with col2:
    search = st.text_input("Search Polymarket", key="manual_pair_poly_search")
    pm_results = p_client.search_markets(search) if search else []
    options = {f'{m["question"]} ({m["condition_id"]})': m["condition_id"] for m in pm_results}
    pid_label = st.selectbox("Pick Polymarket market", [""] + list(options.keys()))
    pid = options.get(pid_label, "")
with col3:
    st.write("") # Spacer
    st.write("") # Spacer
    if st.button("Add Pair"):
        if bid and pid:
            save_manual_pair(bid, pid)
            if notifier:
                notifier.notify_manual_pair(bid, pid)
            st.success("Pair added to DB!")
            st.rerun()
        else:
            st.warning("Please enter both Bodega ID and select a Polymarket market.")

# Show existing manual pairs
manual_pairs = load_manual_pairs()
if manual_pairs:
    st.markdown("**Saved Manual Pairs:**")
    for b_id, p_id in manual_pairs:
        c1, c2 = st.columns([9, 1])
        b_url = f"{BODEGA_API.replace('/api', '')}/marketDetails?id={b_id}"
        p_url = f"https://polymarket.com/event/{p_id}"
        c1.markdown(f"• [Bodega]({b_url}) `({b_id})` ↔ [Polymarket]({p_url}) `({p_id})`", unsafe_allow_html=True)
        if c2.button("❌", key=f"del_pair_{b_id}_{p_id}", help="Delete this pair"):
            delete_manual_pair(b_id, p_id)
            st.rerun()

st.markdown("---")

# —–– Pending New Bodega Markets —–––––––––––––––––––––
st.subheader("🆕 Pending New Bodega Markets")
pending = load_new_bodega_markets()
if not pending:
    st.info("No new Bodega markets awaiting processing.")
else:
    for m in pending:
        st.markdown(f"**{m['market_name']}**  (ID: `{m['market_id']}`)", unsafe_allow_html=True)
        cols = st.columns([3,1,1])
        with cols[0]:
            poly_input = st.text_input("Polymarket Condition ID", key=f"polyid_{m['market_id']}")
        with cols[1]:
            st.write("") # spacer
            st.write("") # spacer
            if st.button("Match", key=f"match_{m['market_id']}"):
                if poly_input:
                    save_manual_pair(m["market_id"], poly_input)
                    remove_new_bodega_market(m["market_id"])
                    if notifier:
                        notifier.notify_manual_pair(m['market_id'], poly_input)
                    st.success(f"Matched Bodega {m['market_name']} ↔ {poly_input}")
                    st.rerun()
                else:
                    st.error("Enter a Polymarket condition ID before matching.")
        with cols[2]:
            st.write("") # spacer
            st.write("") # spacer
            if st.button("Ignore", key=f"ignore_{m['market_id']}"):
                ignore_bodega_market(m["market_id"])
                st.warning(f"Ignored Bodega {m['market_name']}")
                st.rerun()

st.markdown("---")

# Auto-match section
st.subheader("🔄 Auto-Match Markets")
if st.button("Run Auto-Match"):
    with st.spinner("Fetching markets and running fuzzy matching..."):
        bodes = get_all_bodegas()
        polys = get_all_polymarkets()
        matches, ignored_count = fuzzy_match_markets(bodes, polys)
        st.success(f"Auto-match done: {len(matches)} matches found, {ignored_count} pairs ignored.")
        if notifier:
            notifier.notify_auto_match(len(matches), ignored_count)
        for b, p, score in matches:
            add_suggested_match(b["id"], p["condition_id"], score)
        st.rerun()

st.subheader("🔍 Suggested Matches")
suggested = load_suggested_matches()
if not suggested:
    st.info("No fuzzy-match suggestions at this time.")
else:
    for s in suggested:
        try:
            b = b_client.fetch_market_config(s["bodega_id"])
            p = p_client.fetch_market(s["poly_id"])
        except Exception:
            continue
        score = s["score"]
        st.markdown(f"**Bodega:** {b.get('name','?')}\n\n**Polymarket:** {p.get('question','?')}\n\nScore: {score:.1f}")
        cols = st.columns([1,1,4])
        if cols[0].button("Approve", key=f"approve_{s['bodega_id']}_{s['poly_id']}"):
            save_manual_pair(s["bodega_id"], s["poly_id"])
            remove_suggested_match(s["bodega_id"], s["poly_id"])
            st.success("✅ Match approved")
            st.rerun()
        if cols[1].button("Decline", key=f"decline_{s['bodega_id']}_{s['poly_id']}"):
            remove_suggested_match(s["bodega_id"], s["poly_id"])
            st.warning("🚫 Match declined")
            st.rerun()
st.markdown("---")

# 🚀 Check Arbitrage
st.subheader("🚀 Check Arbitrage")

if st.button("Check All Manual Pairs for Arbitrage"):
    with st.spinner("Checking for arbitrage opportunities..."):
        ada_usd = fx_client.get_ada_usd()
        manual_pairs = load_manual_pairs()
        table_rows = []
        
        if not manual_pairs:
            st.warning("No manual pairs to check. Please add some.")
        else:
            prog = st.progress(0)
            for i, (b_id, p_id) in enumerate(manual_pairs, start=1):
                try:
                    pool = b_client.fetch_market_config(b_id)
                    pdata = p_client.fetch_market(p_id)
                    bodega_prediction_info = b_client.fetch_prices(b_id)
                    
                    order_book_yes = pdata.get("order_book_yes")
                    order_book_no = pdata.get("order_book_no")

                    if not all([pool, pdata, bodega_prediction_info, order_book_yes, order_book_no]):
                        st.warning(f"Could not fetch complete data for pair ({b_id}, {p_id}). Skipping.")
                        continue
                    
                    Q_YES = bodega_prediction_info.get("yesVolume_ada", 0)
                    Q_NO = bodega_prediction_info.get("noVolume_ada", 0)
                    
                    # --- DYNAMIC B CALCULATION ---
                    p_bod_yes = bodega_prediction_info.get("yesPrice_ada")

                    if not p_bod_yes:
                        st.warning(f"Skipping pair ({b_id}, {p_id}): Could not fetch live Bodega YES price.")
                        continue
                    
                    try:
                        inferred_B = infer_b(Q_YES, Q_NO, p_bod_yes)
                        st.info(f"Inferred B for market {b_id}: {inferred_B:.2f}")
                    except ValueError as e:
                        st.warning(f"Skipping pair ({b_id}, {p_id}): Could not infer B parameter. Reason: {e}")
                        continue
                    # --- END DYNAMIC B CALCULATION ---

                    x_star, summary, _ = build_arbitrage_table(
                        Q_YES=Q_YES, Q_NO=Q_NO,
                        ORDER_BOOK_YES=order_book_yes, ORDER_BOOK_NO=order_book_no,
                        ADA_TO_USD=ada_usd, FEE_RATE=FEE_RATE, B=inferred_B
                    )
                    
                    if summary and summary.get("profit_usd", 0) > 0:
                        desc = f"{pool['name']} → {pdata['question']}"
                        st.write(f"**Found Opportunity for:** {desc}")
                        
                        if notifier:
                            notifier.notify_arb_opportunity(desc, summary, b_id, p_id, BODEGA_API)
                        
                        row_bodega = {
                            "Market": "Bodega", "Side": summary['bodega_side'],
                            "StartP": f"{summary['p_start']:.4f}", "EndP": f"{summary['p_end']:.4f}",
                            "Shares": f"{summary['bodega_shares']}", "Cost ADA": f"{summary['cost_bod_ada']:.2f}",
                            "Fee ADA": f"{summary['fee_bod_ada']:.2f}", "AvgPoly": "",
                            "Comb ADA": f"{summary['comb_ada']:.2f}", "Comb USD": f"{summary['comb_usd']:.2f}",
                            "Profit ADA": f"{summary['profit_ada']:.2f}", "Profit USD": f"{summary['profit_usd']:.2f}",
                            "Margin": f"{summary['roi']*100:.2f}%", "Fill": "",
                            "Inferred B": f"{summary['inferred_B']:.2f}",
                            "ADA/USD Rate": f"${summary['ada_usd_rate']:.4f}",
                        }
                        row_poly = {
                            "Market": "Polymarket", "Side": summary['polymarket_side'], "Shares": f"{summary['polymarket_shares']}",
                            "Cost ADA": f"{summary['cost_poly_ada']:.2f}", "AvgPoly": f"{summary['avg_poly_price']:.4f}",
                            "Fill": f"{summary['fill']}",
                        }
                        table_rows.append(row_bodega)
                        table_rows.append(row_poly)
                        
                except Exception as e:
                    log.exception("Error checking pair %s / %s", b_id, p_id)
                    st.error(f"Error for ({b_id}, {p_id}): {e}")
                
                prog.progress(i / len(manual_pairs))
            prog.empty()
            
            if table_rows:
                columns = ["Market", "Side", "StartP", "EndP", "Shares", "Cost ADA", "Fee ADA", "AvgPoly",
                           "Comb ADA", "Comb USD", "Profit ADA", "Profit USD", "Margin", "Fill",
                           "Inferred B", "ADA/USD Rate"]
                df = pd.DataFrame(table_rows, columns=columns).fillna('')
                st.dataframe(df)
            else:
                st.info("No arbitrage opportunities found.")

st.markdown("---")
# Manual Bodega markets for testing
st.subheader("🧪 Manual Bodega Markets for Testing")
with st.form("add_manual_bodega_market_form", clear_on_submit=True):
    st.write("Create a fake Bodega market to test fuzzy matching. It will be active for 24 hours.")
    manual_id = st.text_input("Manual Market ID (e.g., TEST-001)", "TEST-001", key="test_market_id")
    manual_name = st.text_input("Manual Market Name", "Will a new Taylor Swift album be released by the end of 2025?", key="test_market_name")
    submitted = st.form_submit_button("Add Test Market")
    if submitted:
        if manual_id and manual_name:
            deadline = int((time.time() + 24*60*60) * 1000)
            add_manual_bodega_market(manual_id, manual_name, deadline)
            st.success(f"Added test market: {manual_name}")
            st.rerun()
        else:
            st.warning("Please provide both an ID and a name.")

manual_test_markets = load_manual_bodega_markets()
if manual_test_markets:
    st.write("Active Test Markets:")
    for m in manual_test_markets:
        c1, c2 = st.columns([4, 1])
        c1.write(f"`{m['id']}`: {m['name']}")
        if c2.button("Delete", key=f"del_manual_{m['id']}"):
            delete_manual_bodega_market(m['id'])
            st.rerun()