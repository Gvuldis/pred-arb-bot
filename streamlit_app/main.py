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
    save_probability_watch,
    load_probability_watches,
    delete_probability_watch,
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
    if markets:
        save_bodega_markets(markets)
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
    for b_id, p_id, is_flipped in manual_pairs:
        c1, c2, c3 = st.columns([8, 1, 1])
        b_url = f"{BODEGA_API.replace('/api', '')}/marketDetails?id={b_id}"
        p_url = f"https://polymarket.com/event/{p_id}"
        
        flip_status_str = " <span style='color: orange; font-weight: bold;'>(Flipped)</span>" if is_flipped else ""

        c1.markdown(f"• [Bodega]({b_url}) `({b_id})` ↔ [Polymarket]({p_url}) `({p_id})`{flip_status_str}", unsafe_allow_html=True)
        
        with c2:
            if st.button("Flip 🔃", key=f"flip_pair_{b_id}_{p_id}", help="Toggle Polymarket outcome order. Use this if 'Yes' on Bodega corresponds to the second outcome on Polymarket."):
                new_flip_state = 1 - is_flipped # toggle 0 to 1 and 1 to 0
                save_manual_pair(b_id, p_id, is_flipped=new_flip_state)
                st.success(f"Pair flipped! Reloading...")
                time.sleep(1)
                st.rerun()
        
        with c3:
            if st.button("❌", key=f"del_pair_{b_id}_{p_id}", help="Delete this pair"):
                delete_manual_pair(b_id, p_id)
                st.rerun()

st.markdown("---")

# --- Probability Watch ---
st.subheader("📈 Probability Watches")
st.markdown("Monitor a Bodega market against an external probability (e.g., from a bookmaker).")

with st.expander("Add New Probability Watch"):
    col_watch_1, col_watch_2 = st.columns(2)
    with col_watch_1:
        watch_bodega_id = st.text_input("Bodega Market ID", key="watch_bodega_id")
        watch_desc = st.text_input("Description (optional, for your reference)", key="watch_desc", placeholder="e.g., 'Man City to win PL (Betfair)'")
    with col_watch_2:
        watch_prob = st.number_input("Expected 'YES' Probability", min_value=0.0, max_value=1.0, value=0.5, step=0.01, key="watch_prob", format="%.3f")
        watch_thresh = st.number_input("Alert Deviation Threshold", min_value=0.01, max_value=1.0, value=0.1, step=0.01, key="watch_thresh", format="%.3f", help="Alert if live probability differs by this amount or more (e.g., 0.1 for 10%)")

    if st.button("Add Watch", key="add_watch_btn"):
        if watch_bodega_id and watch_prob is not None and watch_thresh is not None:
            save_probability_watch(watch_bodega_id, watch_desc, watch_prob, watch_thresh)
            st.success(f"Added watch for Bodega market `{watch_bodega_id}`.")
            st.rerun()
        else:
            st.warning("Please provide a Bodega Market ID, probability, and threshold.")

# Display existing watches
prob_watches = load_probability_watches()
if prob_watches:
    st.markdown("**Active Probability Watches:**")
    for watch in prob_watches:
        b_id = watch['bodega_id']
        try:
            # Fetch market name for better display
            market_info = b_client.fetch_market_config(b_id)
            market_name = market_info.get('name', 'N/A')
        except ValueError:
            # Market might be inactive
            market_name = f"<Inactive Market: {b_id}>"
        except Exception as e:
            market_name = f"<Error fetching name: {e}>"

        b_url = f"{BODEGA_API.replace('/api', '')}/marketDetails?id={b_id}"
        
        c1, c2 = st.columns([8, 1])
        
        desc_text = f"– *{watch['description']}*" if watch['description'] else ""
        c1.markdown(f"""
        - **[{market_name}]({b_url})** {desc_text}
          - **Expected Prob:** `{watch['expected_probability']:.3f}`
          - **Alert Threshold:** `{watch['deviation_threshold']:.3f}`
        """, unsafe_allow_html=True)
        
        if c2.button("🗑️", key=f"del_watch_{b_id}", help="Delete this watch"):
            delete_probability_watch(b_id)
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
            remove_suggested_match(s["bodega_id"], s["poly_id"]) # Prune suggestion if market is dead
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
        all_opportunities = []

        if not manual_pairs:
            st.warning("No manual pairs to check. Please add some.")
        else:
            prog = st.progress(0)
            for i, (b_id, p_id, is_flipped) in enumerate(manual_pairs, start=1):
                try:
                    pool = b_client.fetch_market_config(b_id)
                    pdata = p_client.fetch_market(p_id)

                    if not pdata.get('active') or pdata.get('closed'):
                        st.warning(f"Skipping pair ({b_id}, {p_id}) because Polymarket market is inactive.")
                        continue

                    bodega_prediction_info = b_client.fetch_prices(b_id)
                    
                    order_book_yes = pdata.get("order_book_yes")
                    order_book_no = pdata.get("order_book_no")
                    poly_outcome_name_yes = pdata.get('outcome_yes', 'YES')
                    poly_outcome_name_no = pdata.get('outcome_no', 'NO')

                    if is_flipped:
                        order_book_yes, order_book_no = order_book_no, order_book_yes
                        poly_outcome_name_yes, poly_outcome_name_no = poly_outcome_name_no, poly_outcome_name_yes

                    if not all([pool, pdata, bodega_prediction_info]):
                        st.warning(f"Could not fetch complete data for pair ({b_id}, {p_id}). Skipping.")
                        continue

                    Q_YES = bodega_prediction_info.get("yesVolume_ada", 0)
                    Q_NO = bodega_prediction_info.get("noVolume_ada", 0)
                    p_bod_yes = bodega_prediction_info.get("yesPrice_ada")

                    if not p_bod_yes:
                        st.warning(f"Skipping pair ({b_id}, {p_id}): Could not fetch live Bodega YES price.")
                        continue

                    try:
                        inferred_B = infer_b(Q_YES, Q_NO, p_bod_yes)
                    except ValueError as e:
                        st.warning(f"Skipping pair ({b_id}, {p_id}): Could not infer B parameter. Reason: {e}")
                        continue
                    
                    pair_opportunities = build_arbitrage_table(
                        Q_YES=Q_YES, Q_NO=Q_NO,
                        ORDER_BOOK_YES=order_book_yes, ORDER_BOOK_NO=order_book_no,
                        ADA_TO_USD=ada_usd, FEE_RATE=FEE_RATE, B=inferred_B
                    )

                    if pair_opportunities:
                        pair_opportunities.sort(key=lambda o: o.get("roi", -float('inf')), reverse=True)
                        best_opportunity = pair_opportunities[0]
                        
                        desc = f"{pool['name']} ↔ {pdata['question']}"
                        
                        logical_poly_side = best_opportunity['polymarket_side']
                        if logical_poly_side == 'YES':
                            best_opportunity['polymarket_side'] = poly_outcome_name_yes
                        else: # 'NO'
                            best_opportunity['polymarket_side'] = poly_outcome_name_no
                        
                        all_opportunities.append({
                            "description": desc,
                            "summary": best_opportunity,
                            "b_id": b_id,
                            "p_id": p_id
                        })

                except Exception as e:
                    log.exception("Error checking pair %s / %s", b_id, p_id)
                    st.error(f"Error checking pair ({b_id}, {p_id}): {e}")

                prog.progress(i / len(manual_pairs))
            prog.empty()

            if all_opportunities:
                all_opportunities.sort(key=lambda o: o["summary"].get("roi", -float('inf')), reverse=True)

                table_rows = []
                for opp in all_opportunities:
                    summary = opp['summary']
                    b_id = opp['b_id']
                    p_id = opp['p_id']

                    if summary.get("profit_usd", 0) > 20 and summary.get("roi", 0) > 0.015:
                        if notifier:
                            notifier.notify_arb_opportunity(opp['description'], summary, b_id, p_id, BODEGA_API)
                    
                    row_bodega = {
                        "Pair": opp['description'][:80] + '...' if len(opp['description']) > 80 else opp['description'],
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
                        "Pair": opp['description'][:80] + '...' if len(opp['description']) > 80 else opp['description'],
                        "Market": "Polymarket", "Side": summary['polymarket_side'], "Shares": f"{summary['polymarket_shares']}",
                        "Cost ADA": f"{summary['cost_poly_ada']:.2f}", "AvgPoly": f"{summary['avg_poly_price']:.4f}",
                        "Fill": f"{summary['fill']}",
                    }
                    table_rows.append(row_bodega)
                    table_rows.append(row_poly)

                if table_rows:
                    columns = ["Pair", "Market", "Side", "Profit USD", "Margin", "StartP", "EndP", "Shares", "Cost ADA", "Fee ADA", "AvgPoly",
                               "Comb ADA", "Comb USD", "Profit ADA", "Fill",
                               "Inferred B", "ADA/USD Rate"]
                    df = pd.DataFrame(table_rows, columns=columns).fillna('')

                    # --- ENHANCED STYLING LOGIC ---
                    def get_row_style(row):
                        style = ''
                        # Apply style to the first row of each pair (Bodega row)
                        if row.name % 2 == 0:
                            # A thick top border and extra padding to create space
                            style += 'border-top: 3px solid #555; padding-top: 1em;'
                        # Apply style to the second row of each pair (Polymarket row)
                        else:
                            # Extra padding at the bottom to complete the block
                            style += 'padding-bottom: 1em;'
                        
                        return [style for _ in row]

                    styler = df.style.apply(get_row_style, axis=1)
                    styler.hide(axis="index") # Hide the default dataframe index
                    
                    st.dataframe(styler, use_container_width=True)

            else:
                st.info("No arbitrage opportunities found.")