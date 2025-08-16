import sys, pathlib, time
# Ensure the project root is on Python’s import path
ROOT = pathlib.Path(__file__).parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st
import logging
from datetime import datetime, timezone

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
    set_config_value,
    get_config_value,
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
            # Add with default is_flipped=0 and profit_threshold=25.0
            save_manual_pair(bid, pid, is_flipped=0, profit_threshold_usd=25.0)
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
    for b_id, p_id, is_flipped, profit_threshold in manual_pairs:
        # Display link and delete button
        c1_disp, c2_disp = st.columns([12, 1])
        b_url = f"{BODEGA_API.replace('/api', '')}/marketDetails?id={b_id}"
        p_url = f"https://polymarket.com/event/{p_id}"
        c1_disp.markdown(f"• [Bodega]({b_url}) `({b_id})` ↔ [Polymarket]({p_url}) `({p_id})`")
        if c2_disp.button("❌", key=f"del_pair_{b_id}_{p_id}", help="Delete this pair"):
            delete_manual_pair(b_id, p_id)
            st.rerun()

        # Form for updating threshold and flip status
        with st.form(key=f"form_pair_{b_id}_{p_id}"):
            c1_form, c2_form, c3_form = st.columns([3, 2, 2])
            with c1_form:
                new_threshold = st.number_input(
                    "Profit Alert ($)",
                    value=float(profit_threshold),
                    min_value=0.0,
                    step=5.0,
                    help="Set the minimum USD profit to trigger an alert for this pair."
                )
            with c2_form:
                st.write("") # Spacer
                is_flipped_new = st.checkbox("Flipped", value=bool(is_flipped), help="Check this if 'Yes' on Bodega corresponds to the second outcome (usually 'No') on Polymarket.")
            with c3_form:
                st.write("") # Spacer
                if st.form_submit_button("Update Pair"):
                    save_manual_pair(b_id, p_id, int(is_flipped_new), float(new_threshold))
                    st.success(f"Pair {b_id}/{p_id} updated.")
                    time.sleep(1)
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

# —–– Event Calendars —–––––––––––––––––––––––––––––––––––––––––
st.subheader("🗓️ Event End Date Calendars")

def format_deadline(ms_timestamp):
    """Formats a millisecond UTC timestamp into a readable date and time remaining."""
    if not ms_timestamp or not isinstance(ms_timestamp, (int, float)):
        return "N/A", "N/A", 0
    
    try:
        dt_object = datetime.fromtimestamp(ms_timestamp / 1000, tz=timezone.utc)
        now = datetime.now(timezone.utc)
    except (ValueError, TypeError):
        return "Invalid Date", "N/A", 0

    # Format for display
    date_str = dt_object.strftime("%Y-%m-%d %H:%M UTC")
    
    # Calculate time remaining
    time_diff = dt_object - now
    if time_diff.total_seconds() < 0:
        remaining_str = "Ended"
    else:
        days = time_diff.days
        hours, remainder = divmod(time_diff.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        if days > 0:
            remaining_str = f"{days}d {hours}h left"
        elif hours > 0:
            remaining_str = f"{hours}h {minutes}m left"
        else:
            remaining_str = f"{minutes}m left"
            
    return date_str, remaining_str, ms_timestamp

# Get all bodega markets and create a lookup map
all_bodegas_for_calendar = get_all_bodegas()
bodega_map = {m['id']: {'name': m['name'], 'deadline': m['deadline']} for m in all_bodegas_for_calendar}

# Calendar 1: Matched Markets by End Date (Sorted by soonest to end)
with st.expander("Matched Markets by End Date", expanded=True):
    manual_pairs_for_calendar = load_manual_pairs()
    if not manual_pairs_for_calendar:
        st.info("No manually matched pairs found.")
    else:
        matched_markets = []
        for b_id, p_id, _, _ in manual_pairs_for_calendar:
            if b_id in bodega_map:
                market_info = bodega_map[b_id]
                deadline_str, remaining_str, deadline_ts = format_deadline(market_info.get('deadline'))
                matched_markets.append({
                    "deadline_ts": deadline_ts,
                    "Market Name": market_info.get('name', 'N/A'),
                    "End Date": deadline_str,
                    "Time Remaining": remaining_str,
                    "Bodega ID": b_id,
                    "Polymarket ID": p_id
                })
        
        if not matched_markets:
            st.info("Could not find deadline info for any matched pairs (they may be inactive).")
        else:
            # Sort by the actual timestamp, soonest first
            sorted_matched = sorted(matched_markets, key=lambda x: x['deadline_ts'])
            # Remove the timestamp column before displaying
            for m in sorted_matched:
                del m['deadline_ts']

            df_matched = pd.DataFrame(sorted_matched)
            st.dataframe(df_matched, use_container_width=True, hide_index=True)

# Calendar 2: All Active Bodega Markets by End Date
with st.expander("All Active Bodega Markets by End Date"):
    if not all_bodegas_for_calendar:
        st.info("No active Bodega markets found.")
    else:
        calendar_data = []
        for market in all_bodegas_for_calendar:
            deadline_str, remaining_str, deadline_ts = format_deadline(market.get('deadline'))
            calendar_data.append({
                "deadline_ts": deadline_ts,
                "Market Name": market.get('name', 'N/A'),
                "End Date": deadline_str,
                "Time Remaining": remaining_str,
                "ID": market.get('id', 'N/A')
            })
        
        # Sort markets by deadline timestamp
        sorted_bodegas = sorted(calendar_data, key=lambda x: x['deadline_ts'])
        # Remove the timestamp column before displaying
        for m in sorted_bodegas:
            del m['deadline_ts']
        
        df_all = pd.DataFrame(sorted_bodegas)
        st.dataframe(df_all, use_container_width=True, hide_index=True)


st.markdown("---")

# —–– Pending New Bodega Markets —–––––––––––––––––––––
st.subheader("🆕 Pending New Bodega Markets")
pending = load_new_bodega_markets()
if not pending:
    st.info("No new Bodega markets awaiting processing.")
else:
    for m in pending:
        st.markdown(f"**{m['market_name']}**  (ID: `{m['market_id']}`)", unsafe_allow_html=True)
        cols = st.columns([3, 1, 1])
        
        with cols[0]:
            search_query = st.text_input("Search Polymarket", key=f"poly_search_{m['market_id']}")
            pm_results = p_client.search_markets(search_query) if search_query else []
            options = {f'{res["question"]} ({res["condition_id"]})': res["condition_id"] for res in pm_results}
            selected_label = st.selectbox(
                "Pick Polymarket market",
                [""] + list(options.keys()),
                key=f"poly_select_{m['market_id']}"
            )
            poly_condition_id = options.get(selected_label, "")

        with cols[1]:
            st.write("")  # spacer
            st.write("")  # spacer
            if st.button("Match", key=f"match_{m['market_id']}"):
                if poly_condition_id:
                    save_manual_pair(m["market_id"], poly_condition_id, is_flipped=0, profit_threshold_usd=25.0)
                    remove_new_bodega_market(m["market_id"])
                    if notifier:
                        notifier.notify_manual_pair(m['market_id'], poly_condition_id)
                    st.success(f"Matched Bodega {m['market_name']} ↔ {poly_condition_id}")
                    st.rerun()
                else:
                    st.error("Please search for and select a Polymarket market before matching.")
        
        with cols[2]:
            st.write("")  # spacer
            st.write("")  # spacer
            if st.button("Ignore", key=f"ignore_{m['market_id']}"):
                ignore_bodega_market(m["market_id"])
                st.warning(f"Ignored Bodega {m['market_name']}")
                st.rerun()

st.markdown("---")

# Auto-match section
st.subheader("🔄 Refresh Markets")
if st.button("Run Refresh"):
    with st.spinner("Fetching markets and running fuzzy matching..."):
        bodes = get_all_bodegas()
        polys = get_all_polymarkets()
        """
        matches, ignored_count = fuzzy_match_markets(bodes, polys)
        st.success(f"Auto-match done: {len(matches)} matches found, {ignored_count} pairs ignored.")
        if notifier:
            notifier.notify_auto_match(len(matches), ignored_count)
        for b, p, score in matches:
            add_suggested_match(b["id"], p["condition_id"], score)
        """
        st.rerun()

# 🚀 Check Arbitrage
st.subheader("🚀 Check Arbitrage")

# --- New Section for Auto-check frequency ---
st.markdown("##### Auto-Check Frequency Control")
st.caption("Control how often the background service checks for arbitrage opportunities.")

# Map display names to seconds
frequency_options = {
    "⚡ High (30 seconds)": 30,
    "👍 Normal (3 minutes)": 180,
    "🐌 Low (10 minutes)": 600,
    "⏸️ Paused (1 hour)": 3600,
}
# Map seconds back to display names for setting the current selection
seconds_to_name = {v: k for k, v in frequency_options.items()}

# Get current setting from DB, default to 180 seconds (3 minutes)
current_interval_seconds = int(get_config_value('arb_check_interval_seconds', '180'))
current_selection_name = seconds_to_name.get(current_interval_seconds)

# Find the index of the current selection for the radio button. Fallback to index 1 (Normal).
option_names = list(frequency_options.keys())
try:
    current_index = option_names.index(current_selection_name) if current_selection_name else 1
except ValueError:
    current_index = 1 # Default to Normal if saved value is somehow not in our options

# Display radio buttons
selected_frequency_name = st.radio(
    "Set arbitrage check interval:",
    option_names,
    index=current_index,
    key="arb_frequency_radio",
    horizontal=True,
    label_visibility="collapsed"
)

# If the selection changed, update the DB
selected_seconds = frequency_options[selected_frequency_name]
if selected_seconds != current_interval_seconds:
    set_config_value('arb_check_interval_seconds', str(selected_seconds))
    st.success(f"Arbitrage check frequency set to: **{selected_frequency_name}**. The background service will update within 15 seconds.")
    time.sleep(1) # a small delay for user experience
    st.rerun()

if st.button("Check All Manual Pairs for Arbitrage"):
    with st.spinner("Checking for arbitrage opportunities..."):
        ada_usd = fx_client.get_ada_usd()
        manual_pairs = load_manual_pairs()
        all_opportunities = []

        if not manual_pairs:
            st.warning("No manual pairs to check. Please add some.")
        else:
            prog = st.progress(0)
            for i, (b_id, p_id, is_flipped, profit_threshold) in enumerate(manual_pairs, start=1):
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
                        for opportunity in pair_opportunities:
                            desc = f"{pool['name']} ↔ {pdata['question']}"
                            
                            logical_poly_side = opportunity['polymarket_side']
                            if logical_poly_side == 'YES':
                                opportunity['polymarket_side'] = poly_outcome_name_yes
                            else: # 'NO'
                                opportunity['polymarket_side'] = poly_outcome_name_no
                            
                            all_opportunities.append({
                                "description": desc,
                                "summary": opportunity, # 'summary' is now the full opportunity dict
                                "b_id": b_id,
                                "p_id": p_id,
                                "profit_threshold": profit_threshold
                            })

                except Exception as e:
                    log.exception("Error checking pair %s / %s", b_id, p_id)
                    st.error(f"Error checking pair ({b_id}, {p_id}): {e}")

                prog.progress(i / len(manual_pairs))
            prog.empty()

            if all_opportunities:
                all_opportunities.sort(key=lambda o: o["summary"].get("roi", -float('inf')), reverse=True)
                
                st.subheader("✅ Arbitrage Opportunities Found")

                for opp in all_opportunities:
                    summary = opp['summary']
                    b_id = opp['b_id']
                    p_id = opp['p_id']
                    profit_threshold = opp['profit_threshold']

                    # --- Notification Logic ---
                    if summary.get("profit_usd", 0) > profit_threshold and summary.get("roi", 0) > 0.015:
                        if notifier:
                            notifier.notify_arb_opportunity(opp['description'], summary, b_id, p_id, BODEGA_API)
                    
                    # --- Display Logic ---
                    st.markdown(f"**Pair:** {opp['description']}")
                    
                    main_cols = st.columns(3)
                    main_cols[0].metric("Total Profit (USD)", f"${summary.get('profit_usd', 0):.2f}")
                    main_cols[1].metric("Return on Investment (ROI)", f"{summary.get('roi', 0)*100:.2f}%")
                    main_cols[2].metric("Inferred B", f"{summary.get('inferred_B', 0):.2f}")

                    trade_cols = st.columns(2)
                    with trade_cols[0]:
                        st.markdown("##### 1. Bodega Trade")
                        st.markdown(
                            f"""
                            - **Action:** Buy `{summary['bodega_shares']}` **{summary['bodega_side']}** shares
                            - **Cost:** `₳{summary['cost_bod_ada']:.2f}` (+ `₳{summary['fee_bod_ada']:.2f}` fee)
                            - **Start Price:** `{summary['p_start']:.4f}` → **End Price:** `{summary['p_end']:.4f}`
                            """
                        )
                    with trade_cols[1]:
                        st.markdown("##### 2. Polymarket Hedge")
                        st.markdown(
                            f"""
                            - **Action:** Buy `{summary['polymarket_shares']}` **{summary['polymarket_side']}** shares
                            - **Cost:** `${summary['cost_poly_usd']:.2f}`
                            - **Avg. Price:** `{summary.get('avg_poly_price', 0):.4f}`
                            - **Hedge Complete:** {'✅' if summary['fill'] else '❌'}
                            """
                        )

                    with st.expander("Show Detailed Price Adjustment Analysis"):
                        analysis_data = summary.get('analysis_details', [])
                        if analysis_data:
                            df_analysis = pd.DataFrame(analysis_data)
                            df_display = df_analysis[['adjustment', 'p_end', 'bodega_shares', 'profit_usd', 'roi']].copy()
                            df_display.rename(columns={
                                'adjustment': 'Adj', 'p_end': 'Target Price',
                                'bodega_shares': 'Shares', 'profit_usd': 'Profit ($)', 'roi': 'ROI (%)'
                            }, inplace=True)
                            df_display['ROI (%)'] = df_display['ROI (%)'] * 100
                            
                            st.dataframe(
                                df_display, use_container_width=True, hide_index=True,
                                column_config={
                                    "Adj": st.column_config.NumberColumn(format="%.2f"),
                                    "Target Price": st.column_config.NumberColumn(format="%.4f"),
                                    "Shares": st.column_config.NumberColumn(format="%d"),
                                    "Profit ($)": st.column_config.NumberColumn(format="$%.2f"),
                                    "ROI (%)": st.column_config.NumberColumn(format="%.2f%%")
                                }
                            )
                        else:
                            st.info("No analysis data available.")
                    
                    st.markdown("---")

            else:
                st.info("No arbitrage opportunities found.")