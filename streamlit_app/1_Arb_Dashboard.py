# streamlit_app/1_Arb_Dashboard.py
import sys, pathlib, time
# Ensure the project root is on Python’s import path
ROOT = pathlib.Path(__file__).parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st
import logging
from datetime import datetime, timezone, date, time as dt_time

from config import b_client, m_client, p_client, fx_client, notifier, BODEGA_API, FEE_RATE_BODEGA, FEE_RATE_MYRIAD_BUY, log
from services.polymarket.model import build_arbitrage_table as build_bodega_arb_table, infer_b
from services.myriad.model import build_arbitrage_table_myriad
from track_record import portfolio_summary

# Database helpers
from streamlit_app.db import (
    init_db, save_bodega_markets, save_polymarkets, save_manual_pair,
    load_manual_pairs, delete_manual_pair, load_new_bodega_markets,
    remove_new_bodega_market, ignore_bodega_market, save_probability_watch,
    load_probability_watches, delete_probability_watch, set_config_value, get_config_value,
    save_myriad_markets, load_myriad_markets, load_new_myriad_markets,
    add_new_myriad_market, ignore_myriad_market, remove_new_myriad_market,
    save_manual_pair_myriad, load_manual_pairs_myriad, delete_manual_pair_myriad,
    clear_arb_opportunities
)
# Matching logic
from matching.fuzzy import fetch_all_polymarket_clob_markets, fetch_bodega_v3_active_markets

# Initialize database
init_db()

st.set_page_config(layout="wide")
st.title("🌉 Arb-Bot Dashboard")

# --- Function to save cash values to DB ---
def save_cash_values():
    set_config_value('poly_cash_usd', st.session_state.poly_cash)
    set_config_value('bodega_cash_ada', st.session_state.bodega_cash)
    log.info(f"Saved cash values to DB: Poly={st.session_state.poly_cash}, Bodega={st.session_state.bodega_cash}")

# --- Portfolio Summary Expander ---
with st.expander("📊 Portfolio Summary", expanded=True):
    # Load values from DB or use defaults, store in session state
    poly_cash_from_db = float(get_config_value('poly_cash_usd', '19.0'))
    bodega_cash_from_db = float(get_config_value('bodega_cash_ada', '603.0'))

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        st.number_input(
            "Current Polymarket Cash (USD)", 
            key="poly_cash", 
            value=poly_cash_from_db,
            on_change=save_cash_values
        )
    with col2:
        st.number_input(
            "Current Bodega Cash (ADA)", 
            key="bodega_cash",
            value=bodega_cash_from_db,
            on_change=save_cash_values
        )
    with col3:
        st.write("") # Spacer
        if st.button("Refresh Live Data", key="refresh_summary"):
            st.cache_data.clear()
            st.rerun()

    summary = portfolio_summary.get_portfolio_summary(st.session_state.poly_cash, st.session_state.bodega_cash)

    scol1, scol2, scol3, scol4 = st.columns(4)
    scol1.metric("Live ADA Price", f"${summary.get('live_ada_price', 0):,.2f}")
    scol2.metric("Total Cash Value (USD)", f"${summary.get('total_cash_usd', 0):,.2f}")
    scol3.metric("Open Position Value (Worst Case)", f"${summary.get('position_value_usd', 0):,.2f}")
    scol4.metric("Total Portfolio Value (USD)", f"${summary.get('total_portfolio_value_usd', 0):,.2f}")

# --- Helper functions for calendars ---
@st.cache_data(ttl=300)
def get_all_bodegas():
    return fetch_bodega_v3_active_markets(BODEGA_API)

@st.cache_data(ttl=300)
def get_all_myriads():
    return m_client.fetch_markets()

def format_deadline_ms(ms_timestamp):
    if not ms_timestamp or not isinstance(ms_timestamp, (int, float)): return "N/A", "N/A", 0
    try:
        dt_object = datetime.fromtimestamp(ms_timestamp / 1000, tz=timezone.utc)
        now = datetime.now(timezone.utc)
    except (ValueError, TypeError): return "Invalid Date", "N/A", 0
    date_str = dt_object.strftime("%Y-%m-%d %H:%M UTC")
    time_diff = dt_object - now
    if time_diff.total_seconds() < 0: remaining_str = "Ended"
    else:
        days = time_diff.days
        hours, remainder = divmod(time_diff.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        if days > 0: remaining_str = f"{days}d {hours}h left"
        elif hours > 0: remaining_str = f"{hours}h {minutes}m left"
        else: remaining_str = f"{minutes}m left"
    return date_str, remaining_str, ms_timestamp

def format_deadline_iso(iso_str):
    if not iso_str: return "N/A", "N/A", 0
    try:
        dt_object = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        ts_ms = int(dt_object.timestamp() * 1000)
        return format_deadline_ms(ts_ms)
    except (ValueError, TypeError):
        return "Invalid Date", "N/A", 0

# —–– Event Calendars —–––––––––––––––––––––––––––––––––––––––––
st.subheader("🗓 Event End Date Calendars")
all_bodegas_for_calendar = get_all_bodegas()
bodega_map = {m['id']: {'name': m['name'], 'deadline': m['deadline']} for m in all_bodegas_for_calendar}
all_myriads_for_calendar = get_all_myriads()
myriad_map = {m['slug']: {'name': m['title'], 'expires_at': m['expires_at']} for m in all_myriads_for_calendar}

cal_bodega, cal_myriad = st.tabs(["Bodega Calendar", "Myriad Calendar"])

with cal_bodega:
    with st.expander("Matched Bodega Markets by End Date", expanded=True):
        manual_pairs_for_calendar = load_manual_pairs()
        if not manual_pairs_for_calendar:
            st.info("No manually matched Bodega pairs found.")
        else:
            matched_markets = []
            for b_id, p_id, _, _, _ in manual_pairs_for_calendar:
                if b_id in bodega_map:
                    market_info = bodega_map[b_id]
                    deadline_str, remaining_str, deadline_ts = format_deadline_ms(market_info.get('deadline'))
                    matched_markets.append({ "deadline_ts": deadline_ts, "Market Name": market_info.get('name', 'N/A'), "End Date": deadline_str, "Time Remaining": remaining_str, "Bodega ID": b_id, "Polymarket ID": p_id })
            if not matched_markets:
                st.info("Could not find deadline info for any matched pairs (they may be inactive).")
            else:
                sorted_matched = sorted(matched_markets, key=lambda x: x['deadline_ts'])
                for m in sorted_matched: del m['deadline_ts']
                df_matched = pd.DataFrame(sorted_matched)
                st.dataframe(df_matched, use_container_width=True, hide_index=True)
    with st.expander("All Active Bodega Markets by End Date"):
        if not all_bodegas_for_calendar: st.info("No active Bodega markets found.")
        else:
            calendar_data = []
            for market in all_bodegas_for_calendar:
                deadline_str, remaining_str, deadline_ts = format_deadline_ms(market.get('deadline'))
                calendar_data.append({ "deadline_ts": deadline_ts, "Market Name": market.get('name', 'N/A'), "End Date": deadline_str, "Time Remaining": remaining_str, "ID": market.get('id', 'N/A') })
            sorted_bodegas = sorted(calendar_data, key=lambda x: x['deadline_ts'])
            for m in sorted_bodegas: del m['deadline_ts']
            df_all = pd.DataFrame(sorted_bodegas)
            st.dataframe(df_all, use_container_width=True, hide_index=True)

with cal_myriad:
    with st.expander("Matched Myriad Markets by End Date", expanded=True):
        manual_pairs_myriad_cal = load_manual_pairs_myriad()
        if not manual_pairs_myriad_cal:
            st.info("No manually matched Myriad pairs found.")
        else:
            matched_markets = []
            for m_slug, p_id, _, _, _, _ in manual_pairs_myriad_cal:
                if m_slug in myriad_map:
                    market_info = myriad_map[m_slug]
                    deadline_str, remaining_str, deadline_ts = format_deadline_iso(market_info.get('expires_at'))
                    matched_markets.append({ "deadline_ts": deadline_ts, "Market Name": market_info.get('name', 'N/A'), "End Date": deadline_str, "Time Remaining": remaining_str, "Myriad Slug": m_slug, "Polymarket ID": p_id })
            if not matched_markets:
                st.info("Could not find deadline info for any matched pairs (they may be inactive).")
            else:
                sorted_matched = sorted(matched_markets, key=lambda x: x['deadline_ts'])
                for m in sorted_matched: del m['deadline_ts']
                df_matched = pd.DataFrame(sorted_matched)
                st.dataframe(df_matched, use_container_width=True, hide_index=True)

    with st.expander("All Active Myriad Markets by End Date"):
        if not all_myriads_for_calendar: st.info("No active Myriad markets found.")
        else:
            calendar_data = []
            for market in all_myriads_for_calendar:
                deadline_str, remaining_str, deadline_ts = format_deadline_iso(market.get('expires_at'))
                calendar_data.append({ "deadline_ts": deadline_ts, "Market Name": market.get('title', 'N/A'), "End Date": deadline_str, "Time Remaining": remaining_str, "Slug": market.get('slug', 'N/A') })
            sorted_myriads = sorted(calendar_data, key=lambda x: x['deadline_ts'])
            for m in sorted_myriads: del m['deadline_ts']
            df_all = pd.DataFrame(sorted_myriads)
            st.dataframe(df_all, use_container_width=True, hide_index=True)
st.markdown("---")

# --- TABS FOR BODEGA AND MYRIAD ---
tab_bodega, tab_myriad, tab_other = st.tabs(["Bodega ↔ Polymarket", "Myriad ↔ Polymarket", "Other Tools"])

with tab_bodega:
    st.header("Bodega ↔ Polymarket Pair Management")
    
    with st.expander("➕ Add New Manual Bodega Pair"):
        col1, col2, col3 = st.columns([3,3,1])
        with col1:
            bid = st.text_input("Bodega ID", key="manual_pair_bodega_id")
        with col2:
            search = st.text_input("Search Polymarket", key="manual_pair_poly_search_bodega")
            pm_results = p_client.search_markets(search) if search else []
            options = {f'{m["question"]} ({m["condition_id"]})': m["condition_id"] for m in pm_results}
            pid_label = st.selectbox("Pick Polymarket market", [""] + list(options.keys()), key="bodega_poly_select", index=0)
            pid = options.get(pid_label, "")
        with col3:
            st.write("") # Spacer
            st.write("") # Spacer
            if st.button("Add Bodega Pair"):
                if bid and pid:
                    save_manual_pair(bid, pid, is_flipped=0, profit_threshold_usd=25.0, end_date_override=None)
                    if notifier:
                        notifier.notify_manual_pair("Bodega", bid, pid)
                    st.success("Bodega pair added!")
                    st.rerun()
                else:
                    st.warning("Please provide both Bodega ID and select a Polymarket market.")
    
    manual_pairs_bodega = load_manual_pairs()
    if manual_pairs_bodega:
        with st.expander("📝 Edit Saved Bodega Pairs"):
            sorted_pairs_bodega = sorted(
                [(f"{bodega_map.get(b_id, {'name': 'Unknown'})['name']} ({b_id})", b_id, p_id, is_flipped, profit_threshold, end_date_override)
                 for b_id, p_id, is_flipped, profit_threshold, end_date_override in manual_pairs_bodega],
                key=lambda x: x[0]
            )

            for display_name, b_id, p_id, is_flipped, profit_threshold, end_date_override in sorted_pairs_bodega:
                st.markdown(f"**{display_name}**")
                
                b_url = f"{BODEGA_API.replace('/api', '')}/marketDetails?id={b_id}"
                p_url = f"https://polymarket.com/event/{p_id}"
                
                c1_disp, c2_disp = st.columns([12, 1])
                with c1_disp:
                    st.markdown(f"• [Bodega Link]({b_url}) ↔ [Polymarket Link]({p_url})")
                with c2_disp:
                    if st.button("❌", key=f"del_pair_bodega_{b_id}_{p_id}", help="Delete this pair"):
                        delete_manual_pair(b_id, p_id)
                        st.rerun()

                with st.form(key=f"form_pair_bodega_{b_id}_{p_id}"):
                    default_date, default_time = None, None
                    api_date_ms = bodega_map.get(b_id, {}).get('deadline')
                    display_date_ts = end_date_override if end_date_override else api_date_ms
                    if display_date_ts:
                        dt_obj = datetime.fromtimestamp(display_date_ts / 1000, tz=timezone.utc)
                        default_date = dt_obj.date()
                        default_time = dt_obj.time()

                    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 1, 2])
                    new_threshold = c1.number_input("Profit Alert ($)", value=float(profit_threshold), min_value=0.0, step=5.0, help="Min USD profit for an alert.", key=f"threshold_bodega_{b_id}_{p_id}")
                    end_date_input = c2.date_input("End Date (UTC)", value=default_date, help="Override end date for APY. Clear to use API default.", key=f"date_bodega_{b_id}_{p_id}")
                    end_time_input = c3.time_input("End Time (UTC)", value=default_time, help="Override end time for APY.", key=f"time_bodega_{b_id}_{p_id}")
                    is_flipped_new = c4.checkbox("Flipped", value=bool(is_flipped), help="'Yes' on Bodega maps to 'No' on Polymarket.", key=f"flipped_bodega_{b_id}_{p_id}")
                    
                    if c5.form_submit_button("Update Pair"):
                        new_override_ts = None
                        if end_date_input and end_time_input:
                            combined_dt = datetime.combine(end_date_input, end_time_input, tzinfo=timezone.utc)
                            new_override_ts = int(combined_dt.timestamp() * 1000)
                        
                        save_manual_pair(b_id, p_id, int(is_flipped_new), float(new_threshold), new_override_ts)
                        st.success(f"Pair {b_id}/{p_id} updated.")
                        time.sleep(1)
                        st.rerun()
                st.markdown("---")

    st.subheader("🆕 Pending New Bodega Markets")
    pending_bodega = load_new_bodega_markets()
    if not pending_bodega:
        st.info("No new Bodega markets awaiting processing.")
    else:
        for m in pending_bodega:
            st.markdown(f"**{m['market_name']}**  (ID: `{m['market_id']}`)")
            cols = st.columns([3, 1, 1])
            with cols[0]:
                search_query = st.text_input("Search Polymarket", key=f"poly_search_{m['market_id']}")
                pm_results_bodega = p_client.search_markets(search_query) if search_query else []
                options_bodega = {f'{res["question"]} ({res["condition_id"]})': res["condition_id"] for res in pm_results_bodega}
                selected_label_bodega = st.selectbox("Pick Polymarket market", [""] + list(options_bodega.keys()), key=f"poly_select_{m['market_id']}", index=0)
                poly_condition_id = options_bodega.get(selected_label_bodega, "")
            with cols[1]:
                st.write(""); st.write("")
                if st.button("Match", key=f"match_bodega_{m['market_id']}"):
                    if poly_condition_id:
                        save_manual_pair(m["market_id"], poly_condition_id, 0, 25.0, None)
                        remove_new_bodega_market(m["market_id"])
                        if notifier: notifier.notify_manual_pair("Bodega", m['market_id'], poly_condition_id)
                        st.success(f"Matched!"); st.rerun()
                    else: st.error("Please select a Polymarket market.")
            with cols[2]:
                st.write(""); st.write("")
                if st.button("Ignore", key=f"ignore_bodega_{m['market_id']}"):
                    ignore_bodega_market(m["market_id"]); st.warning(f"Ignored."); st.rerun()
            st.markdown("---")

with tab_myriad:
    st.header("Myriad ↔ Polymarket Pair Management")
    with st.expander("➕ Add New Manual Myriad Pair"):
        mcol1, mcol2, mcol3 = st.columns([3,3,1])
        with mcol1:
            myriad_search = st.text_input("Search Myriad Markets", key="manual_pair_myriad_search")
            myriad_markets_db = load_myriad_markets()
            myriad_results = [m for m in myriad_markets_db if myriad_search.lower() in m['name'].lower()] if myriad_search else []
            myriad_options = {f"{m['name']} ({m['slug']})": m['slug'] for m in myriad_results}
            myriad_label = st.selectbox("Pick Myriad Market", [""] + list(myriad_options.keys()), key="myriad_select", index=0)
            myriad_slug = myriad_options.get(myriad_label, "")
        with mcol2:
            poly_search_myriad = st.text_input("Search Polymarket", key="manual_pair_poly_search_myriad")
            pm_results_myriad = p_client.search_markets(poly_search_myriad) if poly_search_myriad else []
            poly_options_myriad = {f'{m["question"]} ({m["condition_id"]})': m["condition_id"] for m in pm_results_myriad}
            poly_label_myriad = st.selectbox("Pick Polymarket Market", [""] + list(poly_options_myriad.keys()), key="myriad_poly_select", index=0)
            poly_id_myriad = poly_options_myriad.get(poly_label_myriad, "")
        with mcol3:
            st.write("")
            st.write("")
            if st.button("Add Myriad Pair"):
                if myriad_slug and poly_id_myriad:
                    # Default is_autotrade_safe to 0 (False)
                    save_manual_pair_myriad(myriad_slug, poly_id_myriad, 0, 5.0, None, 0)
                    if notifier: notifier.notify_manual_pair("Myriad", myriad_slug, poly_id_myriad)
                    st.success("Myriad pair added!"); st.rerun()
                else: st.warning("Please provide both market selections.")

    manual_pairs_myriad = load_manual_pairs_myriad()
    if manual_pairs_myriad:
        with st.expander("📝 Edit Saved Myriad Pairs"):
            sorted_pairs_myriad = sorted(
                [(f"{myriad_map.get(m_slug, {'name': 'Unknown'})['name']} ({m_slug})", m_slug, p_id, is_flipped, profit_threshold, end_date_override, is_autotrade_safe)
                 for m_slug, p_id, is_flipped, profit_threshold, end_date_override, is_autotrade_safe in manual_pairs_myriad],
                key=lambda x: x[0]
            )

            for display_name, m_slug, p_id, is_flipped, profit_threshold, end_date_override, is_autotrade_safe in sorted_pairs_myriad:
                st.markdown(f"**{display_name}**")
                
                m_url = f"https://app.myriad.social/markets/{m_slug}"
                p_url = f"https://polymarket.com/event/{p_id}"
                
                c1_disp_m, c2_disp_m = st.columns([12,1])
                with c1_disp_m:
                    st.markdown(f"• [Myriad Link]({m_url}) ↔ [Polymarket Link]({p_url})")
                with c2_disp_m:
                    if st.button("❌", key=f"del_pair_myriad_{m_slug}_{p_id}", help="Delete this pair"):
                        delete_manual_pair_myriad(m_slug, p_id)
                        st.rerun()

                with st.form(key=f"form_pair_myriad_{m_slug}_{p_id}"):
                    default_date, default_time = None, None
                    api_date_str = myriad_map.get(m_slug, {}).get('expires_at')
                    final_ts = end_date_override
                    if not final_ts and api_date_str:
                        try:
                            dt_obj = datetime.fromisoformat(api_date_str.replace('Z', '+00:00'))
                            final_ts = int(dt_obj.timestamp() * 1000)
                        except ValueError: pass
                    if final_ts:
                        dt_obj = datetime.fromtimestamp(final_ts / 1000, tz=timezone.utc)
                        default_date = dt_obj.date()
                        default_time = dt_obj.time()
                    
                    c1, c2, c3, c4, c5, c6 = st.columns([2, 2, 2, 1, 1, 2])
                    new_threshold = c1.number_input("Profit Alert ($)", value=float(profit_threshold), min_value=0.0, step=1.0, key=f"threshold_myriad_{m_slug}_{p_id}")
                    end_date_input = c2.date_input("End Date (UTC)", value=default_date, help="Override end date for APY. Clear to use API default.", key=f"date_myriad_{m_slug}_{p_id}")
                    end_time_input = c3.time_input("End Time (UTC)", value=default_time, help="Override end time for APY.", key=f"time_myriad_{m_slug}_{p_id}")
                    is_flipped_new = c4.checkbox("Flipped", value=bool(is_flipped), key=f"flipped_myriad_{m_slug}_{p_id}")
                    is_autotrade_safe_new = c5.checkbox("🤖 Auto", value=bool(is_autotrade_safe), help="Enable automated trading for this pair.", key=f"autotrade_myriad_{m_slug}_{p_id}")

                    if c6.form_submit_button("Update Pair"):
                        new_override_ts = None
                        if end_date_input and end_time_input:
                            combined_dt = datetime.combine(end_date_input, end_time_input, tzinfo=timezone.utc)
                            new_override_ts = int(combined_dt.timestamp() * 1000)
                        save_manual_pair_myriad(m_slug, p_id, int(is_flipped_new), float(new_threshold), new_override_ts, int(is_autotrade_safe_new))
                        st.success(f"Pair {m_slug}/{p_id} updated."); time.sleep(1); st.rerun()
                st.markdown("---")


    st.subheader("🆕 Pending New Myriad Markets")
    pending_myriad = load_new_myriad_markets()
    if not pending_myriad:
        st.info("No new Myriad markets awaiting processing.")
    else:
        for m in pending_myriad:
            st.markdown(f"**{m['market_name']}** (Slug: `{m['market_slug']}`)")
            cols = st.columns([3, 1, 1])
            with cols[0]:
                search_q = st.text_input("Search Polymarket", key=f"poly_search_myriad_{m['market_id']}")
                pm_res = p_client.search_markets(search_q) if search_q else []
                opts = {f'{res["question"]} ({res["condition_id"]})': res["condition_id"] for res in pm_res}
                sel_label = st.selectbox("Pick Polymarket market", [""] + list(opts.keys()), key=f"poly_select_myriad_{m['market_id']}", index=0)
                poly_id = opts.get(sel_label, "")
            with cols[1]:
                st.write("")
                st.write("")
                if st.button("Match", key=f"match_myriad_{m['market_id']}"):
                    if poly_id:
                        save_manual_pair_myriad(m["market_slug"], poly_id, 0, 5.0, None, 0)
                        remove_new_myriad_market(m["market_id"])
                        if notifier: notifier.notify_manual_pair("Myriad", m['market_slug'], poly_id)
                        st.success("Matched!"); st.rerun()
                    else: st.error("Please select a Polymarket market.")
            with cols[2]:
                st.write("")
                st.write("")
                if st.button("Ignore", key=f"ignore_myriad_{m['market_id']}"):
                    ignore_myriad_market(m["market_id"]); st.warning("Ignored."); st.rerun()
            st.markdown("---")

with tab_other:
    st.subheader("📈 Bodega Probability Watches")
    # This section is unchanged
    st.markdown("---")
    st.subheader("🔄 Refresh Markets from APIs")
    if st.button("Run Market Refresh"):
        with st.spinner("Fetching markets..."):
            save_bodega_markets(b_client.fetch_markets())
            save_myriad_markets(m_client.fetch_markets())
            save_polymarkets(fetch_all_polymarket_clob_markets())
            st.success("Market data refreshed.")
            st.rerun()

    st.markdown("---")
    st.subheader("🚨 Admin Actions")
    st.warning("These actions are destructive. Use with caution.")
    if st.button("Clear Pending Autotrade Queue"):
        with st.spinner("Clearing autotrade queue..."):
            cleared_count = clear_arb_opportunities()
            st.success(f"Successfully cleared {cleared_count} pending opportunities from the autotrade queue.")
            time.sleep(2)
            st.rerun()

st.markdown("---")
st.header("🚀 Manual Arbitrage Check")

def calculate_apy(roi: float, end_date_ms: int) -> float:
    """Calculates APY given ROI and an end date timestamp in milliseconds."""
    if not end_date_ms or roi <= 0: return 0.0
    now_utc = datetime.now(timezone.utc)
    end_date_utc = datetime.fromtimestamp(end_date_ms / 1000, tz=timezone.utc)
    time_to_expiry = end_date_utc - now_utc
    days_to_expiry = time_to_expiry.total_seconds() / (24 * 3600)
    if days_to_expiry <= 0.01: return 0.0
    return (roi / days_to_expiry) * 365

st.markdown("##### Auto-Check Frequency Control")
frequency_options = {"⚡ High (30 seconds)": 30, "👍 Normal (3 minutes)": 180, "🐌 Low (10 minutes)": 600, "⏸️ Paused (1 hour)": 3600}
seconds_to_name = {v: k for k, v in frequency_options.items()}
current_interval_seconds = int(get_config_value('arb_check_interval_seconds', '180'))
current_selection_name = seconds_to_name.get(current_interval_seconds)
option_names = list(frequency_options.keys())
try: current_index = option_names.index(current_selection_name) if current_selection_name else 1
except ValueError: current_index = 1
selected_frequency_name = st.radio("Set arbitrage check interval:", option_names, index=current_index, key="arb_frequency_radio", horizontal=True, label_visibility="collapsed")
selected_seconds = frequency_options[selected_frequency_name]
if selected_seconds != current_interval_seconds:
    set_config_value('arb_check_interval_seconds', str(selected_seconds))
    st.success(f"Arbitrage check frequency set to: **{selected_frequency_name}**. The background service will update within 15 seconds.")
    time.sleep(1)
    st.rerun()

if st.button("Check All Manual Pairs for Arbitrage"):
    with st.spinner("Checking all pairs for arbitrage opportunities..."):
        # --- BODEGA CHECK ---
        st.subheader("Bodega ↔ Polymarket Results")
        ada_usd = fx_client.get_ada_usd()
        manual_pairs_bodega_check = load_manual_pairs()
        bodega_results = []
        if not manual_pairs_bodega_check: st.info("No manual Bodega pairs to check.")
        else:
            # --- OPTIMIZATION: Fetch all Bodega market configs once ---
            try:
                all_bodega_markets = b_client.fetch_markets()
                bodega_market_map = {m['id']: m for m in all_bodega_markets}
            except Exception as e:
                st.error(f"Failed to fetch Bodega market configs: {e}")
                bodega_market_map = {}

            prog = st.progress(0, text="Checking Bodega pairs...")
            for i, (b_id, p_id, is_flipped, profit_threshold, end_date_override) in enumerate(manual_pairs_bodega_check, start=1):
                try:
                    # --- OPTIMIZATION: Use pre-fetched market config ---
                    pool = bodega_market_map.get(b_id)
                    if not pool:
                        log.warning(f"Dashboard check: Skipping pair ({b_id}, {p_id}) because Bodega market config was not found.")
                        continue

                    p_data = p_client.fetch_market(p_id)
                    if not p_data.get('active') or p_data.get('closed'): continue
                    
                    final_end_date_ms = end_date_override if end_date_override else pool.get('deadline')
                    
                    bodega_prediction_info = b_client.fetch_prices(b_id)
                    ob_yes, ob_no = p_data.get("order_book_yes"), p_data.get("order_book_no")
                    p_name_yes, p_name_no = p_data.get('outcome_yes', 'YES'), p_data.get('outcome_no', 'NO')
                    if is_flipped:
                        ob_yes, ob_no = ob_no, ob_yes
                        p_name_yes, p_name_no = p_name_no, p_name_yes

                    Q_YES, Q_NO = bodega_prediction_info.get("yesVolume_ada", 0), bodega_prediction_info.get("noVolume_ada", 0)
                    p_bod_yes = bodega_prediction_info.get("yesPrice_ada")
                    if p_bod_yes is None: continue
                    
                    inferred_B = infer_b(Q_YES, Q_NO, p_bod_yes)
                    pair_opps = build_bodega_arb_table(Q_YES, Q_NO, ob_yes, ob_no, ada_usd, FEE_RATE_BODEGA, inferred_B)
                    
                    for opp in pair_opps:
                        opp['apy'] = calculate_apy(opp.get('roi', 0), final_end_date_ms)
                        opp['polymarket_side'] = p_name_yes if opp['polymarket_side'] == 'YES' else p_name_no
                        bodega_results.append({"description": f"{pool['name']} ↔ {p_data['question']}", "summary": opp, "b_id": b_id, "p_id": p_id, "profit_threshold": profit_threshold})
                        if opp['profit_usd'] > profit_threshold and opp.get('roi', 0) > 0.05 and opp.get('apy', 0) >= 0.50:
                            if notifier: notifier.notify_arb_opportunity(f"{pool['name']} ↔ {p_data['question']}", opp, b_id, p_id, BODEGA_API)
                except Exception as e:
                    st.error(f"Error checking Bodega pair ({b_id}, {p_id}): {e}")
                prog.progress(i / len(manual_pairs_bodega_check))
            prog.empty()

            if bodega_results:
                st.info(f"Displaying {len(bodega_results)} potential Bodega trades (profitable or not).")
                bodega_results.sort(key=lambda o: o["summary"].get("profit_usd", 0), reverse=True)
                for opp in bodega_results:
                    summary = opp['summary']
                    profit = summary.get('profit_usd', 0)
                    roi = summary.get('roi', 0)
                    apy = summary.get('apy', 0)
                    threshold = opp['profit_threshold']

                    if profit > threshold and roi > 0.05 and apy >= 0.50:
                        st.markdown(f"**<p style='color:green; font-size: 1.1em;'>PROFITABLE (>{threshold:.2f}$): {opp['description']}</p>**", unsafe_allow_html=True)
                    elif profit > 0:
                        st.markdown(f"**<p style='color:orange; font-size: 1.1em;'>SMALL PROFIT: {opp['description']}</p>**", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**{opp['description']}**")
                    
                    main_cols = st.columns(5)
                    main_cols[0].metric("Potential Profit/Loss (USD)", f"${summary.get('profit_usd', 0):.2f}")
                    main_cols[1].metric("Return on Investment (ROI)", f"{summary.get('roi', 0)*100:.2f}%")
                    main_cols[2].metric("APY", f"{apy*100:.2f}%")
                    main_cols[3].metric("Score (Profit*ROI)", f"{summary.get('score', 0):.4f}")
                    main_cols[4].metric("Inferred B", f"{summary.get('inferred_B', 0):.2f}")

                    trade_cols = st.columns(2)
                    with trade_cols[0]:
                        st.markdown("##### 1. Bodega Trade")
                        st.markdown(f"- **Action:** Buy `{summary['bodega_shares']}` **{summary['bodega_side']}** shares\n- **Cost:** `₳{summary['cost_bod_ada']:.2f}` (+ `₳{summary['fee_bod_ada']:.2f}` fee)\n- **Start Price:** `{summary['p_start']:.4f}` → **End Price:** `{summary['p_end']:.4f}`")
                    with trade_cols[1]:
                        st.markdown("##### 2. Polymarket Hedge")
                        st.markdown(f"- **Action:** Buy `{summary['polymarket_shares']}` **{summary['polymarket_side']}** shares\n- **Cost:** `${summary['cost_poly_usd']:.2f}`\n- **Avg. Price:** `{summary.get('avg_poly_price', 0):.4f}`\n- **Hedge Complete:** {'✅' if summary['fill'] else '❌'}")
                    
                    analysis_data = summary.get('analysis_details', [])
                    if analysis_data:
                        with st.expander("Show Detailed Price Adjustment Analysis"):
                            df_analysis = pd.DataFrame(analysis_data)
                            df_display = df_analysis[['adjustment', 'p_end', 'bodega_shares', 'profit_usd', 'roi', 'score']].copy()
                            df_display.rename(columns={'adjustment': 'Adj', 'p_end': 'Target Price', 'bodega_shares': 'Shares', 'profit_usd': 'Profit ($)', 'roi': 'ROI (%)', 'score': 'Score'}, inplace=True)
                            df_display['ROI (%)'] = df_display['ROI (%)'] * 100
                            st.dataframe(df_display, use_container_width=True, hide_index=True, column_config={
                                "Adj": st.column_config.NumberColumn(format="%.4f"), 
                                "Target Price": st.column_config.NumberColumn(format="%.4f"), 
                                "Shares": st.column_config.NumberColumn(format="%d"), 
                                "Profit ($)": st.column_config.NumberColumn(format="$%.2f"), 
                                "ROI (%)": st.column_config.NumberColumn(format="%.2f%%"),
                                "Score": st.column_config.NumberColumn(format="%.4f")
                            })
                    else:
                        st.caption("Profit/Loss based on a 1-share trade.")
                    st.markdown("---")
            else:
                st.info("No Bodega arbitrage opportunities found.")

        # --- MYRIAD CHECK ---
        st.subheader("Myriad ↔ Polymarket Results")
        manual_pairs_myriad_check = load_manual_pairs_myriad()
        if not manual_pairs_myriad_check: st.info("No manual Myriad pairs to check.")
        else:
            prog_myriad = st.progress(0, text="Checking Myriad pairs...")
            myriad_results = []
            for i, (m_slug, p_id, is_flipped, profit_threshold, end_date_override, _) in enumerate(manual_pairs_myriad_check, start=1):
                try:
                    m_data = m_client.fetch_market_details(m_slug)
                    p_data = p_client.fetch_market(p_id)

                    if not all([m_data, p_data]) or m_data.get('state') != 'open' or not p_data.get('active'): continue
                    
                    final_end_date_ms = None
                    if end_date_override:
                        final_end_date_ms = end_date_override
                    elif m_data.get("expires_at"):
                        dt_obj = datetime.fromisoformat(m_data["expires_at"].replace('Z', '+00:00'))
                        final_end_date_ms = int(dt_obj.timestamp() * 1000)

                    m_prices = m_client.parse_realtime_prices(m_data)
                    if not m_prices:
                        st.warning(f"Could not parse real-time prices for Myriad market {m_slug}, skipping.")
                        continue
                    
                    if m_prices.get('price1') is None or m_prices.get('shares1') is None: continue

                    Q1, Q2 = m_prices['shares1'], m_prices['shares2']
                    B_param = m_data.get('liquidity')
                    if not B_param or B_param <=0:
                        st.warning(f"Myriad market {m_slug} has invalid liquidity parameter ({B_param}). Skipping.")
                        continue

                    obp1, obp2 = p_data.get('order_book_yes'), p_data.get('order_book_no')
                    p_name1, p_name2 = p_data.get('outcome_yes'), p_data.get('outcome_no')
                    if is_flipped:
                        obp1, obp2 = obp2, obp1
                        p_name1, p_name2 = p_name2, p_name1

                    pair_opps = build_arbitrage_table_myriad(
                        Q1, Q2, obp1, obp2, 
                        FEE_RATE_MYRIAD_BUY, B_param,
                        P1_MYR_REALTIME=m_prices['price1']
                    )

                    for opp in pair_opps:
                        opp['apy'] = calculate_apy(opp.get('roi', 0), final_end_date_ms)
                        opp['myriad_side_title'] = m_prices['title1'] if opp['myriad_side'] == 1 else m_prices['title2']
                        opp['polymarket_side_title'] = p_name1 if opp['polymarket_side'] == 1 else p_name2
                        pair_desc = f"{m_data['title']} ↔ {p_data['question']}"
                        myriad_results.append({"description": pair_desc, "summary": opp, "m_slug": m_slug, "p_id": p_id, "profit_threshold": profit_threshold})
                        if opp['profit_usd'] > profit_threshold and opp.get('roi', 0) > 0.025 and opp.get('apy', 0) >= 0.50:
                            if notifier: notifier.notify_arb_opportunity_myriad(pair_desc, opp, m_slug, p_id)
                except Exception as e:
                    st.error(f"Error checking Myriad pair ({m_slug}, {p_id}): {e}")
                prog_myriad.progress(i / len(manual_pairs_myriad_check))
            prog_myriad.empty()

            if myriad_results:
                st.info(f"Displaying {len(myriad_results)} potential Myriad trades (profitable or not).")
                myriad_results.sort(key=lambda o: o["summary"].get("profit_usd", 0), reverse=True)
                for opp in myriad_results:
                    summary = opp['summary']
                    profit, roi, apy = summary.get('profit_usd', 0), summary.get('roi', 0), summary.get('apy', 0)
                    threshold = opp['profit_threshold']

                    if profit > threshold and roi > 0.025 and apy >= 0.50:
                        st.markdown(f"**<p style='color:green; font-size: 1.1em;'>PROFITABLE (>{threshold:.2f}$): {opp['description']}</p>**", unsafe_allow_html=True)
                    elif profit > 0:
                        st.markdown(f"**<p style='color:orange; font-size: 1.1em;'>SMALL PROFIT: {opp['description']}</p>**", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**{opp['description']}**")

                    m_cols = st.columns(5)
                    m_cols[0].metric("Potential Profit/Loss (USD)", f"${profit:.2f}")
                    m_cols[1].metric("ROI", f"{roi*100:.2f}%")
                    m_cols[2].metric("APY", f"{apy*100:.2f}%")
                    m_cols[3].metric("Score (Profit*ROI)", f"{summary.get('score', 0):.4f}")
                    m_cols[4].metric("Liquidity (B)", f"{summary.get('B', 0):.2f}")
                    t_cols = st.columns(2)
                    with t_cols[0]:
                        st.markdown("##### 1. Myriad Trade")
                        st.markdown(f"- **Action:** Buy `{summary['myriad_shares']}` **{summary['myriad_side_title']}** shares\n- **Cost:** `${summary['cost_myr_usd']:.2f}` (+ `${summary['fee_myr_usd']:.2f}` fee)\n- **Start Price:** `{summary['p_start']:.4f}` → **End Price:** `{summary['p_end']:.4f}`")
                    with t_cols[1]:
                        st.markdown("##### 2. Polymarket Hedge")
                        st.markdown(f"- **Action:** Buy `{summary['polymarket_shares']}` **{summary['polymarket_side_title']}** shares\n- **Cost:** `${summary['cost_poly_usd']:.2f}`\n- **Avg. Price:** `{summary.get('avg_poly_price', 0):.4f}`\n- **Hedge Complete:** {'✅' if summary['fill'] else '❌'}")
                    
                    analysis_data = summary.get('analysis_details', [])
                    if analysis_data:
                        with st.expander("Show Detailed Price Adjustment Analysis"):
                            df_analysis = pd.DataFrame(analysis_data)
                            df_display = df_analysis[['adjustment', 'p_end', 'myriad_shares', 'profit_usd', 'roi', 'score']].copy()
                            df_display.rename(columns={'adjustment': 'Adj', 'p_end': 'Target Price', 'myriad_shares': 'Shares', 'profit_usd': 'Profit ($)', 'roi': 'ROI (%)', 'score': 'Score'}, inplace=True)
                            df_display['ROI (%)'] = df_display['ROI (%)'] * 100
                            st.dataframe(df_display, use_container_width=True, hide_index=True, column_config={
                                "Adj": st.column_config.NumberColumn(format="%.4f"), 
                                "Target Price": st.column_config.NumberColumn(format="%.4f"), 
                                "Shares": st.column_config.NumberColumn(format="%d"), 
                                "Profit ($)": st.column_config.NumberColumn(format="$%.2f"), 
                                "ROI (%)": st.column_config.NumberColumn(format="%.2f%%"),
                                "Score": st.column_config.NumberColumn(format="%.4f")
                            })
                    else:
                        st.caption("Profit/Loss based on a 1-share trade.")
                    st.markdown("---")
            else:
                st.info("No Myriad arbitrage opportunities found.")