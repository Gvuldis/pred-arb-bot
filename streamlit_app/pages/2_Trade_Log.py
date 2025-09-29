import sys, pathlib, time
# Ensure the project root is on Python’s import path
ROOT = pathlib.Path(__file__).parent.parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
import pandas as pd
import json
import logging
import requests
from config import POLYMARKET_PROXY_ADDRESS, myriad_account, myriad_contract
from streamlit_app.db import get_conn, get_active_matched_myriad_market_info, clear_all_trade_logs

log = logging.getLogger(__name__)

st.set_page_config(layout="wide", page_title="Automated Trade Log")

# --- CURRENT POSITIONS ---
st.header("Current Positions")

@st.cache_data(ttl=60)
def get_poly_positions(user_address: str):
    """Fetches and processes current positions from the Polymarket Data API."""
    if not user_address:
        return pd.DataFrame()
    try:
        url = "https://data-api.polymarket.com/positions"
        # FIX: Set sizeThreshold to 1 to filter out positions with less than 1 share.
        params = {"user": user_address, "sizeThreshold": 1}
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        positions = response.json()
        if not positions:
            return pd.DataFrame()
        
        df = pd.DataFrame(positions)
        df_display = df[['title', 'outcome', 'size', 'avgPrice', 'curPrice', 'currentValue', 'cashPnl', 'percentPnl']]
        df_display = df_display.rename(columns={
            'title': 'Market', 'outcome': 'Outcome', 'size': 'Shares', 'avgPrice': 'Avg. Price',
            'curPrice': 'Current Price', 'currentValue': 'Value ($)', 'cashPnl': 'PnL ($)', 'percentPnl': 'PnL (%)'
        })
        return df_display
    except Exception as e:
        log.error(f"Failed to fetch Polymarket positions: {e}")
        st.error(f"Could not fetch Polymarket positions: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_myriad_positions(user_address: str):
    """
    Fetches user shares for all manually matched Myriad markets from the smart contract.
    Returns both a DataFrame of positions and a detailed debug log.
    """
    debug_log = []
    
    if not user_address:
        debug_log.append("❌ Error: Myriad user address is not available. Check .env configuration.")
        return pd.DataFrame(), debug_log
    if not myriad_contract:
        debug_log.append("❌ Error: Myriad smart contract is not initialized. Check RPC connection and config.")
        return pd.DataFrame(), debug_log
    
    debug_log.append(f"ℹ️ Checking Myriad positions for address: {user_address}")
    
    # NEW LOGIC: Use manually matched markets instead of traded markets.
    matched_markets = get_active_matched_myriad_market_info()
    if not matched_markets:
        debug_log.append("🤔 No manually matched Myriad markets found in the database. Nothing to check for positions.")
        return pd.DataFrame(), debug_log
        
    debug_log.append(f"🔎 Found {len(matched_markets)} manually matched market(s) to check: {[m['slug'] for m in matched_markets]}")
    positions = []
    
    for market in matched_markets:
        market_id = market.get('id')
        market_slug = market.get('slug', 'N/A')
        debug_log.append(f"\n--- Checking market: '{market_slug}' (ID: {market_id}) ---")
        try:
            if not market_id:
                debug_log.append(f"⚠️ Skipping market '{market_slug}': Market ID is missing from database record.")
                continue

            # This is the on-chain call
            _liquidity, outcomes = myriad_contract.functions.getUserMarketShares(market_id, user_address).call()
            debug_log.append(f"✅ On-chain call successful.")
            debug_log.append(f"   - Raw liquidity returned: {_liquidity}")
            debug_log.append(f"   - Raw outcomes array returned: {outcomes}")
            
            # Shares are scaled by 1e6. We take the integer part as requested.
            shares_outcome_0 = int(outcomes[0] / 1e6)
            shares_outcome_1 = int(outcomes[1] / 1e6)
            debug_log.append(f"   - Calculated shares for Outcome 0: {shares_outcome_0} (from raw value {outcomes[0]})")
            debug_log.append(f"   - Calculated shares for Outcome 1: {shares_outcome_1} (from raw value {outcomes[1]})")

            if shares_outcome_0 > 0:
                positions.append({'Market': market['name'], 'Outcome Index': 0, 'Shares': shares_outcome_0})
                debug_log.append(f"   => Found {shares_outcome_0} shares for Outcome 0. Adding to positions list.")
            if shares_outcome_1 > 0:
                positions.append({'Market': market['name'], 'Outcome Index': 1, 'Shares': shares_outcome_1})
                debug_log.append(f"   => Found {shares_outcome_1} shares for Outcome 1. Adding to positions list.")
                
        except Exception as e:
            error_message = f"❌ ERROR checking market '{market_slug}': {e}"
            debug_log.append(error_message)
            log.error(f"Failed to get Myriad shares for market {market_slug}: {e}", exc_info=True)

    debug_log.append("\n--- Finished ---")
    return pd.DataFrame(positions), debug_log


pos_tab1, pos_tab2 = st.tabs(["Polymarket Positions", "Myriad Positions"])

with pos_tab1:
    if not POLYMARKET_PROXY_ADDRESS:
        st.warning("`POLYMARKET_PROXY_ADDRESS` not set in .env file.")
    else:
        with st.spinner("Fetching Polymarket positions..."):
            df_poly_pos = get_poly_positions(POLYMARKET_PROXY_ADDRESS)
            if df_poly_pos.empty:
                st.info("No open positions found on Polymarket with more than 1 share.")
            else:
                st.dataframe(df_poly_pos, use_container_width=True, hide_index=True, column_config={
                    "Shares": st.column_config.NumberColumn(format="%.2f"),
                    "Value ($)": st.column_config.NumberColumn(format="$%.2f"),
                    "PnL ($)": st.column_config.NumberColumn(format="$%.2f"),
                    "PnL (%)": st.column_config.NumberColumn(format="%.2f%%"),
                })

with pos_tab2:
    if not myriad_account:
        st.warning("`MYRIAD_PRIVATE_KEY` not set in .env file.")
    else:
        with st.spinner("Fetching Myriad positions from on-chain data..."):
            df_myriad_pos, myriad_debug_log = get_myriad_positions(myriad_account.address)
            if df_myriad_pos.empty:
                st.info("No open positions found on Myriad for any of your manually matched markets.")
            else:
                st.dataframe(df_myriad_pos, use_container_width=True, hide_index=True)
            
            with st.expander("Show Myriad Position Fetch Log"):
                st.code("\n".join(myriad_debug_log), language="log")

st.markdown("---")

st.title("🤖 Automated Trade Log")

@st.cache_data(ttl=30)
def load_trade_logs():
    try:
        with get_conn() as conn:
            df = pd.read_sql_query("SELECT * FROM automated_trades_log ORDER BY attempt_timestamp_utc DESC", conn)
        return df
    except Exception as e:
        st.error(f"Failed to load trade logs from database: {e}")
        return pd.DataFrame()

if st.button("Refresh Log"):
    st.cache_data.clear()
    st.rerun()

df_logs = load_trade_logs()

if df_logs.empty:
    st.info("No automated trades have been logged yet.")
else:
    # Quick Summary Metrics
    successful_trades_df = df_logs[df_logs['status'] == 'SUCCESS']
    total_trades = len(df_logs)
    successful_trades = len(successful_trades_df)
    total_profit = successful_trades_df['final_profit_usd'].sum()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Attempts", total_trades)
    col2.metric("Successful Trades", successful_trades)
    col3.metric("Total Estimated Profit", f"${total_profit:,.2f}")

    st.markdown("---")
    st.subheader("All Trade Attempts")
    
    display_df = df_logs.copy()
    
    # Format columns for better readability
    float_cols = ['planned_poly_shares', 'planned_myriad_shares', 'executed_poly_shares', 'executed_myriad_shares']
    currency_cols = ['final_profit_usd', 'executed_poly_cost_usd', 'executed_myriad_cost_usd']
    
    for col in float_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.4f}" if pd.notnull(x) else "N/A")
    for col in currency_cols:
         if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "N/A")

    st.dataframe(display_df[[
        'attempt_timestamp_utc', 'status', 'myriad_slug', 'final_profit_usd', 
        'executed_poly_shares', 'executed_poly_cost_usd', 'executed_myriad_shares', 'executed_myriad_cost_usd', 
        'myriad_api_lookup_status', 'status_message'
    ]].rename(columns={
        'attempt_timestamp_utc': 'Timestamp (UTC)', 'status': 'Status', 'myriad_slug': 'Market Slug',
        'final_profit_usd': 'Est. Profit', 'executed_poly_shares': 'Poly Shares', 'executed_poly_cost_usd': 'Poly Cost',
        'executed_myriad_shares': 'Myriad Shares', 'executed_myriad_cost_usd': 'Myriad Cost',
        'myriad_api_lookup_status': 'Myriad Lookup', 'status_message': 'Message'
    }), use_container_width=True, hide_index=True)
    
    st.subheader("Detailed Logs")
    for index, row in df_logs.iterrows():
        with st.expander(f"**{row['attempt_timestamp_utc']}** | **{row['status']}** on `{row['myriad_slug']}`"):
            st.write(f"**Trade ID:** `{row['trade_id']}`")
            
            det_cols = st.columns(2)
            with det_cols[0]:
                st.markdown("**Polymarket**")
                st.metric("Executed Shares", f"{row['executed_poly_shares']:.4f}" if pd.notnull(row['executed_poly_shares']) else "N/A")
                st.metric("Cost (USD)", f"${row['executed_poly_cost_usd']:.2f}" if pd.notnull(row['executed_poly_cost_usd']) else "N/A")
                st.text_area("Poly TX Details", value=row['poly_tx_hash'], height=100, key=f"poly_tx_{row['trade_id']}")
            with det_cols[1]:
                st.markdown("**Myriad**")
                st.metric("Executed Shares", f"{row['executed_myriad_shares']:.4f}" if pd.notnull(row['executed_myriad_shares']) else "N/A")
                st.metric("Cost (USD)", f"${row['executed_myriad_cost_usd']:.2f}" if pd.notnull(row['executed_myriad_cost_usd']) else "N/A")
                st.text_area("Myriad TX Hash", value=row['myriad_tx_hash'], key=f"myriad_tx_{row['trade_id']}")
                st.caption(f"API Lookup Status: **{row['myriad_api_lookup_status']}**")
                
            st.markdown(f"**Status Message:** `{row['status_message']}`")
            
            with st.container():
                st.markdown("**Full Opportunity Details**")
                try:
                    log_details = json.loads(row['log_details'])
                    st.json(log_details, expanded=False)
                except (json.JSONDecodeError, TypeError):
                    st.text(row['log_details'])

st.markdown("---")
st.subheader("🚨 Admin Actions")
with st.expander("Clear Trade Log History"):
    st.warning("This will permanently delete all automated trade logs from the database. This action cannot be undone.")
    
    if 'confirm_delete_logs' not in st.session_state:
        st.session_state['confirm_delete_logs'] = False

    if st.button("DELETE ALL LOGS"):
        st.session_state['confirm_delete_logs'] = True
    
    if st.session_state['confirm_delete_logs']:
        st.error("Are you absolutely sure?")
        col1, col2, _ = st.columns([1,1,4])
        with col1:
            if st.button("Yes, DELETE", type="primary"):
                with st.spinner("Deleting logs..."):
                    deleted_count = clear_all_trade_logs()
                    st.success(f"Successfully deleted {deleted_count} trade logs.")
                    st.session_state['confirm_delete_logs'] = False
                    st.cache_data.clear()
                    time.sleep(2)
                    st.rerun()
        with col2:
            if st.button("Cancel"):
                st.session_state['confirm_delete_logs'] = False
                st.rerun()
