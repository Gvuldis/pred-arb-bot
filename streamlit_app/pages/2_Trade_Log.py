import sys, pathlib
# Ensure the project root is on Python’s import path
ROOT = pathlib.Path(__file__).parent.parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
import pandas as pd
import json
from streamlit_app.db import get_conn
import logging

log = logging.getLogger(__name__)

st.set_page_config(layout="wide", page_title="Automated Trade Log")
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