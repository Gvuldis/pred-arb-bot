# streamlit_app/pages/2_Portfolio_Tracker.py
import sys
import pathlib
import io
import streamlit as st
import pandas as pd
import logging
from datetime import datetime

# --- Path setup to allow finding the 'track_record' module ---
ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from track_record import database, ingest
from track_record.clients import CoinGeckoClient
# Import the db functions from the main app to access the app_config table
from streamlit_app.db import get_config_value, set_config_value

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

st.set_page_config(layout="wide", page_title="Portfolio Tracker")
st.title("📈 Portfolio Tracker")

try:
    database.init_db()
except Exception as e:
    st.error(f"Failed to initialize the portfolio database: {e}")
    st.stop()

@st.cache_resource
def get_cg_client():
    return CoinGeckoClient()

@st.cache_data
def get_cached_ada_price():
    """Fetches and caches the ADA price for the session."""
    log.info("Fetching fresh ADA price from API...")
    return get_cg_client().get_live_ada_price()

# --- NEW: Function to save cash values to DB ---
def save_cash_values_portfolio():
    set_config_value('poly_cash_usd', st.session_state.current_poly_cash)
    set_config_value('bodega_cash_ada', st.session_state.current_bodega_cash)
    log.info(f"Saved cash values to DB from portfolio page: Poly={st.session_state.current_poly_cash}, Bodega={st.session_state.current_bodega_cash}")


# --- Page Tabs ---
tab_ingest, tab_dashboard, tab_builder = st.tabs(["Data Ingestion", "📊 Portfolio Dashboard", "🛠️ Position Builder"])

# ==================================
#         DATA INGESTION TAB
# ==================================
with tab_ingest:
    st.header("🔄 Update Transaction Data")
    st.markdown("Upload your latest Polymarket CSV to sync transactions. This will also trigger a sync for new on-chain Bodega trades. The process runs in the background and will not add duplicates.")
    st.info("Please set `CARDANO_ADDRESS` and `BLOCKFROST_KEY` in your Streamlit secrets.", icon="🔑")
    cardano_address = st.secrets.get("CARDANO_ADDRESS", "")
    blockfrost_key = st.secrets.get("BLOCKFROST_KEY", "")
    uploaded_file = st.file_uploader("Upload Polymarket Transaction History CSV", type="csv")
    if st.button("Run Data Sync"):
        if not uploaded_file: st.warning("Please upload a Polymarket CSV file.")
        elif not cardano_address or not blockfrost_key: st.error("Missing `CARDANO_ADDRESS` or `BLOCKFROST_KEY` in Streamlit secrets.")
        else:
            with st.spinner("Running data ingestion in parallel... This may take a few minutes."):
                string_data = io.StringIO(uploaded_file.getvalue().decode('utf-8-sig'))
                results = ingest.run_ingestion_in_parallel(string_data, cardano_address, blockfrost_key)
                st.success("Data sync complete!")
                st.json(results)
                st.success("You can now view the updated data on the other tabs. You may need to refresh the page.")

# ==================================
#         DASHBOARD TAB
# ==================================
with tab_dashboard:
    live_ada_price = get_cached_ada_price()
    if live_ada_price == 0.0: st.error("Could not fetch live ADA price. Calculations will be inaccurate.")

    st.sidebar.title("Portfolio Summary")
    if st.sidebar.button("Refresh Live Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Load values from DB or use defaults
    poly_cash_from_db = float(get_config_value('poly_cash_usd', '19.0'))
    bodega_cash_from_db = float(get_config_value('bodega_cash_ada', '603.0'))

    current_cash_poly = st.sidebar.number_input(
        "Current Polymarket Cash (USD)", 
        key="current_poly_cash",
        value=poly_cash_from_db,
        on_change=save_cash_values_portfolio
    )
    current_cash_bodega = st.sidebar.number_input(
        "Current Bodega Cash (ADA)", 
        key="current_bodega_cash",
        value=bodega_cash_from_db,
        on_change=save_cash_values_portfolio
    )
    st.sidebar.metric("Live ADA Price", f"${live_ada_price:,.2f}")
    st.sidebar.markdown("---")

    open_positions_raw = database.get_all_positions_with_transactions()
    open_positions, total_worst_case_payout_from_positions = [], 0
    for pos_raw in open_positions_raw:
        pos = pos_raw.copy()
        bodega_buys = [t for t in pos['bodega_trades'] if t['trade_type'] == 'BUY']
        poly_buys = [t for t in pos['poly_trades'] if t['action'] == 'Buy']
        bodega_shares_bought = sum(t['token_amount'] for t in bodega_buys)
        bodega_ada_spent = sum(t['ada_amount'] for t in bodega_buys)
        bodega_avg_price = (bodega_ada_spent / bodega_shares_bought) if bodega_shares_bought else 0
        poly_shares_bought = sum(t['token_amount'] for t in poly_buys)
        poly_usd_spent = sum(t['usdc_amount'] for t in poly_buys)
        poly_avg_price = (poly_usd_spent / poly_shares_bought) if poly_shares_bought else 0
        net_bodega_shares = sum(t['token_amount'] if t['trade_type'] == 'BUY' else -t['token_amount'] for t in pos['bodega_trades'])
        net_poly_shares = sum(t['token_amount'] if t['action'] == 'Buy' else -t['token_amount'] for t in pos['poly_trades'])
        net_poly_usd_flow = sum(t['usdc_amount'] if t['action'] in ['Sell', 'Redeem'] else -t['usdc_amount'] for t in pos['poly_trades'])
        net_bodega_ada_flow = sum(t['ada_amount'] if t['trade_type'] in ['SELL', 'REDEEM'] else -t['ada_amount'] for t in pos['bodega_trades'])
        cost_basis_usd = -(net_poly_usd_flow + (net_bodega_ada_flow * live_ada_price))
        payout_if_bodega_wins = (net_bodega_shares * live_ada_price * 0.98)
        payout_if_poly_wins = net_poly_shares
        best_case_payout = max(payout_if_bodega_wins, payout_if_poly_wins)
        worst_case_payout = min(payout_if_bodega_wins, payout_if_poly_wins)
        best_case_pnl = best_case_payout - cost_basis_usd
        worst_case_pnl = worst_case_payout - cost_basis_usd
        total_worst_case_payout_from_positions += worst_case_payout
        pos['metrics'] = { 'bodega_shares_bought': bodega_shares_bought, 'bodega_avg_price': bodega_avg_price, 'poly_shares_bought': poly_shares_bought, 'poly_avg_price': poly_avg_price, 'net_bodega_shares': net_bodega_shares, 'net_poly_shares': net_poly_shares, 'cost_basis_usd': cost_basis_usd, 'payout_if_bodega_wins': payout_if_bodega_wins, 'payout_if_poly_wins': payout_if_poly_wins, 'best_case_payout': best_case_payout, 'worst_case_payout': worst_case_payout, 'best_case_pnl': best_case_pnl, 'worst_case_pnl': worst_case_pnl, }
        open_positions.append(pos)
    
    total_cash_value_usd = current_cash_poly + (current_cash_bodega * live_ada_price)
    total_portfolio_value = total_cash_value_usd + total_worst_case_payout_from_positions
    st.sidebar.markdown(f"**Cash Balance:** `${current_cash_poly:,.2f}` (Poly) + `₳{current_cash_bodega:,.2f}` (Bodega)")
    st.sidebar.markdown(f"**Total Cash Value:** `${total_cash_value_usd:,.2f}`")
    st.sidebar.markdown(f"**Open Position Value (Worst Case):** `${total_worst_case_payout_from_positions:,.2f}`")
    st.sidebar.metric("Total Portfolio Value (USD)", f"${total_portfolio_value:,.2f}")
    
    st.header("Open Positions")
    if not open_positions: st.info("No open positions found. Go to the 'Position Builder' to create one.")
    else:
        for pos in open_positions:
            m = pos['metrics']
            with st.expander(f"**{pos['name']}**"):
                col1, col2, col3 = st.columns([2,2,1])
                with col1:
                    st.markdown(f"**Bodega Details**"); st.markdown(f"`{m['bodega_shares_bought']:,.2f}` shares @ `{m['bodega_avg_price']:.4f}` ADA avg."); st.markdown(f"**Net Shares:** `{m['net_bodega_shares']:,.2f}`")
                with col2:
                    st.markdown(f"**Polymarket Details**"); st.markdown(f"`{m['poly_shares_bought']:,.2f}` shares @ `${m['poly_avg_price']:.4f}` avg."); st.markdown(f"**Net Shares:** `{m['net_poly_shares']:,.2f}`")
                with col3:
                    with st.form(key=f"close_form_{pos['id']}"):
                        outcome = st.selectbox("Select Winning Side", ["Bodega Won", "Polymarket Won"], key=f"outcome_{pos['id']}")
                        if st.form_submit_button("Close Position"):
                            final_payout = m['payout_if_bodega_wins'] if outcome == "Bodega Won" else m['payout_if_poly_wins']
                            database.log_completed_trade(pos['id'], pos['name'], m['cost_basis_usd'], final_payout)
                            st.success(f"Position '{pos['name']}' closed and logged."); st.rerun()
                st.markdown("---")
                scol1, scol2 = st.columns(2)
                with scol1: st.metric("Best Case Payout (USD)", f"${m['best_case_payout']:,.2f}"); st.metric("Best Case PnL (USD)", f"${m['best_case_pnl']:,.2f}")
                with scol2: st.metric("Worst Case Payout (USD)", f"${m['worst_case_payout']:,.2f}"); st.metric("Worst Case PnL (USD)", f"${m['worst_case_pnl']:,.2f}")
                if st.toggle("Show Transaction Log", key=f"log_toggle_{pos['id']}"):
                    tcol1, tcol2 = st.columns(2)
                    with tcol1: st.markdown("**Bodega Trades**"); df_b = pd.DataFrame(pos['bodega_trades']); st.dataframe(df_b[['timestamp', 'trade_type', 'token_amount', 'ada_amount']], hide_index=True)
                    with tcol2: st.markdown("**Polymarket Trades**"); df_p = pd.DataFrame(pos['poly_trades']); st.dataframe(df_p[['timestamp', 'action', 'token_amount', 'usdc_amount']], hide_index=True)

    st.header("Trade History (Closed Positions)")
    closed_trades = database.load_completed_trades()
    if not closed_trades: st.info("No closed trades have been logged yet.")
    else:
        for trade in closed_trades:
            trade_id, pnl_correction, correction_reason = trade['id'], trade.get('pnl_correction_usd', 0), trade.get('correction_reason', '')
            total_pnl = trade['net_profit_usd'] + pnl_correction
            col1, col2 = st.columns([3,1])
            with col1:
                st.markdown(f"**{trade['position_name']}** | Closed: {datetime.fromtimestamp(trade['closed_date']).strftime('%Y-%m-%d')}")
                st.metric(f"Final PnL: ${total_pnl:,.2f}", f"Calculated: ${trade['net_profit_usd']:,.2f}, Correction: ${pnl_correction:,.2f}")
                if correction_reason: st.caption(f"Reason: {correction_reason}")
            with col2:
                with st.form(key=f"correction_form_{trade_id}"):
                    correction = st.number_input("PnL Correction (USD)", value=pnl_correction, key=f"corr_{trade_id}")
                    reason = st.text_input("Reason", value=correction_reason, key=f"reason_{trade_id}")
                    if st.form_submit_button("Save Correction"):
                        database.update_pnl_correction(trade_id, correction, reason); st.rerun()
            st.divider()

# ==================================
#         POSITION BUILDER TAB
# ==================================
with tab_builder:
    st.header("🛠️ Position Builder")
    st.markdown("Group raw transactions into a named position. If a name already exists, transactions will be added to it.")
    bodega_txs, poly_txs = database.get_unassigned_transactions()
    with st.form("position_form"):
        position_name = st.text_input("Position Name", placeholder="e.g., Zohran 2025")
        selected_bodega = pd.DataFrame()
        selected_poly = pd.DataFrame()
        st.subheader("Unassigned Bodega Trades")
        if not bodega_txs: st.info("No unassigned Bodega trades found.")
        else:
            df_bodega = pd.DataFrame(bodega_txs); df_bodega['select'] = False
            edited_df_bodega = st.data_editor(df_bodega[['select', 'market_name', 'trade_type', 'token_amount', 'ada_amount', 'id']], hide_index=True, key="bodega_selector")
            selected_bodega = edited_df_bodega[edited_df_bodega['select']]
        st.subheader("Unassigned Polymarket Transactions")
        if not poly_txs: st.info("No unassigned Polymarket transactions found.")
        else:
            df_poly = pd.DataFrame(poly_txs); df_poly['select'] = False
            edited_df_poly = st.data_editor(df_poly[['select', 'market_name', 'action', 'token_name', 'usdc_amount', 'token_amount', 'id']], hide_index=True, key="poly_selector")
            selected_poly = edited_df_poly[edited_df_poly['select']]
        if st.form_submit_button("Create or Add to Position"):
            bodega_ids = selected_bodega['id'].tolist() if not selected_bodega.empty else []
            poly_ids = selected_poly['id'].tolist() if not selected_poly.empty else []
            if not position_name: st.error("Position Name is required.")
            elif not bodega_ids and not poly_ids: st.error("You must select at least one transaction.")
            else:
                database.create_or_update_position(position_name, bodega_ids, poly_ids)
                st.success(f"Successfully updated position: {position_name}"); st.rerun()