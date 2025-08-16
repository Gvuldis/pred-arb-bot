# portfolio_analyzer/app.py
import streamlit as st
import pandas as pd
import logging
from datetime import datetime

import database
from clients import CoinGeckoClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

st.set_page_config(layout="wide", page_title="Portfolio Analyzer")

@st.cache_resource
def get_cg_client(): return CoinGeckoClient()

def portfolio_dashboard_page():
    st.header("📊 Portfolio Dashboard")
    st.markdown("A live, verifiable overview of your portfolio's state and performance.")

    cg_client = get_cg_client()
    live_ada_price = cg_client.get_live_ada_price()
    
    open_positions_raw = database.get_all_positions_with_transactions()
    
    # --- Pre-calculate metrics for all positions to use in summary and details ---
    open_positions = []
    total_worst_case_payout_from_positions = 0

    for pos_raw in open_positions_raw:
        pos = pos_raw.copy()
        
        # --- Detailed Per-Position Calculations ---
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
        
        payout_if_bodega_wins = (net_bodega_shares * live_ada_price * 0.98) # Applying 2% fee
        payout_if_poly_wins = net_poly_shares
        
        # Determine true best/worst cases by comparing outcomes
        best_case_payout = max(payout_if_bodega_wins, payout_if_poly_wins)
        worst_case_payout = min(payout_if_bodega_wins, payout_if_poly_wins)
        
        best_case_pnl = best_case_payout - cost_basis_usd
        worst_case_pnl = worst_case_payout - cost_basis_usd

        total_worst_case_payout_from_positions += worst_case_payout

        # Store all calculated metrics in the position dictionary to avoid recalculation
        pos['metrics'] = {
            'bodega_shares_bought': bodega_shares_bought,
            'bodega_avg_price': bodega_avg_price,
            'poly_shares_bought': poly_shares_bought,
            'poly_avg_price': poly_avg_price,
            'net_bodega_shares': net_bodega_shares,
            'net_poly_shares': net_poly_shares,
            'cost_basis_usd': cost_basis_usd,
            'payout_if_bodega_wins': payout_if_bodega_wins,
            'payout_if_poly_wins': payout_if_poly_wins,
            'best_case_payout': best_case_payout,
            'worst_case_payout': worst_case_payout,
            'best_case_pnl': best_case_pnl,
            'worst_case_pnl': worst_case_pnl,
        }
        open_positions.append(pos)
    
    # --- Overall Portfolio Summary ---
    st.sidebar.title("Portfolio Summary")
    current_cash_poly = st.sidebar.number_input("Current Polymarket Cash (USD)", value=19.0, key="current_poly_cash")
    current_cash_bodega = st.sidebar.number_input("Current Bodega Cash (ADA)", value=603.0, key="current_bodega_cash")
    
    total_cash_value_usd = current_cash_poly + (current_cash_bodega * live_ada_price)
    total_portfolio_value = total_cash_value_usd + total_worst_case_payout_from_positions
    
    st.sidebar.metric("Live ADA Price", f"${live_ada_price:,.2f}")
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Cash Balance:** `${current_cash_poly:,.2f}` (Poly) + `₳{current_cash_bodega:,.2f}` (Bodega)")
    st.sidebar.markdown(f"**Total Cash Value:** `${total_cash_value_usd:,.2f}`")
    st.sidebar.markdown(f"**Open Position Value (Worst Case):** `${total_worst_case_payout_from_positions:,.2f}`")
    st.sidebar.metric("Total Portfolio Value (USD)", f"${total_portfolio_value:,.2f}")

    # --- Position Display ---
    st.subheader("Open Positions")
    if not open_positions:
        st.info("No open positions found. Go to the 'Position Builder' to create one.")
    else:
        for pos in open_positions:
            m = pos['metrics'] # shorthand for metrics
            with st.expander(f"**{pos['name']}**"):
                # --- Display ---
                col1, col2, col3 = st.columns([2,2,1])
                with col1:
                    st.markdown(f"**Bodega Details**")
                    st.markdown(f"`{m['bodega_shares_bought']:,.2f}` shares bought @ `{m['bodega_avg_price']:.4f}` ADA avg.")
                    st.markdown(f"**Net Shares:** `{m['net_bodega_shares']:,.2f}`")
                with col2:
                    st.markdown(f"**Polymarket Details**")
                    st.markdown(f"`{m['poly_shares_bought']:,.2f}` shares bought @ `${m['poly_avg_price']:.4f}` avg.")
                    st.markdown(f"**Net Shares:** `{m['net_poly_shares']:,.2f}`")
                with col3:
                    with st.form(key=f"close_form_{pos['id']}"):
                        outcome = st.selectbox("Select Winning Side", ["Bodega Won", "Polymarket Won"], key=f"outcome_{pos['id']}")
                        if st.form_submit_button("Close Position"):
                            final_payout = m['payout_if_bodega_wins'] if outcome == "Bodega Won" else m['payout_if_poly_wins']
                            database.log_completed_trade(pos['id'], pos['name'], m['cost_basis_usd'], final_payout)
                            st.success(f"Position '{pos['name']}' closed and logged.")
                            st.rerun()
                
                st.markdown("---")
                scol1, scol2 = st.columns(2)
                with scol1:
                    st.metric("Best Case Payout (USD)", f"${m['best_case_payout']:,.2f}", help="The highest potential payout from either outcome.")
                    st.metric("Best Case PnL (USD)", f"${m['best_case_pnl']:,.2f}", help="Profit if the best-case scenario occurs.")
                with scol2:
                    st.metric("Worst Case Payout (USD)", f"${m['worst_case_payout']:,.2f}", help="The lowest potential payout from either outcome.")
                    st.metric("Worst Case PnL (USD)", f"${m['worst_case_pnl']:,.2f}", help="Profit if the worst-case scenario occurs.")

                if st.toggle("Show Transaction Log", key=f"log_toggle_{pos['id']}"):
                    tcol1, tcol2 = st.columns(2)
                    with tcol1:
                        st.markdown("**Bodega Trades**")
                        df_b = pd.DataFrame(pos['bodega_trades'])
                        st.dataframe(df_b[['timestamp', 'trade_type', 'token_amount', 'ada_amount']], hide_index=True)
                    with tcol2:
                        st.markdown("**Polymarket Trades**")
                        df_p = pd.DataFrame(pos['poly_trades'])
                        st.dataframe(df_p[['timestamp', 'action', 'token_amount', 'usdc_amount']], hide_index=True)

    st.subheader("Trade History (Closed Positions)")
    closed_trades = database.load_completed_trades()
    if not closed_trades:
        st.info("No closed trades have been logged yet.")
    else:
        for trade in closed_trades:
            trade_id = trade['id']
            pnl_correction = trade.get('pnl_correction_usd', 0)
            correction_reason = trade.get('correction_reason', '')
            total_pnl = trade['net_profit_usd'] + pnl_correction
            
            col1, col2 = st.columns([3,1])
            with col1:
                st.markdown(f"**{trade['position_name']}** | Closed: {datetime.fromtimestamp(trade['closed_date']).strftime('%Y-%m-%d')}")
                st.metric(f"Final PnL: ${total_pnl:,.2f}", f"Calculated: ${trade['net_profit_usd']:,.2f}, Correction: ${pnl_correction:,.2f}")
                if correction_reason:
                    st.caption(f"Reason: {correction_reason}")
            with col2:
                with st.form(key=f"correction_form_{trade_id}"):
                    correction = st.number_input("PnL Correction (USD)", value=pnl_correction, key=f"corr_{trade_id}")
                    reason = st.text_input("Reason", value=correction_reason, key=f"reason_{trade_id}")
                    if st.form_submit_button("Save Correction"):
                        database.update_pnl_correction(trade_id, correction, reason)
                        st.rerun()
            st.divider()

def position_builder_page():
    st.header("🛠️ Position Builder")
    st.markdown("Group raw transactions into a named position. If a name already exists, transactions will be added to it.")
    bodega_txs, poly_txs = database.get_unassigned_transactions()
    
    selected_bodega = pd.DataFrame()
    selected_poly = pd.DataFrame()

    with st.form("position_form"):
        position_name = st.text_input("Position Name", placeholder="e.g., Zohran 2025")
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
            if not position_name: st.error("Position Name is required.")
            elif selected_bodega.empty and selected_poly.empty: st.error("You must select at least one transaction.")
            else:
                database.create_or_update_position(position_name, selected_bodega['id'].tolist(), selected_poly['id'].tolist())
                st.success(f"Successfully updated position: {position_name}"); st.rerun()

pg = st.navigation([
    st.Page(portfolio_dashboard_page, title="Portfolio Dashboard", icon="📊"),
    st.Page(position_builder_page, title="Position Builder", icon="🛠️")
])
st.sidebar.title("Navigation")
pg.run()