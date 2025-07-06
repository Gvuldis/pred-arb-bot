# streamlit_app/main.py
import sys, pathlib
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
from matching.fuzzy import fuzzy_match_markets, export_matches_to_csv

# —–– CONFIG —–––––––––––––––––––––––––
BODEGA_API = "https://testnet.bodegamarket.io/api"
POLY_API   = "https://clob.polymarket.com"
COIN_API   = "https://api.coingecko.com/api/v3/simple/price?ids=cardano&vs_currencies=usd"
WEBHOOK    = "https://discord.com/api/webhooks/1255893289136160869/ZwX3Qo1JsF_fBD0kdmI8-xaEyvah9TnAV_R7dIHIKdBAwpEvj6VgmP3YcOa7j8zpyAPN"

CSV_DIR     = os.path.join(os.path.dirname(__file__), "..", "csv")
MATCHED_CSV = os.path.join(CSV_DIR, "MATCHED_MARKETS.csv")
IGNORED_CSV = os.path.join(CSV_DIR, "IGNORED_MATCHES.csv")

# —–– CLIENTS —––––––––––––––––––––––––
b_client = BodegaClient(BODEGA_API)
p_client = PolymarketClient(POLY_API)
fx_client = FXClient(COIN_API)
notifier = DiscordNotifier(WEBHOOK)

# —–– UTILS —–––––––––––––––––––––––––
@st.cache_data
def load_df(path):
    try: return pd.read_csv(path, dtype=str)
    except: return pd.DataFrame(columns=["bodega_id","poly_condition_id"])

def save_df(df, path):
    df.to_csv(path, index=False)
@st.cache_data(ttl=300)               # cache for 5 minutes
def get_all_bodegas() -> list:
    return b_client.fetch_markets()

@st.cache_data(ttl=300)
def get_all_polymarkets() -> list:
    return p_client.fetch_all_markets()
# —–– UI —––––––––––––––––––––––––––––
st.set_page_config(layout="wide")
st.title("🌉 Arb-Bot Dashboard")

# 1) Manual Matches
df_matched = load_df(MATCHED_CSV)
df_ignored = load_df(IGNORED_CSV)

st.subheader("➕ Add Manual Pair")
c1,c2,c3 = st.columns([4,4,2])
with c1: bid = st.text_input("Bodega ID")
with c2: pid = st.text_input("Polymarket ID")
with c3:
    if st.button("Add"):
        if bid and pid and not ((df_matched.bodega_id==bid)&(df_matched.poly_condition_id==pid)).any():
            df_matched = pd.concat([df_matched, pd.DataFrame([[bid,pid]],columns=df_matched.columns)])
            save_df(df_matched, MATCHED_CSV)
            st.success("Added!")

st.markdown("---")

# 2) Auto-match
with st.form("auto_match"):
    st.write("### Auto-Match Markets")
    threshold = st.slider("Min similarity", 50, 100, 75)
    go = st.form_submit_button("Run Auto-Match")
    if go:
        bodes = b_client.fetch_markets()
        polys = p_client.fetch_all_markets()
        new   = fuzzy_match_markets(bodes, polys, min_similarity=threshold)
        export_matches_to_csv(new, MATCHED_CSV, IGNORED_CSV)
        st.success("Saved CSV with matches ✅")

st.markdown("---")

# 3) Run Arb-check
st.subheader("🚀 Check Arbitrage")
fees = {"bodega":0.02,"polymarket":0.01}
thresholds = {"min_profit":0.01,"roi":0.015}
ada_usd = fx_client.get_ada_usd()

# fetch live markets
bodes = b_client.fetch_markets()
polys = {m["condition_id"]:m for m in p_client.fetch_all_markets()}

results = []
for _,row in df_matched.iterrows():
    bmk = next((m for m in bodes if m["id"]==row.bodega_id), None)
    pmk = p_client.fetch_market(row.poly_condition_id)
    if not bmk or not pmk: continue

    # normalize
    norm = {
        "bodega": {
            "yes_usd": b_client.fetch_prices(bmk["id"])["yesPrice_ada"] * ada_usd,
            "no_usd":  b_client.fetch_prices(bmk["id"])["noPrice_ada"]  * ada_usd,
            "payout": ada_usd
        },
        "polymarket": {
            "yes_usd": pmk["best_yes_ask"],
            "no_usd":  pmk["best_no_ask"],
            "payout": 1.0
        }
    }

    # two sides
    for side in [("YES/no","bodega","polymarket"),("no/YES","polymarket","bodega")]:
        buy, sell = side[1], side[2]
        cost = norm[buy]["yes_usd"]*(1+fees[buy]) + norm[sell]["no_usd"]*(1+fees[sell])
        profit = max(norm[buy]["payout"],norm[sell]["payout"]) - cost
        roi    = (profit/cost) if cost else 0
        if profit>thresholds["min_profit"] and roi>thresholds["roi"]:
            results.append({
                "pair": f"{bmk['name']} ↔ {pmk['question']}",
                "side": side[0],
                "profit": round(profit,4),
                "roi": f"{roi*100:.2f}%"
            })

df = pd.DataFrame(results)
if df.empty:
    st.info("No arb found right now.")
else:
    st.table(df)
