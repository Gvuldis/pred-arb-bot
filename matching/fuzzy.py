# matching/fuzzy.py

import requests
import csv
import time
import pandas as pd
from difflib import SequenceMatcher
from rapidfuzz.fuzz import token_sort_ratio
from datetime import datetime

now_ms = int(datetime.utcnow().timestamp() * 1000)

def fetch_all_polymarket_clob_markets():
    base_url = "https://clob.polymarket.com/markets"
    all_markets = []
    cursor = ""
    while True:
        url = base_url + (f"?next_cursor={cursor}" if cursor else "")
        resp = requests.get(url)
        resp.raise_for_status()
        result = resp.json()
        all_markets.extend(result['data'])
        cursor = result.get('next_cursor')
        if not cursor or cursor == "LTE=":
            break
    # Only those that are actually tradable
    return [m for m in all_markets if m.get("active") and not m.get("closed")]


def fetch_bodega_v3_active_markets():
    url = "https://testnet.bodegamarket.io/api/getMarketConfigs"
    r = requests.post(url, json={}); r.raise_for_status()
    data = r.json()["marketConfigs"]
    markets = []
    for m in data:
        if m["status"] != "Active":
            continue
        dl = m.get("deadline")
        if not dl or not str(dl).isdigit() or int(dl) < now_ms:
            continue
        markets.append({
            "id": m["id"],
            "name": m["name"],
            "deadline": m["deadline"],
            "options": m.get("options", []),
        })
    return markets

def fuzzy_match_markets(bodega_markets, poly_markets, min_similarity=75):
    matches = []
    used_poly = set()
    for b in bodega_markets:
        best_sim, best_p = 0, None
        for p in poly_markets:
            if p['condition_id'] in used_poly:
                continue
            sim = token_sort_ratio(b['name'], p['question'])
            if sim > best_sim:
                best_sim, best_p = sim, p
        if best_p and best_sim >= min_similarity:
            matches.append((b, best_p, best_sim/100))
            used_poly.add(best_p['condition_id'])
    return matches

def export_matches_to_csv(matches, filename="MATCHED_MARKETS.csv", ignorefile="IGNORED_MATCHES.csv"):
    # same as before...
    import pandas as pd
    new_pairs = set((b['id'], p['condition_id']) for b,p,_ in matches)
    try:
        existing = pd.read_csv(filename)
        existing_pairs = set(zip(existing.bodega_id, existing.poly_condition_id))
    except:
        existing_pairs = set()
    try:
        ignored = pd.read_csv(ignorefile)
        ignored_pairs = set(zip(ignored.bodega_id, ignored.poly_condition_id))
    except:
        ignored_pairs = set()
    to_add = new_pairs - ignored_pairs
    combined = existing_pairs | to_add
    import csv
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["bodega_id","poly_condition_id"])
        for b_id,p_id in sorted(combined):
            w.writerow([b_id,p_id])
