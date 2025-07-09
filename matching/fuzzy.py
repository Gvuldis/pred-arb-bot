# matching/fuzzy.py

"""
Hybrid fuzzy matching and market fetchers for Bodega vs Polymarket.
This version returns matches/ignored without writing to CSV.
"""
import time
import requests
from rapidfuzz import fuzz
from typing import List, Tuple, Dict
from matching.keywords import FOCUS_KEYWORDS
from datetime import datetime

# Configuration constants
THRESHOLD = 85  # minimum combined score to consider a match
now_ms = int(datetime.utcnow().timestamp() * 1000)

#––– FOCUS TERM EXTRACTION –––

def extract_focus_terms(text: str) -> List[str]:
    """
    Return any focus keyword that appears (case-insensitive) in the text.
    """
    text_low = text.lower()
    return [kw for kw in FOCUS_KEYWORDS if kw.lower() in text_low]

#––– HYBRID MATCHING –––

def match_markets(
    bodega_name: str,
    poly_name: str,
    keyword_bonus: int = 20
) -> Tuple[bool, Dict[str, float]]:
    """
    Compare two market titles, applying keyword bonus and fuzzy similarity.
    Returns (matched, details).
    """
    # 1) Keyword bonus
    b_terms = extract_focus_terms(bodega_name)
    p_terms = extract_focus_terms(poly_name)
    shared = list(set(b_terms) & set(p_terms))
    bonus = keyword_bonus if shared else 0

    # 2) Fuzzy scores
    scr_partial = fuzz.partial_ratio(bodega_name, poly_name)
    scr_token = fuzz.token_sort_ratio(bodega_name, poly_name)
    score = max(scr_partial, scr_token) + bonus

    # 3) Threshold check
    matched = score >= THRESHOLD
    details = {
        "score": float(score),
        "partial_ratio": float(scr_partial),
        "token_sort_ratio": float(scr_token),
        "keyword_bonus": bonus,
        "shared_terms": shared
    }
    return matched, details

#––– POLYMARKET FETCHER –––

def fetch_all_polymarket_clob_markets(
    max_retries: int = 4,
    backoff_factor: float = 0.5
) -> List[Dict]:
    """
    Retrieve all active Polymarket CLOB markets with retry/backoff.
    """
    base_url = "https://clob.polymarket.com/markets"
    results = []
    cursor = ""

    while True:
        url = base_url + (f"?next_cursor={cursor}" if cursor else "")
        for attempt in range(max_retries):
            resp = requests.get(url)
            if resp.status_code == 429:
                time.sleep(backoff_factor * (2 ** attempt))
                continue
            resp.raise_for_status()
            break
        else:
            break
        data = resp.json().get("data", [])
        results.extend(data)
        cursor = resp.json().get("next_cursor")
        if not cursor or cursor == "LTE=":
            break
        time.sleep(0.2)
    return [m for m in results if m.get("active") and not m.get("closed")]

#––– BODEGA FETCHER –––

def fetch_bodega_v3_active_markets(api_url: str) -> List[Dict]:
    """
    Retrieve active Bodega V3 markets via POST /getMarketConfigs.
    """
    url = f"{api_url}/getMarketConfigs"
    resp = requests.post(url, json={})
    resp.raise_for_status()
    configs = resp.json().get("marketConfigs", [])

    active = []
    for m in configs:
        if m.get("status") != "Active":
            continue
        dl = m.get("deadline")
        if not dl or not str(dl).isdigit() or int(dl) < now_ms:
            continue
        active.append({
            "id": m["id"],
            "name": m["name"],
            "deadline": dl,
            "options": m.get("options", [])
        })
    return active

#––– FULL AUTO-MATCH –––

def fuzzy_match_markets(
    bodega_markets: List[Dict],
    poly_markets: List[Dict]
) -> Tuple[List[Tuple[Dict, Dict, float]], List[Tuple[str, str, str]]]:
    """
    Auto-match Bodega vs Polymarket markets.
    Returns two lists:
      - matches: [(bodega_market, poly_market, score), ...]
      - ignored: [(bodega_id, poly_condition_id, reason), ...]
    """
    matches, ignored = [], []

    for b in bodega_markets:
        for p in poly_markets:
            ok, det = match_markets(b.get("name", ""), p.get("question", ""))
            if ok:
                matches.append((b, p, det["score"]))
            else:
                ignored.append((b.get("id"), p.get("condition_id"), "score<threshold"))

    return matches, ignored
