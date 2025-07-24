"""
Hybrid fuzzy matching and market fetchers for Bodega vs Polymarket.
This version returns matches/ignored without writing to CSV.
"""
import time
import requests
import logging
from rapidfuzz import fuzz
from typing import List, Tuple, Dict
from matching.keywords import FOCUS_KEYWORDS
from datetime import datetime

log = logging.getLogger(__name__)

# Configuration constants
THRESHOLD = 85  # minimum combined score to consider a match

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
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code == 429:
                    sleep_time = backoff_factor * (2 ** attempt)
                    log.warning(f"Rate limited. Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                    continue
                resp.raise_for_status()
                data = resp.json()
                results.extend(data.get("data", []))
                cursor = data.get("next_cursor")
                break # Success, break retry loop
            except requests.exceptions.RequestException as e:
                log.error(f"Request to {url} failed on attempt {attempt+1}: {e}")
                if attempt == max_retries - 1:
                    raise  # Re-raise the exception if all retries fail
                time.sleep(backoff_factor * (2 ** attempt))
        else: # This else belongs to the for loop, executes if loop finishes without break
             log.error("All retries failed for Polymarket fetch.")
             break

        if not cursor or cursor == "LTE=":
            break
        time.sleep(0.2)
    return [m for m in results if m.get("active") and not m.get("closed")]

#––– BODEGA FETCHER –––

def fetch_bodega_v3_active_markets(api_url: str) -> List[Dict]:
    """
    Retrieve active Bodega V3 markets via POST /getMarketConfigs.
    """
    active_api_markets = []
    try:
        url = f"{api_url}/getMarketConfigs"
        resp = requests.post(url, json={}, timeout=10)
        resp.raise_for_status()
        configs = resp.json().get("marketConfigs", [])
        
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        for m in configs:
            if m.get("status") != "Active":
                continue
            dl = m.get("deadline")
            if not dl or not str(dl).isdigit() or int(dl) < now_ms:
                continue
            active_api_markets.append({
                "id": m["id"],
                "name": m["name"],
                "deadline": dl,
                "options": m.get("options", [])
            })
    except requests.exceptions.RequestException as e:
        log.warning(f"Could not fetch live Bodega markets: {e}. Returning empty list.")

    log.info(f"Loaded {len(active_api_markets)} active markets from API.")
    return active_api_markets

#––– FULL AUTO-MATCH –––

def fuzzy_match_markets(
    bodega_markets: List[Dict],
    poly_markets: List[Dict]
) -> Tuple[List[Tuple[Dict, Dict, float]], int]:
    """
    Auto-match Bodega vs Polymarket markets.
    Returns two items:
      - matches: A list of [(bodega_market, poly_market, score), ...]
      - ignored_count: The total number of pairs that did not meet the threshold.
    """
    matches = []
    ignored_count = 0

    for b in bodega_markets:
        for p in poly_markets:
            ok, det = match_markets(b.get("name", ""), p.get("question", ""))
            if ok:
                matches.append((b, p, det["score"]))
            else:
                ignored_count += 1
    return matches, ignored_count