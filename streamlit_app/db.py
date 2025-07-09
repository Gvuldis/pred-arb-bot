import sqlite3
from pathlib import Path
import time

DB_PATH = Path(__file__).parent.parent / "market_data.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS bodega_markets (
      market_id    TEXT PRIMARY KEY,
      market_name  TEXT,
      deadline     INTEGER,
      fetched_at   INTEGER
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS polymarket_markets (
      condition_id TEXT PRIMARY KEY,
      question     TEXT,
      fetched_at   INTEGER
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS manual_pairs (
      bodega_id          TEXT,
      poly_condition_id  TEXT,
      PRIMARY KEY (bodega_id, poly_condition_id)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS new_bodega_markets (
      market_id    TEXT PRIMARY KEY,
      market_name  TEXT,
      deadline     INTEGER,
      first_seen   INTEGER
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ignored_bodega_markets (
      market_id    TEXT PRIMARY KEY,
      ignored_at   INTEGER
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS suggested_matches (
      bodega_id          TEXT,
      poly_id            TEXT,
      score              REAL,
      first_suggested    INTEGER,
      PRIMARY KEY (bodega_id, poly_id)
    )""")
    conn.commit()
    conn.close()



def save_bodega_markets(markets: list):
    now = int(time.time())
    conn = get_conn(); cur = conn.cursor()
    for m in markets:
        cur.execute("""
            INSERT OR REPLACE INTO bodega_markets
            (market_id, market_name, deadline, fetched_at)
            VALUES (?,?,?,?)
        """, (m["id"], m["name"], m["deadline"], now))
    conn.commit(); conn.close()

def save_polymarkets(markets: list):
    now = int(time.time())
    conn = get_conn(); cur = conn.cursor()
    for m in markets:
        cur.execute("""
            INSERT OR REPLACE INTO polymarket_markets
            (condition_id, question, fetched_at)
            VALUES (?,?,?)
        """, (m["condition_id"], m["question"], now))
    conn.commit(); conn.close()

def load_bodega_markets() -> list:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM bodega_markets")
    rows = cur.fetchall(); conn.close()
    return [dict(r) for r in rows]

def load_polymarkets() -> list:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT * FROM polymarket_markets")
    rows = cur.fetchall(); conn.close()
    return [dict(r) for r in rows]

def load_new_bodega_markets() -> list[dict]:
    """Return all unprocessed Bodega markets."""
    conn, cur = get_conn(), get_conn().cursor()
    cur = conn.cursor()
    cur.execute("SELECT * FROM new_bodega_markets")
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def add_new_bodega_market(m: dict):
    """Insert a newly seen market into the holding table."""
    conn, cur = get_conn(), get_conn().cursor()
    cur.execute("""
      INSERT OR IGNORE INTO new_bodega_markets
      (market_id, market_name, deadline, first_seen)
      VALUES (?,?,?,?)
    """, (m["id"], m["name"], m["deadline"], int(time.time())))
    conn.commit()
    conn.close()

def remove_new_bodega_market(market_id: str):
    """Delete from the holding table once processed."""
    conn, cur = get_conn(), get_conn().cursor()
    cur.execute("DELETE FROM new_bodega_markets WHERE market_id=?", (market_id,))
    conn.commit()
    conn.close()

def ignore_bodega_market(market_id: str):
    """Mark a holding‐table market as ignored and remove it."""
    conn, cur = get_conn(), get_conn().cursor()
    cur.execute("""
      INSERT OR IGNORE INTO ignored_bodega_markets
      (market_id, ignored_at) VALUES (?,?)
    """, (market_id, int(time.time())))
    cur.execute("DELETE FROM new_bodega_markets WHERE market_id=?", (market_id,))
    conn.commit()
    conn.close()
def save_manual_pair(bodega_id: str, poly_id: str):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
      INSERT OR IGNORE INTO manual_pairs (bodega_id, poly_condition_id)
      VALUES (?, ?)
    """, (bodega_id, poly_id))
    conn.commit(); conn.close()

def load_manual_pairs() -> list[tuple]:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT bodega_id, poly_condition_id FROM manual_pairs")
    rows = cur.fetchall(); conn.close()
    return [(r["bodega_id"], r["poly_condition_id"]) for r in rows]
def load_suggested_matches() -> list[dict]:
    """Return all unmatched fuzzy suggestions."""
    conn, cur = get_conn(), get_conn().cursor()
    cur.execute("SELECT * FROM suggested_matches")
    rows = cur.fetchall(); conn.close()
    return [dict(r) for r in rows]

def add_suggested_match(bodega_id: str, poly_id: str, score: float):
    """Insert a new fuzzy-match suggestion if not already present."""
    conn, cur = get_conn(), get_conn().cursor()
    cur.execute("""
      INSERT OR IGNORE INTO suggested_matches
      (bodega_id, poly_id, score, first_suggested)
      VALUES (?,?,?,?)
    """, (bodega_id, poly_id, score, int(time.time())))
    conn.commit(); conn.close()

def remove_suggested_match(bodega_id: str, poly_id: str):
    """Remove a suggestion after approval or decline."""
    conn, cur = get_conn(), get_conn().cursor()
    cur.execute("""
      DELETE FROM suggested_matches
      WHERE bodega_id=? AND poly_id=?
    """, (bodega_id, poly_id))
    conn.commit(); conn.close()

