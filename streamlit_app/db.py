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

