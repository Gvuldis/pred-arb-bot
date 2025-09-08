import sqlite3
from pathlib import Path
import time
from contextlib import contextmanager
import logging

log = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent.parent / "market_data.db"

@contextmanager
def get_conn():
    """Context manager for database connections."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        if conn:
            conn.close()

def init_db():
    """
    Initializes the database, creating tables if they don't exist
    and altering existing tables to add missing columns (migration).
    """
    with get_conn() as conn:
        cur = conn.cursor()
        # --- Bodega Tables ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bodega_markets (
          market_id    TEXT PRIMARY KEY,
          market_name  TEXT,
          deadline     INTEGER,
          fetched_at   INTEGER
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS manual_pairs (
          bodega_id          TEXT,
          poly_condition_id  TEXT,
          is_flipped INTEGER NOT NULL DEFAULT 0,
          profit_threshold_usd REAL NOT NULL DEFAULT 25.0,
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

        # --- Myriad Tables (NEW) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS myriad_markets (
          id         INTEGER PRIMARY KEY,
          slug       TEXT UNIQUE,
          name       TEXT,
          expires_at TEXT,
          fetched_at INTEGER
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS manual_pairs_myriad (
            myriad_slug        TEXT,
            poly_condition_id  TEXT,
            is_flipped         INTEGER NOT NULL DEFAULT 0,
            profit_threshold_usd REAL NOT NULL DEFAULT 25.0,
            PRIMARY KEY (myriad_slug, poly_condition_id)
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS new_myriad_markets (
            market_id     INTEGER PRIMARY KEY,
            market_slug   TEXT,
            market_name   TEXT,
            expires_at    TEXT,
            first_seen    INTEGER
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ignored_myriad_markets (
            market_id     INTEGER PRIMARY KEY,
            ignored_at    INTEGER
        )""")

        # --- Polymarket & Generic Tables ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS polymarket_markets (
          condition_id TEXT PRIMARY KEY,
          question     TEXT,
          fetched_at   INTEGER
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS probability_watches (
          bodega_id             TEXT PRIMARY KEY,
          description           TEXT,
          expected_probability  REAL NOT NULL,
          deviation_threshold   REAL NOT NULL,
          created_at            INTEGER
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS app_config (
          key    TEXT PRIMARY KEY,
          value  TEXT NOT NULL
        )""")

        # --- Schema Migration for manual_pairs table ---
        cur.execute("PRAGMA table_info(manual_pairs)")
        columns = [row['name'] for row in cur.fetchall()]
        if 'is_flipped' not in columns:
            log.info("Migration: Adding 'is_flipped' to 'manual_pairs'.")
            cur.execute("ALTER TABLE manual_pairs ADD COLUMN is_flipped INTEGER NOT NULL DEFAULT 0")
        if 'profit_threshold_usd' not in columns:
            log.info("Migration: Adding 'profit_threshold_usd' to 'manual_pairs'.")
            cur.execute("ALTER TABLE manual_pairs ADD COLUMN profit_threshold_usd REAL NOT NULL DEFAULT 25.0")
        if 'end_date_override' not in columns:
            log.info("Migration: Adding 'end_date_override' to 'manual_pairs'.")
            cur.execute("ALTER TABLE manual_pairs ADD COLUMN end_date_override INTEGER")

        # --- Schema Migration for manual_pairs_myriad table ---
        cur.execute("PRAGMA table_info(manual_pairs_myriad)")
        columns_myriad = [row['name'] for row in cur.fetchall()]
        if 'end_date_override' not in columns_myriad:
            log.info("Migration: Adding 'end_date_override' to 'manual_pairs_myriad'.")
            cur.execute("ALTER TABLE manual_pairs_myriad ADD COLUMN end_date_override INTEGER")

        conn.commit()
        log.info("Database initialization/migration check complete.")


# --- Bodega Functions ---
def save_bodega_markets(markets: list):
    now = int(time.time())
    data = [(m["id"], m["name"], m["deadline"], now) for m in markets]
    with get_conn() as conn:
        conn.executemany("INSERT OR REPLACE INTO bodega_markets (market_id, market_name, deadline, fetched_at) VALUES (?,?,?,?)", data)
        conn.commit()

def load_bodega_markets() -> list:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM bodega_markets").fetchall()
        return [{"id": r["market_id"], "name": r["market_name"], "deadline": r["deadline"], "fetched_at": r["fetched_at"]} for r in rows]

def load_new_bodega_markets() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM new_bodega_markets").fetchall()
        return [dict(r) for r in rows]

def add_new_bodega_market(m: dict):
    with get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO new_bodega_markets (market_id, market_name, deadline, first_seen) VALUES (?,?,?,?)", (m["id"], m["name"], m["deadline"], int(time.time())))
        conn.commit()

def remove_new_bodega_market(market_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM new_bodega_markets WHERE market_id=?", (market_id,))
        conn.commit()

def ignore_bodega_market(market_id: str):
    with get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO ignored_bodega_markets (market_id, ignored_at) VALUES (?,?)", (market_id, int(time.time())))
        conn.execute("DELETE FROM new_bodega_markets WHERE market_id=?", (market_id,))
        conn.commit()

# --- Myriad Functions (NEW) ---
def save_myriad_markets(markets: list):
    now = int(time.time())
    data = [(m.get("id"), m.get("slug"), m.get("title"), m.get("expires_at"), now) for m in markets]
    with get_conn() as conn:
        conn.executemany("INSERT OR REPLACE INTO myriad_markets (id, slug, name, expires_at, fetched_at) VALUES (?,?,?,?,?)", data)
        conn.commit()

def load_myriad_markets() -> list:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM myriad_markets").fetchall()
        return [dict(r) for r in rows]

def add_new_myriad_market(m: dict):
    with get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO new_myriad_markets (market_id, market_slug, market_name, expires_at, first_seen) VALUES (?,?,?,?,?)", (m["id"], m["slug"], m["name"], m["expires_at"], int(time.time())))
        conn.commit()

def load_new_myriad_markets() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM new_myriad_markets").fetchall()
        return [dict(r) for r in rows]

def remove_new_myriad_market(market_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM new_myriad_markets WHERE market_id=?", (market_id,))
        conn.commit()

def ignore_myriad_market(market_id: int):
    with get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO ignored_myriad_markets (market_id, ignored_at) VALUES (?,?)", (market_id, int(time.time())))
        conn.execute("DELETE FROM new_myriad_markets WHERE market_id=?", (market_id,))
        conn.commit()

# --- Polymarket Functions ---
def save_polymarkets(markets: list):
    now = int(time.time())
    data = [(m["condition_id"], m["question"], now) for m in markets]
    with get_conn() as conn:
        conn.executemany("INSERT OR REPLACE INTO polymarket_markets (condition_id, question, fetched_at) VALUES (?,?,?)", data)
        conn.commit()

def load_polymarkets() -> list:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM polymarket_markets").fetchall()
        return [{"condition_id": r["condition_id"], "question": r["question"], "fetched_at": r["fetched_at"]} for r in rows]

# --- Pairing Functions ---
def save_manual_pair(bodega_id: str, poly_id: str, is_flipped: int, profit_threshold_usd: float, end_date_override: int = None):
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO manual_pairs (bodega_id, poly_condition_id, is_flipped, profit_threshold_usd, end_date_override) VALUES (?, ?, ?, ?, ?)", (bodega_id, poly_id, is_flipped, profit_threshold_usd, end_date_override))
        conn.commit()

def load_manual_pairs() -> list[tuple]:
    with get_conn() as conn:
        rows = conn.execute("SELECT bodega_id, poly_condition_id, is_flipped, profit_threshold_usd, end_date_override FROM manual_pairs").fetchall()
        return [(r["bodega_id"], r["poly_condition_id"], r["is_flipped"], r["profit_threshold_usd"], r["end_date_override"]) for r in rows]

def delete_manual_pair(bodega_id: str, poly_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM manual_pairs WHERE bodega_id = ? AND poly_condition_id = ?", (bodega_id, poly_id))
        conn.commit()

def save_manual_pair_myriad(myriad_slug: str, poly_id: str, is_flipped: int, profit_threshold_usd: float, end_date_override: int = None):
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO manual_pairs_myriad (myriad_slug, poly_condition_id, is_flipped, profit_threshold_usd, end_date_override) VALUES (?, ?, ?, ?, ?)", (myriad_slug, poly_id, is_flipped, profit_threshold_usd, end_date_override))
        conn.commit()

def load_manual_pairs_myriad() -> list[tuple]:
    with get_conn() as conn:
        rows = conn.execute("SELECT myriad_slug, poly_condition_id, is_flipped, profit_threshold_usd, end_date_override FROM manual_pairs_myriad").fetchall()
        return [(r["myriad_slug"], r["poly_condition_id"], r["is_flipped"], r["profit_threshold_usd"], r["end_date_override"]) for r in rows]

def delete_manual_pair_myriad(myriad_slug: str, poly_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM manual_pairs_myriad WHERE myriad_slug = ? AND poly_condition_id = ?", (myriad_slug, poly_id))
        conn.commit()

# --- Other Functions ---
def save_probability_watch(bodega_id: str, description: str, expected_prob: float, deviation_threshold: float):
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO probability_watches (bodega_id, description, expected_probability, deviation_threshold, created_at) VALUES (?, ?, ?, ?, ?)", (bodega_id, description, expected_prob, deviation_threshold, int(time.time())))
        conn.commit()

def load_probability_watches() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM probability_watches ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]

def delete_probability_watch(bodega_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM probability_watches WHERE bodega_id = ?", (bodega_id,))
        conn.commit()

def set_config_value(key: str, value: str):
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)", (key, str(value)))
        conn.commit()
        log.info(f"Set config '{key}' to '{value}'")

def get_config_value(key: str, default: str = None) -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM app_config WHERE key = ?", (key,)).fetchone()
        return row['value'] if row else default
