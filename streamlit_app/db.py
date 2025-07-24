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
        # --- Table Creation ---
        # Ensure all tables exist with their base schema.
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
        # Create the table WITHOUT the new column first, in case it's an old DB.
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

        # --- Schema Migration for manual_pairs table ---
        # This section makes the app backwards-compatible with older databases.
        cur.execute("PRAGMA table_info(manual_pairs)")
        columns = [row['name'] for row in cur.fetchall()]
        
        if 'is_flipped' not in columns:
            log.info("Running database migration: Adding 'is_flipped' column to 'manual_pairs' table...")
            # If the column doesn't exist, add it.
            # The NOT NULL DEFAULT 0 is crucial to set a value for existing rows.
            cur.execute("""
            ALTER TABLE manual_pairs
            ADD COLUMN is_flipped INTEGER NOT NULL DEFAULT 0
            """)
            log.info("Migration complete.")

        conn.commit()

def save_bodega_markets(markets: list):
    now = int(time.time())
    data = [(m["id"], m["name"], m["deadline"], now) for m in markets]
    with get_conn() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO bodega_markets
            (market_id, market_name, deadline, fetched_at)
            VALUES (?,?,?,?)
        """, data)
        conn.commit()

def save_polymarkets(markets: list):
    now = int(time.time())
    data = [(m["condition_id"], m["question"], now) for m in markets]
    with get_conn() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO polymarket_markets
            (condition_id, question, fetched_at)
            VALUES (?,?,?)
        """, data)
        conn.commit()

def load_bodega_markets() -> list:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM bodega_markets").fetchall()
        return [
            {
                "id": r["market_id"],
                "name": r["market_name"],
                "deadline": r["deadline"],
                "fetched_at": r["fetched_at"]
            } for r in rows
        ]

def load_polymarkets() -> list:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM polymarket_markets").fetchall()
        return [
            {
                "condition_id": r["condition_id"],
                "question": r["question"],
                "fetched_at": r["fetched_at"]
            } for r in rows
        ]

def load_new_bodega_markets() -> list[dict]:
    """Return all unprocessed Bodega markets."""
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM new_bodega_markets").fetchall()
        return [dict(r) for r in rows]

def add_new_bodega_market(m: dict):
    """Insert a newly seen market into the holding table."""
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO new_bodega_markets
            (market_id, market_name, deadline, first_seen)
            VALUES (?,?,?,?)
        """, (m["id"], m["name"], m["deadline"], int(time.time())))
        conn.commit()

def remove_new_bodega_market(market_id: str):
    """Delete from the holding table once processed."""
    with get_conn() as conn:
        conn.execute("DELETE FROM new_bodega_markets WHERE market_id=?", (market_id,))
        conn.commit()

def ignore_bodega_market(market_id: str):
    """Mark a holding‐table market as ignored and remove it."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO ignored_bodega_markets
            (market_id, ignored_at) VALUES (?,?)
        """, (market_id, int(time.time())))
        cur.execute("DELETE FROM new_bodega_markets WHERE market_id=?", (market_id,))
        conn.commit()

def save_manual_pair(bodega_id: str, poly_id: str, is_flipped: int = 0):
    """Saves or updates a manual pair, including its flipped status."""
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO manual_pairs (bodega_id, poly_condition_id, is_flipped)
            VALUES (?, ?, ?)
        """, (bodega_id, poly_id, is_flipped))
        conn.commit()

def load_manual_pairs() -> list[tuple]:
    """Loads manual pairs including their flipped status."""
    with get_conn() as conn:
        rows = conn.execute("SELECT bodega_id, poly_condition_id, is_flipped FROM manual_pairs").fetchall()
        return [(r["bodega_id"], r["poly_condition_id"], r["is_flipped"]) for r in rows]

def delete_manual_pair(bodega_id: str, poly_id: str):
    """Deletes a manual pair from the database."""
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM manual_pairs WHERE bodega_id = ? AND poly_condition_id = ?",
            (bodega_id, poly_id)
        )
        conn.commit()

def load_suggested_matches() -> list[dict]:
    """Return all unmatched fuzzy suggestions."""
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM suggested_matches").fetchall()
        return [dict(r) for r in rows]

def add_suggested_match(bodega_id: str, poly_id: str, score: float):
    """Insert a new fuzzy-match suggestion if not already present."""
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO suggested_matches
            (bodega_id, poly_id, score, first_suggested)
            VALUES (?,?,?,?)
        """, (bodega_id, poly_id, score, int(time.time())))
        conn.commit()

def remove_suggested_match(bodega_id: str, poly_id: str):
    """Remove a suggestion after approval or decline."""
    with get_conn() as conn:
        conn.execute("""
            DELETE FROM suggested_matches
            WHERE bodega_id=? AND poly_id=?
        """, (bodega_id, poly_id))
        conn.commit()