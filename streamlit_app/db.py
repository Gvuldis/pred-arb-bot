# streamlit_app/db.py
import sqlite3
from pathlib import Path
import time
from contextlib import contextmanager
import logging
import json
from typing import Optional, Dict

log = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent.parent / "market_data.db"

@contextmanager
def get_conn():
    """Context manager for database connections."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10) # Added timeout
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

        # --- Myriad Tables ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS myriad_markets (
          id         INTEGER PRIMARY KEY,
          slug       TEXT UNIQUE,
          name       TEXT,
          expires_at TEXT,
          fee        REAL,
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
        cur.execute("""
        CREATE TABLE IF NOT EXISTS polymarket_trades_log (
            trade_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            market_id TEXT NOT NULL,
            matched_amount REAL NOT NULL,
            match_time INTEGER NOT NULL,
            full_response_json TEXT NOT NULL
        )""")

        # --- NEW TABLES FOR ARB-EXECUTOR ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS automated_trades_log (
            trade_id TEXT PRIMARY KEY,
            attempt_timestamp_utc TEXT NOT NULL,
            myriad_slug TEXT,
            polymarket_condition_id TEXT,
            status TEXT NOT NULL,
            status_message TEXT,
            planned_poly_shares REAL,
            planned_myriad_shares REAL,
            executed_poly_shares REAL,
            executed_poly_cost_usd REAL,
            executed_myriad_shares REAL,
            executed_myriad_cost_usd REAL,
            poly_tx_hash TEXT,
            myriad_tx_hash TEXT,
            final_profit_usd REAL,
            log_details TEXT,
            myriad_api_lookup_status TEXT DEFAULT 'PENDING'
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_cooldowns (
            market_key TEXT PRIMARY KEY,
            last_trade_attempt_utc TEXT NOT NULL
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS arb_opportunities (
            opportunity_id TEXT PRIMARY KEY,
            timestamp_utc TEXT NOT NULL,
            message_json TEXT NOT NULL
        )""")

        # --- Schema Migrations ---
        cur.execute("PRAGMA table_info(manual_pairs)")
        columns = [row['name'] for row in cur.fetchall()]
        if 'end_date_override' not in columns:
            log.info("Migration: Adding 'end_date_override' to 'manual_pairs'.")
            cur.execute("ALTER TABLE manual_pairs ADD COLUMN end_date_override INTEGER")

        cur.execute("PRAGMA table_info(manual_pairs_myriad)")
        columns_myriad = [row['name'] for row in cur.fetchall()]
        if 'end_date_override' not in columns_myriad:
            log.info("Migration: Adding 'end_date_override' to 'manual_pairs_myriad'.")
            cur.execute("ALTER TABLE manual_pairs_myriad ADD COLUMN end_date_override INTEGER")
        if 'is_autotrade_safe' not in columns_myriad:
            log.info("Migration: Adding 'is_autotrade_safe' to 'manual_pairs_myriad'.")
            cur.execute("ALTER TABLE manual_pairs_myriad ADD COLUMN is_autotrade_safe INTEGER NOT NULL DEFAULT 0")
        
        cur.execute("PRAGMA table_info(automated_trades_log)")
        columns_auto_trade = [row['name'] for row in cur.fetchall()]
        if 'executed_poly_cost_usd' not in columns_auto_trade:
            log.info("Migration: Adding 'executed_poly_cost_usd' to 'automated_trades_log'.")
            cur.execute("ALTER TABLE automated_trades_log ADD COLUMN executed_poly_cost_usd REAL")
        if 'executed_myriad_cost_usd' not in columns_auto_trade:
            log.info("Migration: Adding 'executed_myriad_cost_usd' to 'automated_trades_log'.")
            cur.execute("ALTER TABLE automated_trades_log ADD COLUMN executed_myriad_cost_usd REAL")
        if 'myriad_api_lookup_status' not in columns_auto_trade:
            log.info("Migration: Adding 'myriad_api_lookup_status' to 'automated_trades_log'.")
            cur.execute("ALTER TABLE automated_trades_log ADD COLUMN myriad_api_lookup_status TEXT DEFAULT 'PENDING'")
        
        cur.execute("PRAGMA table_info(myriad_markets)")
        columns_myriad_markets = [row['name'] for row in cur.fetchall()]
        if 'fee' not in columns_myriad_markets:
            log.info("Migration: Adding 'fee' to 'myriad_markets'.")
            cur.execute("ALTER TABLE myriad_markets ADD COLUMN fee REAL")

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

# --- Myriad Functions ---
def save_myriad_markets(markets: list):
    now = int(time.time())
    data = [(m.get("id"), m.get("slug"), m.get("title"), m.get("expires_at"), m.get("fee"), now) for m in markets]
    with get_conn() as conn:
        conn.executemany("INSERT OR REPLACE INTO myriad_markets (id, slug, name, expires_at, fee, fetched_at) VALUES (?,?,?,?,?,?)", data)
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

def save_poly_trades(trades: list):
    """Saves a list of Polymarket trades to the database, ignoring duplicates."""
    with get_conn() as conn:
        to_insert = []
        for trade in trades:
            trade_id = trade.get('id')
            if not trade_id:
                continue

            matched_amount = 0.0
            maker_orders = trade.get('maker_orders', [])
            for maker_order in maker_orders:
                try:
                    matched_amount += float(maker_order.get('matched_amount', '0'))
                except (ValueError, TypeError):
                    log.warning(f"Could not parse matched_amount in trade {trade_id}: {maker_order.get('matched_amount')}")

            record = (
                trade_id,
                trade.get('taker_order_id'),
                trade.get('market'),
                matched_amount,
                trade.get('match_time'),
                json.dumps(trade)
            )
            to_insert.append(record)
        
        if to_insert:
            conn.executemany("""
                INSERT OR IGNORE INTO polymarket_trades_log 
                (trade_id, order_id, market_id, matched_amount, match_time, full_response_json) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, to_insert)
            conn.commit()
            log.info(f"Saved/updated {len(to_insert)} Polymarket trades in the log.")

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

def save_manual_pair_myriad(myriad_slug: str, poly_id: str, is_flipped: int, profit_threshold_usd: float, end_date_override: Optional[int], is_autotrade_safe: int):
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO manual_pairs_myriad (myriad_slug, poly_condition_id, is_flipped, profit_threshold_usd, end_date_override, is_autotrade_safe) VALUES (?, ?, ?, ?, ?, ?)", (myriad_slug, poly_id, is_flipped, profit_threshold_usd, end_date_override, is_autotrade_safe))
        conn.commit()

def load_manual_pairs_myriad() -> list[tuple]:
    with get_conn() as conn:
        rows = conn.execute("SELECT myriad_slug, poly_condition_id, is_flipped, profit_threshold_usd, end_date_override, is_autotrade_safe FROM manual_pairs_myriad").fetchall()
        return [(r["myriad_slug"], r["poly_condition_id"], r["is_flipped"], r["profit_threshold_usd"], r["end_date_override"], r["is_autotrade_safe"]) for r in rows]

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

# --- Arb Executor Functions ---

def add_arb_opportunity(opportunity: Dict):
    """Adds a new arbitrage opportunity to the queue."""
    with get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO arb_opportunities (opportunity_id, timestamp_utc, message_json) VALUES (?, ?, ?)",
                     (opportunity['opportunity_id'], opportunity['timestamp_utc'], json.dumps(opportunity)))
        conn.commit()
        log.info(f"Queued arbitrage opportunity {opportunity['opportunity_id']} for {opportunity['market_identifiers']['myriad_slug']}")

def pop_arb_opportunity() -> Optional[Dict]:
    """Atomically retrieves and deletes the oldest opportunity from the queue."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            row = cur.execute("SELECT opportunity_id, message_json FROM arb_opportunities ORDER BY timestamp_utc ASC LIMIT 1").fetchone()
            if row:
                opp_id = row['opportunity_id']
                message_json = row['message_json']
                cur.execute("DELETE FROM arb_opportunities WHERE opportunity_id = ?", (opp_id,))
                conn.commit()
                log.info(f"Popped opportunity {opp_id} from queue.")
                return json.loads(message_json)
            else:
                conn.commit() # release lock
                return None
        except Exception as e:
            conn.rollback()
            log.error(f"Error popping opportunity from queue: {e}", exc_info=True)
            return None

def clear_arb_opportunities() -> int:
    """Deletes all entries from the arb_opportunities table and returns the count of deleted rows."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            count_row = cur.execute("SELECT COUNT(*) FROM arb_opportunities").fetchone()
            count = count_row[0] if count_row else 0
            
            if count > 0:
                cur.execute("DELETE FROM arb_opportunities")
                log.warning(f"Cleared {count} pending arbitrage opportunities from the queue.")
            
            conn.commit()
            return count
        except Exception as e:
            conn.rollback()
            log.error(f"Error clearing opportunity queue: {e}", exc_info=True)
            return 0

def log_trade_attempt(trade_log: Dict):
    """Inserts or replaces a record in the automated_trades_log."""
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO automated_trades_log (
                trade_id, attempt_timestamp_utc, myriad_slug, polymarket_condition_id,
                status, status_message, planned_poly_shares, planned_myriad_shares,
                executed_poly_shares, executed_poly_cost_usd, executed_myriad_shares,
                executed_myriad_cost_usd, poly_tx_hash, myriad_tx_hash, final_profit_usd,
                log_details, myriad_api_lookup_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade_log.get('trade_id'), trade_log.get('attempt_timestamp_utc'),
            trade_log.get('myriad_slug'), trade_log.get('polymarket_condition_id'),
            trade_log.get('status'), trade_log.get('status_message'),
            trade_log.get('planned_poly_shares'), trade_log.get('planned_myriad_shares'),
            trade_log.get('executed_poly_shares'), trade_log.get('executed_poly_cost_usd'),
            trade_log.get('executed_myriad_shares'), trade_log.get('executed_myriad_cost_usd'),
            trade_log.get('poly_tx_hash'), trade_log.get('myriad_tx_hash'),
            trade_log.get('final_profit_usd'), json.dumps(trade_log.get('log_details')),
            trade_log.get('myriad_api_lookup_status', 'PENDING')
        ))
        conn.commit()

def update_trade_log_myriad_details(trade_id: str, details: dict):
    """Updates a trade log with confirmed Myriad details after API lookup."""
    with get_conn() as conn:
        conn.execute("""
            UPDATE automated_trades_log
            SET executed_myriad_shares = ?,
                executed_myriad_cost_usd = ?,
                myriad_api_lookup_status = ?
            WHERE trade_id = ?
        """, (
            details.get('executed_myriad_shares'),
            details.get('executed_myriad_cost_usd'),
            details.get('myriad_api_lookup_status'),
            trade_id
        ))
        conn.commit()
        log.info(f"Updated Myriad trade details in DB for trade {trade_id}.")

def update_trade_log_myriad_status(trade_id: str, status: str):
    """Updates just the Myriad API lookup status for a trade log."""
    with get_conn() as conn:
        conn.execute("""
            UPDATE automated_trades_log
            SET myriad_api_lookup_status = ?
            WHERE trade_id = ?
        """, (status, trade_id))
        conn.commit()
        log.warning(f"Updated Myriad lookup status to '{status}' for trade {trade_id}.")

def get_market_cooldown(market_key: str) -> Optional[str]:
    """Gets the last trade attempt timestamp for a market."""
    with get_conn() as conn:
        row = conn.execute("SELECT last_trade_attempt_utc FROM market_cooldowns WHERE market_key = ?", (market_key,)).fetchone()
        return row['last_trade_attempt_utc'] if row else None

def update_market_cooldown(market_key: str, timestamp_utc: str):
    """Updates the cooldown timestamp for a market."""
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO market_cooldowns (market_key, last_trade_attempt_utc) VALUES (?, ?)", (market_key, timestamp_utc))
        conn.commit()

def get_all_traded_myriad_market_info() -> list:
    """
    Returns a list of dicts with unique market info for all markets
    that have been involved in an automated trade attempt.
    """
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT DISTINCT T2.id, T2.slug, T2.name
            FROM automated_trades_log AS T1
            JOIN myriad_markets AS T2 ON T1.myriad_slug = T2.slug
            WHERE T2.id IS NOT NULL
        """).fetchall()
        return [dict(r) for r in rows]

def get_active_matched_myriad_market_info() -> list:
    """
    Returns a list of dicts with unique market info for all markets
    that have been manually paired.
    """
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT DISTINCT T2.id, T2.slug, T2.name
            FROM manual_pairs_myriad AS T1
            JOIN myriad_markets AS T2 ON T1.myriad_slug = T2.slug
            WHERE T2.id IS NOT NULL
        """).fetchall()
        return [dict(r) for r in rows]

def clear_all_trade_logs() -> int:
    """Deletes all entries from the automated_trades_log table."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            count_row = cur.execute("SELECT COUNT(*) FROM automated_trades_log").fetchone()
            count = count_row[0] if count_row else 0
            
            if count > 0:
                cur.execute("DELETE FROM automated_trades_log")
                log.warning(f"Cleared {count} trade logs.")
            
            conn.commit()
            return count
        except Exception as e:
            conn.rollback()
            log.error(f"Error clearing trade logs: {e}", exc_info=True)
            return 0