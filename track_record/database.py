# portfolio_analyzer/database.py
import sqlite3
from contextlib import contextmanager
import logging
from datetime import datetime

log = logging.getLogger(__name__)
DB_PATH = "track_record/portfolio.db"

@contextmanager
def get_db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_db_conn() as conn:
        log.info("Initializing database...")
        cursor = conn.cursor()
        
        # --- Polymarket Transactions Table ---
        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_polymarket_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, tx_hash TEXT NOT NULL, timestamp INTEGER NOT NULL,
            market_name TEXT NOT NULL, action TEXT NOT NULL, token_name TEXT NOT NULL,
            usdc_amount REAL NOT NULL, token_amount REAL NOT NULL, status TEXT DEFAULT 'unassigned'
        )""")
        # Add a UNIQUE index to prevent duplicate transaction hashes.
        # This is the key to making polymarket ingestion idempotent.
        try:
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_polymarket_tx_hash ON raw_polymarket_transactions (tx_hash)")
            log.info("Unique index on raw_polymarket_transactions.tx_hash ensured.")
        except sqlite3.OperationalError as e:
            log.error(f"Could not create unique index on polymarket transactions, possibly due to existing duplicates: {e}")
            raise e

        # --- Bodega Trades Table ---
        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS bodega_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, market_name TEXT NOT NULL, trade_type TEXT NOT NULL,
            timestamp INTEGER NOT NULL, ada_amount REAL NOT NULL, token_name TEXT NOT NULL,
            token_amount INTEGER NOT NULL, status TEXT DEFAULT 'unassigned'
        )""")

        # --- Migration for bodega_trades ---
        # Add columns for transaction hashes if they don't exist
        cursor.execute("PRAGMA table_info(bodega_trades)")
        columns = [row['name'] for row in cursor.fetchall()]
        if 'tx_hash_1' not in columns:
            log.info("Migrating bodega_trades: adding tx_hash_1 column...")
            cursor.execute("ALTER TABLE bodega_trades ADD COLUMN tx_hash_1 TEXT")
        if 'tx_hash_2' not in columns:
            log.info("Migrating bodega_trades: adding tx_hash_2 column...")
            cursor.execute("ALTER TABLE bodega_trades ADD COLUMN tx_hash_2 TEXT")
        
        # Add a composite UNIQUE index on the two hashes to prevent duplicate logical trades
        try:
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_bodega_trades_tx_hashes ON bodega_trades (tx_hash_1, tx_hash_2)")
            log.info("Unique index on bodega_trades(tx_hash_1, tx_hash_2) ensured.")
        except sqlite3.OperationalError as e:
            log.error(f"Could not create unique index on bodega trades, possibly due to existing duplicates: {e}")
            raise e

        # --- Other Tables ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL
        )""")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS position_links (
            position_id INTEGER, trade_id INTEGER, trade_type TEXT,
            PRIMARY KEY (trade_id, trade_type),
            FOREIGN KEY (position_id) REFERENCES positions (id) ON DELETE CASCADE
        )""")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS completed_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, position_name TEXT NOT NULL,
            closed_date INTEGER NOT NULL, cost_basis_usd REAL NOT NULL,
            final_payout_usd REAL NOT NULL, net_profit_usd REAL NOT NULL,
            pnl_correction_usd REAL DEFAULT 0,
            correction_reason TEXT DEFAULT ''
        )""")
        conn.commit()
        log.info("Database initialized successfully.")

def add_logical_bodega_trade(market_name, trade_type, timestamp, ada_amount, token_name, token_amount, tx_hash_1, tx_hash_2):
    with get_db_conn() as conn:
        # INSERT OR IGNORE will silently fail if the (tx_hash_1, tx_hash_2) pair already exists
        conn.execute("""
            INSERT OR IGNORE INTO bodega_trades 
            (market_name, trade_type, timestamp, ada_amount, token_name, token_amount, tx_hash_1, tx_hash_2) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (market_name, trade_type, timestamp, ada_amount, token_name, token_amount, tx_hash_1, tx_hash_2))
        conn.commit()

def add_raw_polymarket_tx(tx_hash, timestamp, market_name, action, token_name, usdc_amount, token_amount):
    with get_db_conn() as conn:
        # INSERT OR IGNORE will silently fail if the tx_hash already exists
        conn.execute("""
            INSERT OR IGNORE INTO raw_polymarket_transactions 
            (tx_hash, timestamp, market_name, action, token_name, usdc_amount, token_amount) 
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (tx_hash, timestamp, market_name, action, token_name, usdc_amount, token_amount))
        conn.commit()

def get_unassigned_transactions():
    with get_db_conn() as conn:
        bodega_txs = conn.execute("SELECT * FROM bodega_trades WHERE status = 'unassigned'").fetchall()
        poly_txs = conn.execute("SELECT * FROM raw_polymarket_transactions WHERE status = 'unassigned'").fetchall()
        return [dict(row) for row in bodega_txs], [dict(row) for row in poly_txs]

def create_or_update_position(name, bodega_trade_ids, poly_tx_ids):
    with get_db_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM positions WHERE name = ?", (name,))
        existing_pos = cursor.fetchone()
        if existing_pos:
            position_id = existing_pos['id']
            log.info(f"Adding transactions to existing position: '{name}' (ID: {position_id})")
        else:
            cursor.execute("INSERT INTO positions (name) VALUES (?)", (name,))
            position_id = cursor.lastrowid
            log.info(f"Created new position: '{name}' (ID: {position_id})")

        for trade_id in bodega_trade_ids:
            cursor.execute("INSERT OR IGNORE INTO position_links (position_id, trade_id, trade_type) VALUES (?, ?, 'bodega')", (position_id, trade_id))
            cursor.execute("UPDATE bodega_trades SET status = 'assigned' WHERE id = ?", (trade_id,))
        for tx_id in poly_tx_ids:
            cursor.execute("INSERT OR IGNORE INTO position_links (position_id, trade_id, trade_type) VALUES (?, ?, 'poly')", (position_id, tx_id))
            cursor.execute("UPDATE raw_polymarket_transactions SET status = 'assigned' WHERE id = ?", (tx_id,))
        conn.commit()

def get_all_positions_with_transactions():
    with get_db_conn() as conn:
        positions = conn.execute("SELECT * FROM positions").fetchall()
        portfolio = []
        for pos in positions:
            pos_dict = dict(pos)
            bodega_links = conn.execute("SELECT b.* FROM bodega_trades b JOIN position_links l ON b.id = l.trade_id WHERE l.position_id = ? AND l.trade_type = 'bodega'", (pos['id'],)).fetchall()
            poly_links = conn.execute("SELECT p.* FROM raw_polymarket_transactions p JOIN position_links l ON p.id = l.trade_id WHERE l.position_id = ? AND l.trade_type = 'poly'", (pos['id'],)).fetchall()
            pos_dict['bodega_trades'] = [dict(row) for row in bodega_links]
            pos_dict['poly_trades'] = [dict(row) for row in poly_links]
            portfolio.append(pos_dict)
        return portfolio

def log_completed_trade(position_id: int, position_name: str, cost_basis: float, final_payout: float):
    with get_db_conn() as conn:
        net_profit = final_payout - cost_basis
        conn.execute("INSERT INTO completed_trades (position_name, closed_date, cost_basis_usd, final_payout_usd, net_profit_usd) VALUES (?, ?, ?, ?, ?)",
                     (position_name, int(datetime.now().timestamp()), cost_basis, final_payout, net_profit))
        conn.execute("DELETE FROM positions WHERE id = ?", (position_id,))
        conn.commit()
        log.info(f"Moved position '{position_name}' to completed trades.")

def load_completed_trades():
    with get_db_conn() as conn:
        return [dict(row) for row in conn.execute("SELECT * FROM completed_trades ORDER BY closed_date DESC").fetchall()]

def update_pnl_correction(trade_id: int, correction_usd: float, reason: str):
    with get_db_conn() as conn:
        conn.execute("UPDATE completed_trades SET pnl_correction_usd = ?, correction_reason = ? WHERE id = ?",
                     (correction_usd, reason, trade_id))
        conn.commit()
        log.info(f"Updated PnL correction for trade ID {trade_id}.")