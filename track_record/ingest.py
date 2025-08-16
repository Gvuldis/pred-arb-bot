import csv
import logging
import io
import concurrent.futures
from typing import TextIO

from . import database
from .clients import CardanoClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

PAIRING_WINDOW_SECONDS = 600 # 10 minutes

def ingest_bodega_data(cardano_client: CardanoClient, address: str):
    log.info("Starting Bodega data ingestion and trade pairing...")
    atomic_events = cardano_client.get_atomic_events(address)
    processed_hashes, trade_count = set(), 0
    
    for i, event in enumerate(atomic_events):
        if event['tx_hash'] in processed_hashes:
            continue
        
        market_name, timestamp = event['market_name'], event['timestamp']
        
        # --- Pair BUYs ---
        if "Buy Position" in event['message']:
            for next_event in atomic_events[i+1:]:
                if "Process Trade Positions" in next_event['message'] and \
                   next_event['market_name'] == market_name and \
                   abs(next_event['timestamp'] - timestamp) < PAIRING_WINDOW_SECONDS:
                    
                    ada_cost = abs(event['ada_change'])
                    for unit, qty in next_event['token_changes'].items():
                        if qty > 0:
                            database.add_logical_bodega_trade(
                                market_name, 'BUY', timestamp, ada_cost, unit, qty, 
                                event['tx_hash'], next_event['tx_hash']
                            )
                            trade_count += 1
                            processed_hashes.add(event['tx_hash'])
                            processed_hashes.add(next_event['tx_hash'])
                            log.info(f"Paired BUY for: {market_name}")
                            break # Found the token, move to next event
                    break # Found the paired process event, move on
        
        # --- Pair SELLs ---
        elif "Sell Position" in event['message']:
            for next_event in atomic_events[i+1:]:
                if "Process Trade Positions" in next_event['message'] and \
                   next_event['market_name'] == market_name and \
                   abs(next_event['timestamp'] - timestamp) < PAIRING_WINDOW_SECONDS:
                    
                    ada_gain = next_event['ada_change']
                    for unit, qty in event['token_changes'].items():
                        if qty < 0:
                            database.add_logical_bodega_trade(
                                market_name, 'SELL', timestamp, ada_gain, unit, abs(qty),
                                event['tx_hash'], next_event['tx_hash']
                            )
                            trade_count += 1
                            processed_hashes.add(event['tx_hash'])
                            processed_hashes.add(next_event['tx_hash'])
                            log.info(f"Paired SELL for: {market_name}")
                            break
                    break

        # --- Pair REDEEMs ---
        elif "Reward Position" in event['message']:
            for next_event in atomic_events[i+1:]:
                if "Process Reward Positions" in next_event['message'] and \
                   next_event['market_name'] == market_name and \
                   abs(next_event['timestamp'] - timestamp) < PAIRING_WINDOW_SECONDS:
                    
                    ada_gain = next_event['ada_change']
                    for unit, qty in event['token_changes'].items():
                        if qty < 0:
                            database.add_logical_bodega_trade(
                                market_name, 'REDEEM', timestamp, ada_gain, unit, abs(qty),
                                event['tx_hash'], next_event['tx_hash']
                            )
                            trade_count += 1
                            processed_hashes.add(event['tx_hash'])
                            processed_hashes.add(next_event['tx_hash'])
                            log.info(f"Paired REDEEM for: {market_name}")
                            break
                    break
    
    log.info(f"Ingestion scan complete. Found {trade_count} new logical Bodega trades.")
    return trade_count

def ingest_polymarket_data(file_obj: TextIO):
    log.info(f"Starting Polymarket data ingestion from file...")
    count = 0
    try:
        # Use DictReader directly on the file-like object
        reader = csv.DictReader(file_obj)
        for row in reader:
            action = row.get("action")
            tx_hash = row.get("hash")
            # Ensure hash exists to avoid adding rows without a unique key
            if action in ["Buy", "Sell", "Redeem", "Lost"] and tx_hash:
                database.add_raw_polymarket_tx(
                    tx_hash=tx_hash,
                    timestamp=int(row.get("timestamp")),
                    market_name=row.get("marketName"),
                    action=action,
                    token_name=row.get("tokenName") or action.upper(),
                    usdc_amount=float(row.get("usdcAmount")),
                    token_amount=float(row.get("tokenAmount"))
                )
                count += 1
    except Exception as e:
        log.error(f"Failed to parse Polymarket CSV: {e}", exc_info=True)
        raise e
    log.info(f"Ingestion scan complete. Found {count} new Polymarket transactions.")
    return count

def run_ingestion_in_parallel(polymarket_csv_file: TextIO, cardano_address: str, blockfrost_key: str):
    """
    Runs both Polymarket and Bodega ingestion processes concurrently.
    Returns a summary of the results.
    """
    results = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit Polymarket ingestion to the thread pool
        poly_future = executor.submit(ingest_polymarket_data, polymarket_csv_file)
        
        # Submit Bodega ingestion to the thread pool
        cardano_client = CardanoClient(project_id=blockfrost_key)
        bodega_future = executor.submit(ingest_bodega_data, cardano_client, cardano_address)

        try:
            poly_count = poly_future.result()
            results['polymarket'] = f"Success: Ingested {poly_count} transactions."
        except Exception as e:
            results['polymarket'] = f"Error: {e}"

        try:
            bodega_count = bodega_future.result()
            results['bodega'] = f"Success: Ingested {bodega_count} trades."
        except Exception as e:
            results['bodega'] = f"Error: {e}"
            
    return results