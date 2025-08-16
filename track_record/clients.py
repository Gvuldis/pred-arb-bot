# portfolio_analyzer/clients.py
import requests
import logging
from blockfrost import BlockFrostApi, ApiError
from datetime import datetime

log = logging.getLogger(__name__)

class CoinGeckoClient:
    def get_live_ada_price(self):
        try:
            url = "https://api.coingecko.com/api/v3/simple/price?ids=cardano&vs_currencies=usd"
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()['cardano']['usd']
        except Exception as e:
            log.error(f"Could not fetch live ADA price: {e}")
            return 0.0

class CardanoClient:
    def __init__(self, project_id: str):
        self.api = BlockFrostApi(project_id=project_id)

    def get_atomic_events(self, address: str):
        log.info(f"Fetching all atomic on-chain events for address {address}...")
        atomic_events = []
        try:
            tx_hashes = self.api.address_transactions(address, gather_pages=True)
            for tx_info in tx_hashes:
                try:
                    tx_hash = tx_info.tx_hash
                    tx_meta = self.api.transaction_metadata(tx_hash, return_type='json')
                    tx_utxos = self.api.transaction_utxos(tx_hash)
                    tx_details = self.api.transaction(tx_hash)
                    message, market_name = "", ""
                    for meta in tx_meta:
                        if meta.get('label') == '674' and 'msg' in meta.get('json_metadata', {}):
                            msg_parts = meta['json_metadata']['msg']
                            if "Bodega Market" in msg_parts[0]:
                                message, market_name = msg_parts[0], msg_parts[1] if len(msg_parts) > 1 else ""
                                break
                    if not message: continue
                    ada_change, token_changes = 0, {}
                    for i in tx_utxos.inputs:
                        if i.address == address:
                            for asset in i.amount:
                                if asset.unit == 'lovelace': ada_change -= int(asset.quantity)
                                else: token_changes[asset.unit] = token_changes.get(asset.unit, 0) - int(asset.quantity)
                    for o in tx_utxos.outputs:
                        if o.address == address:
                            for asset in o.amount:
                                if asset.unit == 'lovelace': ada_change += int(asset.quantity)
                                else: token_changes[asset.unit] = token_changes.get(asset.unit, 0) + int(asset.quantity)
                    atomic_events.append({
                        "tx_hash": tx_hash, "timestamp": tx_details.block_time, "message": message,
                        "market_name": market_name, "ada_change": ada_change / 1_000_000, "token_changes": token_changes
                    })
                except ApiError as e:
                    if e.status_code != 404: log.warning(f"API Error processing tx {tx_info.tx_hash}: {e}")
                except Exception as e:
                    log.error(f"Unexpected error processing tx {tx_info.tx_hash}: {e}")
        except ApiError as e:
            log.error(f"Failed to fetch transactions for address {address}: {e}")
        log.info(f"Found {len(atomic_events)} potentially relevant on-chain events.")
        return sorted(atomic_events, key=lambda x: x['timestamp'])