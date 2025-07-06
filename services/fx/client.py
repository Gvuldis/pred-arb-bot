# services/fx/client.py

import requests

class FXClient:
    def __init__(self, coingecko_url:str):
        self.url = coingecko_url

    def get_ada_usd(self) -> float:
        # stubbed to constant if you prefer
        try:
            r = requests.get(self.url); r.raise_for_status()
            return float(r.json()['cardano']['usd'])
        except:
            return 0.6
