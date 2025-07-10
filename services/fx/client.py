import requests
import logging

log = logging.getLogger(__name__)

class FXClient:
    def __init__(self, coingecko_url:str):
        self.url = coingecko_url
        self.fallback_price = 0.60

    def get_ada_usd(self) -> float:
        """
        Fetches the current ADA to USD conversion rate from CoinGecko.
        Returns a fallback value if the API call fails.
        """
        try:
            r = requests.get(self.url, timeout=5)
            r.raise_for_status()
            return float(r.json()['cardano']['usd'])
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to fetch ADA price from CoinGecko: {e}")
        except (KeyError, ValueError) as e:
            log.error(f"Failed to parse ADA price from CoinGecko response: {e}")
        
        log.warning(f"Returning fallback ADA price: ${self.fallback_price}")
        return self.fallback_price