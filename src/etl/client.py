import time
import requests
import logging
from typing import Iterator
from src.config import API_TOKEN

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GoszakupClient:
    def __init__(self, base_url: str = 'https://ows.goszakup.gov.kz'):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {API_TOKEN}',
            'Content-Type': 'application/json',
        })
        self.base_url = base_url.rstrip('/')
        self.rate_limit_pause = 0.35 

    def get(self, path: str, params: dict = None, max_retries: int = 4) -> dict:
        url = f'{self.base_url}{path}'
        time.sleep(self.rate_limit_pause)
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 429:
                    sleep_time = 2 ** attempt * 5
                    logger.warning(f"Rate limited (429). Sleeping for {sleep_time}s...")
                    time.sleep(sleep_time)
                    continue
                    
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}. Retrying...")
                time.sleep(2 ** attempt * 3)
                
        raise RuntimeError(f"Failed to fetch {url} after {max_retries} retries.")

    def paginate(self, path: str, params: dict = None) -> Iterator[dict]:
        params = (params or {}).copy()
        params['limit'] = 200
        
        while True:
            logger.info(f"Fetching page from {path}...")
            data = self.get(path, params)
            
            items = data if isinstance(data, list) else data.get('items', [])
            
            if not items:
                break
                
            for item in items:
                yield item
                
            next_page = data.get('next_page') if isinstance(data, dict) else None
            if not next_page:
                break
                
            params['next_page'] = next_page