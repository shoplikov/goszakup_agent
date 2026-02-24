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

    def paginate(self, path: str, params: dict = None, max_pages: int = None) -> Iterator[dict]:
        params = (params or {}).copy()
        if 'limit' not in params:
            params['limit'] = 200
            
        page = 1
        seen_ids = set()
        
        while True:
            logger.info(f"Fetching page {page} from {path}...")
            data = self.get(path, params)
            
            items = data if isinstance(data, list) else data.get('items', [])
            
            if not items:
                break
                
            new_items_count = 0
            for item in items:
                # skip if we have already seen this exact record ID
                item_id = item.get('id')
                if item_id and item_id in seen_ids:
                    continue
                
                if item_id:
                    seen_ids.add(item_id)
                
                new_items_count += 1
                yield item
                
            # if the entire page was just duplicates we've already seen, the API is looping
            if new_items_count == 0:
                logger.warning(f"API returned redundant data at page {page}. Breaking infinite loop.")
                break
                
            if max_pages and page >= max_pages:
                logger.info(f"Reached max_pages limit ({max_pages}). Stopping.")
                break
                
            # extract next_page token
            next_page = data.get('next_page') if isinstance(data, dict) else None
            
            # the token is missing or identical to the one we just requested
            if not next_page or str(params.get('next_page')) == str(next_page):
                break
                
            params['next_page'] = next_page
            page += 1