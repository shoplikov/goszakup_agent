import logging
from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.db.models import RefKato
from src.etl.client import GoszakupClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_kato_dictionary(client: GoszakupClient, db: Session):
    logger.info("Loading Reference Dictionary (KATO)")
    try:
        seen_codes = {row[0] for row in db.query(RefKato.code).all()}
        
        added_count = 0
        
        for item in client.paginate('/v3/refs/ref_kato'):
            code = item.get('code')
            
            if code and code not in seen_codes:
                full_name_ru = item.get('full_name_ru') or item.get('name_ru')
                full_name_kz = item.get('full_name_kz') or item.get('name_kz')
                
                kato = RefKato(
                    code=code,
                    full_name_ru=full_name_ru,
                    full_name_kz=full_name_kz
                )
                db.add(kato)
                seen_codes.add(code)
                added_count += 1
                
                if added_count % 50 == 0:
                    db.commit()
                    logger.info(f"Inserted {added_count} KATO records.")
                    
        db.commit()
        logger.info(f"KATO Dictionary Loaded. Added {added_count} geographic regions.")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to load KATO dictionary: {e}")

if __name__ == "__main__":
    client = GoszakupClient()
    db_session = SessionLocal()
    try:
        load_kato_dictionary(client, db_session)
    finally:
        db_session.close()