import logging
from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.db.models import Subject
from src.etl.client import GoszakupClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def enrich_subjects(client: GoszakupClient, db: Session):
    subjects_to_update = db.query(Subject).filter(Subject.name_ru.is_(None)).all()
    total = len(subjects_to_update)
    
    logger.info(f"Found {total} subjects missing names. Starting enrichment.")
    
    updated_count = 0
    for idx, subject in enumerate(subjects_to_update, 1):
        if not subject.bin:
            continue
            
        try:
            endpoint = f'/v3/subject/biin/{subject.bin}'
            data = client.get(endpoint)
            
            item = data[0] if isinstance(data, list) and len(data) > 0 else data
            
            if isinstance(item, dict):
                subject.name_ru = item.get('name_ru')
                subject.name_kz = item.get('name_kz')
                updated_count += 1
                
        except Exception as e:
            logger.debug(f"Failed to fetch profile for BIN {subject.bin}: {e}")
            
        if idx % 50 == 0:
            db.commit()
            logger.info(f"Processed {idx}/{total} subjects.")
            
    db.commit()
    logger.info(f"Enrichment Complete. Updated {updated_count} supplier/customer names.")

if __name__ == "__main__":
    client = GoszakupClient()
    db_session = SessionLocal()
    try:
        enrich_subjects(client, db_session)
    finally:
        db_session.close()