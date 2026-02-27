import logging
from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.db.models import Lot
from src.utils.cleaners import sanitize_lot_text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_database_lots(db: Session):
    # Iterates through all existing lots and cleans their text fields.
    logger.info("Starting lot name cleaning")
    
    batch_size = 5000
    offset = 0
    updated_count = 0
    
    while True:
        # fetching lots in batches
        lots_batch = db.query(Lot).offset(offset).limit(batch_size).all()
        if not lots_batch:
            break
            
        for lot in lots_batch:
            original_ru = lot.name_ru
            
            cleaned_ru = sanitize_lot_text(original_ru)
            
            # Only update and commit if the text was actually changed
            if original_ru != cleaned_ru:
                lot.name_ru = cleaned_ru
                updated_count += 1
        
        db.commit()
        offset += batch_size
        logger.info(f"Scanned {offset} lots")
        
    logger.info(f"Cleaning Complete. Updated {updated_count} lot names")

if __name__ == "__main__":
    db_session = SessionLocal()
    try:
        clean_database_lots(db_session)
    finally:
        db_session.close()