import logging
from sqlalchemy.orm import Session
from sqlalchemy import func
from src.db.session import SessionLocal
from src.db.models import PlanPoint, RefEnstru
from src.etl.client import GoszakupClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def enrich_enstru(client: GoszakupClient, db: Session):
    subquery = db.query(RefEnstru.code)
    
    missing_ktrus = db.query(
        PlanPoint.ref_enstru_code, 
        func.max(PlanPoint.id).label('sample_plan_id')
    ).filter(
        PlanPoint.ref_enstru_code.isnot(None),
        ~PlanPoint.ref_enstru_code.in_(subquery)
    ).group_by(
        PlanPoint.ref_enstru_code
    ).all()

    total = len(missing_ktrus)
    logger.info(f"Found {total} unique KTRU codes missing descriptions. Starting enrichment.")
    
    added_count = 0
    for idx, (ktru_code, plan_id) in enumerate(missing_ktrus, 1):
        try:
            data = client.get(f'/v3/plans/view/{plan_id}')
            
            name_ru = data.get('name_ru')
            name_kz = data.get('name_kz')
            
            name_ru = name_ru if name_ru and str(name_ru).strip() else "Неизвестное наименование"
            name_kz = name_kz if name_kz and str(name_kz).strip() else "Белгісіз атау"
            
            new_enstru = RefEnstru(
                code=ktru_code,
                name_ru=name_ru,
                name_kz=name_kz
            )
            db.add(new_enstru)
            added_count += 1
            
        except Exception as e:
            logger.debug(f"Failed to fetch details for KTRU {ktru_code} (Plan {plan_id}): {e}")
            
        # Commit periodically
        if idx % 50 == 0:
            db.commit()
            logger.info(f"Processed {idx}/{total} KTRU codes.")
            
    db.commit()
    logger.info(f"Enrichment Complete. Added {added_count} KTRU descriptions.")

if __name__ == "__main__":
    client = GoszakupClient()
    db_session = SessionLocal()
    try:
        enrich_enstru(client, db_session)
    finally:
        db_session.close()