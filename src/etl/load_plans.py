from datetime import datetime
from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.db.models import PlanPoint
from src.etl.client import GoszakupClient

# Logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ALL_BINS = [
    '000740001307', '020240002363', '050740004819', '051040005150',
    '140340016539', '150540000186', '210240033968', '210941010761',
    '780140000023', '900640000128', '960440000220', '970940001378',
    '981140001551', '990340005977', '020440003656', '100140011059',
    '171041003124', '230740013340', '940740000911', '971040001050',
    '990740002243', '030440003698', '120940001946', '210240019348',
    '231040023028', '940940000384', '980440001034'
]

def load_annual_plans(bins: list[str], db: Session):
    client = GoszakupClient()
    
    for bin_number in bins:
        logger.info(f"Starting extraction for BIN: {bin_number}")
        endpoint = f'/v3/plans/{bin_number}'
        
        records_added = 0
        for item in client.paginate(endpoint):
            kato_list = item.get('kato', [])
            kato_code = kato_list[0].get('ref_kato_code') if kato_list else None
            
            date_appr_str = item.get('date_approved')
            date_appr = datetime.strptime(date_appr_str, "%Y-%m-%d %H:%M:%S") if date_appr_str else None

            existing = db.query(PlanPoint).filter(PlanPoint.id == item['id']).first()
            if existing:
                continue
                
            plan = PlanPoint(
                id=item['id'],
                subject_biin=item.get('subject_biin'),
                ref_enstru_code=item.get('ref_enstru_code'),
                price=item.get('price'),
                count=item.get('count'),
                amount=item.get('amount'),
                date_approved=date_appr,
                kato_code=kato_code
            )
            
            db.add(plan)
            records_added += 1
            
            if records_added % 100 == 0:
                db.commit()
                logger.info(f"  Inserted {records_added} records.")

        db.commit()
        logger.info(f"Total new plans added for {bin_number}: {records_added}")

if __name__ == "__main__":
    db_session = SessionLocal()
    try:
        load_annual_plans(ALL_BINS, db_session)
    finally:
        db_session.close()