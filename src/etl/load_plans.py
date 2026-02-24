from datetime import datetime
from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.db.models import PlanPoint
from src.etl.client import GoszakupClient

TEST_BIN = '000740001307' 

def load_annual_plans(bin_number: str, db: Session):
    client = GoszakupClient()
    
    print(f"Starting extraction for BIN: {bin_number}")
    
    # Use the paginate method to get all plans for this organization
    endpoint = f'/v3/plans/{bin_number}'
    
    records_added = 0
    for item in client.paginate(endpoint):
        
        # --- Transform ---
        # We need to extract the KATO code safely, as it's nested in a list
        kato_list = item.get('kato', [])
        kato_code = kato_list[0].get('ref_kato_code') if kato_list else None
        
        # Safely parse the date string into a Python datetime object
        date_appr_str = item.get('date_approved')
        date_appr = datetime.strptime(date_appr_str, "%Y-%m-%d %H:%M:%S") if date_appr_str else None

        # Check if the record already exists to avoid duplicates
        existing = db.query(PlanPoint).filter(PlanPoint.id == item['id']).first()
        if existing:
            continue
            
        # Map the JSON data to our SQLAlchemy Model
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
        
        # Commit in batches so we don't overwhelm the database RAM
        if records_added % 100 == 0:
            db.commit()
            print(f"  Inserted {records_added} records...")

    # Final commit for any remaining records
    db.commit()
    print(f"Finished! Total new plans added for {bin_number}: {records_added}")

if __name__ == "__main__":
    # Open a database session and run the loader
    db_session = SessionLocal()
    try:
        load_annual_plans(TEST_BIN, db_session)
    finally:
        db_session.close()