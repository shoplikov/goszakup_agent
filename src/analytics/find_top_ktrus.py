from sqlalchemy import func
from src.db.session import SessionLocal
from src.db.models import ContractUnit, PlanPoint

def get_top_available_ktrus():
    """Queries the database for KTRU codes that have the most pricing data."""
    db = SessionLocal()
    try:
        # Perform an inner join to only get KTRUs that have actual contract units attached
        results = (
            db.query(
                PlanPoint.ref_enstru_code, 
                func.count(ContractUnit.id).label('unit_count')
            )
            .join(ContractUnit, ContractUnit.pln_point_id == PlanPoint.id)
            .group_by(PlanPoint.ref_enstru_code)
            .order_by(func.count(ContractUnit.id).desc())
            .limit(5)
            .all()
        )
        
        print("=== Top KTRU Codes in Local DB ===")
        if not results:
            print("No linked contract units found. Ensure both plans and contracts are loaded.")
            return

        for ktru, count in results:
            print(f"KTRU: {ktru} | Pricing Data Points: {count}")
            
    finally:
        db.close()

if __name__ == "__main__":
    get_top_available_ktrus()