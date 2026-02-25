from datetime import datetime
from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.etl.client import GoszakupClient
from src.db.models import Contract, ContractUnit, PlanPoint

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

def load_contracts_and_units(bins: list[str], db: Session):
    client = GoszakupClient()
    
    valid_plan_ids = {row[0] for row in db.query(PlanPoint.id).all()}
    logger.info(f"Cached {len(valid_plan_ids)} valid plans.")
    
    for bin_number in bins:
        logger.info(f"Starting extraction for BIN: {bin_number}")
        
        contracts_endpoint = f'/v3/contract/customer/{bin_number}'
        contracts_added = 0
        units_added = 0
        
        for item in client.paginate(contracts_endpoint):
            contract_id = item.get('id')
            if not contract_id:
                continue
                
            crdate_str = item.get('crdate')
            crdate = datetime.strptime(crdate_str, "%Y-%m-%d %H:%M:%S") if crdate_str else None
            
            existing_contract = db.query(Contract).filter(Contract.id == contract_id).first()
            if not existing_contract:
                contract = Contract(
                    id=contract_id,
                    contract_number=item.get('contract_number'),
                    crdate=crdate,
                    contract_sum=item.get('contract_sum'),
                    supplier_biin=item.get('supplier_biin'),
                    customer_bin=item.get('customer_bin'),
                    ref_contract_status_id=item.get('ref_contract_status_id')
                )
                db.add(contract)
                contracts_added += 1
            
            units_endpoint = f'/v3/contract/{contract_id}/units'
            try:
                units_data = client.get(units_endpoint)
                units_items = units_data if isinstance(units_data, list) else units_data.get('items', [])
                
                for u_item in units_items:
                    unit_id = u_item.get('id')
                    
                    if not unit_id or db.query(ContractUnit).filter(ContractUnit.id == unit_id).first():
                        continue
                        
                    raw_pln_id = u_item.get('pln_point_id')
                    safe_pln_id = raw_pln_id if raw_pln_id in valid_plan_ids else None
                        
                    unit = ContractUnit(
                        id=unit_id,
                        contract_id=contract_id,
                        pln_point_id=safe_pln_id,
                        item_price=u_item.get('item_price'),
                        quantity=u_item.get('quantity'),
                        total_sum=u_item.get('total_sum')
                    )
                    db.add(unit)
                    units_added += 1
                    
            except Exception as e:
                logger.error(f"Failed to fetch units for contract {contract_id}: {e}")

            if (contracts_added + units_added) % 200 == 0:
                db.commit()
                logger.info(f"  Committed batch of: ({contracts_added} contracts, {units_added} units)")
                
        db.commit()
        logger.info(f"Finished BIN {bin_number}. Added {contracts_added} contracts and {units_added} units.")

if __name__ == "__main__":
    db_session = SessionLocal()
    try:
        load_contracts_and_units(ALL_BINS, db_session)
    finally:
        db_session.close()