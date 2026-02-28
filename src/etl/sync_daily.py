import logging
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.db.models import PlanPoint, Contract, ContractUnit
from src.etl.client import GoszakupClient
from src.etl.load_historical import TARGET_BINS, upsert_subject, parse_date
from src.etl.enrich_missing_announcements import backfill_announcements, ensure_announcement

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SYNC_WINDOW_DAYS = 3
CUTOFF_DATE = datetime.now() - timedelta(days=SYNC_WINDOW_DAYS)

def sync_data_for_bin(client: GoszakupClient, db: Session, bin_number: str):
    logger.info(f"syncing bin {bin_number}")
    upsert_subject(db, bin_number, is_customer=True)

    existing_plans = {row[0] for row in db.query(PlanPoint.id).filter(PlanPoint.subject_biin == bin_number).all()}
    existing_contracts = {row[0] for row in db.query(Contract.id).filter(Contract.customer_bin == bin_number).all()}
    existing_units = {row[0] for row in db.query(ContractUnit.id).all()}
    valid_plan_ids = existing_plans.copy()

    logger.info("plans")
    for item in client.paginate(f'/v3/plans/{bin_number}'):
        last_update = parse_date(item.get('index_date')) or parse_date(item.get('timestamp'))
        if last_update and last_update < CUTOFF_DATE:
            break
        if item['id'] in existing_plans:
            continue
        kato_list = item.get('kato', [])
        plan = PlanPoint(
            id=item['id'], 
            subject_biin=bin_number, 
            ref_enstru_code=item.get('ref_enstru_code'),
            ref_units_code=item.get('ref_units_code'), 
            price=item.get('price'),
            count=item.get('count'), 
            amount=item.get('amount'),
            date_approved=parse_date(item.get('date_approved')),
            kato_code=kato_list[0].get('ref_kato_code') if kato_list else None
        )
        db.merge(plan)
        valid_plan_ids.add(item['id'])
    db.commit()

    logger.info("contracts and units")
    for item in client.paginate(f'/v3/contract/customer/{bin_number}'):
        last_update = parse_date(item.get('index_date')) or parse_date(item.get('crdate'))
        if last_update and last_update < CUTOFF_DATE:
            break
        contract_id = item.get('id')
        if contract_id not in existing_contracts:
            trd_buy_id = item.get('trd_buy_id')
            if trd_buy_id and not ensure_announcement(client, db, trd_buy_id):
                logger.warning(f"skipping contract {contract_id}: announcement {trd_buy_id} could not be backfilled")
                continue

            raw_supplier_bin = item.get('supplier_biin')
            supplier_bin = raw_supplier_bin if raw_supplier_bin and str(raw_supplier_bin).strip() else None

            if supplier_bin:
                upsert_subject(db, supplier_bin, is_supplier=True)
            contract = Contract(
                id=contract_id, 
                contract_number=item.get('contract_number'), 
                trd_buy_id=trd_buy_id if trd_buy_id else None,
                crdate=parse_date(item.get('crdate')), 
                contract_sum=item.get('contract_sum'),
                supplier_biin=supplier_bin, 
                customer_bin=bin_number,
                ref_contract_status_id=item.get('ref_contract_status_id')
            )
            db.merge(contract)
            try:
                units_data = client.get(f'/v3/contract/{contract_id}/units')
                units_items = units_data if isinstance(units_data, list) else units_data.get('items', [])
                for u_item in units_items:
                    unit_id = u_item.get('id')
                    if unit_id in existing_units:
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
                    db.merge(unit)
            except Exception:
                pass
    db.commit()

if __name__ == "__main__":
    client = GoszakupClient()
    db_session = SessionLocal()
    try:
        logger.info(f"daily sync started, cutoff {CUTOFF_DATE}")
        for bin_code in TARGET_BINS:
            sync_data_for_bin(client, db_session, bin_code)
        logger.info("backfilling missing announcements")
        backfill_announcements(client, db_session)
        logger.info("daily sync done")
    finally:
        db_session.close()
