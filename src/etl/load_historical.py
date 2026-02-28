import logging
from datetime import datetime
from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.db.models import Subject, PlanPoint, Announcement, Lot, Contract, ContractUnit, RefUnit
from src.etl.client import GoszakupClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TARGET_BINS = [
    '000740001307', '020240002363', '050740004819', '051040005150',
    '140340016539', '150540000186', '210240033968', '210941010761',
    '780140000023', '900640000128', '960440000220', '970940001378',
    '981140001551', '990340005977', '020440003656', '100140011059',
    '171041003124', '230740013340', '940740000911', '971040001050',
    '990740002243', '030440003698', '120940001946', '210240019348',
    '231040023028', '940940000384', '980440001034'
]

CUTOFF_DATE = datetime(2024, 1, 1)

def load_reference_dictionaries(client: GoszakupClient, db: Session):
    logger.info("Loading Reference Dictionaries (Units)")
    try:
        seen_codes = {row[0] for row in db.query(RefUnit.code).all()}
        for item in client.paginate('/v3/refs/ref_units'):
            code = item.get('code')
            if code and code not in seen_codes:
                unit = RefUnit(
                    code=code, 
                    name_ru=item.get('name_ru'), 
                    name_kz=item.get('name_kz')
                    )
                db.add(unit)
                seen_codes.add(code)
        db.commit()
        logger.info("reference units loaded")
    except Exception as e:
        db.rollback()
        logger.error(f"reference units failed: {e}")

def upsert_subject(db, bin_number, name_ru=None, is_customer=False, is_supplier=False):
    if not bin_number:
        return None
    subject = db.query(Subject).filter(Subject.bin == bin_number).first()
    if not subject:
        subject = Subject(bin=bin_number, name_ru=name_ru, is_customer=is_customer, is_supplier=is_supplier)
        db.add(subject)
        db.commit()
    return subject

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except (ValueError, TypeError):
            return None

def load_data_for_bin(client: GoszakupClient, db: Session, bin_number: str):
    logger.info(f"processing bin {bin_number}")
    upsert_subject(db, bin_number, is_customer=True)

    logger.info("plans")
    for item in client.paginate(f'/v3/plans/{bin_number}'):
        date_appr = parse_date(item.get('date_approved'))
        if date_appr and date_appr < CUTOFF_DATE:
            continue
        if not db.query(PlanPoint).filter(PlanPoint.id == item['id']).first():
            kato_list = item.get('kato', [])
            kato_code = kato_list[0].get('ref_kato_code') if kato_list else None
            unit_code = item.get('ref_units_code')
            if unit_code and not db.query(RefUnit).filter(RefUnit.code == unit_code).first():
                db.add(RefUnit(code=unit_code, name_ru="Неизвестный код", name_kz="Белгісіз код"))
                db.commit()
            plan = PlanPoint(
                id=item['id'], 
                subject_biin=bin_number, 
                ref_enstru_code=item.get('ref_enstru_code'),
                ref_units_code=unit_code, 
                price=item.get('price'), 
                count=item.get('count'),
                amount=item.get('amount'), 
                date_approved=date_appr, 
                kato_code=kato_code
            )
            db.add(plan)
    db.commit()

    logger.info("announcements and lots")
    for item in client.paginate('/v3/trd-buy', params={'customer_bin': bin_number}):
        pub_date = parse_date(item.get('publish_date'))
        if pub_date and pub_date < CUTOFF_DATE:
            continue
        anno_id = item.get('id')
        if not db.query(Announcement).filter(Announcement.id == anno_id).first():
            anno = Announcement(
                id=anno_id, 
                number_anno=item.get('number_anno'), 
                name_ru=item.get('name_ru'),
                org_bin=bin_number, 
                total_sum=item.get('total_sum'), 
                publish_date=pub_date,
                start_date=parse_date(item.get('start_date')), 
                end_date=parse_date(item.get('end_date')),
                ref_buy_status_id=item.get('ref_buy_status_id')
            )
            db.add(anno)
            try:
                lots_data = client.get(f'/v3/lots/trd-buy/{anno_id}')
                lots_items = lots_data if isinstance(lots_data, list) else lots_data.get('items', [])
                for l_item in lots_items:
                    if not db.query(Lot).filter(Lot.id == l_item.get('id')).first():
                        lot = Lot(
                            id=l_item.get('id'), 
                            trd_buy_id=anno_id, 
                            lot_number=l_item.get('lot_number'),
                            name_ru=l_item.get('name_ru'), 
                            amount=l_item.get('amount'),
                            count=l_item.get('count'), 
                            customer_bin=bin_number,
                            ref_lot_status_id=l_item.get('ref_lot_status_id')
                        )
                        db.add(lot)
            except Exception as e:
                logger.warning(f"lots for anno {anno_id}: {e}")
    db.commit()

    logger.info("contracts and units")
    valid_plan_ids = {row[0] for row in db.query(PlanPoint.id).all()}
    valid_anno_ids = {row[0] for row in db.query(Announcement.id).all()}
    for item in client.paginate(f'/v3/contract/customer/{bin_number}'):
        crdate = parse_date(item.get('crdate'))
        if crdate and crdate < CUTOFF_DATE:
            continue
        contract_id = item.get('id')
        raw_supplier_bin = item.get('supplier_biin')
        supplier_bin = raw_supplier_bin if raw_supplier_bin and str(raw_supplier_bin).strip() else None
        if supplier_bin:
            upsert_subject(db, supplier_bin, is_supplier=True)
        if not db.query(Contract).filter(Contract.id == contract_id).first():
            raw_trd_id = item.get('trd_buy_id')
            safe_trd_id = raw_trd_id if raw_trd_id in valid_anno_ids else None
            contract = Contract(
                id=contract_id, contract_number=item.get('contract_number'), trd_buy_id=safe_trd_id,
                crdate=crdate, contract_sum=item.get('contract_sum'), supplier_biin=supplier_bin,
                customer_bin=bin_number, ref_contract_status_id=item.get('ref_contract_status_id')
            )
            db.add(contract)
            
            try:
                units_data = client.get(f'/v3/contract/{contract_id}/units')
                units_items = units_data if isinstance(units_data, list) else units_data.get('items', [])
                for u_item in units_items:
                    unit_id = u_item.get('id')
                    raw_pln_id = u_item.get('pln_point_id')
                    safe_pln_id = raw_pln_id if raw_pln_id in valid_plan_ids else None
                    if not db.query(ContractUnit).filter(ContractUnit.id == unit_id).first():
                        unit = ContractUnit(
                            id=unit_id, contract_id=contract_id, pln_point_id=safe_pln_id,
                            item_price=u_item.get('item_price'), quantity=u_item.get('quantity'),
                            total_sum=u_item.get('total_sum')
                        )
                        db.add(unit)
            except Exception:
                pass
    db.commit()

if __name__ == "__main__":
    client = GoszakupClient()
    db_session = SessionLocal()
    try:
        load_reference_dictionaries(client, db_session)
        for bin_code in TARGET_BINS:
            load_data_for_bin(client, db_session, bin_code)
        logger.info("historical load done")
    finally:
        db_session.close()
