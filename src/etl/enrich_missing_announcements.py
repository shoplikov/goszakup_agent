import logging
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text
from src.db.session import SessionLocal
from src.db.models import Announcement, Lot, Subject
from src.etl.client import GoszakupClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_date(date_str: str):
    if not date_str:
        return None
    clean_str = str(date_str).replace('T', ' ').split('.')[0]
    try:
        return datetime.strptime(clean_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(clean_str, "%Y-%m-%d")
        except ValueError:
            return None

def upsert_subject(db: Session, bin_number: str):
    if not bin_number:
        return
    subject = db.query(Subject).filter(Subject.bin == bin_number).first()
    if not subject:
        subject = Subject(bin=bin_number)
        db.merge(subject)
        db.commit()

def ensure_announcement(client, db, trd_buy_id):
    if not trd_buy_id:
        return True
    if db.query(Announcement.id).filter(Announcement.id == trd_buy_id).first():
        return True
    try:
        data = client.get(f'/v3/trd-buy/{trd_buy_id}')
        item = data[0] if isinstance(data, list) and len(data) > 0 else data
        if not item or 'id' not in item:
            return False
        org_bin = item.get('org_bin')
        upsert_subject(db, org_bin)
        anno = Announcement(
            id=item.get('id'),
            number_anno=item.get('number_anno'),
            name_ru=item.get('name_ru'),
            org_bin=org_bin,
            total_sum=item.get('total_sum'),
            publish_date=parse_date(item.get('publish_date')),
            start_date=parse_date(item.get('start_date')),
            end_date=parse_date(item.get('end_date')),
            ref_buy_status_id=item.get('ref_buy_status_id'),
        )
        db.merge(anno)
        lots_data = client.get(f'/v3/lots/trd-buy/{trd_buy_id}')
        lots_items = lots_data if isinstance(lots_data, list) else lots_data.get('items', [])
        for l_item in lots_items:
            cust_bin = l_item.get('customer_bin')
            upsert_subject(db, cust_bin)
            lot = Lot(
                id=l_item.get('id'),
                trd_buy_id=trd_buy_id,
                lot_number=l_item.get('lot_number'),
                name_ru=l_item.get('name_ru'), amount=l_item.get('amount'),
                count=l_item.get('count'), customer_bin=cust_bin,
                ref_lot_status_id=l_item.get('ref_lot_status_id')
            )
            db.merge(lot)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        logger.debug(f"ensure announcement {trd_buy_id}: {e}")
        return False

def backfill_announcements(client, db):
    logger.info("backfilling missing announcements")
    query = text("""
        SELECT DISTINCT trd_buy_id 
        FROM contracts 
        WHERE trd_buy_id IS NOT NULL 
          AND trd_buy_id NOT IN (SELECT id FROM announcements)
    """)
    missing_ids = [row[0] for row in db.execute(query).fetchall()]
    if not missing_ids:
        logger.info("no missing announcements")
        return
    logger.info(f"found {len(missing_ids)} missing, fetching")
    added_count = 0
    for idx, trd_buy_id in enumerate(missing_ids, 1):
        try:
            data = client.get(f'/v3/trd-buy/{trd_buy_id}')
            item = data[0] if isinstance(data, list) and len(data) > 0 else data
            if not item or 'id' not in item:
                continue
            org_bin = item.get('org_bin')
            upsert_subject(db, org_bin)
            anno = Announcement(
                id=item.get('id'), number_anno=item.get('number_anno'), name_ru=item.get('name_ru'),
                org_bin=org_bin, total_sum=item.get('total_sum'),
                publish_date=parse_date(item.get('publish_date')), start_date=parse_date(item.get('start_date')),
                end_date=parse_date(item.get('end_date')), ref_buy_status_id=item.get('ref_buy_status_id')
            )
            db.merge(anno)
            lots_data = client.get(f'/v3/lots/trd-buy/{trd_buy_id}')
            lots_items = lots_data if isinstance(lots_data, list) else lots_data.get('items', [])
            for l_item in lots_items:
                cust_bin = l_item.get('customer_bin')
                upsert_subject(db, cust_bin)
                lot = Lot(
                    id=l_item.get('id'),
                    trd_buy_id=trd_buy_id, 
                    lot_number=l_item.get('lot_number'),
                    name_ru=l_item.get('name_ru'), 
                    amount=l_item.get('amount'), 
                    count=l_item.get('count'),
                    customer_bin=cust_bin, 
                    ref_lot_status_id=l_item.get('ref_lot_status_id')
                )
                db.merge(lot)
            db.commit()
            added_count += 1
            if idx % 50 == 0:
                logger.info(f"processed {idx}/{len(missing_ids)}")
        except Exception as e:
            db.rollback()
            logger.debug(f"backfill {trd_buy_id}: {e}")
    logger.info(f"backfill done, added {added_count}")

if __name__ == "__main__":
    client = GoszakupClient()
    db_session = SessionLocal()
    try:
        backfill_announcements(client, db_session)
    finally:
        db_session.close()
