import pandas as pd
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel
from src.db.session import SessionLocal
from src.db.models import Contract, ContractUnit, PlanPoint

# Output models for the agent
class PriceDeviationResult(BaseModel):
    enstru_code: str
    weighted_average_price: float
    target_price: float
    deviation_percentage: float
    is_anomalous: bool
    sample_size_units: int
    top_k_links: List[str]

class VolumeAnomalyResult(BaseModel):
    customer_bin: str
    enstru_code: str
    yearly_volumes: Dict[int, float]
    is_anomalous: bool
    description: str
    top_k_links: List[str]

class FairPriceResult(BaseModel):
    enstru_code: str
    kato_code: Optional[str]
    time_period: str
    median_price: float
    fair_min: float
    fair_max: float
    confidence: str
    top_k_links: List[str]

# Analytical functions
def check_price_deviation(db: Session, enstru_code: str, target_price: float) -> Optional[PriceDeviationResult]:
    query = db.query(
        ContractUnit.item_price,
        ContractUnit.quantity,
        ContractUnit.contract_id  
    ).join(
        PlanPoint, ContractUnit.pln_point_id == PlanPoint.id
    ).filter(
        PlanPoint.ref_enstru_code == enstru_code,
        ContractUnit.item_price != None,
        ContractUnit.quantity != None
    )
    
    results = query.all()
    if not results:
        return None
        
    df = pd.DataFrame(results, columns=['price', 'quantity', 'contract_id'])
    df['price'] = pd.to_numeric(df['price'])
    df['quantity'] = pd.to_numeric(df['quantity'])
    
    # calculating weighted avg: sum(price * quantity) / sum(quantity)
    total_value = (df['price'] * df['quantity']).sum()
    total_quantity = df['quantity'].sum()
    
    if total_quantity == 0:
        return None
        
    w_avg_price = total_value / total_quantity
    
    # calculating deviation
    deviation = ((target_price - w_avg_price) / w_avg_price) * 100
    is_anomalous = abs(deviation) > 30.0
    
    # the top 3 most expensive contracts as links
    top_k_ids = df.nlargest(3, 'price')['contract_id'].unique()
    top_k_links = [f"https://goszakup.gov.kz/ru/contract/show/{cid}" for cid in top_k_ids]
    
    return PriceDeviationResult(
        enstru_code=enstru_code,
        weighted_average_price=float(w_avg_price),
        target_price=target_price,
        deviation_percentage=float(deviation),
        is_anomalous=is_anomalous,
        sample_size_units=len(df),
        top_k_links=top_k_links
    )

def detect_volume_anomaly(db: Session, customer_bin: str, enstru_code: str) -> Optional[VolumeAnomalyResult]:
    query = db.query(
        func.extract('year', Contract.crdate).label('year'),
        func.sum(ContractUnit.quantity).label('total_qty'),
        func.max(Contract.id).label('latest_contract_id') # Fetch a sample ID
    ).join(
        ContractUnit, Contract.id == ContractUnit.contract_id
    ).join(
        PlanPoint, ContractUnit.pln_point_id == PlanPoint.id
    ).filter(
        Contract.customer_bin == customer_bin,
        PlanPoint.ref_enstru_code == enstru_code
    ).group_by(
        func.extract('year', Contract.crdate)
    ).order_by('year')

    results = query.all()
    if not results:
        return None

    yearly_vols = {int(row.year): float(row.total_qty) for row in results}
    sample_ids = [row.latest_contract_id for row in results if row.latest_contract_id]
    
    # anomaly - if the latest year's volume is > 200% of the historical average
    is_anomalous = False
    description = "Normal volume trends."
    
    years = sorted(list(yearly_vols.keys()))
    if len(years) > 1:
        latest_year = years[-1]
        latest_vol = yearly_vols[latest_year]
        historical_vols = [yearly_vols[y] for y in years[:-1]]
        hist_avg = sum(historical_vols) / len(historical_vols)
        
        if hist_avg > 0 and latest_vol > (hist_avg * 2): # x2 increase
            is_anomalous = True
            description = f"Volume in {latest_year} ({latest_vol}) is significantly higher than historical average ({hist_avg:.2f})."

    # the top 3 contracts as links
    top_k_links = [f"https://goszakup.gov.kz/ru/contract/show/{cid}" for cid in sample_ids[-3:]]

    return VolumeAnomalyResult(
        customer_bin=customer_bin,
        enstru_code=enstru_code,
        yearly_volumes=yearly_vols,
        is_anomalous=is_anomalous,
        description=description,
        top_k_links=top_k_links
    )

def get_fair_price_bounds(db: Session, enstru_code: str, kato_code: Optional[str] = None, year_filter: Optional[int] = None) -> Optional[FairPriceResult]:
    query = db.query(
        ContractUnit.item_price,
        ContractUnit.contract_id
    ).join(
        PlanPoint, ContractUnit.pln_point_id == PlanPoint.id
    ).join(
        Contract, ContractUnit.contract_id == Contract.id
    ).filter(
        PlanPoint.ref_enstru_code == enstru_code,
        ContractUnit.item_price != None
    )

    if kato_code:
        query = query.filter(PlanPoint.kato_code == kato_code)
    if year_filter:
        query = query.filter(func.extract('year', Contract.crdate) == year_filter)

    results = query.all()
    if not results or len(results) < 3:
        return None

    df = pd.DataFrame(results, columns=['price', 'contract_id'])
    df['price'] = pd.to_numeric(df['price'])
    
    q1 = df['price'].quantile(0.25)
    q3 = df['price'].quantile(0.75)
    iqr = q3 - q1
    median = df['price'].median()

    lower_bound = max(0, q1 - (1.5 * iqr))
    upper_bound = q3 + (1.5 * iqr)

    # the top 3 contracts as links
    median_examples = df.iloc[(df['price'] - median).abs().argsort()[:3]]
    top_k_links = [f"https://goszakup.gov.kz/ru/contract/show/{cid}" for cid in median_examples['contract_id'].unique()]

    return FairPriceResult(
        enstru_code=enstru_code,
        kato_code=kato_code,
        time_period=str(year_filter) if year_filter else "All Time",
        median_price=float(median),
        fair_min=float(lower_bound),
        fair_max=float(upper_bound),
        confidence="High" if len(df) >= 30 else "Medium",
        top_k_links=top_k_links
    )