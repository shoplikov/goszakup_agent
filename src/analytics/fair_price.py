import pandas as pd
from typing import Optional
from pydantic import BaseModel
from sqlalchemy.orm import Session
from src.db.session import SessionLocal
from src.db.models import ContractUnit, PlanPoint

class PriceBenchmark(BaseModel):
    enstru_code: str
    kato_code: Optional[str]
    sample_size: int
    median_price: float
    fair_min: float
    fair_max: float
    confidence: str

def get_fair_price_bounds(db: Session, enstru_code: str, kato_code: Optional[str] = None) -> Optional[PriceBenchmark]:
    """
    Calculates the statistical fair price range for a given KTRU code using IQR.
    """
    # 1. Query the database for all contract units matching the KTRU code
    query = db.query(ContractUnit.item_price).join(
        PlanPoint, ContractUnit.pln_point_id == PlanPoint.id
    ).filter(
        PlanPoint.ref_enstru_code == enstru_code
    )

    # Apply regional filter if provided
    if kato_code:
        query = query.filter(PlanPoint.kato_code == kato_code)

    results = query.all()
    
    if not results:
        return None

    # 2. Load into Pandas for fast statistical math
    df = pd.DataFrame(results, columns=['item_price'])
    df['item_price'] = pd.to_numeric(df['item_price'], errors='coerce').dropna()

    sample_size = len(df)
    if sample_size < 3:
        # Not enough data for reliable statistics
        median = float(df['item_price'].median()) if sample_size > 0 else 0.0
        return PriceBenchmark(
            enstru_code=enstru_code,
            kato_code=kato_code,
            sample_size=sample_size,
            median_price=median,
            fair_min=median * 0.9, # Fallback rule-based logic
            fair_max=median * 1.1,
            confidence="Low - Insufficient sample size"
        )

    # 3. Calculate IQR (Interquartile Range)
    q1 = df['item_price'].quantile(0.25)
    q3 = df['item_price'].quantile(0.75)
    iqr = q3 - q1
    median = df['item_price'].median()

    # 4. Define upper and lower bounds for "Fairness"
    # Prices outside these bounds are statistically anomalous
    lower_bound = max(0, q1 - (1.5 * iqr)) 
    upper_bound = q3 + (1.5 * iqr)

    # Determine confidence based on sample size
    confidence = "High" if sample_size >= 30 else "Medium"

    return PriceBenchmark(
        enstru_code=enstru_code,
        kato_code=kato_code,
        sample_size=sample_size,
        median_price=float(median),
        fair_min=float(lower_bound),
        fair_max=float(upper_bound),
        confidence=confidence
    )
