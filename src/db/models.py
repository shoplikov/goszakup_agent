from sqlalchemy import Column, BigInteger, String, Numeric, DateTime, ForeignKey, Integer
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class PlanPoint(Base):
    """
    Stores Annual Plans. 
    Crucial because it holds the 'ref_enstru_code' (KTRU code) and 'kato' (region).
    """
    __tablename__ = 'plans'

    id = Column(BigInteger, primary_key=True)
    subject_biin = Column(String)           # BIN of the buyer
    ref_enstru_code = Column(String)        # The specific item code (e.g., 324012.300.000000)
    price = Column(Numeric)                 # Planned price
    count = Column(Numeric)                 # Planned quantity
    amount = Column(Numeric)                # Planned total amount
    date_approved = Column(DateTime)
    
    # We will extract the KATO code from the nested JSON in the API and store it here
    # This is required for the "Regional Coefficient" in your Fair Price metric
    kato_code = Column(String)              

    # Relationship to contract units
    units = relationship("ContractUnit", back_populates="plan_point")

class Contract(Base):
    """
    Stores the main contract details.
    """
    __tablename__ = 'contracts'

    id = Column(BigInteger, primary_key=True)
    contract_number = Column(String)
    crdate = Column(DateTime)               # Creation date (helps with "Time Factor/Inflation")
    contract_sum = Column(Numeric)          # Total sum of the contract
    supplier_biin = Column(String)          # Who won the contract
    customer_bin = Column(String)           # Who bought the items
    ref_contract_status_id = Column(Integer)

    # Relationship to the individual items in this contract
    units = relationship("ContractUnit", back_populates="contract")

class ContractUnit(Base):
    """
    Stores the granular items inside a contract.
    This is the core table for finding "Anomalies" and "Fair Price".
    """
    __tablename__ = 'contract_units'

    id = Column(BigInteger, primary_key=True)
    contract_id = Column(BigInteger, ForeignKey('contracts.id'))
    pln_point_id = Column(BigInteger, ForeignKey('plans.id')) # Links back to the KTRU code!
    
    item_price = Column(Numeric)            # The actual price paid per unit
    quantity = Column(Numeric)              # The actual quantity bought
    total_sum = Column(Numeric)             # item_price * quantity
    
    # Setup relationships for easy querying in Python
    contract = relationship("Contract", back_populates="units")
    plan_point = relationship("PlanPoint", back_populates="units")