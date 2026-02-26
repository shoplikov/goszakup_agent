from sqlalchemy import Column, BigInteger, String, Numeric, DateTime, ForeignKey, Integer, Boolean
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class RefUnit(Base):
    """
    Reference dictionary for units of measurement.
    Required for normalizing quantities (e.g., converting grams to kilograms).
    """
    __tablename__ = 'ref_units'

    code = Column(String, primary_key=True)  # API uses string codes for units
    name_ru = Column(String)
    name_kz = Column(String)

class Subject(Base):
    """
    Stores both Customers (Заказчики) and Suppliers (Поставщики).
    """
    __tablename__ = 'subjects'

    pid = Column(BigInteger, primary_key=True)
    bin = Column(String, unique=True, index=True) # Indexed for fast lookups
    name_ru = Column(String)
    name_kz = Column(String)
    
    is_customer = Column(Boolean, default=False)
    is_supplier = Column(Boolean, default=False)

class PlanPoint(Base):
    __tablename__ = 'plans'

    id = Column(BigInteger, primary_key=True)
    subject_biin = Column(String, ForeignKey('subjects.bin'), nullable=True)
    ref_enstru_code = Column(String, index=True)
    
    # Link to the reference dictionary
    ref_units_code = Column(String, ForeignKey('ref_units.code'), nullable=True) 
    
    price = Column(Numeric)
    count = Column(Numeric)
    amount = Column(Numeric)
    date_approved = Column(DateTime)
    kato_code = Column(String)

    units = relationship("ContractUnit", back_populates="plan_point")

class Announcement(Base):
    """
    Stores Procurement Announcements (Объявления / trd-buy).
    """
    __tablename__ = 'announcements'

    id = Column(BigInteger, primary_key=True)
    number_anno = Column(String, index=True)
    name_ru = Column(String)
    org_bin = Column(String, ForeignKey('subjects.bin'), nullable=True)
    
    total_sum = Column(Numeric)
    publish_date = Column(DateTime)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    ref_buy_status_id = Column(Integer)
    
    lots = relationship("Lot", back_populates="announcement")

class Lot(Base):
    """
    Stores individual Lots within an Announcement.
    """
    __tablename__ = 'lots'

    id = Column(BigInteger, primary_key=True)
    trd_buy_id = Column(BigInteger, ForeignKey('announcements.id'), nullable=True)
    lot_number = Column(String)
    
    name_ru = Column(String)
    description_ru = Column(String)
    
    amount = Column(Numeric)
    count = Column(Numeric)
    
    customer_bin = Column(String, ForeignKey('subjects.bin'), nullable=True)
    ref_lot_status_id = Column(Integer)

    announcement = relationship("Announcement", back_populates="lots")

class Contract(Base):
    __tablename__ = 'contracts'

    id = Column(BigInteger, primary_key=True)
    contract_number = Column(String)
    
    # Link back to the announcement that triggered this contract
    trd_buy_id = Column(BigInteger, ForeignKey('announcements.id'), nullable=True) 
    
    crdate = Column(DateTime)
    contract_sum = Column(Numeric)
    
    supplier_biin = Column(String, ForeignKey('subjects.bin'), nullable=True)
    customer_bin = Column(String, ForeignKey('subjects.bin'), nullable=True)
    ref_contract_status_id = Column(Integer)

    units = relationship("ContractUnit", back_populates="contract")

class ContractUnit(Base):
    __tablename__ = 'contract_units'

    id = Column(BigInteger, primary_key=True)
    contract_id = Column(BigInteger, ForeignKey('contracts.id'))
    pln_point_id = Column(BigInteger, ForeignKey('plans.id'), nullable=True)
    
    item_price = Column(Numeric)
    quantity = Column(Numeric)
    total_sum = Column(Numeric)
    
    contract = relationship("Contract", back_populates="units")
    plan_point = relationship("PlanPoint", back_populates="units")