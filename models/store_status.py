from sqlalchemy import Column, String, Boolean, DateTime, Integer
from sqlalchemy.orm import declarative_base

base = declarative_base()


class StoreStatus(base):
    __tablename__ = 'store_status'
    id = Column(Integer, primary_key=True)
    store_id = Column(String(25))
    status = Column(Boolean)
    timestamp_utc = Column(DateTime)
