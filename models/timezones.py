
from sqlalchemy import Column, String, Boolean, DateTime, Integer
from sqlalchemy.orm import declarative_base

base = declarative_base()

class Timezones(base):
    __tablename__ = 'timezones'
    id = Column(Integer, primary_key=True)
    store_id = Column(String(25))
    timezone_str = Column(String(30))
