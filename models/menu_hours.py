from sqlalchemy import Column, String, SmallInteger, DateTime, Integer
from sqlalchemy.orm import declarative_base

base = declarative_base()

class MenuHours(base):
    __tablename__ = 'menu_hours'
    id = Column(Integer, primary_key=True)
    store_id = Column(String(25))
    day = Column(SmallInteger)
    start_time_local = Column(DateTime)
    end_time_local = Column(DateTime)
