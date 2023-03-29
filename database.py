import sqlalchemy
from sqlalchemy.orm import sessionmaker


class Database():
    engine = sqlalchemy.create_engine('postgresql://postgres:yk107@localhost/postgres')
    def __init__(self):
        self.SESSION = sessionmaker(bind=self.engine)

db = Database()