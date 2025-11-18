import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class PostgresSQLClient:
    def __init__(self, database, user, password, host='0.0.0.0', port='5434'):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.engine = None
        self.Session = None
        self._connect()

    def _connect(self):
        connection = f"postgresql://{self.user}:{self.password}@{self.host}/{self.database}"
        self.engine = create_engine(connection)
        self.Session = sessionmaker(bind=self.engine)

    def create_all(self):
        Base.metadata.create_all(self.engine)

    def drop_all(self):
        Base.metadata.drop_all(self.engine)

    def get_session(self):
        return self.Session()
    
    def execute_query(self, query, values=None):
        with self.engine.connect() as connection:
            if values:
                connection.execute(query, values)
            else:
                connection.execute(query)

    def get_columns(self, table):
        df = pd.read_sql(f"SELECT * FROM {table} LIMIT 0", self.engine)
        return df.columns
