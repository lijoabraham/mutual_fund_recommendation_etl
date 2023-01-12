from sqlalchemy import create_engine
from sqlalchemy.engine import url
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.scoping import scoped_session

def _connect_to_database(config, echo_sql=False):

        connection_url = url.URL(
            drivername='mysql+mysqlconnector',
            username='root',
            password='root',
            host='host.docker.internal',
            port='3306',
            database='superset')

        engine = create_engine(connection_url, echo=echo_sql)
        
        db_session = scoped_session(sessionmaker(autocommit=False,
                                                 autoflush=True,
                                                 bind=engine))
        return db_session