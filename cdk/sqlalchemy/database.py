from sqlalchemy import Table, Column, String, DATE, MetaData, Integer, DATETIME
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base
import urllib.parse
from os import environ, getenv

rds_host  = environ['prod_NBIN_RDS_HOST']
username = environ['prod_NBIN_SQL_USERNAME']
password = urllib.parse.quote_plus(environ['prod_NBIN_SQL_PASSWORD'])
db_name = getenv('prod_NBIN_DN_NAME', 'nbinDataFeed')
environment='production'

rds_host  = "criwycoituxs.ca-central-1.rds.amazonaws.com"
username = "nbinFeed"
password = urllib.parse.quote_plus("QWe@lth!1")
db_name = "nbinDataFeed"

engine = create_engine(f'mysql+pymysql://{username}:{password}@qw-{environment}.{rds_host}/{db_name}')

Base = declarative_base()
metadata = MetaData()
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
query = db_session.query
                                         

metadata_obj = MetaData()

class IbmsmProcessLog(Base):
    __table__ = Table(
        "ibmsm_process_log",
        metadata_obj,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('filename', String(255)),
        Column('process_date', DATE),
        Column('mtime', DATETIME)
    )

