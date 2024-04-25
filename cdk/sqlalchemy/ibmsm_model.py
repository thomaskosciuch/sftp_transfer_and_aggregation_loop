from sqlalchemy import Column, Integer, String, Float, Date, Boolean, create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Create an engine and create tables in the database
engine = create_engine('sqlite:///securities.db')
Base.metadata.create_all(engine)

