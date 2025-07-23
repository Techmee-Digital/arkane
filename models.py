import os
from sqlalchemy import (
    create_engine,
    Column, Integer, String, DateTime
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Lead(Base):
    __tablename__ = 'leads'
    id          = Column(Integer, primary_key=True, autoincrement=True)
    email       = Column(String(255), nullable=False)
    company     = Column(String(255), nullable=False)
    quarter     = Column(String(10),   nullable=False)
    campaign    = Column(String(100),  nullable=False)
    upload_date = Column(DateTime,     nullable=False)

# ⚠️ If you already have a leads table, drop or rename it before running:
#    DROP TABLE IF EXISTS leads;
Base.metadata.create_all(engine)
