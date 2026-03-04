# SQL layer: SQLAlchemy with SQLite (or MySQL via DATABASE_URL).
import os
from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Numeric, Date, DateTime, ForeignKey, text
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from datetime import datetime

from .config import DATABASE_URL

# SQLite needs check_same_thread=False for FastAPI
connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    connect_args["check_same_thread"] = False

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Period(Base):
    __tablename__ = "periods"
    period_id = Column(Integer, primary_key=True, autoincrement=True)
    fiscal_year = Column(Integer, nullable=False)
    quarter = Column(String(2), nullable=False)
    start_date = Column(String(10), nullable=False)
    end_date = Column(String(10), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Employer(Base):
    __tablename__ = "employers"
    employer_id = Column(Integer, primary_key=True, autoincrement=True)
    employer_name = Column(String(255), nullable=False)
    employer_location = Column(String(255))
    employer_country = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)


class Case(Base):
    __tablename__ = "cases"
    case_id = Column(Integer, primary_key=True, autoincrement=True)
    period_id = Column(Integer, ForeignKey("periods.period_id"), nullable=False)
    employer_id = Column(Integer, ForeignKey("employers.employer_id"), nullable=False)
    soc_title = Column(String(255))
    visa_class = Column(String(50))
    job_title = Column(String(255))
    full_time_position = Column(String(1))
    worksite = Column(String(255))
    wage = Column(Numeric(12, 2))
    unit_of_pay = Column(String(10))
    case_status = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
