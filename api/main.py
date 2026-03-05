# Task 3: CRUD and time-series endpoints for SQL and MongoDB.
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

from .config import PROJECT_ROOT
from .database_sql import init_db, get_db, Period, Employer, Case
from .database_mongo import get_mongo


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield
    # shutdown if needed
    pass


app = FastAPI(title="LCA Time-Series API", lifespan=lifespan)


# --- Pydantic schemas ---
class PeriodCreate(BaseModel):
    fiscal_year: int
    quarter: str
    start_date: str
    end_date: str


class EmployerCreate(BaseModel):
    employer_name: str
    employer_location: Optional[str] = None
    employer_country: Optional[str] = None


class CaseCreate(BaseModel):
    period_id: int
    employer_id: int
    soc_title: Optional[str] = None
    visa_class: Optional[str] = None
    job_title: Optional[str] = None
    full_time_position: Optional[str] = None
    worksite: Optional[str] = None
    wage: Optional[float] = None
    unit_of_pay: Optional[str] = None
    case_status: Optional[str] = None


class CaseUpdate(BaseModel):
    """All fields optional for partial (PATCH-style) PUT updates."""
    period_id: Optional[int] = None
    employer_id: Optional[int] = None
    soc_title: Optional[str] = None
    visa_class: Optional[str] = None
    job_title: Optional[str] = None
    full_time_position: Optional[str] = None
    worksite: Optional[str] = None
    wage: Optional[float] = None
    unit_of_pay: Optional[str] = None
    case_status: Optional[str] = None


class CaseResponse(BaseModel):
    case_id: int
    period_id: int
    employer_id: int
    soc_title: Optional[str]
    visa_class: Optional[str]
    job_title: Optional[str]
    wage: Optional[float]
    case_status: Optional[str]
    created_at: Optional[datetime]

    class Config:
        from_attributes = True


# --- SQL CRUD: Periods ---
@app.post("/sql/periods", response_model=dict)
def sql_create_period(p: PeriodCreate, db: Session = Depends(get_db)):
    period = Period(
        fiscal_year=p.fiscal_year,
        quarter=p.quarter,
        start_date=p.start_date,
        end_date=p.end_date,
    )
    db.add(period)
    db.commit()
    db.refresh(period)
    return {"period_id": period.period_id, "fiscal_year": period.fiscal_year, "quarter": period.quarter}


@app.get("/sql/periods", response_model=List[dict])
def sql_list_periods(db: Session = Depends(get_db)):
    rows = db.query(Period).all()
    return [{"period_id": r.period_id, "fiscal_year": r.fiscal_year, "quarter": r.quarter} for r in rows]


# --- SQL CRUD: Employers ---
@app.post("/sql/employers", response_model=dict)
def sql_create_employer(e: EmployerCreate, db: Session = Depends(get_db)):
    emp = Employer(
        employer_name=e.employer_name,
        employer_location=e.employer_location,
        employer_country=e.employer_country,
    )
    db.add(emp)
    db.commit()
    db.refresh(emp)
    return {"employer_id": emp.employer_id, "employer_name": emp.employer_name}


@app.get("/sql/employers", response_model=List[dict])
def sql_list_employers(db: Session = Depends(get_db)):
    rows = db.query(Employer).all()
    return [{"employer_id": r.employer_id, "employer_name": r.employer_name} for r in rows]


# --- SQL CRUD: Cases ---
@app.post("/sql/cases", response_model=dict)
def sql_create_case(c: CaseCreate, db: Session = Depends(get_db)):
    case = Case(
        period_id=c.period_id,
        employer_id=c.employer_id,
        soc_title=c.soc_title,
        visa_class=c.visa_class,
        job_title=c.job_title,
        full_time_position=c.full_time_position,
        worksite=c.worksite,
        wage=float(c.wage) if c.wage is not None else None,
        unit_of_pay=c.unit_of_pay,
        case_status=c.case_status,
    )
    db.add(case)
    db.commit()
    db.refresh(case)
    return {"case_id": case.case_id}


@app.get("/sql/cases", response_model=List[dict])
def sql_list_cases(limit: int = Query(100, le=1000), db: Session = Depends(get_db)):
    rows = db.query(Case).order_by(Case.case_id.desc()).limit(limit).all()
    return [
        {
            "case_id": r.case_id,
            "period_id": r.period_id,
            "employer_id": r.employer_id,
            "wage": float(r.wage) if r.wage else None,
            "case_status": r.case_status,
        }
        for r in rows
    ]


@app.get("/sql/cases/{case_id}", response_model=dict)
def sql_get_case(case_id: int, db: Session = Depends(get_db)):
    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise HTTPException(404, "Case not found")
    return {
        "case_id": case.case_id,
        "period_id": case.period_id,
        "employer_id": case.employer_id,
        "soc_title": case.soc_title,
        "visa_class": case.visa_class,
        "job_title": case.job_title,
        "wage": float(case.wage) if case.wage else None,
        "case_status": case.case_status,
        "created_at": case.created_at.isoformat() if case.created_at else None,
    }


@app.put("/sql/cases/{case_id}", response_model=dict)
def sql_update_case(case_id: int, c: CaseUpdate, db: Session = Depends(get_db)):
    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise HTTPException(404, "Case not found")
    # Only overwrite fields that were explicitly provided
    if c.period_id is not None:   case.period_id = c.period_id
    if c.employer_id is not None: case.employer_id = c.employer_id
    if c.soc_title is not None:   case.soc_title = c.soc_title
    if c.visa_class is not None:  case.visa_class = c.visa_class
    if c.job_title is not None:   case.job_title = c.job_title
    if c.wage is not None:        case.wage = float(c.wage)
    if c.case_status is not None: case.case_status = c.case_status
    if c.worksite is not None:    case.worksite = c.worksite
    if c.unit_of_pay is not None: case.unit_of_pay = c.unit_of_pay
    db.commit()
    return {"case_id": case_id, "updated": True}


@app.delete("/sql/cases/{case_id}", response_model=dict)
def sql_delete_case(case_id: int, db: Session = Depends(get_db)):
    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise HTTPException(404, "Case not found")
    db.delete(case)
    db.commit()
    return {"case_id": case_id, "deleted": True}


# --- SQL Time-series: latest record ---
@app.get("/sql/cases/ts/latest", response_model=dict)
def sql_latest_case(db: Session = Depends(get_db)):
    case = db.query(Case).order_by(Case.created_at.desc()).first()
    if not case:
        raise HTTPException(404, "No cases found")
    return {
        "case_id": case.case_id,
        "period_id": case.period_id,
        "employer_id": case.employer_id,
        "wage": float(case.wage) if case.wage else None,
        "case_status": case.case_status,
        "created_at": case.created_at.isoformat() if case.created_at else None,
    }


# --- SQL Time-series: by date range (via period start_date/end_date) ---
@app.get("/sql/cases/ts/date_range", response_model=List[dict])
def sql_cases_by_date_range(
    start_date: str = Query(..., description="e.g. 2021-01-01"),
    end_date: str = Query(..., description="e.g. 2022-12-31"),
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(Case)
        .join(Period, Case.period_id == Period.period_id)
        .filter(Period.start_date <= end_date, Period.end_date >= start_date)
        .order_by(Period.start_date)
        .limit(limit)
        .all()
    )
    return [
        {
            "case_id": r.case_id,
            "period_id": r.period_id,
            "wage": float(r.wage) if r.wage else None,
            "case_status": r.case_status,
        }
        for r in rows
    ]


# --- MongoDB CRUD and time-series (cache) ---
def _doc_to_response(d):
    if not d:
        return None
    d = dict(d)
    d.pop("_id", None)
    for field in ("created_at", "cached_at"):
        val = d.get(field)
        if val and not isinstance(val, str):
            d[field] = val.isoformat()
    return d


@app.post("/mongo/cases", response_model=dict)
def mongo_create_case(body: dict):
    coll = get_mongo()
    if coll is None:
        raise HTTPException(503, "MongoDB not available")
    body["cached_at"] = datetime.utcnow()
    if "created_at" not in body:
        body["created_at"] = datetime.utcnow()
    coll.insert_one(body)
    return {"inserted": True, "case_id": body.get("case_id")}


@app.get("/mongo/cases", response_model=List[dict])
def mongo_list_cases(limit: int = Query(100, le=1000)):
    coll = get_mongo()
    if coll is None:
        raise HTTPException(503, "MongoDB not available")
    cursor = coll.find().sort("created_at", -1).limit(limit)
    return [_doc_to_response(d) for d in cursor]


@app.get("/mongo/cases/{case_id}", response_model=dict)
def mongo_get_case(case_id: int):
    coll = get_mongo()
    if coll is None:
        raise HTTPException(503, "MongoDB not available")
    d = coll.find_one({"case_id": case_id})
    if not d:
        raise HTTPException(404, "Case not found")
    return _doc_to_response(d)


@app.put("/mongo/cases/{case_id}", response_model=dict)
def mongo_update_case(case_id: int, body: dict):
    coll = get_mongo()
    if coll is None:
        raise HTTPException(503, "MongoDB not available")
    body["cached_at"] = datetime.utcnow()
    r = coll.update_one({"case_id": case_id}, {"$set": body})
    if r.matched_count == 0:
        raise HTTPException(404, "Case not found")
    return {"updated": True, "case_id": case_id}


@app.delete("/mongo/cases/{case_id}", response_model=dict)
def mongo_delete_case(case_id: int):
    coll = get_mongo()
    if coll is None:
        raise HTTPException(503, "MongoDB not available")
    r = coll.delete_one({"case_id": case_id})
    if r.deleted_count == 0:
        raise HTTPException(404, "Case not found")
    return {"deleted": True, "case_id": case_id}


@app.get("/mongo/cases/ts/latest", response_model=dict)
def mongo_latest_case():
    coll = get_mongo()
    if coll is None:
        raise HTTPException(503, "MongoDB not available")
    d = coll.find_one(sort=[("created_at", -1)])
    if not d:
        raise HTTPException(404, "No documents found")
    return _doc_to_response(d)


@app.get("/mongo/cases/ts/date_range", response_model=List[dict])
def mongo_cases_by_date_range(
    start_date: str = Query(..., description="e.g. 2021-01-01"),
    end_date: str = Query(..., description="e.g. 2022-12-31"),
    limit: int = Query(100, le=1000),
):
    coll = get_mongo()
    if coll is None:
        raise HTTPException(503, "MongoDB not available")
    cursor = coll.find(
        {
            "period.start_date": {"$gte": start_date},
            "period.end_date": {"$lte": end_date},
        }
    ).sort("period.start_date", 1).limit(limit)
    return [_doc_to_response(d) for d in cursor]


@app.get("/")
def root():
    return {"message": "LCA Time-Series API", "docs": "/docs"}
