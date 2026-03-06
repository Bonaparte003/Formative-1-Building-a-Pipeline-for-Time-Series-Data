# Data Flow: SQL (Main) and MongoDB (Cache)

## Architecture Overview

```
                    ┌─────────────────────────────────────────┐
                    │           Raw LCA CSVs (archive/)         │
                    └─────────────────────┬───────────────────┘
                                          │
                                          ▼
                    ┌─────────────────────────────────────────┐
                    │  Preprocessing (Task 1)                  │
                    │  – Unify schema, add fiscal_year/quarter │
                    │  – Train/test split, feature engineering │
                    └─────────────────────┬───────────────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              │                           │                           │
              ▼                           ▼                           ▼
┌─────────────────────────┐  ┌─────────────────────────┐  ┌─────────────────────────┐
│  MySQL (main DB)         │  │  Model training          │  │  MongoDB (cache)        │
│  – periods               │  │  – Load from SQL or      │  │  – Populated from SQL   │
│  – employers             │  │    processed data        │  │  – Indexed for fast read │
│  – cases                 │  │  – Save model to disk    │  │  – Latest / date range   │
└────────────┬─────────────┘  └─────────────────────────┘  └────────────┬────────────┘
             │                                                           │
             │         ┌─────────────────────────────────┐              │
             └────────▶│  API (Task 3)                    │◀─────────────┘
                       │  – CRUD for SQL and MongoDB      │
                       │  – GET latest record             │
                       │  – GET by date range             │
                       └────────────────┬────────────────┘
                                        │
                                        ▼
                       ┌─────────────────────────────────┐
                       │  Prediction script (Task 4)      │
                       │  – Fetch from API → preprocess   │
                       │  – Load model → predict          │
                       └─────────────────────────────────┘
```

## Responsibilities

| Component    | Role |
|-------------|------|
| **MySQL**   | Single source of truth; all authoritative writes; full history. |
| **MongoDB** | Cache for read-heavy time-series endpoints (latest, date range); can be synced from SQL on write or periodically. |
| **API**     | Exposes CRUD and time-series endpoints for both backends; can implement “read from MongoDB first, fallback to SQL” for performance. |

## ERD Placement

The **ERD** in `docs/ERD.md` describes only the **relational (MySQL)** schema. The MongoDB collection is a denormalized cache view of the same data; its design is in `docs/MONGODB_DESIGN.md`.
