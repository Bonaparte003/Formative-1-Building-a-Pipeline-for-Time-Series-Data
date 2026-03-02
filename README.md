# Machine Learning Pipeline – Time Series (Formative 1)

Group assignment: time-series data preprocessing, modeling, and database design (MySQL + MongoDB cache).

## Dataset

LCA (Labor Condition Application) data by fiscal year (`archive/LCA_FY_2017.csv` … `LCA_FY_2022.csv`). Time dimension: **fiscal year + quarter**. Prediction target: e.g. **Prevailing Wage** (regression) or **Case Status** (classification).

## Architecture

- **MySQL**: main database (source of truth) – 3 tables: `periods`, `employers`, `cases`. See `docs/ERD.md` and `database/sql/schema.sql`.
- **MongoDB**: cache for fast retrieval (latest record, date range). See `docs/MONGODB_DESIGN.md`.
- **API**: CRUD and time-series endpoints for both SQL and MongoDB (Task 3).
- **Prediction script**: fetch from API → preprocess → load model → predict (Task 4).

## Repository structure

```
├── archive/              # Raw LCA CSVs
├── data/                 # Processed data (Task 1)
├── database/
│   ├── sql/              # Schema + queries
│   └── mongodb/          # Sample docs + queries
├── docs/                 # ERD, MongoDB design, data flow
├── notebooks/            # EDA and analytical questions (Task 1)
├── scripts/              # preprocess, train_model, predict
├── api/                  # CRUD + time-series endpoints (Task 3)
├── models/               # Saved model + config
├── outputs/              # Figures, experiments, predictions
├── ASSIGNMENT_STRUCTURE.md
└── README.md
```

## Setup

1. **Python**: Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # or venv\Scripts\activate on Windows
   pip install -r requirements.txt
   ```
2. **SQL**: The API uses SQLite by default (`data/lca.db`). For MySQL, set `DATABASE_URL` and run `database/sql/schema.sql`.
3. **MongoDB** (optional cache): Set `MONGODB_URI`; create collection `lca_records` and indexes (see `docs/MONGODB_DESIGN.md`).

## How to run the full pipeline

From the project root (with `venv` activated):

```bash
# 1. Preprocess raw CSVs → data/processed/lca_unified.csv
python scripts/preprocess.py

# 2. EDA and 5 analytical questions (figures → outputs/figures/)
python scripts/eda.py

# 3. Train model and save experiment table (models/, outputs/experiments/)
python scripts/train_model.py

# 4. Seed SQL DB for API (uses SQLite by default)
python scripts/seed_db.py

# 5. Start API
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# 6. In a second terminal: seed MongoDB cache (requires API to be running)
 python scripts/seed_mongo.py

# 7. Run prediction script end-to-end (fetches from API, preprocesses, predicts)
python scripts/predict.py
```

API docs: http://localhost:8000/docs

## Tasks (summary)

| Task | Description |
|------|-------------|
| **1** | Preprocessing, EDA, ≥5 analytical questions (≥2 with lags/moving averages), train model with ≥2 experiments. |
| **2** | Relational schema (3+ tables), ERD, SQL DDL and ≥3 queries; MongoDB collection design, sample docs, ≥3 queries. |
| **3** | CRUD (POST, GET, PUT, DELETE) and time-series (latest, date range) for both SQL and MongoDB. |
| **4** | Prediction script: fetch → preprocess → load model → predict. |

## Deliverables

- **Report (PDF)**: Problem definition, dataset justification, implementation of Tasks 1–4, ERD figure, query results, team contributions.
- **GitHub repo**: Code, this README, clear structure and commit history.

See **ASSIGNMENT_STRUCTURE.md** for the full roadmap and ERD emphasis.
