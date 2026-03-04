import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# SQL: use SQLite by default so the app runs without MySQL
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{PROJECT_ROOT / 'data' / 'lca.db'}")
# MongoDB: optional cache
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "lca_cache")
MONGODB_COLLECTION = "lca_records"
