# MongoDB cache layer.
# Tries real MongoDB first; falls back to mongomock for local development
# when a MongoDB server is not available.
from .config import MONGODB_URI, MONGODB_DB, MONGODB_COLLECTION

_client = None
_db = None
_using_mock = False


def get_mongo():
    """Return the MongoDB collection (real or mock).  Returns None only if
    mongomock is also unavailable."""
    global _client, _db, _using_mock

    # ── Try real MongoDB ────────────────────────────────────────────────────
    if _client is None:
        try:
            from pymongo import MongoClient
            c = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
            c.admin.command("ping")          # raises if not reachable
            _client = c
            _db = _client[MONGODB_DB]
            _using_mock = False
            print("[MongoDB] Connected to real MongoDB.")
        except Exception:
            # ── Fall back to mongomock ──────────────────────────────────────
            try:
                import mongomock
                _client = mongomock.MongoClient()
                _db = _client[MONGODB_DB]
                _using_mock = True
                print("[MongoDB] Real MongoDB unavailable – using mongomock (in-memory).")
            except Exception as e:
                print(f"[MongoDB] mongomock also unavailable: {e}")
                return None

    # ── If real client was previously initialised, re-ping ─────────────────
    if not _using_mock:
        try:
            _client.admin.command("ping")
        except Exception:
            _client = None
            return get_mongo()          # retry / fall back

    return _db[MONGODB_COLLECTION]


def is_using_mock() -> bool:
    """True when running on mongomock instead of a real MongoDB server."""
    get_mongo()   # ensure initialised
    return _using_mock
