# db_manager.py
import logging
from pymongo import MongoClient, errors
from pymongo.collection import Collection
from typing import List, Dict
from datetime import datetime
import hashlib
from config import MONGO_URI, MONGO_DB

logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self, mongo_uri: str = MONGO_URI, db_name: str = MONGO_DB):
        self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[db_name]
        self.raw_col: Collection = self.db["raw_articles"]
        # Ensure unique index on url_hash
        try:
            self.raw_col.create_index("url_hash", unique=True)
            logger.debug("Ensured unique index on url_hash")
        except Exception as e:
            logger.exception("Failed to create index: %s", e)

    @staticmethod
    def _hash_url(url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def insert_articles(self, articles: List[Dict]) -> Dict[str, int]:
        """Insert articles. Returns stats dict."""
        added = 0
        skipped = 0
        errors = 0
        for art in articles:
            art = dict(art)  # copy
            art.setdefault("fetched_at", datetime.utcnow())
            url = art.get("url", "")
            art["url_hash"] = self._hash_url(url)
            try:
                self.raw_col.insert_one(art)
                added += 1
            except errors.DuplicateKeyError:
                skipped += 1
            except Exception:
                logger.exception("Error inserting article: %s", url)
                errors += 1
        return {"added": added, "duplicates": skipped, "errors": errors}

    def close(self):
        self.client.close()
