# config.py
import os
from dotenv import load_dotenv

load_dotenv()  # loads .env from cwd

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "news_db")
FETCH_PAGE_SIZE = int(os.getenv("FETCH_PAGE_SIZE", "100"))  # if using paged APIs
USER_AGENT = os.getenv("USER_AGENT", "DataFetcher/1.0 (+https://example.com)")
