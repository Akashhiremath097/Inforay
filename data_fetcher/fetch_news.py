# fetch_news.py
import logging
import requests
from time import sleep
from typing import List, Dict
from config import NEWSAPI_KEY, USER_AGENT, FETCH_PAGE_SIZE
from clean_data import clean_batch
from db_manager import DBManager
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/fetch_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fetch_news")

NEWSAPI_ENDPOINT = "https://newsapi.org/v2/top-headlines"
# You can switch to other endpoints or RSS feeds as needed.

def fetch_from_newsapi(page_size: int = 100, country: str = "us") -> List[Dict]:
    if not NEWSAPI_KEY:
        raise RuntimeError("NEWSAPI_KEY not set in environment.")
    headers = {"User-Agent": USER_AGENT}
    params = {
        "apiKey": NEWSAPI_KEY,
        "pageSize": page_size,
        "country": country,
    }
    tries = 0
    while tries < 3:
        try:
            resp = requests.get(NEWSAPI_ENDPOINT, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            articles = data.get("articles", [])
            logger.info("Fetched %d raw articles from NewsAPI", len(articles))
            return articles
        except Exception as e:
            tries += 1
            logger.warning("NewsAPI call failed (try %d): %s", tries, e)
            sleep(2 ** tries)
    logger.error("NewsAPI failed after retries")
    return []

def main():
    logger.info("Starting fetch run")
    db = DBManager()
    try:
        raw = fetch_from_newsapi(page_size=FETCH_PAGE_SIZE)
        cleaned = clean_batch(raw)
        stats_cleanup = {"raw_fetched": len(raw), "cleaned": len(cleaned)}
        logger.info("Cleaned articles: %d (fetched %d)", stats_cleanup["cleaned"], stats_cleanup["raw_fetched"])
        stats = db.insert_articles(cleaned)
        logger.info("Insert stats: added=%d duplicates=%d errors=%d", stats["added"], stats["duplicates"], stats["errors"])
    finally:
        db.close()
        logger.info("Fetch run complete")

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    main()
