# clean_data.py
from bs4 import BeautifulSoup
from typing import Dict, List
from datetime import datetime
import dateutil.parser

REQUIRED_FIELDS = ["title", "url", "content", "publishedAt"]

def strip_html(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def normalize_article(raw: Dict) -> Dict:
    """Return cleaned article or None if invalid."""
    article = {}
    # Pull common shapes from NewsAPI style or generic RSS
    article["title"] = raw.get("title") or raw.get("headline") or ""
    article["url"] = raw.get("url") or raw.get("link") or ""
    content = raw.get("content") or raw.get("description") or raw.get("summary") or ""
    # Remove HTML
    article["content"] = strip_html(content)
    # Source
    source = raw.get("source")
    if isinstance(source, dict):
        article["source"] = source.get("name") or source.get("id") or ""
    else:
        article["source"] = raw.get("source") or raw.get("source_name") or ""
    # publishedAt normalization
    pub = raw.get("publishedAt") or raw.get("pubDate") or raw.get("published") or ""
    try:
        if pub:
            dt = dateutil.parser.parse(pub)
        else:
            dt = datetime.utcnow()
    except Exception:
        dt = datetime.utcnow()
    article["publishedAt"] = dt
    # Validate required
    for k in REQUIRED_FIELDS:
        if not article.get(k):
            return None
    # Trim fields
    article["title"] = article["title"].strip()
    article["url"] = article["url"].strip()
    return article

def clean_batch(raw_articles: List[Dict]) -> List[Dict]:
    cleaned = []
    seen_urls = set()
    for raw in raw_articles:
        art = normalize_article(raw)
        if not art:
            continue
        if art["url"] in seen_urls:
            continue
        seen_urls.add(art["url"])
        cleaned.append(art)
    return cleaned
