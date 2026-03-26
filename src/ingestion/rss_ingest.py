import calendar
import hashlib
import re
from datetime import datetime, timezone
from html import unescape

import feedparser
from psycopg.types.json import Jsonb

from src.db.connection import get_connection


GET_ACTIVE_RSS_SOURCES_SQL = """
SELECT source_id, source_name, feed_url
FROM app.sources
WHERE source_type = 'rss'
  AND is_active = TRUE
  AND feed_url IS NOT NULL
ORDER BY source_id;
"""

INSERT_ARTICLE_SQL = """
INSERT INTO app.articles_raw (
    source_id,
    external_guid,
    url,
    url_hash,
    title,
    summary,
    content,
    author_name,
    language_code,
    published_at,
    published_date,
    fetched_at,
    raw_payload
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT DO NOTHING
RETURNING article_id;
"""

INSERT_PROCESSED_PLACEHOLDER_SQL = """
INSERT INTO app.articles_processed (article_id)
VALUES (%s)
ON CONFLICT DO NOTHING;
"""


def strip_html(text: str | None) -> str | None:
    if not text:
        return None
    text = unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def build_url_hash(url: str) -> str:
    normalized = url.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def parse_entry_datetime(entry) -> datetime | None:
    for key in ("published_parsed", "updated_parsed"):
        parsed_value = entry.get(key)
        if parsed_value:
            return datetime.fromtimestamp(
                calendar.timegm(parsed_value),
                tz=timezone.utc
            )

    return None


def extract_content(entry) -> str | None:
    content_items = entry.get("content", [])
    if content_items and isinstance(content_items, list):
        first_item = content_items[0]
        if isinstance(first_item, dict):
            return strip_html(first_item.get("value"))

    return strip_html(entry.get("summary"))


def build_raw_payload(entry) -> dict:
    tags = []
    for tag in entry.get("tags", []):
        if isinstance(tag, dict):
            term = tag.get("term")
            if term:
                tags.append(term)

    links = []
    for link in entry.get("links", []):
        if isinstance(link, dict):
            href = link.get("href")
            if href:
                links.append(href)

    return {
        "entry_id": entry.get("id"),
        "title": entry.get("title"),
        "summary": entry.get("summary"),
        "published": entry.get("published"),
        "updated": entry.get("updated"),
        "author": entry.get("author"),
        "tags": tags,
        "links": links,
    }


def ingest_feed(cur, source_id: int, source_name: str, feed_url: str) -> tuple[int, int]:
    print(f"\nFetching: {source_name}")
    print(f"Feed URL: {feed_url}")

    parsed_feed = feedparser.parse(feed_url)

    inserted_count = 0
    skipped_count = 0

    if getattr(parsed_feed, "bozo", False):
        print(f"Warning: feedparser marked this feed as bozo for {source_name}")

    language_code = parsed_feed.feed.get("language")

    for entry in parsed_feed.entries:
        url = entry.get("link")
        title = strip_html(entry.get("title"))

        if not url or not title:
            skipped_count += 1
            continue

        url_hash = build_url_hash(url)
        external_guid = entry.get("id") or entry.get("guid")
        summary = strip_html(entry.get("summary"))
        content = extract_content(entry)
        author_name = entry.get("author")
        published_at = parse_entry_datetime(entry)
        published_date = published_at.date() if published_at else None
        fetched_at = datetime.now(timezone.utc)
        raw_payload = build_raw_payload(entry)

        cur.execute(
            INSERT_ARTICLE_SQL,
            (
                source_id,
                external_guid,
                url,
                url_hash,
                title,
                summary,
                content,
                author_name,
                language_code,
                published_at,
                published_date,
                fetched_at,
                Jsonb(raw_payload),
            ),
        )

        inserted_row = cur.fetchone()

        if inserted_row:
            article_id = inserted_row[0]
            cur.execute(INSERT_PROCESSED_PLACEHOLDER_SQL, (article_id,))
            inserted_count += 1
        else:
            skipped_count += 1

    return inserted_count, skipped_count


def main():
    total_inserted = 0
    total_skipped = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(GET_ACTIVE_RSS_SOURCES_SQL)
            sources = cur.fetchall()

            if not sources:
                print("No active RSS sources found in app.sources.")
                return

            for source_id, source_name, feed_url in sources:
                inserted, skipped = ingest_feed(cur, source_id, source_name, feed_url)
                total_inserted += inserted
                total_skipped += skipped
                print(f"Inserted: {inserted} | Skipped/duplicate: {skipped}")

    print("\nRSS ingestion complete.")
    print(f"Total inserted: {total_inserted}")
    print(f"Total skipped/duplicate: {total_skipped}")


if __name__ == "__main__":
    main()