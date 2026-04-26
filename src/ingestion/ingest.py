import calendar
import hashlib
import re
from datetime import datetime, timezone
from html import unescape
from typing import Any

import feedparser
import requests
from psycopg.types.json import Jsonb

from src.db.connection import get_connection


GET_ACTIVE_SOURCES_SQL = """
SELECT
    source_id,
    source_name,
    source_type,
    feed_url,
    domain_name
FROM app.sources
WHERE source_type IN ('rss', 'api')
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

HEADERS = {
    "User-Agent": "oil-price-forecasting-student-project/1.0"
}


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


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        cleaned = value.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def insert_article(
    cur,
    source_id: int,
    external_guid: str | None,
    url: str,
    title: str,
    summary: str | None,
    content: str | None,
    author_name: str | None,
    language_code: str | None,
    published_at: datetime | None,
    raw_payload: dict[str, Any],
) -> bool:
    url_hash = build_url_hash(url)
    published_date = published_at.date() if published_at else None
    fetched_at = datetime.now(timezone.utc)

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
        return True

    return False


# ============================================================
# RSS ingestion
# ============================================================

def parse_entry_datetime(entry) -> datetime | None:
    for key in ("published_parsed", "updated_parsed"):
        parsed_value = entry.get(key)

        if parsed_value:
            return datetime.fromtimestamp(
                calendar.timegm(parsed_value),
                tz=timezone.utc,
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


def ingest_rss_feed(cur, source_id: int, source_name: str, feed_url: str) -> tuple[int, int]:
    print(f"\nFetching RSS: {source_name}")
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

        external_guid = entry.get("id") or entry.get("guid")
        summary = strip_html(entry.get("summary"))
        content = extract_content(entry)
        author_name = entry.get("author")
        published_at = parse_entry_datetime(entry)
        raw_payload = build_raw_payload(entry)

        inserted = insert_article(
            cur=cur,
            source_id=source_id,
            external_guid=external_guid,
            url=url,
            title=title,
            summary=summary,
            content=content,
            author_name=author_name,
            language_code=language_code,
            published_at=published_at,
            raw_payload=raw_payload,
        )

        if inserted:
            inserted_count += 1
        else:
            skipped_count += 1

    return inserted_count, skipped_count


# ============================================================
# API helpers
# ============================================================

def fetch_json(url: str) -> dict[str, Any] | list[Any] | None:
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)

        print(f"Status code: {response.status_code}")

        if response.status_code != 200:
            print("Failed response:")
            print(response.text[:500])
            return None

        if not response.text.strip():
            print("Empty response body.")
            return None

        try:
            return response.json()
        except ValueError:
            print("Failed to parse JSON response.")
            print("First 500 characters returned:")
            print(response.text[:500])
            return None

    except requests.RequestException as exc:
        print(f"Request failed: {exc}")
        return None


# ============================================================
# Hacker News ingestion
# ============================================================

def ingest_hacker_news(cur, source_id: int, source_name: str, feed_url: str) -> tuple[int, int]:
    print(f"\nFetching Hacker News API: {source_name}")
    print(f"URL: {feed_url}")

    payload = fetch_json(feed_url)

    if not isinstance(payload, dict):
        return 0, 0

    hits = payload.get("hits", [])

    inserted_count = 0
    skipped_count = 0

    for item in hits:
        if not isinstance(item, dict):
            skipped_count += 1
            continue

        object_id = item.get("objectID")
        title = strip_html(item.get("title") or item.get("story_title"))

        url = item.get("url")

        if not url and object_id:
            url = f"https://news.ycombinator.com/item?id={object_id}"

        if not title or not url:
            skipped_count += 1
            continue

        created_at = parse_iso_datetime(item.get("created_at"))

        content = strip_html(
            item.get("story_text")
            or item.get("comment_text")
            or title
        )

        summary = content

        raw_payload = {
            "source_api": "hacker_news_algolia",
            "objectID": object_id,
            "title": item.get("title"),
            "story_title": item.get("story_title"),
            "url": item.get("url"),
            "author": item.get("author"),
            "points": item.get("points"),
            "num_comments": item.get("num_comments"),
            "created_at": item.get("created_at"),
            "story_text": item.get("story_text"),
            "comment_text": item.get("comment_text"),
        }

        inserted = insert_article(
            cur=cur,
            source_id=source_id,
            external_guid=f"hn_{object_id}" if object_id else None,
            url=url,
            title=title,
            summary=summary,
            content=content,
            author_name=item.get("author"),
            language_code="en",
            published_at=created_at,
            raw_payload=raw_payload,
        )

        if inserted:
            inserted_count += 1
        else:
            skipped_count += 1

    return inserted_count, skipped_count


# ============================================================
# Mastodon ingestion
# ============================================================

def ingest_mastodon(cur, source_id: int, source_name: str, feed_url: str) -> tuple[int, int]:
    print(f"\nFetching Mastodon API: {source_name}")
    print(f"URL: {feed_url}")

    payload = fetch_json(feed_url)

    if not isinstance(payload, list):
        return 0, 0

    inserted_count = 0
    skipped_count = 0

    for item in payload:
        if not isinstance(item, dict):
            skipped_count += 1
            continue

        status_id = item.get("id")
        url = item.get("url")
        content = strip_html(item.get("content"))

        if not status_id or not url or not content:
            skipped_count += 1
            continue

        title = content[:120]
        created_at = parse_iso_datetime(item.get("created_at"))

        account = item.get("account") or {}
        author_name = None

        if isinstance(account, dict):
            author_name = account.get("acct")

        raw_payload = {
            "source_api": "mastodon",
            "id": status_id,
            "url": url,
            "created_at": item.get("created_at"),
            "content": item.get("content"),
            "language": item.get("language"),
            "replies_count": item.get("replies_count"),
            "reblogs_count": item.get("reblogs_count"),
            "favourites_count": item.get("favourites_count"),
            "account": {
                "acct": author_name,
                "bot": account.get("bot") if isinstance(account, dict) else None,
            },
        }

        inserted = insert_article(
            cur=cur,
            source_id=source_id,
            external_guid=f"mastodon_{status_id}",
            url=url,
            title=title,
            summary=content,
            content=content,
            author_name=author_name,
            language_code=item.get("language") or "en",
            published_at=created_at,
            raw_payload=raw_payload,
        )

        if inserted:
            inserted_count += 1
        else:
            skipped_count += 1

    return inserted_count, skipped_count


# ============================================================
# Source router
# ============================================================

def ingest_source(
    cur,
    source_id: int,
    source_name: str,
    source_type: str,
    feed_url: str,
    domain_name: str | None,
) -> tuple[int, int]:
    if source_type == "rss":
        return ingest_rss_feed(cur, source_id, source_name, feed_url)

    if source_type == "api" and domain_name == "hn.algolia.com":
        return ingest_hacker_news(cur, source_id, source_name, feed_url)

    if source_type == "api" and domain_name == "mastodon.social":
        return ingest_mastodon(cur, source_id, source_name, feed_url)

    print(f"\nSkipping unsupported source: {source_name} | {source_type} | {domain_name}")
    return 0, 0


def main():
    total_inserted = 0
    total_skipped = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(GET_ACTIVE_SOURCES_SQL)
            sources = cur.fetchall()

            if not sources:
                print("No active RSS/API sources found in app.sources.")
                return

            for source_id, source_name, source_type, feed_url, domain_name in sources:
                inserted, skipped = ingest_source(
                    cur=cur,
                    source_id=source_id,
                    source_name=source_name,
                    source_type=source_type,
                    feed_url=feed_url,
                    domain_name=domain_name,
                )

                total_inserted += inserted
                total_skipped += skipped

                print(f"Inserted: {inserted} | Skipped/duplicate: {skipped}")

    print("\nRSS/API ingestion complete.")
    print(f"Total inserted: {total_inserted}")
    print(f"Total skipped/duplicate: {total_skipped}")


if __name__ == "__main__":
    main()