from src.db.connection import get_connection
from src.ingestion.rss_sources import RSS_SOURCES


UPSERT_SOURCE_SQL = """
INSERT INTO app.sources (
    source_name,
    source_type,
    base_url,
    feed_url,
    domain_name,
    topic_focus,
    is_active
)
VALUES (
    %(source_name)s,
    %(source_type)s,
    %(base_url)s,
    %(feed_url)s,
    %(domain_name)s,
    %(topic_focus)s,
    TRUE
)
ON CONFLICT (source_name) DO UPDATE
SET
    source_type = EXCLUDED.source_type,
    base_url = EXCLUDED.base_url,
    feed_url = EXCLUDED.feed_url,
    domain_name = EXCLUDED.domain_name,
    topic_focus = EXCLUDED.topic_focus,
    is_active = TRUE,
    updated_at = NOW()
RETURNING source_id, source_name, feed_url;
"""


def main():
    print("Seeding RSS sources into app.sources...")

    with get_connection() as conn:
        with conn.cursor() as cur:
            for source in RSS_SOURCES:
                cur.execute(UPSERT_SOURCE_SQL, source)
                source_id, source_name, feed_url = cur.fetchone()
                print(f"Upserted source_id={source_id} | {source_name} | {feed_url}")

    print("Done.")


if __name__ == "__main__":
    main()