from src.db.connection import get_connection


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
RETURNING source_id, source_name;
"""


SOURCE = {
    "source_name": "Yahoo Finance Brent Futures",
    "source_type": "website",
    "base_url": "https://finance.yahoo.com",
    "feed_url": None,
    "domain_name": "finance.yahoo.com",
    "topic_focus": "oil_market,brent,historical_prices",
}


def main():
    print("Seeding oil price source into app.sources...")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_SOURCE_SQL, SOURCE)
            source_id, source_name = cur.fetchone()
            print(f"Upserted source_id={source_id} | {source_name}")

    print("Done.")


if __name__ == "__main__":
    main()