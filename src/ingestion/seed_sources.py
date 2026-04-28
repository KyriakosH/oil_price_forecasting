from src.db.connection import get_connection
from src.ingestion.sources import ALL_SOURCES


def seed_sources() -> None:
    inserted = 0
    updated = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            for source in ALL_SOURCES:
                cur.execute(
                    """
                    SELECT source_id
                    FROM app.sources
                    WHERE source_name = %s;
                    """,
                    (source["source_name"],),
                )

                existing = cur.fetchone()

                if existing:
                    cur.execute(
                        """
                        UPDATE app.sources
                        SET
                            source_type = %s,
                            base_url = %s,
                            feed_url = %s,
                            domain_name = %s,
                            topic_focus = %s,
                            is_active = TRUE,
                            updated_at = NOW()
                        WHERE source_name = %s;
                        """,
                        (
                            source["source_type"],
                            source["base_url"],
                            source["feed_url"],
                            source["domain_name"],
                            source["topic_focus"],
                            source["source_name"],
                        ),
                    )
                    updated += 1
                else:
                    cur.execute(
                        """
                        INSERT INTO app.sources (
                            source_name,
                            source_type,
                            base_url,
                            feed_url,
                            domain_name,
                            topic_focus,
                            is_active
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, TRUE);
                        """,
                        (
                            source["source_name"],
                            source["source_type"],
                            source["base_url"],
                            source["feed_url"],
                            source["domain_name"],
                            source["topic_focus"],
                        ),
                    )
                    inserted += 1

        conn.commit()

    print(f"Sources seeded successfully.")
    print(f"Inserted: {inserted}")
    print(f"Updated: {updated}")
    print(f"Total sources processed: {inserted + updated}")


def main() -> None:
    seed_sources()


if __name__ == "__main__":
    main()