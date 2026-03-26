from src.db.connection import get_connection


def main():
    print("Connecting to PostgreSQL...")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = 'app'
                ORDER BY table_name;
                """
            )
            rows = cur.fetchall()

    print("Connection successful.")
    print(f"Found {len(rows)} tables in schema 'app':")

    for schema_name, table_name in rows:
        print(f"- {schema_name}.{table_name}")


if __name__ == "__main__":
    main()