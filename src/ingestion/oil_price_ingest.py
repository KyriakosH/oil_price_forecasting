from datetime import datetime, timezone

import yfinance as yf

from src.db.connection import get_connection


BENCHMARK_CODE = "BRENT"
YAHOO_TICKER = "BZ=F"
START_DATE = "2007-01-01"


GET_SOURCE_ID_SQL = """
SELECT source_id
FROM app.sources
WHERE source_name = %s
LIMIT 1;
"""

UPSERT_OIL_PRICE_SQL = """
INSERT INTO app.oil_prices (
    benchmark_code,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    adjusted_close_price,
    volume,
    currency_code,
    source_id,
    ingested_at
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (benchmark_code, price_date) DO UPDATE
SET
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    adjusted_close_price = EXCLUDED.adjusted_close_price,
    volume = EXCLUDED.volume,
    currency_code = EXCLUDED.currency_code,
    source_id = EXCLUDED.source_id,
    ingested_at = EXCLUDED.ingested_at;
"""


def to_python_value(value):
    if value is None:
        return None

    try:
        if value != value:
            return None
    except Exception:
        pass

    try:
        return value.item()
    except Exception:
        return value


def fetch_brent_history():
    print(f"Downloading {YAHOO_TICKER} from Yahoo Finance...")

    df = yf.download(
        YAHOO_TICKER,
        start=START_DATE,
        end=None,
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        raise RuntimeError("No data returned from Yahoo Finance.")

    # Flatten MultiIndex columns if present
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)

    required_cols = ["Open", "High", "Low", "Close"]
    for col in required_cols:
        if col not in df.columns:
            raise RuntimeError(f"Expected column '{col}' not found in downloaded data.")

    return df


def main():
    df = fetch_brent_history()
    ingested_at = datetime.now(timezone.utc)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(GET_SOURCE_ID_SQL, ("Yahoo Finance Brent Futures",))
            row = cur.fetchone()

            if not row:
                raise RuntimeError(
                    "Source 'Yahoo Finance Brent Futures' not found. "
                    "Run python -m src.ingestion.oil_price_seed_source first."
                )

            source_id = row[0]

            upserted_count = 0

            for price_date, record in df.iterrows():
                open_price = to_python_value(record.get("Open"))
                high_price = to_python_value(record.get("High"))
                low_price = to_python_value(record.get("Low"))
                close_price = to_python_value(record.get("Close"))
                adjusted_close_price = to_python_value(record.get("Adj Close"))
                volume = to_python_value(record.get("Volume"))

                cur.execute(
                    UPSERT_OIL_PRICE_SQL,
                    (
                        BENCHMARK_CODE,
                        price_date.date(),
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        adjusted_close_price,
                        volume,
                        "USD",
                        source_id,
                        ingested_at,
                    ),
                )
                upserted_count += 1

    print(f"Oil price ingestion complete. Upserted {upserted_count} rows into app.oil_prices.")


if __name__ == "__main__":
    main()