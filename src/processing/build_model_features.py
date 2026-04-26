from src.db.connection import get_connection


UPSERT_SQL = """
WITH brent_prices AS (
    SELECT
        op.benchmark_code,
        op.price_date AS feature_date,
        op.close_price,
        op.volume,

        LAG(op.close_price, 1) OVER (
            PARTITION BY op.benchmark_code
            ORDER BY op.price_date
        ) AS lag_close_1d,

        LAG(op.close_price, 3) OVER (
            PARTITION BY op.benchmark_code
            ORDER BY op.price_date
        ) AS lag_close_3d,

        CASE
            WHEN LAG(op.close_price, 1) OVER (
                PARTITION BY op.benchmark_code
                ORDER BY op.price_date
            ) IS NULL
            OR LAG(op.close_price, 1) OVER (
                PARTITION BY op.benchmark_code
                ORDER BY op.price_date
            ) = 0
            THEN NULL
            ELSE (
                op.close_price
                / LAG(op.close_price, 1) OVER (
                    PARTITION BY op.benchmark_code
                    ORDER BY op.price_date
                )
            ) - 1
        END AS return_1d,

        AVG(op.close_price) OVER (
            PARTITION BY op.benchmark_code
            ORDER BY op.price_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_mean_3d,

        AVG(op.close_price) OVER (
            PARTITION BY op.benchmark_code
            ORDER BY op.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_mean_7d,

        STDDEV_SAMP(op.close_price) OVER (
            PARTITION BY op.benchmark_code
            ORDER BY op.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_volatility_7d,

        COALESCE(
            LEAD(op.price_date, 1) OVER (
                PARTITION BY op.benchmark_code
                ORDER BY op.price_date
            ),
            CASE
                WHEN EXTRACT(ISODOW FROM op.price_date) = 5 THEN (op.price_date + INTERVAL '3 day')::date
                WHEN EXTRACT(ISODOW FROM op.price_date) = 6 THEN (op.price_date + INTERVAL '2 day')::date
                ELSE (op.price_date + INTERVAL '1 day')::date
            END
        ) AS target_date,

        LEAD(op.close_price, 1) OVER (
            PARTITION BY op.benchmark_code
            ORDER BY op.price_date
        ) AS target_close_next_day
    FROM app.oil_prices op
    WHERE op.benchmark_code = 'BRENT'
),
price_enriched AS (
    SELECT
        bp.*,

        LAG(bp.return_1d, 1) OVER (
            PARTITION BY bp.benchmark_code
            ORDER BY bp.feature_date
        ) AS lag_return_1d,

        LAG(bp.return_1d, 3) OVER (
            PARTITION BY bp.benchmark_code
            ORDER BY bp.feature_date
        ) AS lag_return_3d,

        CASE
            WHEN bp.close_price IS NULL
              OR bp.target_close_next_day IS NULL
              OR bp.close_price = 0
            THEN NULL
            ELSE (bp.target_close_next_day / bp.close_price) - 1
        END AS target_return_next_day
    FROM brent_prices bp
),
news_mapped AS (
    SELECT
        (
            SELECT MIN(op.price_date)
            FROM app.oil_prices op
            WHERE op.benchmark_code = 'BRENT'
              AND op.price_date >= dnf.feature_date
        ) AS mapped_feature_date,
        dnf.article_count,
        dnf.avg_sentiment,
        dnf.weighted_avg_sentiment
    FROM app.daily_news_features dnf
),
news_by_trading_day AS (
    SELECT
        mapped_feature_date AS feature_date,
        SUM(article_count)::integer AS news_article_count_1d,
        CASE
            WHEN SUM(article_count) = 0 THEN NULL
            ELSE SUM(avg_sentiment * article_count)::numeric / NULLIF(SUM(article_count), 0)
        END AS news_avg_sentiment_1d,
        CASE
            WHEN SUM(article_count) = 0 THEN NULL
            ELSE SUM(weighted_avg_sentiment * article_count)::numeric / NULLIF(SUM(article_count), 0)
        END AS news_weighted_sentiment_1d
    FROM news_mapped
    WHERE mapped_feature_date IS NOT NULL
    GROUP BY mapped_feature_date
),
final_features AS (
    SELECT
        pe.benchmark_code,
        pe.feature_date,
        pe.target_date,
        pe.close_price,
        pe.return_1d,
        pe.lag_close_1d,
        pe.lag_close_3d,
        pe.lag_return_1d,
        pe.lag_return_3d,
        pe.rolling_mean_3d,
        pe.rolling_mean_7d,
        pe.rolling_volatility_7d,
        COALESCE(nbt.news_article_count_1d, 0) AS news_article_count_1d,
        nbt.news_avg_sentiment_1d,
        nbt.news_weighted_sentiment_1d,
        AVG(nbt.news_avg_sentiment_1d) OVER (
            PARTITION BY pe.benchmark_code
            ORDER BY pe.feature_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS news_avg_sentiment_3d,
        AVG(nbt.news_weighted_sentiment_1d) OVER (
            PARTITION BY pe.benchmark_code
            ORDER BY pe.feature_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS news_weighted_sentiment_3d,
        pe.target_close_next_day,
        pe.target_return_next_day,
        CASE
            WHEN pe.target_return_next_day > 0 THEN 1
            WHEN pe.target_return_next_day < 0 THEN -1
            WHEN pe.target_return_next_day = 0 THEN 0
            ELSE NULL
        END AS target_direction_next_day
    FROM price_enriched pe
    LEFT JOIN news_by_trading_day nbt
        ON pe.feature_date = nbt.feature_date
)
INSERT INTO app.model_features (
    benchmark_code,
    feature_date,
    target_date,
    close_price,
    return_1d,
    lag_close_1d,
    lag_close_3d,
    lag_return_1d,
    lag_return_3d,
    rolling_mean_3d,
    rolling_mean_7d,
    rolling_volatility_7d,
    news_article_count_1d,
    news_avg_sentiment_1d,
    news_weighted_sentiment_1d,
    news_avg_sentiment_3d,
    news_weighted_sentiment_3d,
    target_close_next_day,
    target_return_next_day,
    target_direction_next_day
)
SELECT
    benchmark_code,
    feature_date,
    target_date,
    close_price,
    return_1d,
    lag_close_1d,
    lag_close_3d,
    lag_return_1d,
    lag_return_3d,
    rolling_mean_3d,
    rolling_mean_7d,
    rolling_volatility_7d,
    news_article_count_1d,
    news_avg_sentiment_1d,
    news_weighted_sentiment_1d,
    news_avg_sentiment_3d,
    news_weighted_sentiment_3d,
    target_close_next_day,
    target_return_next_day,
    target_direction_next_day
FROM final_features
ON CONFLICT (benchmark_code, feature_date) DO UPDATE
SET
    target_date = EXCLUDED.target_date,
    close_price = EXCLUDED.close_price,
    return_1d = EXCLUDED.return_1d,
    lag_close_1d = EXCLUDED.lag_close_1d,
    lag_close_3d = EXCLUDED.lag_close_3d,
    lag_return_1d = EXCLUDED.lag_return_1d,
    lag_return_3d = EXCLUDED.lag_return_3d,
    rolling_mean_3d = EXCLUDED.rolling_mean_3d,
    rolling_mean_7d = EXCLUDED.rolling_mean_7d,
    rolling_volatility_7d = EXCLUDED.rolling_volatility_7d,
    news_article_count_1d = EXCLUDED.news_article_count_1d,
    news_avg_sentiment_1d = EXCLUDED.news_avg_sentiment_1d,
    news_weighted_sentiment_1d = EXCLUDED.news_weighted_sentiment_1d,
    news_avg_sentiment_3d = EXCLUDED.news_avg_sentiment_3d,
    news_weighted_sentiment_3d = EXCLUDED.news_weighted_sentiment_3d,
    target_close_next_day = EXCLUDED.target_close_next_day,
    target_return_next_day = EXCLUDED.target_return_next_day,
    target_direction_next_day = EXCLUDED.target_direction_next_day;
"""

COUNT_SQL = """
SELECT COUNT(*) FROM app.model_features WHERE benchmark_code = 'BRENT';
"""

LATEST_SQL = """
SELECT
    benchmark_code,
    feature_date,
    target_date,
    close_price,
    return_1d,
    lag_close_1d,
    lag_close_3d,
    lag_return_1d,
    lag_return_3d,
    rolling_mean_3d,
    rolling_mean_7d,
    rolling_volatility_7d,
    news_article_count_1d,
    news_avg_sentiment_1d,
    news_weighted_sentiment_1d,
    news_avg_sentiment_3d,
    news_weighted_sentiment_3d,
    target_close_next_day,
    target_return_next_day,
    target_direction_next_day
FROM app.model_features
WHERE benchmark_code = 'BRENT'
ORDER BY feature_date DESC
LIMIT 5;
"""


def build_model_features() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_SQL)

            cur.execute(COUNT_SQL)
            total_rows = cur.fetchone()[0]

            cur.execute(LATEST_SQL)
            latest_rows = cur.fetchall()

        conn.commit()

    print(f"BRENT model_features rows: {total_rows}")
    print("Latest 5 rows:")
    for row in latest_rows:
        print(row)


if __name__ == "__main__":
    build_model_features()