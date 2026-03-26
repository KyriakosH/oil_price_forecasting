from src.db.connection import get_connection


DELETE_DAILY_NEWS_FEATURES_SQL = """
DELETE FROM app.daily_news_features;
"""

DELETE_DAILY_TOPIC_FEATURES_SQL = """
DELETE FROM app.daily_topic_features;
"""

INSERT_DAILY_NEWS_FEATURES_SQL = """
INSERT INTO app.daily_news_features (
    feature_date,
    article_count,
    distinct_source_count,
    avg_sentiment,
    weighted_avg_sentiment,
    avg_relevance,
    positive_article_count,
    neutral_article_count,
    negative_article_count,
    created_at,
    updated_at
)
SELECT
    ar.published_date AS feature_date,
    COUNT(*) AS article_count,
    COUNT(DISTINCT ar.source_id) AS distinct_source_count,
    ROUND(AVG(ap.sentiment_score)::numeric, 4) AS avg_sentiment,
    ROUND(
        CASE
            WHEN SUM(COALESCE(ap.relevance_score, 0)) > 0
            THEN SUM(ap.sentiment_score * ap.relevance_score) / SUM(ap.relevance_score)
            ELSE NULL
        END
    ::numeric, 4) AS weighted_avg_sentiment,
    ROUND(AVG(ap.relevance_score)::numeric, 4) AS avg_relevance,
    SUM(CASE WHEN ap.sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_article_count,
    SUM(CASE WHEN ap.sentiment_label = 'neutral' THEN 1 ELSE 0 END) AS neutral_article_count,
    SUM(CASE WHEN ap.sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_article_count,
    NOW(),
    NOW()
FROM app.articles_raw ar
JOIN app.articles_processed ap
    ON ap.article_id = ar.article_id
WHERE ap.processing_status = 'processed'
  AND ar.published_date IS NOT NULL
  AND COALESCE(ap.relevance_score, 0) > 0
GROUP BY ar.published_date
ORDER BY ar.published_date;
"""

INSERT_DAILY_TOPIC_FEATURES_SQL = """
INSERT INTO app.daily_topic_features (
    feature_date,
    topic_id,
    article_count,
    avg_sentiment,
    weighted_avg_sentiment,
    avg_relevance,
    created_at,
    updated_at
)
SELECT
    ar.published_date AS feature_date,
    ats.topic_id,
    COUNT(*) AS article_count,
    ROUND(AVG(ap.sentiment_score)::numeric, 4) AS avg_sentiment,
    ROUND(
        CASE
            WHEN SUM(COALESCE(ats.topic_score, 0)) > 0
            THEN SUM(ap.sentiment_score * ats.topic_score) / SUM(ats.topic_score)
            ELSE NULL
        END
    ::numeric, 4) AS weighted_avg_sentiment,
    ROUND(AVG(ats.topic_score)::numeric, 4) AS avg_relevance,
    NOW(),
    NOW()
FROM app.articles_raw ar
JOIN app.articles_processed ap
    ON ap.article_id = ar.article_id
JOIN app.article_topic_scores ats
    ON ats.article_id = ar.article_id
WHERE ap.processing_status = 'processed'
  AND ar.published_date IS NOT NULL
GROUP BY ar.published_date, ats.topic_id
ORDER BY ar.published_date, ats.topic_id;
"""


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            print("Refreshing app.daily_news_features...")
            cur.execute(DELETE_DAILY_NEWS_FEATURES_SQL)
            cur.execute(INSERT_DAILY_NEWS_FEATURES_SQL)

            print("Refreshing app.daily_topic_features...")
            cur.execute(DELETE_DAILY_TOPIC_FEATURES_SQL)
            cur.execute(INSERT_DAILY_TOPIC_FEATURES_SQL)

    print("Daily news aggregation complete.")


if __name__ == "__main__":
    main()