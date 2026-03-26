import re
from datetime import datetime, timezone

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.db.connection import get_connection


TOPIC_KEYWORDS = {
    "oil_market": [
        "oil", "brent", "wti", "crude", "barrel", "petroleum", "refinery", "energy market"
    ],
    "economy": [
        "inflation", "gdp", "interest rate", "central bank", "recession", "growth",
        "economy", "economic", "macro", "unemployment"
    ],
    "war": [
        "war", "military", "missile", "attack", "conflict", "invasion", "troops", "strike"
    ],
    "sanctions": [
        "sanction", "embargo", "restriction", "blacklist", "price cap", "ban"
    ],
    "tariffs": [
        "tariff", "duties", "import tax", "trade barrier", "customs duty"
    ],
    "supply": [
        "supply", "production", "output", "opec", "opec+", "inventory", "stocks",
        "drilling", "exports"
    ],
    "demand": [
        "demand", "consumption", "industrial activity", "travel demand", "aviation demand",
        "imports", "usage"
    ],
}


FETCH_PENDING_ARTICLES_SQL = """
SELECT
    ar.article_id,
    ar.title,
    ar.summary,
    ar.content,
    ar.published_date
FROM app.articles_raw ar
JOIN app.articles_processed ap
    ON ap.article_id = ar.article_id
WHERE ap.processing_status = 'pending'
ORDER BY ar.article_id;
"""

GET_TOPICS_SQL = """
SELECT topic_id, topic_name
FROM app.topics
ORDER BY topic_id;
"""

DELETE_EXISTING_TOPIC_SCORES_SQL = """
DELETE FROM app.article_topic_scores
WHERE article_id = %s;
"""

INSERT_TOPIC_SCORE_SQL = """
INSERT INTO app.article_topic_scores (
    article_id,
    topic_id,
    topic_score,
    is_primary_topic
)
VALUES (%s, %s, %s, %s);
"""

UPDATE_ARTICLE_PROCESSED_SQL = """
UPDATE app.articles_processed
SET
    cleaned_text = %s,
    relevance_score = %s,
    sentiment_score = %s,
    sentiment_label = %s,
    processing_status = %s,
    sentiment_model_name = %s,
    sentiment_model_version = %s,
    processed_at = %s,
    notes = %s
WHERE article_id = %s;
"""


def clean_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r"http\S+", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[^a-z0-9\s\-\+\./]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_combined_text(title: str | None, summary: str | None, content: str | None) -> str:
    parts = [title or "", summary or "", content or ""]
    return " ".join(part for part in parts if part).strip()


def compute_topic_scores(cleaned_text: str) -> dict[str, float]:
    scores = {}

    for topic_name, keywords in TOPIC_KEYWORDS.items():
        hits = 0
        for keyword in keywords:
            if keyword in cleaned_text:
                hits += 1

        if keywords:
            score = round(hits / len(keywords), 4)
        else:
            score = 0.0

        scores[topic_name] = score

    return scores


def compute_relevance_score(topic_scores: dict[str, float]) -> float:
    return round(max(topic_scores.values()) if topic_scores else 0.0, 4)


def sentiment_label_from_score(compound_score: float) -> str:
    if compound_score >= 0.05:
        return "positive"
    if compound_score <= -0.05:
        return "negative"
    return "neutral"


def main():
    analyzer = SentimentIntensityAnalyzer()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(GET_TOPICS_SQL)
            topic_rows = cur.fetchall()
            topic_name_to_id = {topic_name: topic_id for topic_id, topic_name in topic_rows}

            cur.execute(FETCH_PENDING_ARTICLES_SQL)
            articles = cur.fetchall()

            if not articles:
                print("No pending articles found.")
                return

            print(f"Found {len(articles)} pending articles to process.")

            processed_count = 0

            for article_id, title, summary, content, published_date in articles:
                combined_text = build_combined_text(title, summary, content)

                if not combined_text:
                    cur.execute(
                        UPDATE_ARTICLE_PROCESSED_SQL,
                        (
                            None,
                            0.0,
                            0.0,
                            "neutral",
                            "failed",
                            "vader",
                            "3.3.2",
                            datetime.now(timezone.utc),
                            "No usable text found in title/summary/content.",
                            article_id,
                        ),
                    )
                    continue

                cleaned = clean_text(combined_text)
                topic_scores = compute_topic_scores(cleaned)
                relevance_score = compute_relevance_score(topic_scores)

                sentiment = analyzer.polarity_scores(cleaned)
                sentiment_score = round(sentiment["compound"], 4)
                sentiment_label = sentiment_label_from_score(sentiment_score)

                cur.execute(DELETE_EXISTING_TOPIC_SCORES_SQL, (article_id,))

                positive_topics = {
                    topic_name: score
                    for topic_name, score in topic_scores.items()
                    if score > 0
                }

                primary_topic = None
                if positive_topics:
                    primary_topic = max(positive_topics, key=positive_topics.get)

                for topic_name, score in positive_topics.items():
                    topic_id = topic_name_to_id[topic_name]
                    cur.execute(
                        INSERT_TOPIC_SCORE_SQL,
                        (
                            article_id,
                            topic_id,
                            score,
                            topic_name == primary_topic,
                        ),
                    )

                note = f"Processed from published_date={published_date}" if published_date else "Processed with missing published_date."

                cur.execute(
                    UPDATE_ARTICLE_PROCESSED_SQL,
                    (
                        cleaned,
                        relevance_score,
                        sentiment_score,
                        sentiment_label,
                        "processed",
                        "vader",
                        "3.3.2",
                        datetime.now(timezone.utc),
                        note,
                        article_id,
                    ),
                )

                processed_count += 1

    print(f"Article processing complete. Processed {processed_count} articles.")


if __name__ == "__main__":
    main()