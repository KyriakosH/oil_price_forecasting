import json
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.db.connection import get_connection


FIELD_WEIGHTS = {
    "title": 0.5,
    "summary": 0.3,
    "content": 0.2,
}

TOPIC_SATURATION_HITS = 3.0

TOPIC_KEYWORDS = {
    "oil_market": [
        "oil",
        "brent",
        "wti",
        "crude",
        "barrel",
        "petroleum",
        "refinery",
        "energy market",
        "energy",
        "baker hughes",
        "tanker",
        "tankers",
        "shipping",
        "shipment",
        "maritime",
        "strait of hormuz",
        "hormuz",
        "gulf",
        "export route",
        "oil route",
        "chokepoint",
        "natural gas",
        "gas",
        "lng",
        "liquefied natural gas",
        "power market",
        "electricity",
        "power generation",
        "energy security",
        "utility",
        "utilities",
        "reactor",
        "nuclear",
    ],
    "economy": [
        "inflation",
        "gdp",
        "interest rate",
        "central bank",
        "recession",
        "growth",
        "economy",
        "economic",
        "macro",
        "unemployment",
        "federal reserve",
        "fed",
        "ecb",
        "boj",
        "bank of japan",
    ],
    "war": [
        "war",
        "military",
        "missile",
        "attack",
        "conflict",
        "invasion",
        "troops",
        "strike",
        "naval",
        "blockade",
    ],
    "sanctions": [
        "sanction",
        "embargo",
        "restriction",
        "blacklist",
        "price cap",
        "ban",
    ],
    "tariffs": [
        "tariff",
        "duties",
        "import tax",
        "trade barrier",
        "customs duty",
    ],
    "supply": [
        "supply",
        "production",
        "output",
        "opec",
        "opec+",
        "inventory",
        "stocks",
        "drilling",
        "exports",
        "export",
        "shipping",
        "shipment",
        "disruption",
        "closure",
        "reopen",
        "reopening",
        "transit",
        "capacity",
        "generation",
        "electricity generation",
        "power generation",
        "reactor restart",
        "restart",
    ],
    "demand": [
        "demand",
        "consumption",
        "industrial activity",
        "travel demand",
        "aviation demand",
        "imports",
        "usage",
        "electricity demand",
        "power demand",
        "gas demand",
        "fuel demand",
        "displace",
        "displacing",
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
JOIN app.articles_processed ap ON ap.article_id = ar.article_id
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


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""

    text = text.lower()
    text = re.sub(r"http\S+", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[^a-z0-9\s\-\+\./]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_combined_text(
    title: Optional[str],
    summary: Optional[str],
    content: Optional[str],
) -> str:
    parts = [title or "", summary or "", content or ""]
    return " ".join(part for part in parts if part).strip()


def keyword_in_text(keyword: str, text: str) -> bool:
    if not text:
        return False

    escaped = re.escape(keyword.strip().lower())
    escaped = escaped.replace(r"\ ", r"\s+")
    pattern = rf"(?<!\w){escaped}(?!\w)"
    return re.search(pattern, text) is not None


def compute_topic_scores(
    cleaned_title: str,
    cleaned_summary: str,
    cleaned_content: str,
) -> Tuple[Dict[str, float], Dict[str, List[str]]]:
    scores: Dict[str, float] = {}
    keyword_hits: Dict[str, List[str]] = {}

    for topic_name, keywords in TOPIC_KEYWORDS.items():
        weighted_hits = 0.0
        matched_keywords = []

        for keyword in keywords:
            keyword_weight = 0.0

            if keyword_in_text(keyword, cleaned_title):
                keyword_weight += FIELD_WEIGHTS["title"]

            if keyword_in_text(keyword, cleaned_summary):
                keyword_weight += FIELD_WEIGHTS["summary"]

            if keyword_in_text(keyword, cleaned_content):
                keyword_weight += FIELD_WEIGHTS["content"]

            keyword_weight = min(keyword_weight, 1.0)

            if keyword_weight > 0:
                weighted_hits += keyword_weight
                matched_keywords.append(keyword)

        score = round(min(1.0, weighted_hits / TOPIC_SATURATION_HITS), 4)
        scores[topic_name] = score
        keyword_hits[topic_name] = sorted(set(matched_keywords))

    return scores, keyword_hits


def compute_relevance_score(topic_scores: Dict[str, float]) -> float:
    if not topic_scores:
        return 0.0

    positive_scores = sorted(
        (score for score in topic_scores.values() if score > 0),
        reverse=True,
    )

    if not positive_scores:
        return 0.0

    top_1 = positive_scores[0]
    top_2 = positive_scores[1] if len(positive_scores) > 1 else 0.0

    relevance = min(1.0, (0.8 * top_1) + (0.2 * top_2))
    return round(relevance, 4)


def sentiment_label_from_score(compound_score: float) -> str:
    if compound_score >= 0.05:
        return "positive"
    if compound_score <= -0.05:
        return "negative"
    return "neutral"


def build_processing_note(
    published_date,
    primary_topic: Optional[str],
    keyword_hits: Dict[str, List[str]],
) -> str:
    non_empty_hits = {k: v for k, v in keyword_hits.items() if v}

    if published_date:
        prefix = f"Processed from published_date={published_date}"
    else:
        prefix = "Processed with missing published_date"

    return (
        f"{prefix}; primary_topic={primary_topic}; "
        f"keyword_hits={json.dumps(non_empty_hits, ensure_ascii=False)}"
    )


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

                cleaned_title = clean_text(title)
                cleaned_summary = clean_text(summary)
                cleaned_content = clean_text(content)
                cleaned_combined = clean_text(combined_text)

                topic_scores, keyword_hits = compute_topic_scores(
                    cleaned_title=cleaned_title,
                    cleaned_summary=cleaned_summary,
                    cleaned_content=cleaned_content,
                )
                relevance_score = compute_relevance_score(topic_scores)

                sentiment = analyzer.polarity_scores(cleaned_combined)
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

                note = build_processing_note(
                    published_date=published_date,
                    primary_topic=primary_topic,
                    keyword_hits=keyword_hits,
                )

                cur.execute(
                    UPDATE_ARTICLE_PROCESSED_SQL,
                    (
                        cleaned_combined,
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

        conn.commit()

    print(f"Article processing complete. Processed {processed_count} articles.")


if __name__ == "__main__":
    main()