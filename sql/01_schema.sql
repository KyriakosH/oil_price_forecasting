CREATE SCHEMA IF NOT EXISTS app;

SET search_path TO app, public;

CREATE TABLE IF NOT EXISTS sources (
 source_id BIGSERIAL PRIMARY KEY,
 source_name TEXT NOT NULL,
 source_type TEXT NOT NULL CHECK (source_type IN ('rss', 'api', 'website', 'manual')),
 base_url TEXT NOT NULL,
 feed_url TEXT,
 domain_name TEXT,
 topic_focus TEXT,
 is_active BOOLEAN NOT NULL DEFAULT TRUE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 CONSTRAINT uq_sources_source_name UNIQUE (source_name),
 CONSTRAINT uq_sources_feed_url UNIQUE (feed_url)
);

CREATE TABLE IF NOT EXISTS articles_raw (
 article_id BIGSERIAL PRIMARY KEY,
 source_id BIGINT NOT NULL REFERENCES sources(source_id) ON DELETE RESTRICT,
 external_guid TEXT,
 url TEXT NOT NULL,
 url_hash CHAR(64) NOT NULL,
 title TEXT NOT NULL,
 summary TEXT,
 content TEXT,
 author_name TEXT,
 language_code VARCHAR(10),
 published_at TIMESTAMPTZ,
 published_date DATE,
 fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 raw_payload JSONB,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 CONSTRAINT uq_articles_raw_url_hash UNIQUE (url_hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_articles_raw_source_guid
 ON articles_raw(source_id, external_guid)
 WHERE external_guid IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_articles_raw_source_id
 ON articles_raw(source_id);

CREATE INDEX IF NOT EXISTS ix_articles_raw_published_date
 ON articles_raw(published_date);

CREATE INDEX IF NOT EXISTS ix_articles_raw_published_at
 ON articles_raw(published_at);

CREATE TABLE IF NOT EXISTS articles_processed (
 article_id BIGINT PRIMARY KEY REFERENCES articles_raw(article_id) ON DELETE CASCADE,
 cleaned_text TEXT,
 relevance_score NUMERIC(6,4),
 sentiment_score NUMERIC(6,4),
 sentiment_label TEXT CHECK (sentiment_label IN ('negative', 'neutral', 'positive')),
 processing_status TEXT NOT NULL DEFAULT 'pending'
 CHECK (processing_status IN ('pending', 'processed', 'failed')),
 sentiment_model_name TEXT,
 sentiment_model_version TEXT,
 processed_at TIMESTAMPTZ,
 notes TEXT
);

CREATE INDEX IF NOT EXISTS ix_articles_processed_processing_status
 ON articles_processed(processing_status);

CREATE INDEX IF NOT EXISTS ix_articles_processed_sentiment_score
 ON articles_processed(sentiment_score);

CREATE TABLE IF NOT EXISTS topics (
 topic_id SMALLSERIAL PRIMARY KEY,
 topic_name TEXT NOT NULL,
 description TEXT,
 CONSTRAINT uq_topics_topic_name UNIQUE (topic_name)
);

INSERT INTO topics (topic_name, description)
VALUES
 ('oil_market', 'General oil and energy market related content'),
 ('economy', 'Macroeconomic and financial conditions'),
 ('war', 'War or armed conflict related content'),
 ('sanctions', 'Sanctions and restrictive measures'),
 ('tariffs', 'Tariffs and trade barriers'),
 ('supply', 'Oil supply, production, inventories, OPEC+'),
 ('demand', 'Oil demand, industrial use, transport demand')
ON CONFLICT (topic_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS article_topic_scores (
 article_id BIGINT NOT NULL REFERENCES articles_raw(article_id) ON DELETE CASCADE,
 topic_id SMALLINT NOT NULL REFERENCES topics(topic_id) ON DELETE RESTRICT,
 topic_score NUMERIC(6,4) NOT NULL,
 is_primary_topic BOOLEAN NOT NULL DEFAULT FALSE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY (article_id, topic_id)
);

CREATE INDEX IF NOT EXISTS ix_article_topic_scores_topic_id
 ON article_topic_scores(topic_id);

CREATE INDEX IF NOT EXISTS ix_article_topic_scores_topic_score
 ON article_topic_scores(topic_score);

CREATE TABLE IF NOT EXISTS daily_news_features (
 feature_date DATE PRIMARY KEY,
 article_count INTEGER NOT NULL DEFAULT 0 CHECK (article_count >= 0),
 distinct_source_count INTEGER NOT NULL DEFAULT 0 CHECK (distinct_source_count >= 0),
 avg_sentiment NUMERIC(8,4),
 weighted_avg_sentiment NUMERIC(8,4),
 avg_relevance NUMERIC(8,4),
 positive_article_count INTEGER NOT NULL DEFAULT 0 CHECK (positive_article_count >= 0),
 neutral_article_count INTEGER NOT NULL DEFAULT 0 CHECK (neutral_article_count >= 0),
 negative_article_count INTEGER NOT NULL DEFAULT 0 CHECK (negative_article_count >= 0),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_topic_features (
 feature_date DATE NOT NULL,
 topic_id SMALLINT NOT NULL REFERENCES topics(topic_id) ON DELETE RESTRICT,
 article_count INTEGER NOT NULL DEFAULT 0 CHECK (article_count >= 0),
 avg_sentiment NUMERIC(8,4),
 weighted_avg_sentiment NUMERIC(8,4),
 avg_relevance NUMERIC(8,4),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY (feature_date, topic_id)
);

CREATE INDEX IF NOT EXISTS ix_daily_topic_features_topic_id
 ON daily_topic_features(topic_id);

CREATE TABLE IF NOT EXISTS oil_prices (
 price_id BIGSERIAL PRIMARY KEY,
 benchmark_code TEXT NOT NULL CHECK (benchmark_code IN ('BRENT', 'WTI')),
 price_date DATE NOT NULL,
 open_price NUMERIC(12,4),
 high_price NUMERIC(12,4),
 low_price NUMERIC(12,4),
 close_price NUMERIC(12,4),
 adjusted_close_price NUMERIC(12,4),
 volume BIGINT,
 currency_code CHAR(3) NOT NULL DEFAULT 'USD',
 source_id BIGINT REFERENCES sources(source_id) ON DELETE SET NULL,
 ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 CONSTRAINT uq_oil_prices_benchmark_date UNIQUE (benchmark_code, price_date)
);

CREATE INDEX IF NOT EXISTS ix_oil_prices_price_date
 ON oil_prices(price_date);

CREATE INDEX IF NOT EXISTS ix_oil_prices_benchmark_code
 ON oil_prices(benchmark_code);

CREATE TABLE IF NOT EXISTS model_features (
 feature_row_id BIGSERIAL PRIMARY KEY,
 benchmark_code TEXT NOT NULL CHECK (benchmark_code IN ('BRENT', 'WTI')),
 feature_date DATE NOT NULL,
 target_date DATE,
 close_price NUMERIC(12,4),
 return_1d NUMERIC(12,6),
 lag_close_1d NUMERIC(12,4),
 lag_close_3d NUMERIC(12,4),
 lag_return_1d NUMERIC(12,6),
 lag_return_3d NUMERIC(12,6),
 rolling_mean_3d NUMERIC(12,6),
 rolling_mean_7d NUMERIC(12,6),
 rolling_volatility_7d NUMERIC(12,6),
 news_article_count_1d INTEGER,
 news_avg_sentiment_1d NUMERIC(8,4),
 news_weighted_sentiment_1d NUMERIC(8,4),
 news_avg_sentiment_3d NUMERIC(8,4),
 news_weighted_sentiment_3d NUMERIC(8,4),
 target_close_next_day NUMERIC(12,4),
 target_return_next_day NUMERIC(12,6),
 target_direction_next_day SMALLINT CHECK (target_direction_next_day IN (-1, 0, 1)),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 CONSTRAINT uq_model_features_benchmark_date UNIQUE (benchmark_code, feature_date)
);

CREATE INDEX IF NOT EXISTS ix_model_features_feature_date
 ON model_features(feature_date);

CREATE TABLE IF NOT EXISTS model_runs (
 model_run_id BIGSERIAL PRIMARY KEY,
 model_name TEXT NOT NULL,
 model_version TEXT NOT NULL,
 target_name TEXT NOT NULL,
 benchmark_code TEXT NOT NULL CHECK (benchmark_code IN ('BRENT', 'WTI')),
 run_status TEXT NOT NULL DEFAULT 'started'
 CHECK (run_status IN ('started', 'completed', 'failed')),
 training_start_date DATE,
 training_end_date DATE,
 validation_start_date DATE,
 validation_end_date DATE,
 test_start_date DATE,
 test_end_date DATE,
 mae NUMERIC(12,6),
 rmse NUMERIC(12,6),
 mape NUMERIC(12,6),
 directional_accuracy NUMERIC(8,4),
 notes TEXT,
 started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_model_runs_model_name
 ON model_runs(model_name);

CREATE TABLE IF NOT EXISTS predictions (
 prediction_id BIGSERIAL PRIMARY KEY,
 model_run_id BIGINT REFERENCES model_runs(model_run_id) ON DELETE SET NULL,
 benchmark_code TEXT NOT NULL CHECK (benchmark_code IN ('BRENT', 'WTI')),
 feature_date DATE NOT NULL,
 prediction_for_date DATE NOT NULL,
 predicted_close NUMERIC(12,4),
 predicted_return_1d NUMERIC(12,6),
 predicted_direction SMALLINT CHECK (predicted_direction IN (-1, 0, 1)),
 confidence_score NUMERIC(8,4),
 actual_close NUMERIC(12,4),
 actual_return_1d NUMERIC(12,6),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 is_backtest BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS ix_predictions_prediction_for_date
 ON predictions(prediction_for_date);

CREATE INDEX IF NOT EXISTS ix_predictions_model_run_id
 ON predictions(model_run_id);

CREATE TABLE IF NOT EXISTS monitoring_logs (
 monitoring_id BIGSERIAL PRIMARY KEY,
 log_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 log_type TEXT NOT NULL CHECK (log_type IN ('data_quality', 'feature_drift', 'model_drift', 'pipeline', 'system')),
 severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'error')),
 component_name TEXT NOT NULL,
 message TEXT NOT NULL,
 metric_name TEXT,
 metric_value NUMERIC(14,6),
 details JSONB
);

CREATE INDEX IF NOT EXISTS ix_monitoring_logs_log_timestamp
 ON monitoring_logs(log_timestamp);

CREATE INDEX IF NOT EXISTS ix_monitoring_logs_log_type
 ON monitoring_logs(log_type);

CREATE OR REPLACE VIEW app.v_daily_news_price_alignment AS
SELECT
 op.benchmark_code,
 op.price_date,
 op.open_price,
 op.high_price,
 op.low_price,
 op.close_price,
 op.adjusted_close_price,
 op.volume,
 dnf.article_count,
 dnf.distinct_source_count,
 dnf.avg_sentiment,
 dnf.weighted_avg_sentiment,
 dnf.avg_relevance,
 dnf.positive_article_count,
 dnf.neutral_article_count,
 dnf.negative_article_count
FROM app.oil_prices op
LEFT JOIN app.daily_news_features dnf
 ON dnf.feature_date = op.price_date
WHERE op.benchmark_code = 'BRENT';