# Oil Price Forecasting

Local Python + PostgreSQL data science project for forecasting **Brent crude oil prices** using:

- historical oil market data
- news/RSS sentiment
- automated feature engineering
- machine learning models
- monitoring and retraining
- an interactive Streamlit dashboard

This project implements a full end-to-end data science pipeline, from data collection and storage to modelling, monitoring, and presentation.

---

## Project Goal

The goal of this project is to investigate whether **news sentiment improves short-term Brent oil price forecasting** compared to using historical price features alone.

We combine:

- **historical Brent price data** from Yahoo Finance
- **RSS-based news articles** from financial, economic, energy, and sanctions-related sources
- **sentiment analysis and topic extraction**
- **supervised learning models** for next-day price prediction

---

## Research Question

**Does the inclusion of news sentiment improve the prediction of Brent oil prices compared to using only historical price data?**

---

## Key Outcome

In the current experiments:

- **Quantile Regression** achieved the best performance
- **Linear Regression** was a very close second
- **Random Forest**, **XGBoost**, and ensemble models did not outperform the simpler baselines
- sentiment features did **not** produce a meaningful improvement over price-only features in the current setup

This is an important result of the project: it suggests that short-term oil price movements are difficult to improve using simple daily aggregated sentiment alone.

---

## Project Scope

The system is designed as a complete pipeline covering the main stages of a data science lifecycle:

1. **Question identification**
2. **Data collection**
3. **Data storage**
4. **Data processing**
5. **Feature engineering**
6. **Model training and evaluation**
7. **Monitoring and retraining**
8. **Dashboard-based presentation**

---

## Data Sources

### 1. Historical Oil Prices
Historical oil price data is collected from **Yahoo Finance** using the `yfinance` library.

- Benchmark: **Brent crude futures**
- Ticker: `BZ=F`
- Daily fields include:
  - open
  - high
  - low
  - close
  - adjusted close
  - volume

### 2. News / RSS Sources
News data is collected using RSS feeds from official and reliable sources, including:

- EIA
- ECB
- IMF
- Federal Reserve
- EU sanctions / guidance feeds

These sources provide information related to:

- oil supply and demand
- macroeconomic conditions
- monetary policy
- sanctions and tariffs
- geopolitical developments

---

## System Architecture

The project follows a modular pipeline:

**Sources → Ingestion → Database → Processing → Feature Engineering → Modelling → Monitoring → Dashboard**

Technologies used:

- **Python**
- **PostgreSQL**
- **RSS feeds / APIs**
- **Streamlit**
- **Plotly**

---

## Database Design

All data is stored in a PostgreSQL database.

Main database tables include:

- `sources`
- `articles_raw`
- `articles_processed`
- `topics`
- `article_topic_scores`
- `daily_news_features`
- `daily_topic_features`
- `oil_prices`
- `model_features`
- `model_runs`
- `predictions`
- `monitoring_logs`

The database also includes the view:

- `v_daily_news_price_alignment`

---

## Data Processing

### Article Processing
News articles are cleaned and processed before modelling.

Main processing steps include:

- HTML removal
- text cleaning and normalization
- article sentiment scoring
- topic keyword scoring
- relevance scoring

### Sentiment Analysis
Sentiment is computed using **VADER**.

Each article is assigned:

- a sentiment score
- a sentiment label
- a processing status

### Topic Extraction
Topic scores are assigned using keyword-based logic across themes such as:

- oil market
- economy
- war
- sanctions
- tariffs
- supply
- demand

---

## Feature Engineering

Daily features are built from processed news and historical oil prices.

### News Features
Examples include:

- daily article count
- distinct source count
- average sentiment
- weighted average sentiment
- topic-based daily aggregates

### Historical Price Features
Examples include:

- lagged prices
- lagged returns
- rolling averages
- rolling volatility

These are combined into `app.model_features`, which is the final modelling dataset.

---

## Modelling Approach

Two main modelling tracks are used:

### Model A
**Historical price features only**

### Model B
**Historical price + sentiment features**

The following models are evaluated:

- Linear Regression
- Quantile Regression
- Random Forest
- XGBoost
- simple ensemble models

### Training Strategy
- time-based split
- leakage avoidance
- model comparison using:
  - **MAE**
  - **RMSE**

### Current Finding
The simpler models performed best under the current feature setup.

---

## Monitoring and Retraining

The project includes a monitoring loop for model revision.

### Monitoring
Recent prediction error is checked using stored prediction results.

The system logs:

- recent MAE
- previous MAE
- retraining recommendations
- pipeline actions

### Auto-Retraining
If degradation is detected, the system can trigger retraining automatically.

A **cooldown guard** is included to prevent repeated unnecessary retraining on every run.

This logic is stored in:

- `src/monitoring/retraining_check.py`
- `src/monitoring/auto_retrain.py`

---

## Dashboard

A Streamlit dashboard is included for visualising the full pipeline output.

The dashboard shows:

- Brent price trend
- daily sentiment and article count
- next-day forecast snapshot
- actual vs predicted comparison
- evaluation metrics
- configurable filters for date range, source, model, and sentiment mode

Dashboard entry point:

- `src/dashboard/app.py`

---

## Project Structure

```text
oil_price_forecasting/
│
├── sql/
│   ├── 00_create_database.sql
│   └── 01_schema.sql
│
├── src/
│   ├── config/
│   │   └── settings.py
│   │
│   ├── db/
│   │   ├── connection.py
│   │   └── test_connection.py
│   │
│   ├── ingestion/
│   │   ├── oil_price_seed_source.py
│   │   ├── oil_price_ingest.py
│   │   ├── rss_sources.py
│   │   ├── rss_seed_sources.py
│   │   └── rss_ingest.py
│   │
│   ├── processing/
│   │   ├── article_processor.py
│   │   ├── aggregate_daily_news.py
│   │   └── build_model_features.py
│   │
│   ├── modeling/
│   │   ├── train_model_a.py
│   │   └── train_model_b.py
│   │
│   ├── monitoring/
│   │   ├── retraining_check.py
│   │   └── auto_retrain.py
│   │
│   ├── pipeline/
│   │   └── daily_pipeline.py
│   │
│   └── dashboard/
│       └── app.py
│
├── docs/
├── .env.example
├── requirements.txt
└── README.md