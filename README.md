# Oil Price Forecasting

A local Python + PostgreSQL data science project for forecasting Brent crude oil prices using historical market data, RSS/news sentiment, feature engineering, machine learning models, monitoring, and a Grafana dashboard.

The project implements an end-to-end data science pipeline:

```text
Data Sources
    ↓
Ingestion
    ↓
PostgreSQL Database
    ↓
Article Processing and Sentiment Scoring
    ↓
Daily Feature Engineering
    ↓
Model Training and Evaluation
    ↓
Prediction Monitoring and Retraining Checks
    ↓
Grafana Dashboard
```

---

## Project Goal

The goal of this project is to investigate whether news sentiment improves short-term Brent crude oil price forecasting compared to models that use only historical price features.

The system combines:

- historical Brent crude oil price data
- RSS/news articles from economic, financial, energy, and geopolitical sources
- sentiment analysis
- topic and relevance scoring
- daily aggregated modelling features
- supervised machine learning models
- model monitoring and retraining logic
- dashboard-based presentation in Grafana

---

## Research Question

**Does the inclusion of news sentiment improve the prediction of Brent crude oil prices compared to using only historical price data?**

---

## Current Finding

In the current experiments:

- Quantile Regression achieved the strongest result overall.
- Linear Regression was a close second.
- Random Forest, XGBoost, and simple ensemble models did not outperform the simpler regression models.
- Sentiment features did not provide a meaningful improvement over price-only features in the current setup.

This is an important project result, not a failure. It suggests that short-term Brent oil price movements are difficult to improve using simple daily aggregated sentiment features alone. Annoying, yes. Scientifically useful, also yes.

---

## Data Science Lifecycle Coverage

This project covers the main stages of a data science lifecycle:

| Stage | Implementation in this project |
|---|---|
| Question identification | Defines a measurable forecasting problem around Brent crude oil prices |
| Data collection | Collects oil prices and RSS/news articles |
| Data storage | Stores raw, processed, aggregated, and model output data in PostgreSQL |
| Data processing | Cleans articles, scores sentiment, assigns topics, and prepares modelling data |
| Data quality | Handles duplicates, missing values, invalid records, and pipeline status logs |
| Modelling | Trains and evaluates regression and machine learning models |
| Presentation | Uses Grafana dashboards to present trends, forecasts, sentiment, and model results |
| Monitoring and revision | Tracks prediction errors and supports retraining checks |

---

## Technologies Used

- Python
- PostgreSQL
- Grafana
- RSS feeds
- Yahoo Finance data through `yfinance`
- VADER sentiment analysis
- pandas
- NumPy
- scikit-learn
- XGBoost
- psycopg

---

## Data Sources

### 1. Historical Oil Prices

Historical Brent crude oil price data is collected using the `yfinance` Python library.

Main benchmark:

```text
BZ=F
```

Stored oil price fields include:

- open price
- high price
- low price
- close price
- adjusted close price
- volume

### 2. News and RSS Sources

News data is collected from RSS feeds related to energy markets, economics, monetary policy, sanctions, and geopolitical events.

Examples of source categories include:

- EIA energy updates
- ECB updates
- IMF news
- Federal Reserve updates
- sanctions and policy-related sources

The articles are used to produce daily sentiment and topic features.

---

## Database Design

All project data is stored in PostgreSQL under the `app` schema.

Main tables include:

| Table | Purpose |
|---|---|
| `app.sources` | Stores article and market data sources |
| `app.articles_raw` | Stores raw ingested RSS/news articles |
| `app.articles_processed` | Stores cleaned article text, sentiment, labels, and processing status |
| `app.topics` | Stores project topic definitions |
| `app.article_topic_scores` | Stores article-level topic scores |
| `app.daily_news_features` | Stores daily aggregated news and sentiment features |
| `app.daily_topic_features` | Stores daily topic-level aggregates |
| `app.oil_prices` | Stores historical Brent crude oil prices |
| `app.model_features` | Stores the final modelling dataset |
| `app.model_runs` | Stores model training runs and evaluation metrics |
| `app.predictions` | Stores generated predictions and actual values |
| `app.monitoring_logs` | Stores monitoring and retraining check outputs |

The database also includes a view:

```text
app.v_daily_news_price_alignment
```

This view supports validation between daily news features and oil price records.

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
│   │   ├── ingest.py
│   │   ├── oil_price_ingest.py
│   │   ├── oil_price_seed_source.py
│   │   ├── rss_ingest.py
│   │   ├── rss_seed_sources.py
│   │   └── rss_sources.py
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
│   │   ├── auto_retrain.py
│   │   └── retraining_check.py
│   │
│   └── pipeline/
│       └── daily_pipeline.py
│
├── dashboard-1777194551165.json
├── oil_price_dashboard.json.json
├── .env.example
├── requirements.txt
└── README.md
```

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/KyriakosH/oil_price_forecasting.git
cd oil_price_forecasting
```

---

### 2. Create a Python Virtual Environment

Windows:

```bash
python -m venv .venv
.venv\Scripts\activate
```

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Create the PostgreSQL Database

Open PostgreSQL or pgAdmin and run:

```sql
CREATE DATABASE oil_price_forecasting;
```

Then run the schema file:

```bash
psql -U postgres -d oil_price_forecasting -f sql/01_schema.sql
```

If your username is not `postgres`, replace it with your own PostgreSQL username.

---

### 5. Configure Environment Variables

Create a `.env` file in the project root.

Use `.env.example` as the template:

```env
PGHOST=localhost
PGPORT=5432
PGDATABASE=oil_price_forecasting
PGUSER=postgres
PGPASSWORD=your_postgresql_password
```

Replace `your_postgresql_password` with your actual PostgreSQL password.

Do not commit your real `.env` file to GitHub. Humanity has leaked enough passwords already.

---

## Running the Project

Run all commands from the project root folder.

### 1. Test the Database Connection

```bash
python -m src.db.test_connection
```

Expected result:

```text
Database connection successful
```

The exact wording may differ depending on the script output, but the command should run without an error.

---

### 2. Seed Data Sources

Seed oil price source:

```bash
python -m src.ingestion.oil_price_seed_source
```

Seed RSS/news sources:

```bash
python -m src.ingestion.rss_seed_sources
```

---

### 3. Ingest Oil Prices

```bash
python -m src.ingestion.oil_price_ingest
```

This fetches Brent crude oil price data and inserts it into `app.oil_prices`.

---

### 4. Ingest RSS Articles

```bash
python -m src.ingestion.ingest
```

This fetches RSS/news articles and inserts new articles into `app.articles_raw`.

---

### 5. Process Articles

```bash
python -m src.processing.article_processor
```

This cleans article text, calculates sentiment, assigns sentiment labels, and stores processed article outputs.

---

### 6. Aggregate Daily News Features

```bash
python -m src.processing.aggregate_daily_news
```

This creates daily article counts, sentiment averages, weighted sentiment values, and source-level aggregates.

---

### 7. Build Model Features

```bash
python -m src.processing.build_model_features
```

This combines oil price features and news features into `app.model_features`.

---

### 8. Train Model A

Model A uses historical price features only.

```bash
python -m src.modeling.train_model_a
```

---

### 9. Train Model B

Model B uses historical price features plus sentiment features.

```bash
python -m src.modeling.train_model_b
```

---

### 10. Run Monitoring Check

```bash
python -m src.monitoring.retraining_check
```

This checks recent prediction error and logs monitoring results.

---

### 11. Run Automatic Retraining Check

```bash
python -m src.monitoring.auto_retrain
```

This checks whether retraining should be triggered based on monitoring logic.

---

### 12. Run the Full Daily Pipeline

```bash
python -m src.pipeline.daily_pipeline
```

The daily pipeline runs:

1. oil price ingestion
2. RSS/news ingestion
3. article processing
4. daily news aggregation
5. model feature building
6. retraining check
7. automatic retraining logic

---

## Modelling Approach

The project compares two feature sets.

### Model A: Price-Only Features

Uses historical oil price indicators such as:

- lagged close prices
- lagged returns
- rolling averages
- rolling volatility

### Model B: Price + Sentiment Features

Uses all Model A features plus sentiment features such as:

- daily article count
- daily average sentiment
- daily weighted sentiment
- 3-day average sentiment
- 3-day weighted sentiment

### Models Evaluated

The following models are trained and compared:

- Linear Regression
- Quantile Regression
- Random Forest
- XGBoost
- Simple average ensemble

### Evaluation Metrics

The models are evaluated using:

- MAE: Mean Absolute Error
- RMSE: Root Mean Squared Error

The project uses a time-based split to avoid future data leaking into model training.

---

## Grafana Setup

### 1. Install Grafana

Install Grafana on your computer.

For Windows, use the official installer from Grafana’s documentation:

```text
https://grafana.com/docs/grafana/latest/setup-grafana/installation/
```

---

### 2. Open Grafana

After installation, open your browser and go to:

```text
http://localhost:3000
```

If this is your first time opening Grafana, sign in with:

```text
username: admin
password: admin
```

Then set a new password when prompted.

---

### 3. Create the PostgreSQL Data Source

In Grafana:

1. Click **Connections**
2. Click **Add new connection**
3. Search for **PostgreSQL**
4. Select **PostgreSQL**
5. Click **Add new data source**

Grafana already includes a built-in PostgreSQL data source, so no extra plugin is needed.

---

### 4. Enter the Database Settings

Fill in the PostgreSQL data source form with the project database details.

Use these values:

| Setting | Value |
|---|---|
| Name | `OilPricePostgres` |
| Host URL | Your PostgreSQL host and port, for example `localhost:5432` |
| Database name | `oil_price_forecasting` |
| Username | Your PostgreSQL username |
| Password | Your PostgreSQL password |
| SSL/TLS | Disable |

Then click:

```text
Save & test
```

The connection must succeed before continuing.

---

### 5. Import the Dashboard

After the PostgreSQL data source is working:

1. Click **Dashboards**
2. Click **New**
3. Click **Import dashboard**
4. Upload the dashboard JSON file from the repository
5. If Grafana asks for a data source, select `OilPricePostgres`
6. Click **Import**

Dashboard JSON files included in the repository:

```text
dashboard-1777194551165.json
oil_price_dashboard.json.json
```

Use the final dashboard JSON file that matches the submitted version of the project.

---

### 6. Check That the Dashboard Works

After importing, the dashboard should open and show the main panels.

The dashboard should include controls such as:

- Forecasting Model
- Sentiment Aggregation
- News Source
- Date Range

Change the controls and confirm that the charts update correctly.

---

### 7. If Something Does Not Work

If the dashboard opens but panels show `No data` or errors, check that:

- PostgreSQL is running
- the data source `OilPricePostgres` passes `Save & test`
- the database name is correct
- the schema, tables, and columns exist
- the database user has permission to read the tables
- the pipeline has been run and the tables contain data
- the selected dashboard date range includes dates that exist in the database

Useful SQL checks:

```sql
SELECT COUNT(*) FROM app.oil_prices;
SELECT COUNT(*) FROM app.articles_raw;
SELECT COUNT(*) FROM app.articles_processed;
SELECT COUNT(*) FROM app.daily_news_features;
SELECT COUNT(*) FROM app.model_features;
SELECT COUNT(*) FROM app.model_runs;
SELECT COUNT(*) FROM app.predictions;
```

---

## Dashboard Contents

The Grafana dashboard presents:

- Brent crude oil price trends
- daily article counts
- average daily sentiment
- positive and negative article views
- model prediction vs actual close price
- prediction error
- model comparison metrics
- monitoring and retraining indicators

The dashboard allows users to explore how market prices, article sentiment, and forecast performance change over time.

---

## Data Quality and Ethics

The project uses public market data and public RSS/news sources. No personal or private user data is collected.

Main data quality checks include:

- avoiding duplicate article insertion
- handling missing article dates
- storing raw and processed versions separately
- using processing status fields
- keeping model runs and predictions traceable
- storing monitoring logs for review

Main ethical considerations:

- the project does not produce financial advice
- forecasts should not be treated as guaranteed market predictions
- public news sentiment may be incomplete, biased, delayed, or over-represented by certain sources
- model limitations are documented through evaluation metrics and monitoring logs

---

## Limitations

The current system has several limitations:

- RSS feeds do not capture all relevant global oil market news.
- Daily aggregated sentiment may be too simple to capture real market reactions.
- Oil prices are affected by many variables not included in this prototype.
- News may affect prices before or after publication dates.
- The models are experimental and should not be used for trading decisions.
- Forecasting short-term commodity prices is difficult because markets react to complex and fast-changing information.

---

## Expected Output

After running the full pipeline, the database should contain:

- oil price records in `app.oil_prices`
- raw articles in `app.articles_raw`
- processed sentiment records in `app.articles_processed`
- daily news aggregates in `app.daily_news_features`
- modelling rows in `app.model_features`
- model results in `app.model_runs`
- predictions in `app.predictions`
- monitoring records in `app.monitoring_logs`

Grafana should then display the imported dashboard using the PostgreSQL data source.

---

## Repository Link

```text
https://github.com/KyriakosH/oil_price_forecasting
```

---

## Authors

CSE 473/525 Data Science Group Project

Project topic:

```text
Oil Price Forecasting using Historical Market Data and News Sentiment
```
