# Oil Price Forecasting

Local Python + PostgreSQL project for oil price forecasting using historical oil prices and news/RSS sentiment.

In this project, we explore how data science and machine learning techniques can be used to understand and predict oil price movements, focusing on the Brent crude oil benchmark. The main idea is to combine historical oil price data with sentiment extracted from financial and economic news, in order to improve short-term forecasting performance. The goal is to demonstrate how different types of data (numerical and textual) can be combined into a single pipeline, and how this approach can be applied to real-world problems in financial markets.

## Motivation

Oil price forecasting is a complex problem, influenced not only by past prices but also by external factors such as geopolitical events, economic conditions, and market expectations. Recent research suggests that sentiment extracted from news articles can provide useful additional information for forecasting models. 
For this reason, in this project we attempt to combine historical oil price data, news articles from reliable RSS sources and sentiment analysis techniques in order to build a more informative dataset for prediction.


## Project Overview

The system is designed as a complete data pipeline that includes:
1. Data collection from external sources
2. Storage in a relational database (PostgreSQL)
3. Data processing and cleaning
4. Feature engineering (including sentiment features)
5. Preparation for machine learning models

The main research question of the project is:
Does the inclusion of news sentiment improve the prediction of Brent oil prices compared to using only historical price data? 


## Data Sources

### Historical Oil Prices
- Data is collected from Yahoo Finance using the yfinance library
- We use the Brent futures ticker (BZ=F)
- Includes daily values such as open, high, low, close and volume

### News & Sentiment Data
News data is collected using RSS feeds from reliable sources such as:
  - EIA (Energy Information Administration)
  - ECB (European Central Bank)
  - IMF
    
  These sources provide information about:
  - Oil supply and demand
  - Economic conditions
  - Geopolitical events (wars, sanctions, tariffs)

The articles are processed and stored for sentiment analysis.


## Data Pipeline

The system follows an ETL (Extract – Transform – Load) approach:

### Data Collection
  - Oil prices fetched from Yahoo Finance
  - Articles fetched from RSS feeds using feedparser
    
### Data Storage
All data is stored in a PostgreSQL database using a structured schema. The database includes tables such as:
  - sources (data sources)
  - articles_raw (raw news data)
  - articles_processed (processed articles)
  - oil_prices (historical prices)
  - daily_news_features (aggregated sentiment)
  - model_features (final dataset for ML) 

### Data Processing
News articles go through a preprocessing step where:
- HTML is removed
- Text is cleaned and normalized
- Relevant content is extracted

### Sentiment Analysis & Topic Extraction
- Sentiment is computed using the VADER model
- Each article is assigned a sentiment score (positive / neutral / negative)
- Topic scores are calculated based on keywords (e.g., oil, economy, war, supply, demand)

### Feature Engineering
Processed data is aggregated into daily features, including:
- Average sentiment per day
- Number of articles
- Topic-based metrics
These are later combined with historical price features (lags, returns, rolling averages).


### Modelling
In the next phase, we will develop and evaluate forecasting models using:
  - Tree-based and ensemble methods (Random Forest, Boosting models)
  - Training strategy
      - Time-based split (to preserve temporal order)
      - Avoidance of data leakage
  - Hyperparameter tuning - Grid Search on selected parameter combinations
  - Evaluation metrics (MAE, RMSE, Directional Accuracy)
  - Experiments
      - Model A: Historical price features only
      - Model B: Historical + sentiment features
  - Post-processing
      - Store predictions in the database
      - Compare predictions with actual values for evaluation


## System Architecture
The system is structured as a modular pipeline:
Sources → Ingestion → Database → Processing → Feature Engineering → Modelling → Dashboard
Technologies used:
- Python (data processing)
- PostgreSQL (data storage)
- RSS feeds & APIs (data collection)


## Project Structure

- `src/` - Python source code
- `sql/` - SQL scripts for database creation and schema
- `docs/` - project documentation, diagrams, and report assets


## Installation & Setup

### Install dependencies

    pip install -r requirements.txt

### Configure database
  Create a .env file based on .env.example:
  
    PGHOST=localhost
    PGPORT=5432
    PGDATABASE=oil_price_forecasting
    PGUSER=postgres
    PGPASSWORD=your_password
    
### Create database
  Run:
  
    sql/00_create_database.sql
    sql/01_schema.sql
  
## Running the Pipeline

Step 1 – Insert sources

    python -m src.ingestion.oil_price_seed_source
    python -m src.ingestion.rss_seed_sources

Step 2 – Data ingestion

    python -m src.ingestion.oil_price_ingest
    python -m src.ingestion.rss_ingest

Step 3 – Process articles

    python -m src.processing.article_processor

Step 4 – Aggregate features

    python -m src.processing.aggregate_daily_news

## Current Status

Initial project structure created. At this stage (Phase A), the following have been implemented:
- Database schema and structure
- Data ingestion pipeline (oil prices & RSS feeds)
- Article processing and sentiment analysis
- Daily feature aggregation
Machine learning models and dashboard are planned for the next phase.