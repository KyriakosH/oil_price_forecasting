from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from src.db.connection import get_connection


st.set_page_config(
    page_title="Oil Price Forecasting Dashboard",
    page_icon="📈",
    layout="wide",
)


@st.cache_data(ttl=300)
def get_sources() -> list[str]:
    sql = """
    SELECT source_name
    FROM app.sources
    WHERE is_active = TRUE
      AND source_type = 'rss'
    ORDER BY source_name;
    """
    with get_connection() as conn:
        df = pd.read_sql(sql, conn)
    return ["All Sources"] + df["source_name"].tolist()


@st.cache_data(ttl=300)
def get_model_names() -> list[str]:
    sql = """
    SELECT DISTINCT model_name
    FROM app.model_runs
    WHERE benchmark_code = 'BRENT'
    ORDER BY model_name;
    """
    with get_connection() as conn:
        df = pd.read_sql(sql, conn)
    return df["model_name"].tolist()


@st.cache_data(ttl=300)
def get_default_dates():
    sql = """
    SELECT MIN(price_date) AS min_date, MAX(price_date) AS max_date
    FROM app.oil_prices
    WHERE benchmark_code = 'BRENT';
    """
    with get_connection() as conn:
        df = pd.read_sql(sql, conn)

    min_date = pd.to_datetime(df.loc[0, "min_date"]).date()
    max_date = pd.to_datetime(df.loc[0, "max_date"]).date()

    default_start = max(min_date, max_date - timedelta(days=30))
    return min_date, max_date, default_start, max_date


@st.cache_data(ttl=300)
def get_price_trend(start_date: date, end_date: date) -> pd.DataFrame:
    sql = """
    SELECT
        price_date,
        close_price
    FROM app.oil_prices
    WHERE benchmark_code = 'BRENT'
      AND price_date BETWEEN %s AND %s
    ORDER BY price_date;
    """
    with get_connection() as conn:
        df = pd.read_sql(sql, conn, params=(start_date, end_date))
    return df


@st.cache_data(ttl=300)
def get_daily_sentiment(
    start_date: date,
    end_date: date,
    source_name: str,
    sentiment_mode: str,
) -> pd.DataFrame:
    if source_name == "All Sources":
        if sentiment_mode == "Weighted by Relevance":
            sql = """
            SELECT
                ar.published_date AS feature_date,
                COUNT(*) AS article_count,
                CASE
                    WHEN SUM(COALESCE(ap.relevance_score, 0)) = 0 THEN NULL
                    ELSE
                        SUM(COALESCE(ap.sentiment_score, 0) * COALESCE(ap.relevance_score, 0))
                        / SUM(COALESCE(ap.relevance_score, 0))
                END AS sentiment_value
            FROM app.articles_raw ar
            LEFT JOIN app.articles_processed ap
                ON ap.article_id = ar.article_id
            WHERE ar.published_date BETWEEN %s AND %s
              AND ap.processing_status = 'processed'
            GROUP BY ar.published_date
            ORDER BY ar.published_date;
            """
            params = (start_date, end_date)
        else:
            sql = """
            SELECT
                feature_date,
                article_count,
                avg_sentiment AS sentiment_value
            FROM app.daily_news_features
            WHERE feature_date BETWEEN %s AND %s
            ORDER BY feature_date;
            """
            params = (start_date, end_date)
    else:
        if sentiment_mode == "Weighted by Relevance":
            sql = """
            SELECT
                ar.published_date AS feature_date,
                COUNT(*) AS article_count,
                CASE
                    WHEN SUM(COALESCE(ap.relevance_score, 0)) = 0 THEN NULL
                    ELSE
                        SUM(COALESCE(ap.sentiment_score, 0) * COALESCE(ap.relevance_score, 0))
                        / SUM(COALESCE(ap.relevance_score, 0))
                END AS sentiment_value
            FROM app.articles_raw ar
            JOIN app.sources s
                ON s.source_id = ar.source_id
            LEFT JOIN app.articles_processed ap
                ON ap.article_id = ar.article_id
            WHERE ar.published_date BETWEEN %s AND %s
              AND s.source_name = %s
              AND ap.processing_status = 'processed'
            GROUP BY ar.published_date
            ORDER BY ar.published_date;
            """
            params = (start_date, end_date, source_name)
        else:
            sql = """
            SELECT
                ar.published_date AS feature_date,
                COUNT(*) AS article_count,
                AVG(ap.sentiment_score) AS sentiment_value
            FROM app.articles_raw ar
            JOIN app.sources s
                ON s.source_id = ar.source_id
            LEFT JOIN app.articles_processed ap
                ON ap.article_id = ar.article_id
            WHERE ar.published_date BETWEEN %s AND %s
              AND s.source_name = %s
              AND ap.processing_status = 'processed'
            GROUP BY ar.published_date
            ORDER BY ar.published_date;
            """
            params = (start_date, end_date, source_name)

    with get_connection() as conn:
        df = pd.read_sql(sql, conn, params=params)
    return df


@st.cache_data(ttl=300)
def get_latest_model_run(model_name: str):
    sql = """
    SELECT
        model_run_id,
        model_name,
        model_version,
        mae,
        rmse,
        finished_at
    FROM app.model_runs
    WHERE model_name = %s
      AND benchmark_code = 'BRENT'
    ORDER BY model_run_id DESC
    LIMIT 1;
    """
    with get_connection() as conn:
        df = pd.read_sql(sql, conn, params=(model_name,))
    if df.empty:
        return None
    return df.iloc[0].to_dict()


@st.cache_data(ttl=300)
def get_predictions(model_run_id: int, start_date: date, end_date: date) -> pd.DataFrame:
    sql = """
    SELECT
        feature_date,
        prediction_for_date,
        predicted_close,
        actual_close
    FROM app.predictions
    WHERE model_run_id = %s
      AND prediction_for_date BETWEEN %s AND %s
    ORDER BY prediction_for_date;
    """
    with get_connection() as conn:
        df = pd.read_sql(sql, conn, params=(model_run_id, start_date, end_date))
    return df


def make_price_chart(price_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=price_df["price_date"],
            y=price_df["close_price"],
            mode="lines+markers",
            name="Brent Price",
        )
    )
    fig.update_layout(
        title="Brent Price Trend",
        xaxis_title="Date",
        yaxis_title="Brent Price",
        height=280,
        margin=dict(l=20, r=20, t=40, b=20),
    )
    return fig


def make_sentiment_chart(sentiment_df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=sentiment_df["feature_date"],
            y=sentiment_df["article_count"],
            name="Article Count",
            opacity=0.6,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=sentiment_df["feature_date"],
            y=sentiment_df["sentiment_value"],
            mode="lines+markers",
            name="Sentiment",
        ),
        secondary_y=True,
    )
    fig.update_layout(
        title="Daily Sentiment and Article Count",
        height=280,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    fig.update_yaxes(title_text="Articles", secondary_y=False)
    fig.update_yaxes(title_text="Sentiment", secondary_y=True)
    return fig


def make_actual_vs_predicted_chart(pred_df: pd.DataFrame) -> go.Figure:
    recent_df = pred_df.tail(10).copy()

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=recent_df["prediction_for_date"],
            y=recent_df["actual_close"],
            name="Actual",
        )
    )
    fig.add_trace(
        go.Bar(
            x=recent_df["prediction_for_date"],
            y=recent_df["predicted_close"],
            name="Predicted",
        )
    )
    fig.update_layout(
        title="Actual versus Predicted Comparison",
        xaxis_title="Forecast Horizon / Recent Days",
        yaxis_title="Price",
        barmode="group",
        height=320,
        margin=dict(l=20, r=20, t=40, b=20),
    )
    return fig


def format_currency(value):
    if pd.isna(value):
        return "N/A"
    return f"${value:,.2f}"


def main():
    st.title("Oil Price Forecasting Dashboard")

    min_date, max_date, default_start, default_end = get_default_dates()
    models = get_model_names()
    sources = get_sources()

    if not models:
        st.error("No model runs found in the database.")
        return

    default_model = "ModelB_QuantileRegression" if "ModelB_QuantileRegression" in models else models[0]

    top_left, top_right = st.columns([2, 1.3])

    with top_left:
        start_date, end_date = st.date_input(
            "Date Range",
            value=(default_start, default_end),
            min_value=min_date,
            max_value=max_date,
        )

        if isinstance(start_date, tuple) or isinstance(start_date, list):
            # guard for odd streamlit behavior
            start_date, end_date = start_date

        price_df = get_price_trend(start_date, end_date)
        if price_df.empty:
            st.warning("No price data found for the selected range.")
        else:
            st.plotly_chart(make_price_chart(price_df), use_container_width=True)

    with top_right:
        source_name = st.selectbox("News Source", sources, index=0)
        selected_model = st.selectbox(
            "Forecasting Model",
            models,
            index=models.index(default_model) if default_model in models else 0,
        )
        sentiment_mode = st.selectbox(
            "Sentiment Aggregation",
            ["Average Daily Score", "Weighted by Relevance"],
            index=0,
        )

        sentiment_df = get_daily_sentiment(start_date, end_date, source_name, sentiment_mode)
        if sentiment_df.empty:
            st.warning("No sentiment data found for the selected filters.")
        else:
            st.plotly_chart(make_sentiment_chart(sentiment_df), use_container_width=True)

    latest_run = get_latest_model_run(selected_model)
    if latest_run is None:
        st.error("No runs found for the selected model.")
        return

    pred_df = get_predictions(latest_run["model_run_id"], start_date, end_date)

    bottom_left, bottom_middle, bottom_right = st.columns([1, 1.5, 1])

    with bottom_left:
        st.subheader("Next-Day Forecast")

        if pred_df.empty:
            st.info("No predictions found for the selected range.")
        else:
            latest_pred = pred_df.sort_values("prediction_for_date").iloc[-1]
            abs_error = None
            if pd.notna(latest_pred["actual_close"]):
                abs_error = abs(latest_pred["predicted_close"] - latest_pred["actual_close"])

            st.write(f"**Predicted Brent Price:** {format_currency(latest_pred['predicted_close'])}")
            st.write(f"**Actual Brent Price:** {format_currency(latest_pred['actual_close'])}")
            st.write(f"**Forecast Error:** {format_currency(abs_error) if abs_error is not None else 'N/A'}")
            st.write(f"**Selected Model:** {selected_model}")
            st.write(f"**Prediction Date:** {pd.to_datetime(latest_pred['prediction_for_date']).date()}")

        st.markdown("---")
        st.subheader("Evaluation Snapshot")
        st.write(f"**MAE:** {latest_run['mae']:.4f}" if pd.notna(latest_run["mae"]) else "**MAE:** N/A")
        st.write(f"**RMSE:** {latest_run['rmse']:.4f}" if pd.notna(latest_run["rmse"]) else "**RMSE:** N/A")
        st.write(f"**Run Version:** {latest_run['model_version']}")
        st.write(f"**Finished At:** {latest_run['finished_at']}")

    with bottom_middle:
        if pred_df.empty:
            st.info("No actual vs predicted comparison available.")
        else:
            st.plotly_chart(make_actual_vs_predicted_chart(pred_df), use_container_width=True)

    with bottom_right:
        st.subheader("Filters and Configuration")
        st.write(f"**Date Range:** {start_date} to {end_date}")
        st.write(f"**News Source:** {source_name}")
        st.write(f"**Forecasting Model:** {selected_model}")
        st.write(f"**Sentiment Aggregation:** {sentiment_mode}")
        st.button("Apply Filters", type="primary", use_container_width=True)

        st.markdown("---")
        st.caption(
            "This dashboard reads the latest prices, sentiment aggregates, "
            "model runs, and predictions from PostgreSQL."
        )


if __name__ == "__main__":
    main()