from __future__ import annotations

from math import sqrt
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import QuantileRegressor
from sklearn.metrics import (
    accuracy_score,
    mean_absolute_error,
    mean_squared_error,
)
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV

from src.db.connection import get_connection


FEATURE_SQL = """
SELECT
    feature_date,
    target_date,
    close_price,
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
ORDER BY feature_date;
"""


INSERT_MODEL_RUN_SQL = """
INSERT INTO app.model_runs (
    model_name,
    model_version,
    target_name,
    benchmark_code,
    run_status,
    training_start_date,
    training_end_date,
    validation_start_date,
    validation_end_date,
    test_start_date,
    test_end_date,
    mae,
    rmse,
    directional_accuracy,
    notes,
    finished_at
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, NOW()
)
RETURNING model_run_id;
"""


INSERT_LIVE_PREDICTION_SQL = """
INSERT INTO app.predictions (
    model_run_id,
    benchmark_code,
    feature_date,
    prediction_for_date,
    predicted_close,
    predicted_return_1d,
    predicted_direction,
    confidence_score,
    actual_close,
    actual_return_1d,
    created_at,
    is_backtest
)
VALUES (
    %s, 'BRENT', %s, %s,
    %s, %s, %s, %s,
    NULL, NULL, NOW(), FALSE
);
"""


INSERT_BACKTEST_PREDICTION_SQL = """
INSERT INTO app.predictions (
    model_run_id,
    benchmark_code,
    feature_date,
    prediction_for_date,
    predicted_close,
    predicted_return_1d,
    predicted_direction,
    confidence_score,
    actual_close,
    actual_return_1d,
    created_at,
    is_backtest
)
VALUES (
    %s, 'BRENT', %s, %s,
    %s, %s, %s, %s,
    %s, %s, NOW(), TRUE
);
"""


def load_features() -> pd.DataFrame:
    with get_connection() as conn:
        df = pd.read_sql(FEATURE_SQL, conn)

    df["feature_date"] = pd.to_datetime(df["feature_date"])
    df["target_date"] = pd.to_datetime(df["target_date"])

    numeric_cols = [
        "close_price",
        "lag_close_1d",
        "lag_close_3d",
        "lag_return_1d",
        "lag_return_3d",
        "rolling_mean_3d",
        "rolling_mean_7d",
        "rolling_volatility_7d",
        "news_article_count_1d",
        "news_avg_sentiment_1d",
        "news_weighted_sentiment_1d",
        "news_avg_sentiment_3d",
        "news_weighted_sentiment_3d",
        "target_close_next_day",
        "target_return_next_day",
        "target_direction_next_day",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    news_cols = [
        "news_article_count_1d",
        "news_avg_sentiment_1d",
        "news_weighted_sentiment_1d",
        "news_avg_sentiment_3d",
        "news_weighted_sentiment_3d",
    ]
    df[news_cols] = df[news_cols].fillna(0)

    return df


def next_business_day(d: pd.Timestamp) -> pd.Timestamp:
    next_day = d + pd.Timedelta(days=1)

    # Saturday -> Monday
    if next_day.weekday() == 5:
        next_day += pd.Timedelta(days=2)

    # Sunday -> Monday
    elif next_day.weekday() == 6:
        next_day += pd.Timedelta(days=1)

    return next_day


def split_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    n = len(df)
    train_end = int(n * 0.70)
    val_end = int(n * 0.80)

    train_df = df.iloc[:train_end].copy()
    val_df = df.iloc[train_end:val_end].copy()
    test_df = df.iloc[val_end:].copy()

    return train_df, val_df, test_df


def get_feature_columns() -> list[str]:
    return [
        "lag_close_1d",
        "lag_close_3d",
        "lag_return_1d",
        "lag_return_3d",
        "rolling_mean_3d",
        "rolling_mean_7d",
        "rolling_volatility_7d",
        "news_article_count_1d",
        "news_avg_sentiment_1d",
        "news_weighted_sentiment_1d",
        "news_avg_sentiment_3d",
        "news_weighted_sentiment_3d",
    ]


def train_price_model(X_train: pd.DataFrame, y_train: pd.Series):
    """
    Uses Quantile Regression because your current repo already says this was
    one of the best-performing models. We are not summoning an LSTM demon
    two days before delivery.
    """
    tscv = TimeSeriesSplit(n_splits=3)

    search = GridSearchCV(
        estimator=QuantileRegressor(quantile=0.5, solver="highs"),
        param_grid={"alpha": [0.0, 0.01, 0.1, 1.0]},
        scoring="neg_mean_absolute_error",
        cv=tscv,
        n_jobs=-1,
    )

    search.fit(X_train, y_train)
    print(f"Best Quantile Regression params: {search.best_params_}")
    return search.best_estimator_


def train_direction_model(X_train: pd.DataFrame, y_train: pd.Series):
    """
    Direction model:
    - 1 means UP
    - -1 means DOWN
    - 0 means FLAT

    Random Forest is simple, handles non-linear relationships, and gives
    class probabilities for dashboard confidence.
    """
    model = RandomForestClassifier(
        n_estimators=400,
        max_depth=6,
        min_samples_leaf=3,
        random_state=42,
        class_weight="balanced",
        n_jobs=-1,
    )

    model.fit(X_train, y_train)
    return model


def direction_to_label(direction_value: int) -> str:
    if direction_value == 1:
        return "UP"
    if direction_value == -1:
        return "DOWN"
    return "FLAT"


def calculate_predicted_return(
    predicted_close: float,
    current_close: float,
) -> float:
    if current_close == 0 or pd.isna(current_close):
        return np.nan

    return (predicted_close / current_close) - 1


def insert_model_run(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    mae: float,
    rmse: float,
    directional_accuracy: float,
) -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                INSERT_MODEL_RUN_SQL,
                (
                    "NextDay_Price_And_Direction_Model",
                    "final",
                    "target_close_next_day_and_target_direction_next_day",
                    "BRENT",
                    "completed",
                    train_df["feature_date"].min().date(),
                    train_df["feature_date"].max().date(),
                    val_df["feature_date"].min().date(),
                    val_df["feature_date"].max().date(),
                    test_df["feature_date"].min().date(),
                    test_df["feature_date"].max().date(),
                    float(mae),
                    float(rmse),
                    float(directional_accuracy),
                    (
                        "Final next-day model. Predicts Brent next-day close price "
                        "using Quantile Regression and next-day UP/DOWN direction "
                        "using Random Forest classification with price and sentiment features."
                    ),
                ),
            )
            model_run_id = cur.fetchone()[0]
            conn.commit()

    return model_run_id


def insert_backtest_predictions(
    model_run_id: int,
    test_df: pd.DataFrame,
) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for _, row in test_df.iterrows():
                cur.execute(
                    INSERT_BACKTEST_PREDICTION_SQL,
                    (
                        model_run_id,
                        row["feature_date"].date(),
                        row["target_date"].date(),
                        float(row["predicted_close"]),
                        float(row["predicted_return_1d"]),
                        int(row["predicted_direction"]),
                        float(row["direction_confidence"]),
                        float(row["target_close_next_day"]),
                        float(row["target_return_next_day"]),
                    ),
                )

            conn.commit()


def insert_live_prediction(
    model_run_id: int,
    latest_row: pd.Series,
    predicted_close: float,
    predicted_return: float,
    predicted_direction: int,
    confidence_score: float,
) -> None:
    feature_date = latest_row["feature_date"]
    target_date = latest_row["target_date"]

    if pd.isna(target_date):
        target_date = next_business_day(feature_date)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                INSERT_LIVE_PREDICTION_SQL,
                (
                    model_run_id,
                    feature_date.date(),
                    target_date.date(),
                    float(predicted_close),
                    float(predicted_return),
                    int(predicted_direction),
                    float(confidence_score),
                ),
            )

            conn.commit()


def main() -> None:
    df = load_features()
    feature_cols = get_feature_columns()

    labelled_df = df.dropna(
        subset=feature_cols
        + [
            "target_close_next_day",
            "target_return_next_day",
            "target_direction_next_day",
        ]
    ).copy()


    live_candidates = df.dropna(subset=feature_cols + ["close_price"]).copy()

    if len(labelled_df) < 50:
        raise ValueError(
            f"Not enough labelled rows to train safely. Found {len(labelled_df)} rows."
        )

    if live_candidates.empty:
        raise ValueError("No valid latest feature row found for live prediction.")

    train_df, val_df, test_df = split_data(labelled_df)

    X_train = train_df[feature_cols]
    y_price_train = train_df["target_close_next_day"]
    y_direction_train = train_df["target_direction_next_day"].astype(int)

    X_test = test_df[feature_cols]
    y_price_test = test_df["target_close_next_day"]
    y_direction_test = test_df["target_direction_next_day"].astype(int)

    price_model = train_price_model(X_train, y_price_train)
    direction_model = train_direction_model(X_train, y_direction_train)

    test_df["predicted_close"] = price_model.predict(X_test)
    test_df["predicted_return_1d"] = (
        test_df["predicted_close"] / test_df["close_price"]
    ) - 1

    test_df["predicted_direction"] = direction_model.predict(X_test).astype(int)

    direction_proba = direction_model.predict_proba(X_test)
    test_df["direction_confidence"] = direction_proba.max(axis=1)

    mae = mean_absolute_error(y_price_test, test_df["predicted_close"])
    rmse = sqrt(mean_squared_error(y_price_test, test_df["predicted_close"]))
    directional_accuracy = accuracy_score(
        y_direction_test,
        test_df["predicted_direction"],
    )

    model_run_id = insert_model_run(
        train_df=train_df,
        val_df=val_df,
        test_df=test_df,
        mae=mae,
        rmse=rmse,
        directional_accuracy=directional_accuracy,
    )

    insert_backtest_predictions(model_run_id, test_df)

    latest_row = live_candidates.sort_values("feature_date").iloc[-1]
    X_live = latest_row[feature_cols].to_frame().T

    live_predicted_close = float(price_model.predict(X_live)[0])
    live_predicted_return = calculate_predicted_return(
        predicted_close=live_predicted_close,
        current_close=float(latest_row["close_price"]),
    )

    live_predicted_direction = int(direction_model.predict(X_live)[0])
    live_confidence = float(direction_model.predict_proba(X_live).max(axis=1)[0])

    insert_live_prediction(
        model_run_id=model_run_id,
        latest_row=latest_row,
        predicted_close=live_predicted_close,
        predicted_return=live_predicted_return,
        predicted_direction=live_predicted_direction,
        confidence_score=live_confidence,
    )

    print("\n=== Next-Day Prediction Model Complete ===")
    print(f"Rows used: {len(labelled_df)}")
    print(f"Train rows: {len(train_df)}")
    print(f"Validation rows: {len(val_df)}")
    print(f"Test rows: {len(test_df)}")
    print(f"MAE: {mae:.6f}")
    print(f"RMSE: {rmse:.6f}")
    print(f"Directional accuracy: {directional_accuracy:.4f}")

    target_date = latest_row["target_date"]
    if pd.isna(target_date):
        target_date = next_business_day(latest_row["feature_date"])

    print("\n=== Live Next-Day Forecast ===")
    print(f"Feature date: {latest_row['feature_date'].date()}")
    print(f"Prediction for: {target_date.date()}")
    print(f"Current close: {float(latest_row['close_price']):.4f}")
    print(f"Predicted close: {live_predicted_close:.4f}")
    print(f"Predicted return: {live_predicted_return:.6f}")
    print(
        f"Predicted direction: {direction_to_label(live_predicted_direction)} "
        f"({live_predicted_direction})"
    )
    print(f"Confidence: {live_confidence:.4f}")


if __name__ == "__main__":
    main()