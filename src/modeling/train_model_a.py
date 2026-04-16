from math import sqrt
from typing import Dict, Tuple

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, QuantileRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, TimeSeriesSplit
from xgboost import XGBRegressor

from src.db.connection import get_connection


FEATURE_SQL = """
SELECT
    feature_date,
    target_date,
    lag_close_1d,
    lag_close_3d,
    lag_return_1d,
    lag_return_3d,
    rolling_mean_3d,
    rolling_mean_7d,
    rolling_volatility_7d,
    target_close_next_day
FROM app.model_features
WHERE benchmark_code = 'BRENT'
  AND lag_close_1d IS NOT NULL
  AND lag_close_3d IS NOT NULL
  AND lag_return_1d IS NOT NULL
  AND lag_return_3d IS NOT NULL
  AND rolling_mean_3d IS NOT NULL
  AND rolling_mean_7d IS NOT NULL
  AND rolling_volatility_7d IS NOT NULL
  AND target_close_next_day IS NOT NULL
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
    notes,
    finished_at
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, NOW()
)
RETURNING model_run_id;
"""

INSERT_PREDICTION_SQL = """
INSERT INTO app.predictions (
    model_run_id,
    benchmark_code,
    feature_date,
    prediction_for_date,
    predicted_close,
    actual_close,
    created_at,
    is_backtest
)
VALUES (
    %s, %s, %s, %s, %s, %s, NOW(), TRUE
);
"""


def load_features() -> pd.DataFrame:
    with get_connection() as conn:
        df = pd.read_sql(FEATURE_SQL, conn)

    df["feature_date"] = pd.to_datetime(df["feature_date"])
    df["target_date"] = pd.to_datetime(df["target_date"])
    return df


def split_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    n = len(df)
    train_end = int(n * 0.7)
    val_end = int(n * 0.8)

    train_df = df.iloc[:train_end].copy()
    val_df = df.iloc[train_end:val_end].copy()
    test_df = df.iloc[val_end:].copy()

    return train_df, val_df, test_df


def insert_model_run(
    model_name: str,
    model_version: str,
    benchmark_code: str,
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    mae: float,
    rmse: float,
    notes: str,
) -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                INSERT_MODEL_RUN_SQL,
                (
                    model_name,
                    model_version,
                    "target_close_next_day",
                    benchmark_code,
                    "completed",
                    train_df["feature_date"].min().date(),
                    train_df["feature_date"].max().date(),
                    val_df["feature_date"].min().date(),
                    val_df["feature_date"].max().date(),
                    test_df["feature_date"].min().date(),
                    test_df["feature_date"].max().date(),
                    float(mae),
                    float(rmse),
                    notes,
                ),
            )
            model_run_id = cur.fetchone()[0]
        conn.commit()

    return model_run_id


def insert_predictions(
    model_run_id: int,
    benchmark_code: str,
    prediction_df: pd.DataFrame,
    prediction_col: str,
) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for _, row in prediction_df.iterrows():
                cur.execute(
                    INSERT_PREDICTION_SQL,
                    (
                        model_run_id,
                        benchmark_code,
                        row["feature_date"].date(),
                        row["target_date"].date(),
                        float(row[prediction_col]),
                        float(row["target_close_next_day"]),
                    ),
                )
        conn.commit()


def evaluate_predictions(
    y_true: pd.Series,
    y_pred: pd.Series,
) -> Tuple[float, float]:
    mae = mean_absolute_error(y_true, y_pred)
    rmse = sqrt(mean_squared_error(y_true, y_pred))
    return mae, rmse


def build_models(X_train: pd.DataFrame, y_train: pd.Series) -> Dict[str, object]:
    tscv = TimeSeriesSplit(n_splits=3)

    lr_model = LinearRegression()
    lr_model.fit(X_train, y_train)

    qr_search = GridSearchCV(
        estimator=QuantileRegressor(quantile=0.5, solver="highs"),
        param_grid={"alpha": [0.0, 0.01, 0.1, 1.0]},
        scoring="neg_mean_absolute_error",
        cv=tscv,
        n_jobs=-1,
    )
    qr_search.fit(X_train, y_train)
    qr_model = qr_search.best_estimator_

    rf_search = RandomizedSearchCV(
        estimator=RandomForestRegressor(random_state=42, n_jobs=-1),
        param_distributions={
            "n_estimators": [100, 200, 400, 600],
            "max_depth": [4, 6, 8, None],
            "min_samples_leaf": [1, 2, 4],
            "min_samples_split": [2, 5, 10],
        },
        n_iter=12,
        scoring="neg_mean_absolute_error",
        cv=tscv,
        random_state=42,
        n_jobs=-1,
    )
    rf_search.fit(X_train, y_train)
    rf_model = rf_search.best_estimator_

    xgb_search = RandomizedSearchCV(
        estimator=XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            n_jobs=-1,
        ),
        param_distributions={
            "n_estimators": [100, 200, 400, 600],
            "max_depth": [2, 3, 5, 7],
            "learning_rate": [0.03, 0.05, 0.1, 0.15],
            "subsample": [0.7, 0.8, 1.0],
            "colsample_bytree": [0.7, 0.8, 1.0],
        },
        n_iter=15,
        scoring="neg_mean_absolute_error",
        cv=tscv,
        random_state=42,
        n_jobs=-1,
    )
    xgb_search.fit(X_train, y_train)
    xgb_model = xgb_search.best_estimator_

    print("\nBest params:")
    print(f"Quantile Regression: {qr_search.best_params_}")
    print(f"Random Forest:       {rf_search.best_params_}")
    print(f"XGBoost:             {xgb_search.best_params_}")

    return {
        "LinearRegression": lr_model,
        "QuantileRegression": qr_model,
        "RandomForest": rf_model,
        "XGBoost": xgb_model,
    }


def train_model_a() -> None:
    df = load_features()

    feature_cols = [
        "lag_close_1d",
        "lag_close_3d",
        "lag_return_1d",
        "lag_return_3d",
        "rolling_mean_3d",
        "rolling_mean_7d",
        "rolling_volatility_7d",
    ]
    target_col = "target_close_next_day"

    train_df, val_df, test_df = split_data(df)

    X_train = train_df[feature_cols]
    y_train = train_df[target_col]
    X_test = test_df[feature_cols]
    y_test = test_df[target_col]

    models = build_models(X_train, y_train)

    test_df["pred_lr"] = models["LinearRegression"].predict(X_test)
    test_df["pred_qr"] = models["QuantileRegression"].predict(X_test)
    test_df["pred_rf"] = models["RandomForest"].predict(X_test)
    test_df["pred_xgb"] = models["XGBoost"].predict(X_test)

    test_df["pred_ensemble"] = (
        test_df["pred_lr"]
        + test_df["pred_qr"]
        + test_df["pred_rf"]
        + test_df["pred_xgb"]
    ) / 4.0

    metrics = {}
    for label, col in {
        "LinearRegression": "pred_lr",
        "QuantileRegression": "pred_qr",
        "RandomForest": "pred_rf",
        "XGBoost": "pred_xgb",
        "Ensemble": "pred_ensemble",
    }.items():
        mae, rmse = evaluate_predictions(y_test, test_df[col])
        metrics[label] = {"mae": mae, "rmse": rmse}

    run_ids = {}
    run_ids["LinearRegression"] = insert_model_run(
        "ModelA_LinearRegression",
        "final",
        "BRENT",
        train_df,
        val_df,
        test_df,
        metrics["LinearRegression"]["mae"],
        metrics["LinearRegression"]["rmse"],
        "Final Model A: historical-price-only linear regression baseline.",
    )
    run_ids["QuantileRegression"] = insert_model_run(
        "ModelA_QuantileRegression",
        "final",
        "BRENT",
        train_df,
        val_df,
        test_df,
        metrics["QuantileRegression"]["mae"],
        metrics["QuantileRegression"]["rmse"],
        "Final Model A: historical-price-only quantile regression.",
    )
    run_ids["RandomForest"] = insert_model_run(
        "ModelA_RandomForest",
        "final",
        "BRENT",
        train_df,
        val_df,
        test_df,
        metrics["RandomForest"]["mae"],
        metrics["RandomForest"]["rmse"],
        "Final Model A: historical-price-only random forest.",
    )
    run_ids["XGBoost"] = insert_model_run(
        "ModelA_XGBoost",
        "final",
        "BRENT",
        train_df,
        val_df,
        test_df,
        metrics["XGBoost"]["mae"],
        metrics["XGBoost"]["rmse"],
        "Final Model A: historical-price-only XGBoost.",
    )
    run_ids["Ensemble"] = insert_model_run(
        "ModelA_Ensemble_All",
        "final",
        "BRENT",
        train_df,
        val_df,
        test_df,
        metrics["Ensemble"]["mae"],
        metrics["Ensemble"]["rmse"],
        "Final Model A: simple average of LR, QR, RF, XGB.",
    )

    insert_predictions(run_ids["LinearRegression"], "BRENT", test_df, "pred_lr")
    insert_predictions(run_ids["QuantileRegression"], "BRENT", test_df, "pred_qr")
    insert_predictions(run_ids["RandomForest"], "BRENT", test_df, "pred_rf")
    insert_predictions(run_ids["XGBoost"], "BRENT", test_df, "pred_xgb")
    insert_predictions(run_ids["Ensemble"], "BRENT", test_df, "pred_ensemble")

    print(f"\nRows used: {len(df)}")
    print(f"Train rows: {len(train_df)}")
    print(f"Validation rows: {len(val_df)}")
    print(f"Test rows: {len(test_df)}")

    print("\n=== Model A Comparison ===")
    for model_name, vals in metrics.items():
        print(f"{model_name:<18} -> MAE: {vals['mae']:.6f}, RMSE: {vals['rmse']:.6f}")

    print("\nLatest predictions:")
    print(
        test_df[
            [
                "feature_date",
                "target_date",
                "target_close_next_day",
                "pred_lr",
                "pred_qr",
                "pred_rf",
                "pred_xgb",
                "pred_ensemble",
            ]
        ]
        .tail(10)
        .to_string(index=False)
    )

    latest_row = test_df.iloc[-1]
    print("\n=== Tomorrow Prediction Summary (Model A) ===")
    print(f"Feature date used: {latest_row['feature_date'].date()}")
    print(f"Prediction for date: {latest_row['target_date'].date()}")
    print(f"Linear Regression prediction: {latest_row['pred_lr']:.4f}")
    print(f"Quantile Regression pred:    {latest_row['pred_qr']:.4f}")
    print(f"Random Forest prediction:    {latest_row['pred_rf']:.4f}")
    print(f"XGBoost prediction:          {latest_row['pred_xgb']:.4f}")
    print(f"Ensemble prediction:         {latest_row['pred_ensemble']:.4f}")


if __name__ == "__main__":
    train_model_a()