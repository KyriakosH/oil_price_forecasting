import json
from dataclasses import dataclass
from typing import List

import pandas as pd

from src.db.connection import get_connection


PREDICTION_SQL = """
WITH latest_runs AS (
    SELECT DISTINCT ON (mr.model_name)
        mr.model_run_id,
        mr.model_name,
        mr.model_version
    FROM app.model_runs mr
    WHERE mr.benchmark_code = 'BRENT'
    ORDER BY mr.model_name, mr.model_run_id DESC
)
SELECT
    lr.model_run_id,
    lr.model_name,
    lr.model_version,
    p.feature_date,
    p.prediction_for_date,
    p.predicted_close,
    p.actual_close
FROM latest_runs lr
JOIN app.predictions p
    ON p.model_run_id = lr.model_run_id
WHERE p.benchmark_code = 'BRENT'
  AND p.actual_close IS NOT NULL
ORDER BY lr.model_name, p.prediction_for_date;
"""

INSERT_LOG_SQL = """
INSERT INTO app.monitoring_logs (
    log_type,
    severity,
    component_name,
    message,
    metric_name,
    metric_value,
    details
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s::jsonb
);
"""


@dataclass
class RetrainingDecision:
    model_name: str
    severity: str
    message: str
    recent_mae: float
    previous_mae: float | None
    should_retrain: bool
    details: dict


def load_predictions() -> pd.DataFrame:
    with get_connection() as conn:
        df = pd.read_sql(PREDICTION_SQL, conn)

    df["feature_date"] = pd.to_datetime(df["feature_date"])
    df["prediction_for_date"] = pd.to_datetime(df["prediction_for_date"])
    return df


def evaluate_model(group: pd.DataFrame, recent_window: int = 30) -> RetrainingDecision:
    group = group.sort_values("prediction_for_date").copy()
    group["abs_error"] = (group["predicted_close"] - group["actual_close"]).abs()

    model_name = group["model_name"].iloc[0]
    total_rows = len(group)

    if total_rows < recent_window:
        recent_mae = float(group["abs_error"].mean())
        return RetrainingDecision(
            model_name=model_name,
            severity="info",
            message=f"Not enough predictions yet for robust retraining decision on {model_name}.",
            recent_mae=recent_mae,
            previous_mae=None,
            should_retrain=False,
            details={
                "rows_available": total_rows,
                "recent_window": recent_window,
                "reason": "insufficient_history",
            },
        )

    recent = group.tail(recent_window)
    recent_mae = float(recent["abs_error"].mean())

    previous = group.iloc[-(2 * recent_window):-recent_window]
    previous_mae = float(previous["abs_error"].mean()) if len(previous) > 0 else None

    should_retrain = False
    severity = "info"
    message = f"{model_name} is stable. No retraining needed."

    if previous_mae is not None:
        relative_change = (
            (recent_mae - previous_mae) / previous_mae if previous_mae != 0 else 0.0
        )

        if recent_mae > previous_mae * 1.30:
            should_retrain = True
            severity = "error"
            message = (
                f"{model_name} degraded sharply. Recent MAE={recent_mae:.4f}, "
                f"previous MAE={previous_mae:.4f}. Retraining strongly recommended."
            )
        elif recent_mae > previous_mae * 1.15:
            should_retrain = True
            severity = "warning"
            message = (
                f"{model_name} degraded. Recent MAE={recent_mae:.4f}, "
                f"previous MAE={previous_mae:.4f}. Retraining recommended."
            )
        else:
            should_retrain = False
            severity = "info"
            message = (
                f"{model_name} stable. Recent MAE={recent_mae:.4f}, "
                f"previous MAE={previous_mae:.4f}."
            )

        details = {
            "rows_available": total_rows,
            "recent_window": recent_window,
            "recent_mae": recent_mae,
            "previous_mae": previous_mae,
            "relative_change": relative_change,
            "should_retrain": should_retrain,
        }
    else:
        details = {
            "rows_available": total_rows,
            "recent_window": recent_window,
            "recent_mae": recent_mae,
            "previous_mae": None,
            "should_retrain": False,
            "reason": "no_previous_window",
        }

    return RetrainingDecision(
        model_name=model_name,
        severity=severity,
        message=message,
        recent_mae=recent_mae,
        previous_mae=previous_mae,
        should_retrain=should_retrain,
        details=details,
    )


def log_decisions(decisions: List[RetrainingDecision]) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for d in decisions:
                cur.execute(
                    INSERT_LOG_SQL,
                    (
                        "model_drift",
                        d.severity,
                        d.model_name,
                        d.message,
                        "recent_mae",
                        d.recent_mae,
                        json.dumps(d.details),
                    ),
                )
        conn.commit()


def main() -> None:
    df = load_predictions()

    if df.empty:
        print("No predictions with actual_close found. Nothing to evaluate.")
        return

    decisions: List[RetrainingDecision] = []
    for _, group in df.groupby("model_name"):
        decisions.append(evaluate_model(group, recent_window=30))

    log_decisions(decisions)

    print("\n=== Retraining Check Results ===")
    for d in decisions:
        print(f"\nModel: {d.model_name}")
        print(f"Severity: {d.severity}")
        print(f"Recent MAE: {d.recent_mae:.6f}")
        print(
            "Previous MAE: "
            + (f"{d.previous_mae:.6f}" if d.previous_mae is not None else "None")
        )
        print(f"Should retrain: {d.should_retrain}")
        print(f"Message: {d.message}")


if __name__ == "__main__":
    main()