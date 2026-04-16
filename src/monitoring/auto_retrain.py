from datetime import datetime, timedelta, timezone
import json

from src.db.connection import get_connection
from src.modeling.train_model_a import train_model_a
from src.modeling.train_model_b import train_model_b


LATEST_FLAGS_SQL = """
WITH latest_logs AS (
    SELECT DISTINCT ON (component_name)
        monitoring_id,
        log_timestamp,
        component_name,
        severity,
        message,
        details
    FROM app.monitoring_logs
    WHERE log_type = 'model_drift'
      AND (
            component_name LIKE 'ModelA_%'
            OR component_name LIKE 'ModelB_%'
          )
    ORDER BY component_name, monitoring_id DESC
)
SELECT
    component_name,
    severity,
    message,
    details
FROM latest_logs
ORDER BY component_name;
"""

LATEST_RUN_SQL = """
SELECT MAX(finished_at)
FROM app.model_runs
WHERE model_name LIKE %s;
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


def insert_log(
    log_type: str,
    severity: str,
    component_name: str,
    message: str,
    metric_name: str | None = None,
    metric_value: float | None = None,
    details: dict | None = None,
) -> None:
    details_json = json.dumps(details or {})

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                INSERT_LOG_SQL,
                (
                    log_type,
                    severity,
                    component_name,
                    message,
                    metric_name,
                    metric_value,
                    details_json,
                ),
            )
        conn.commit()


def load_latest_flags() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(LATEST_FLAGS_SQL)
            rows = cur.fetchall()

    results = []
    for component_name, severity, message, details in rows:
        if details is None:
            details = {}

        if isinstance(details, str):
            details = json.loads(details)

        results.append(
            {
                "component_name": component_name,
                "severity": severity,
                "message": message,
                "details": details,
                "should_retrain": bool(details.get("should_retrain", False)),
            }
        )

    return results


def should_retrain_group(flags: list[dict], prefix: str) -> bool:
    relevant = [f for f in flags if f["component_name"].startswith(prefix)]
    return any(f["should_retrain"] for f in relevant)


def recently_retrained(model_prefix: str, hours: int = 24) -> bool:
    pattern = f"{model_prefix}%"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(LATEST_RUN_SQL, (pattern,))
            latest_finished_at = cur.fetchone()[0]

    if latest_finished_at is None:
        return False

    now_utc = datetime.now(timezone.utc)
    return latest_finished_at >= now_utc - timedelta(hours=hours)


def auto_retrain() -> None:
    flags = load_latest_flags()

    if not flags:
        print("No model drift flags found.")
        insert_log(
            log_type="pipeline",
            severity="info",
            component_name="AutoRetrainer",
            message="No model drift flags found. Nothing to retrain.",
            details={"action": "none"},
        )
        return

    retrain_a = should_retrain_group(flags, "ModelA_")
    retrain_b = should_retrain_group(flags, "ModelB_")

    print("\n=== Auto Retrain Decision ===")
    print(f"Retrain Model A group: {retrain_a}")
    print(f"Retrain Model B group: {retrain_b}")

    insert_log(
        log_type="pipeline",
        severity="info",
        component_name="AutoRetrainer",
        message="Auto-retrain decision computed.",
        details={
            "retrain_model_a": retrain_a,
            "retrain_model_b": retrain_b,
            "flag_count": len(flags),
        },
    )

    if retrain_a:
        if recently_retrained("ModelA_", hours=24):
            print("\nModel A retraining skipped: cooldown active.")
            insert_log(
                log_type="pipeline",
                severity="info",
                component_name="AutoRetrainer",
                message="Model A retraining skipped because cooldown is active.",
                details={"group": "ModelA", "cooldown_hours": 24},
            )
        else:
            print("\nRetraining Model A...")
            insert_log(
                log_type="pipeline",
                severity="warning",
                component_name="AutoRetrainer",
                message="Retraining Model A triggered by model drift.",
                details={"group": "ModelA"},
            )
            train_model_a()
            insert_log(
                log_type="pipeline",
                severity="info",
                component_name="AutoRetrainer",
                message="Model A retraining completed successfully.",
                details={"group": "ModelA"},
            )

    if retrain_b:
        if recently_retrained("ModelB_", hours=24):
            print("\nModel B retraining skipped: cooldown active.")
            insert_log(
                log_type="pipeline",
                severity="info",
                component_name="AutoRetrainer",
                message="Model B retraining skipped because cooldown is active.",
                details={"group": "ModelB", "cooldown_hours": 24},
            )
        else:
            print("\nRetraining Model B...")
            insert_log(
                log_type="pipeline",
                severity="warning",
                component_name="AutoRetrainer",
                message="Retraining Model B triggered by model drift.",
                details={"group": "ModelB"},
            )
            train_model_b()
            insert_log(
                log_type="pipeline",
                severity="info",
                component_name="AutoRetrainer",
                message="Model B retraining completed successfully.",
                details={"group": "ModelB"},
            )

    if not retrain_a and not retrain_b:
        print("\nNo retraining needed.")
        insert_log(
            log_type="pipeline",
            severity="info",
            component_name="AutoRetrainer",
            message="No retraining was needed after evaluation.",
            details={"action": "skipped"},
        )


if __name__ == "__main__":
    auto_retrain()