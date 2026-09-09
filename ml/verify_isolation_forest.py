"""Verify persisted Isolation Forest outputs and their model artifacts."""
import sys
from pathlib import Path

import joblib

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection


BATCH_ID = 1
MODELS_DIR = Path(__file__).resolve().parents[1] / "models"


def _table_columns(cur, table_name):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'analytics' AND table_name = %s;
        """,
        (table_name,),
    )
    return {row[0] for row in cur.fetchall()}


def _verify_artifacts(prefix):
    failures = []
    for suffix in (
        "imputer.joblib",
        "scaler.joblib",
        "isolation_forest.joblib",
        "feature_names.joblib",
    ):
        artifact = MODELS_DIR / f"{prefix}_{suffix}"
        if not artifact.is_file():
            failures.append(f"Missing artifact: {artifact.name}")
            continue
        try:
            joblib.load(artifact)
        except Exception as error:
            failures.append(f"Unreadable artifact {artifact.name}: {error}")
    return failures


def _verify_results(conn, feature_table, anomaly_table, identifier_column, prefix):
    failures = []
    with conn.cursor() as cur:
        feature_columns = _table_columns(cur, feature_table)
        anomaly_columns = _table_columns(cur, anomaly_table)
        required_columns = {
            identifier_column,
            "anomaly_score",
            "is_anomaly",
            "anomaly_rank",
        }
        missing_columns = required_columns - anomaly_columns
        if missing_columns:
            failures.append(
                f"analytics.{anomaly_table} is missing: {', '.join(sorted(missing_columns))}"
            )
            failures.extend(_verify_artifacts(prefix))
            return failures

        uses_batch = "batch_id" in feature_columns
        if uses_batch and "batch_id" not in anomaly_columns:
            failures.append(f"analytics.{anomaly_table} is missing batch_id")
            failures.extend(_verify_artifacts(prefix))
            return failures

        feature_filter = "WHERE batch_id = %s" if uses_batch else ""
        feature_params = (BATCH_ID,) if uses_batch else ()
        anomaly_filter = "WHERE batch_id = %s" if uses_batch else ""
        anomaly_params = (BATCH_ID,) if uses_batch else ()

        cur.execute(
            f"SELECT COUNT(*) FROM analytics.{feature_table} {feature_filter};",
            feature_params,
        )
        feature_count = cur.fetchone()[0]
        cur.execute(
            f"SELECT COUNT(*) FROM analytics.{anomaly_table} {anomaly_filter};",
            anomaly_params,
        )
        anomaly_count = cur.fetchone()[0]
        if feature_count != anomaly_count:
            failures.append(
                f"{anomaly_table} row count {anomaly_count} does not match "
                f"{feature_table} row count {feature_count}."
            )

        duplicate_key = (
            f"batch_id, {identifier_column}" if uses_batch else identifier_column
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {duplicate_key}
                FROM analytics.{anomaly_table}
                {anomaly_filter}
                GROUP BY {duplicate_key}
                HAVING COUNT(*) > 1
            ) duplicates;
            """,
            anomaly_params,
        )
        if cur.fetchone()[0]:
            failures.append(f"Duplicate {duplicate_key} values found in {anomaly_table}.")

        cur.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE anomaly_score IS NULL),
                COUNT(*) FILTER (WHERE anomaly_score < 0 OR anomaly_score > 1),
                MIN(anomaly_rank),
                MAX(anomaly_rank),
                COUNT(DISTINCT anomaly_rank)
            FROM analytics.{anomaly_table}
            {anomaly_filter};
            """,
            anomaly_params,
        )
        null_scores, out_of_range, min_rank, max_rank, distinct_ranks = cur.fetchone()
        if null_scores:
            failures.append(f"{anomaly_table} contains {null_scores} NULL anomaly scores.")
        if out_of_range:
            failures.append(f"{anomaly_table} contains {out_of_range} scores outside [0, 1].")
        if anomaly_count and (
            min_rank != 1 or max_rank != anomaly_count or distinct_ranks != anomaly_count
        ):
            failures.append(
                f"{anomaly_table} anomaly_rank is not a complete 1..{anomaly_count} ranking."
            )

    failures.extend(_verify_artifacts(prefix))
    return failures


def main():
    conn = get_connection()
    try:
        failures = _verify_results(
            conn, "node_features", "node_anomalies", "node_id", "node"
        )
        failures.extend(
            _verify_results(
                conn,
                "transaction_features",
                "transaction_anomalies",
                "txid",
                "tx",
            )
        )
    finally:
        conn.close()

    if failures:
        print("VERIFICATION AUDIT RESULT: CHECKS FAILED")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("VERIFICATION AUDIT RESULT: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
