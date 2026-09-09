"""Verify persisted DBSCAN cluster assignments and model artifacts."""
import sys
from pathlib import Path

import joblib

from train_clustering import NODE_FEATURES

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection


BATCH_ID = 1
MODELS_DIR = Path(__file__).resolve().parents[1] / "models"


def _failure(check, expected, actual):
    return f"FAILED CHECK: {check}\n  EXPECTED: {expected}\n  ACTUAL: {actual}"


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


def _verify_artifacts():
    failures = []
    artifacts = (
        "dbscan_imputer.joblib",
        "dbscan_scaler.joblib",
        "dbscan_model.joblib",
        "dbscan_feature_names.joblib",
    )
    for name in artifacts:
        path = MODELS_DIR / name
        if not path.is_file():
            failures.append(_failure("artifact exists", name, "missing"))
            continue
        try:
            artifact = joblib.load(path)
        except Exception as error:
            failures.append(_failure("artifact loads", name, str(error)))
            continue
        if name == "dbscan_feature_names.joblib" and artifact != NODE_FEATURES:
            failures.append(
                _failure("saved feature names", NODE_FEATURES, artifact)
            )
    return failures


def _verify_database(conn):
    failures = []
    with conn.cursor() as cur:
        feature_columns = _table_columns(cur, "node_features")
        cluster_columns = _table_columns(cur, "node_clusters")
        required_cluster_columns = {"node_id", "cluster_id", "is_noise"}
        missing_cluster_columns = required_cluster_columns - cluster_columns
        if missing_cluster_columns:
            failures.append(
                _failure(
                    "node_clusters required columns",
                    sorted(required_cluster_columns),
                    f"missing {sorted(missing_cluster_columns)}",
                )
            )
            return failures

        uses_batch = "batch_id" in feature_columns
        if uses_batch and "batch_id" not in cluster_columns:
            failures.append(_failure("batch-aware cluster output", "batch_id column", "missing"))
            return failures

        feature_filter = "WHERE features.batch_id = %s" if uses_batch else ""
        cluster_filter = "WHERE batch_id = %s" if uses_batch else ""
        parameters = (BATCH_ID,) if uses_batch else ()
        join_condition = "features.node_id = clusters.node_id"
        if uses_batch:
            join_condition += " AND features.batch_id = clusters.batch_id"

        cur.execute(
            f"SELECT COUNT(*) FROM analytics.node_features {feature_filter};",
            parameters,
        )
        feature_count = cur.fetchone()[0]
        cur.execute(
            f"SELECT COUNT(*) FROM analytics.node_clusters {cluster_filter};",
            parameters,
        )
        cluster_count = cur.fetchone()[0]
        if feature_count != cluster_count:
            failures.append(
                _failure("row-count reconciliation", feature_count, cluster_count)
            )

        duplicate_key = "batch_id, node_id" if uses_batch else "node_id"
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {duplicate_key}
                FROM analytics.node_clusters
                {cluster_filter}
                GROUP BY {duplicate_key}
                HAVING COUNT(*) > 1
            ) duplicates;
            """,
            parameters,
        )
        duplicate_count = cur.fetchone()[0]
        if duplicate_count:
            failures.append(
                _failure("duplicate cluster assignments", 0, duplicate_count)
            )

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM analytics.node_features features
            LEFT JOIN analytics.node_clusters clusters ON {join_condition}
            {feature_filter}
            AND clusters.node_id IS NULL;
            """,
            parameters,
        )
        missing_assignments = cur.fetchone()[0]
        if missing_assignments:
            failures.append(
                _failure("feature rows without cluster assignment", 0, missing_assignments)
            )

        reverse_filter = "WHERE clusters.batch_id = %s" if uses_batch else ""
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM analytics.node_clusters clusters
            LEFT JOIN analytics.node_features features ON {join_condition}
            {reverse_filter}
            AND features.node_id IS NULL;
            """,
            parameters,
        )
        unknown_nodes = cur.fetchone()[0]
        if unknown_nodes:
            failures.append(
                _failure("cluster rows without feature row", 0, unknown_nodes)
            )

        cur.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE cluster_id IS NULL),
                COUNT(*) FILTER (WHERE is_noise IS NULL),
                COUNT(*) FILTER (WHERE cluster_id = -1 AND is_noise IS NOT TRUE),
                COUNT(*) FILTER (WHERE cluster_id <> -1 AND is_noise IS NOT FALSE)
            FROM analytics.node_clusters
            {cluster_filter};
            """,
            parameters,
        )
        null_clusters, null_noise, invalid_noise, invalid_cluster = cur.fetchone()
        for check, actual in (
            ("NULL cluster IDs", null_clusters),
            ("NULL noise flags", null_noise),
            ("noise label/flag mismatch", invalid_noise),
            ("non-noise label/flag mismatch", invalid_cluster),
        ):
            if actual:
                failures.append(_failure(check, 0, actual))

        cur.execute(
            f"""
            SELECT cluster_id, COUNT(*)
            FROM analytics.node_clusters
            {cluster_filter}
            GROUP BY cluster_id
            ORDER BY cluster_id;
            """,
            parameters,
        )
        distribution = cur.fetchall()
        noise_count = next((count for cluster_id, count in distribution if cluster_id == -1), 0)
        cluster_total = sum(1 for cluster_id, _ in distribution if cluster_id != -1)
        noise_percentage = (noise_count / cluster_count * 100) if cluster_count else 0.0
        print(f"Number of clusters: {cluster_total}")
        print(f"Number of noise nodes: {noise_count}")
        print(f"Noise percentage: {noise_percentage:.2f}%")
        print("Cluster size distribution:")
        for cluster_id, count in distribution:
            print(f"  Cluster {cluster_id}: {count} nodes")

    return failures


def main():
    failures = []
    try:
        conn = get_connection()
        try:
            failures.extend(_verify_database(conn))
        finally:
            conn.close()
    except Exception as error:
        failures.append(_failure("database connection", "reachable PostgreSQL database", str(error)))
    failures.extend(_verify_artifacts())

    if failures:
        print("VERIFICATION AUDIT RESULT: CHECKS FAILED")
        for failure in failures:
            print(failure)
        return 1

    print("VERIFICATION AUDIT RESULT: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
