"""Verify persisted SHAP explanations without rerunning ML stages."""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import joblib

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1
MODELS_DIR = Path(__file__).resolve().parents[1] / "models"


def _failure(check, expected, actual):
    return f"FAILED CHECK: {check}\n  EXPECTED: {expected}\n  ACTUAL: {actual}"


def verify_artifacts():
    failures = []
    artifacts = [
        "node_imputer.joblib",
        "node_scaler.joblib",
        "node_isolation_forest.joblib",
        "node_feature_names.joblib"
    ]
    for artifact_name in artifacts:
        path = MODELS_DIR / artifact_name
        if not path.is_file():
            failures.append(_failure(f"Artifact {artifact_name} exists", "Yes", "No"))
            continue
        try:
            joblib.load(path)
        except Exception as e:
            failures.append(_failure(f"Artifact {artifact_name} is loadable", "Yes", f"Exception: {e}"))
    return failures


def verify_database(conn):
    failures = []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'analytics' AND table_name = 'node_explanations'
            );
            """
        )
        if not cur.fetchone()[0]:
            failures.append(_failure("Table analytics.node_explanations exists", "Yes", "No"))
            return failures

    df = pd.read_sql("SELECT * FROM analytics.node_explanations WHERE batch_id = %s", conn, params=(BATCH_ID,))
    nodes_df = pd.read_sql("SELECT node_id FROM analytics.node_features WHERE batch_id = %s", conn, params=(BATCH_ID,))

    feature_cols = joblib.load(MODELS_DIR / "node_feature_names.joblib")
    expected_num_features = len(feature_cols)
    expected_num_nodes = len(nodes_df)

    actual_row_count = len(df)
    expected_row_count = expected_num_nodes * expected_num_features
    if actual_row_count != expected_row_count:
        failures.append(_failure("Expected row count", expected_row_count, actual_row_count))

    explanation_nodes = set(df['node_id'])
    feature_nodes = set(nodes_df['node_id'])

    missing_nodes = feature_nodes - explanation_nodes
    if missing_nodes:
        failures.append(_failure("Every node has explanation rows", "Yes", f"{len(missing_nodes)} nodes missing"))

    invalid_nodes = explanation_nodes - feature_nodes
    if invalid_nodes:
        failures.append(_failure("No explanation rows refer to nonexistent nodes", "Yes", f"{len(invalid_nodes)} nonexistent nodes referenced"))

    for node_id, group in df.groupby('node_id'):
        node_features = set(group['feature_name'])
        missing_feats = set(feature_cols) - node_features
        if missing_feats:
            failures.append(_failure(f"Node {node_id} has all saved training features", "Yes", f"Missing features: {missing_feats}"))
            break

    duplicates = df.duplicated(subset=['batch_id', 'node_id', 'feature_name']).sum()
    if duplicates > 0:
        failures.append(_failure("No duplicate batch_id + node_id + feature_name", 0, duplicates))

    invalid_features = set(df['feature_name']) - set(feature_cols)
    if invalid_features:
        failures.append(_failure("Every feature_name belongs to saved features", "Yes", f"Invalid: {invalid_features}"))

    null_feature_values = df['feature_value'].isna().sum()
    if null_feature_values > 0:
        failures.append(_failure("feature_value is not NULL", 0, null_feature_values))

    null_shap = df['shap_value'].isna().sum()
    if null_shap > 0:
        failures.append(_failure("shap_value is not NULL", 0, null_shap))

    null_abs_shap = df['absolute_shap_value'].isna().sum()
    if null_abs_shap > 0:
        failures.append(_failure("absolute_shap_value is not NULL", 0, null_abs_shap))

    abs_diff = (df['absolute_shap_value'].astype(float) - df['shap_value'].astype(float).abs()).abs()
    tolerance_failures = (abs_diff > 1e-5).sum()
    if tolerance_failures > 0:
        failures.append(_failure("absolute_shap_value equals ABS(shap_value)", 0, tolerance_failures))

    for node_id, group in df.groupby('node_id'):
        ranks = group['feature_rank'].astype(int).sort_values().values

        if (ranks == 1).sum() != 1:
            failures.append(_failure(f"Node {node_id} has exactly one rank 1", 1, (ranks == 1).sum()))
            break

        expected_ranks = np.arange(1, len(group) + 1)
        if not np.array_equal(ranks, expected_ranks):
            failures.append(_failure(f"Node {node_id} ranks are contiguous", "Yes", "No"))
            break

        group_sorted = group.sort_values('feature_rank')
        abs_shap_vals = group_sorted['absolute_shap_value'].astype(float).values
        if not np.all(np.diff(-abs_shap_vals) >= -1e-8):
            failures.append(_failure(f"Node {node_id} rank order follows descending absolute_shap_value", "Yes", "No"))
            break

    return failures


def main():
    failures = verify_artifacts()

    if not failures:
        try:
            conn = get_connection()
            try:
                failures.extend(verify_database(conn))
            finally:
                conn.close()
        except Exception as e:
            failures.append(_failure("Database connection and querying", "Success", str(e)))

    if failures:
        print("VERIFICATION AUDIT RESULT: CHECKS FAILED")
        for f in failures:
            print(f)
        return 1

    print("VERIFICATION AUDIT RESULT: ALL CHECKS PASSED")
    return 0

if __name__ == "__main__":
    sys.exit(main())
