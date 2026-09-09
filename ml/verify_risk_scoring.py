"""Verify stored node risk scores without rerunning upstream ML stages."""
import sys
from pathlib import Path

import pandas as pd

from score_and_explain import (
    ANOMALY_WEIGHT,
    BATCH_ID,
    DBSCAN_NOISE_WEIGHT,
    risk_level_for_score,
)

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection


ALLOWED_LEVELS = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
SCORE_TOLERANCE = 0.00011


def _failure(check, expected, actual):
    return f"FAILED CHECK: {check}\n  EXPECTED: {expected}\n  ACTUAL: {actual}"


def _load_results(conn):
    anomalies = pd.read_sql(
        """
        SELECT node_id, anomaly_score, is_anomaly
        FROM analytics.node_anomalies
        WHERE batch_id = %s;
        """,
        conn,
        params=(BATCH_ID,),
    )
    clusters = pd.read_sql(
        """
        SELECT node_id, cluster_id, is_noise
        FROM analytics.node_clusters
        WHERE batch_id = %s;
        """,
        conn,
        params=(BATCH_ID,),
    )
    risks = pd.read_sql(
        """
        SELECT node_id, anomaly_score, is_anomaly, cluster_id, is_noise,
               combined_risk_score, risk_level, risk_rank
        FROM analytics.node_risk_scores
        WHERE batch_id = %s;
        """,
        conn,
        params=(BATCH_ID,),
    )
    return anomalies, clusters, risks


def _verify_results(anomalies, clusters, risks):
    failures = []
    for name, data in (
        ("node_anomalies", anomalies),
        ("node_clusters", clusters),
        ("node_risk_scores", risks),
    ):
        duplicates = int(data["node_id"].duplicated().sum())
        if duplicates:
            failures.append(_failure(f"duplicate node IDs in {name}", 0, duplicates))

    anomaly_ids = set(anomalies["node_id"])
    cluster_ids = set(clusters["node_id"])
    risk_ids = set(risks["node_id"])
    matched_ids = anomaly_ids & cluster_ids

    if len(risks) != len(matched_ids):
        failures.append(
            _failure("risk row count", len(matched_ids), len(risks))
        )
    for check, expected, actual in (
        ("anomaly entities missing risk rows", anomaly_ids, risk_ids),
        ("cluster entities missing risk rows", cluster_ids, risk_ids),
    ):
        if expected != actual:
            failures.append(
                _failure(check, "same entity set", f"missing={sorted(expected - actual)}")
            )

    source_results = anomalies.merge(
        clusters, on="node_id", how="inner", validate="one_to_one"
    )
    compared_results = risks.merge(
        source_results,
        on="node_id",
        how="inner",
        suffixes=("_risk", "_source"),
        validate="one_to_one",
    )
    if len(compared_results) == len(risks):
        source_mismatches = (
            (compared_results["anomaly_score_risk"] != compared_results["anomaly_score_source"])
            | (compared_results["is_anomaly_risk"] != compared_results["is_anomaly_source"])
            | (compared_results["cluster_id_risk"] != compared_results["cluster_id_source"])
            | (compared_results["is_noise_risk"] != compared_results["is_noise_source"])
        )
        if source_mismatches.any():
            failures.append(
                _failure("risk input preservation", 0, int(source_mismatches.sum()))
            )

    numeric_scores = pd.to_numeric(risks["combined_risk_score"], errors="coerce")
    if numeric_scores.isna().any():
        failures.append(_failure("NULL combined risk scores", 0, int(numeric_scores.isna().sum())))
    out_of_range = int((~numeric_scores.between(0.0, 1.0)).sum())
    if out_of_range:
        failures.append(_failure("combined risk score range violations", 0, out_of_range))

    invalid_levels = set(risks["risk_level"].dropna()) - ALLOWED_LEVELS
    null_levels = int(risks["risk_level"].isna().sum())
    if null_levels:
        failures.append(_failure("NULL risk levels", 0, null_levels))
    if invalid_levels:
        failures.append(_failure("risk level values", sorted(ALLOWED_LEVELS), sorted(invalid_levels)))

    expected_scores = (
        ANOMALY_WEIGHT * pd.to_numeric(risks["anomaly_score"], errors="coerce")
        + DBSCAN_NOISE_WEIGHT * risks["is_noise"].astype(float)
    )
    formula_differences = (numeric_scores - expected_scores).abs()
    formula_errors = int((formula_differences > SCORE_TOLERANCE).sum())
    if formula_errors:
        failures.append(_failure("combined risk formula", 0, formula_errors))

    expected_levels = numeric_scores.apply(risk_level_for_score)
    level_errors = int((risks["risk_level"] != expected_levels).sum())
    if level_errors:
        failures.append(_failure("risk-level thresholds", 0, level_errors))

    ranks = pd.to_numeric(risks["risk_rank"], errors="coerce")
    expected_ranks = set(range(1, len(risks) + 1))
    actual_ranks = set(ranks.dropna().astype(int))
    if actual_ranks != expected_ranks:
        failures.append(_failure("risk ranking", sorted(expected_ranks), sorted(actual_ranks)))
    elif len(risks):
        highest_score = numeric_scores.max()
        rank_one_score = numeric_scores[ranks == 1].iloc[0]
        if rank_one_score != highest_score:
            failures.append(_failure("rank 1 score", highest_score, rank_one_score))

    print("Risk distribution:")
    for level in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
        print(f"  {level}: {(risks['risk_level'] == level).sum()}")
    return failures


def main():
    try:
        conn = get_connection()
        try:
            anomalies, clusters, risks = _load_results(conn)
        finally:
            conn.close()
    except Exception as error:
        print("VERIFICATION AUDIT RESULT: CHECKS FAILED")
        print(_failure("database query", "available risk-scoring tables", str(error)))
        return 1

    failures = _verify_results(anomalies, clusters, risks)
    if failures:
        print("VERIFICATION AUDIT RESULT: CHECKS FAILED")
        for failure in failures:
            print(failure)
        return 1

    print("VERIFICATION AUDIT RESULT: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
