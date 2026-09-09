"""Train Isolation Forest anomaly detectors from validated analytics features."""
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection


BATCH_ID = 1
MODELS_DIR = Path(__file__).resolve().parents[1] / "models"

NODE_FEATURES = [
    "peer_degree", "outgoing_connections", "incoming_connections", "total_connections",
    "avg_connection_duration_ms", "observations_as_observer", "transactions_observed",
    "unique_peers_as_observer", "avg_observer_delay_ms", "propagations_as_peer",
    "transactions_propagated", "unique_observers_as_peer",
    "propagation_observation_ratio", "ip_diversity",
]

TRANSACTION_FEATURES = [
    "fee", "size", "ratio_fee_size", "rarity_score", "inputs", "outputs",
    "total_observations", "unique_observers", "unique_peers",
    "unique_observer_countries", "unique_peer_countries",
    "min_propagation_delay_ms", "max_propagation_delay_ms", "avg_propagation_delay_ms",
    "median_propagation_delay_ms", "observed_propagation_span_ms",
    "creation_to_first_observation_ms", "inv_ratio", "tx_ratio",
]


def _load_feature_data(conn, table_name, identifier_column, feature_columns):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'analytics' AND table_name = %s;
            """,
            (table_name,),
        )
        available_columns = {row[0] for row in cur.fetchall()}

    required_columns = {"batch_id", identifier_column, *feature_columns}
    missing_columns = sorted(required_columns - available_columns)
    if missing_columns:
        raise ValueError(
            f"analytics.{table_name} is missing required columns: "
            f"{', '.join(missing_columns)}"
        )

    select_columns = ", ".join([identifier_column, *feature_columns])
    query = (
        f"SELECT {select_columns} FROM analytics.{table_name} "
        "WHERE batch_id = %s;"
    )
    data = pd.read_sql(query, conn, params=(BATCH_ID,))
    if data.empty:
        raise ValueError(
            f"No rows found in analytics.{table_name} for batch {BATCH_ID}."
        )
    return data


def _preprocess_features(data, feature_columns, entity_name):
    numeric_data = data[feature_columns].apply(pd.to_numeric, errors="coerce")
    numeric_data = numeric_data.replace([np.inf, -np.inf], np.nan)

    all_missing = numeric_data.columns[numeric_data.isna().all()].tolist()
    if all_missing:
        raise ValueError(
            f"{entity_name} features are entirely missing after numeric conversion: "
            f"{', '.join(all_missing)}"
        )

    missing_values = int(numeric_data.isna().sum().sum())
    if missing_values:
        print(
            f"  Replacing {missing_values} missing or non-finite {entity_name} "
            "feature values with per-feature medians."
        )

    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    imputed_data = imputer.fit_transform(numeric_data)
    return imputer, scaler, scaler.fit_transform(imputed_data)


def _normalized_anomaly_scores(decision_scores):
    """Convert Isolation Forest's higher-is-normal score into higher-is-anomalous."""
    anomaly_scores = -np.asarray(decision_scores, dtype=float)
    minimum = anomaly_scores.min()
    maximum = anomaly_scores.max()
    if maximum == minimum:
        return np.zeros_like(anomaly_scores)
    return (anomaly_scores - minimum) / (maximum - minimum)


def _persist_anomalies(conn, table_name, identifier_column, data):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS analytics;
            DROP TABLE IF EXISTS analytics.{table_name} CASCADE;
            CREATE TABLE analytics.{table_name} (
                batch_id INTEGER NOT NULL,
                {identifier_column} VARCHAR(64) NOT NULL,
                raw_decision_score NUMERIC(10, 6) NOT NULL,
                anomaly_score NUMERIC(6, 4) NOT NULL,
                is_anomaly BOOLEAN NOT NULL,
                anomaly_rank INTEGER NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (batch_id, {identifier_column})
            );
            """
        )

        insert_sql = (
            f"INSERT INTO analytics.{table_name} "
            f"(batch_id, {identifier_column}, raw_decision_score, anomaly_score, "
            "is_anomaly, anomaly_rank) VALUES (%s, %s, %s, %s, %s, %s);"
        )
        rows = [
            (
                BATCH_ID,
                row[identifier_column],
                round(float(row["raw_decision_score"]), 6),
                round(float(row["anomaly_score"]), 4),
                bool(row["is_anomaly"]),
                int(row["anomaly_rank"]),
            )
            for _, row in data.iterrows()
        ]
        cur.executemany(insert_sql, rows)
    conn.commit()


def _train_detector(
    table_name,
    identifier_column,
    feature_columns,
    contamination,
    artifact_prefix,
    result_table,
):
    MODELS_DIR.mkdir(exist_ok=True)
    conn = get_connection()
    try:
        data = _load_feature_data(conn, table_name, identifier_column, feature_columns)
        print(f"Loaded {len(data)} {identifier_column} records.")

        imputer, scaler, scaled_features = _preprocess_features(
            data, feature_columns, identifier_column
        )
        model = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=100,
        )
        model.fit(scaled_features)

        decision_scores = model.decision_function(scaled_features)
        data["raw_decision_score"] = decision_scores
        data["anomaly_score"] = _normalized_anomaly_scores(decision_scores)
        data["is_anomaly"] = model.predict(scaled_features) == -1
        data["anomaly_rank"] = (
            data["anomaly_score"]
            .rank(method="first", ascending=False)
            .astype(int)
        )

        joblib.dump(imputer, MODELS_DIR / f"{artifact_prefix}_imputer.joblib")
        joblib.dump(scaler, MODELS_DIR / f"{artifact_prefix}_scaler.joblib")
        joblib.dump(model, MODELS_DIR / f"{artifact_prefix}_isolation_forest.joblib")
        joblib.dump(feature_columns, MODELS_DIR / f"{artifact_prefix}_feature_names.joblib")

        _persist_anomalies(conn, result_table, identifier_column, data)
        print(f"Detected {int(data['is_anomaly'].sum())} anomalous records.")
        print(f"Persisted {len(data)} rows to analytics.{result_table}.")
    finally:
        conn.close()


def train_node_anomaly_detector():
    print("============================================================")
    print("   STEP 3: TRAIN ISOLATION FOREST - NODE ANOMALIES")
    print("============================================================\n")
    _train_detector(
        "node_features", "node_id", NODE_FEATURES, 0.08, "node", "node_anomalies"
    )


def train_transaction_anomaly_detector():
    print("============================================================")
    print("   STEP 3: TRAIN ISOLATION FOREST - TX ANOMALIES")
    print("============================================================\n")
    _train_detector(
        "transaction_features",
        "txid",
        TRANSACTION_FEATURES,
        0.05,
        "tx",
        "transaction_anomalies",
    )
    print(">>> STEP 3 ISOLATION FOREST MODELS TRAINED SUCCESSFULLY! <<<")


if __name__ == "__main__":
    train_node_anomaly_detector()
    train_transaction_anomaly_detector()
