"""Train DBSCAN clusters from validated node behavior features."""
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection


BATCH_ID = 1
MODELS_DIR = Path(__file__).resolve().parents[1] / "models"
DBSCAN_EPS = 2.5
DBSCAN_MIN_SAMPLES = 3

NODE_FEATURES = [
    "peer_degree", "outgoing_connections", "incoming_connections", "total_connections",
    "avg_connection_duration_ms", "observations_as_observer", "transactions_observed",
    "unique_peers_as_observer", "avg_observer_delay_ms", "propagations_as_peer",
    "transactions_propagated", "unique_observers_as_peer",
    "propagation_observation_ratio", "ip_diversity",
]


def _load_node_features(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'analytics' AND table_name = 'node_features';
            """
        )
        available_columns = {row[0] for row in cur.fetchall()}

    required_columns = {"batch_id", "node_id", "country", "node_type", *NODE_FEATURES}
    missing_columns = sorted(required_columns - available_columns)
    if missing_columns:
        raise ValueError(
            "analytics.node_features is missing required columns: "
            f"{', '.join(missing_columns)}"
        )

    select_columns = ", ".join(["node_id", "country", "node_type", *NODE_FEATURES])
    data = pd.read_sql(
        f"SELECT {select_columns} FROM analytics.node_features WHERE batch_id = %s;",
        conn,
        params=(BATCH_ID,),
    )
    if data.empty:
        raise ValueError(f"No node features found for batch {BATCH_ID}.")
    return data


def _preprocess_features(data):
    numeric_data = data[NODE_FEATURES].apply(pd.to_numeric, errors="coerce")
    numeric_data = numeric_data.replace([np.inf, -np.inf], np.nan)

    all_missing = numeric_data.columns[numeric_data.isna().all()].tolist()
    if all_missing:
        raise ValueError(
            "Node features are entirely missing after numeric conversion: "
            f"{', '.join(all_missing)}"
        )

    missing_values = int(numeric_data.isna().sum().sum())
    if missing_values:
        print(
            f"Replacing {missing_values} missing or non-finite feature values "
            "with per-feature medians."
        )

    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    imputed_data = imputer.fit_transform(numeric_data)
    return imputer, scaler, scaler.fit_transform(imputed_data)


def _print_summary(data, labels):
    cluster_sizes = pd.Series(labels).value_counts().sort_index()
    noise_count = int((labels == -1).sum())
    cluster_count = int(sum(label != -1 for label in cluster_sizes.index))
    noise_percentage = (noise_count / len(data)) * 100

    print("DBSCAN Clustering Results:")
    print(f"  Nodes analyzed: {len(data)}")
    print(f"  Number of features used: {len(NODE_FEATURES)}")
    print(f"  Feature names used: {', '.join(NODE_FEATURES)}")
    print(f"  DBSCAN eps: {DBSCAN_EPS}")
    print(f"  DBSCAN min_samples: {DBSCAN_MIN_SAMPLES}")
    print(f"  Number of clusters found: {cluster_count}")
    print(f"  Number of noise nodes: {noise_count}")
    print(f"  Noise percentage: {noise_percentage:.2f}%")
    print("  Cluster size distribution:")
    for cluster_id, size in cluster_sizes.items():
        print(f"    Cluster {cluster_id}: {size} nodes")


def _persist_clusters(conn, data, labels):
    data["cluster_id"] = labels
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS analytics;
            DROP TABLE IF EXISTS analytics.node_clusters CASCADE;
            CREATE TABLE analytics.node_clusters (
                batch_id INTEGER NOT NULL,
                node_id VARCHAR(64) NOT NULL,
                cluster_id INTEGER NOT NULL,
                is_noise BOOLEAN NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (batch_id, node_id)
            );

            DROP TABLE IF EXISTS analytics.cluster_summaries CASCADE;
            CREATE TABLE analytics.cluster_summaries (
                batch_id INTEGER NOT NULL,
                cluster_id INTEGER NOT NULL,
                node_count INTEGER NOT NULL,
                is_noise BOOLEAN NOT NULL,
                avg_peer_degree NUMERIC(10, 2),
                avg_connections NUMERIC(10, 2),
                avg_observer_delay_ms NUMERIC(12, 3),
                top_country VARCHAR(10),
                top_node_type VARCHAR(20),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (batch_id, cluster_id)
            );
            """
        )

        cur.executemany(
            """
            INSERT INTO analytics.node_clusters (batch_id, node_id, cluster_id, is_noise)
            VALUES (%s, %s, %s, %s);
            """,
            [
                (BATCH_ID, row.node_id, int(row.cluster_id), bool(row.cluster_id == -1))
                for row in data.itertuples(index=False)
            ],
        )

        for cluster_id, cluster_data in data.groupby("cluster_id", sort=True):
            country_mode = cluster_data["country"].dropna().mode()
            node_type_mode = cluster_data["node_type"].dropna().mode()
            cur.execute(
                """
                INSERT INTO analytics.cluster_summaries (
                    batch_id, cluster_id, node_count, is_noise, avg_peer_degree,
                    avg_connections, avg_observer_delay_ms, top_country, top_node_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    BATCH_ID,
                    int(cluster_id),
                    len(cluster_data),
                    bool(cluster_id == -1),
                    round(float(cluster_data["peer_degree"].mean()), 2),
                    round(float(cluster_data["total_connections"].mean()), 2),
                    round(float(cluster_data["avg_observer_delay_ms"].mean()), 3),
                    str(country_mode.iloc[0]) if not country_mode.empty else "UNKNOWN",
                    str(node_type_mode.iloc[0]) if not node_type_mode.empty else "UNKNOWN",
                ),
            )
    conn.commit()


def train_dbscan_clustering():
    print("============================================================")
    print("   STEP 4: DBSCAN CLUSTERING - NODE BEHAVIOR")
    print("============================================================\n")

    MODELS_DIR.mkdir(exist_ok=True)
    conn = get_connection()
    try:
        data = _load_node_features(conn)
        imputer, scaler, scaled_features = _preprocess_features(data)
        model = DBSCAN(eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES)
        labels = model.fit_predict(scaled_features)

        _print_summary(data, labels)
        _persist_clusters(conn, data, labels)
        joblib.dump(imputer, MODELS_DIR / "dbscan_imputer.joblib")
        joblib.dump(scaler, MODELS_DIR / "dbscan_scaler.joblib")
        joblib.dump(model, MODELS_DIR / "dbscan_model.joblib")
        joblib.dump(NODE_FEATURES, MODELS_DIR / "dbscan_feature_names.joblib")
        print(f"Saved DBSCAN model and preprocessing artifacts to {MODELS_DIR}")
    finally:
        conn.close()

    print(">>> STEP 4 DBSCAN CLUSTERING COMPLETED SUCCESSFULLY! <<<")


if __name__ == "__main__":
    train_dbscan_clustering()
