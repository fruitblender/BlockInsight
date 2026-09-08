"""
Step 4: DBSCAN Entity Clustering (SRS §24)
Runs DBSCAN clustering on standardized node features.
Identifies dense peer behavioral clusters and isolates noise/outliers (cluster -1).
Persists cluster assignments to analytics.node_clusters and summaries to analytics.cluster_summaries.
"""
import sys
import os
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
import joblib

# Add ingestion directory to path for database connection
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1
MODELS_DIR = Path(__file__).resolve().parents[1] / "models"


def train_dbscan_clustering():
    print("============================================================")
    print("   STEP 4: DBSCAN CLUSTERING - NODE BEHAVIOR")
    print("============================================================\n")

    conn = get_connection()
    
    query = """
        SELECT 
            node_id,
            ip,
            country,
            asn,
            node_type,
            peer_degree,
            outgoing_connections,
            incoming_connections,
            total_connections,
            avg_connection_duration_ms,
            observations_as_observer,
            transactions_observed,
            unique_peers_as_observer,
            avg_observer_delay_ms,
            propagations_as_peer,
            transactions_propagated,
            unique_observers_as_peer,
            propagation_observation_ratio,
            ip_diversity,
            COALESCE(degree_centrality, 0.0) AS degree_centrality,
            COALESCE(betweenness_centrality, 0.0) AS betweenness_centrality,
            COALESCE(closeness_centrality, 0.0) AS closeness_centrality,
            COALESCE(community_id, 0) AS community_id
        FROM analytics.node_features
        WHERE batch_id = %s;
    """
    df_nodes = pd.read_sql(query, conn, params=(BATCH_ID,))
    print(f"Loaded {len(df_nodes)} node records for clustering.")

    scaler = joblib.load(MODELS_DIR / "node_scaler.joblib")
    feature_cols = joblib.load(MODELS_DIR / "node_feature_names.joblib")

    X = df_nodes[feature_cols].values.astype(float)
    X_scaled = scaler.transform(X)

    # Run DBSCAN
    db = DBSCAN(eps=2.5, min_samples=3)
    labels = db.fit_predict(X_scaled)
    df_nodes['cluster_id'] = labels

    unique_clusters = sorted(list(set(labels)))
    n_noise = list(labels).count(-1)
    n_clusters = len(unique_clusters) - (1 if -1 in unique_clusters else 0)

    print(f"DBSCAN Clustering Results:")
    print(f"  Total clusters formed: {n_clusters}")
    print(f"  Outliers/Noise nodes (cluster -1): {n_noise} out of {len(df_nodes)}")
    for c in unique_clusters:
        count = (labels == c).sum()
        print(f"    Cluster {c:>2}: {count} nodes")

    # Persist to analytics.node_clusters
    cur = conn.cursor()
    cur.execute("""
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
    """)
    conn.commit()

    # Insert node cluster assignments
    insert_node_cluster_sql = """
        INSERT INTO analytics.node_clusters (batch_id, node_id, cluster_id, is_noise)
        VALUES (%s, %s, %s, %s);
    """
    for _, row in df_nodes.iterrows():
        cur.execute(insert_node_cluster_sql, (
            BATCH_ID,
            row['node_id'],
            int(row['cluster_id']),
            bool(row['cluster_id'] == -1)
        ))
    conn.commit()

    # Insert cluster summaries
    insert_summary_sql = """
        INSERT INTO analytics.cluster_summaries (
            batch_id, cluster_id, node_count, is_noise, avg_peer_degree,
            avg_connections, avg_observer_delay_ms, top_country, top_node_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    for c in unique_clusters:
        sub = df_nodes[df_nodes['cluster_id'] == c]
        cur.execute(insert_summary_sql, (
            BATCH_ID,
            int(c),
            len(sub),
            bool(c == -1),
            round(float(sub['peer_degree'].mean()), 2),
            round(float(sub['total_connections'].mean()), 2),
            round(float(sub['avg_observer_delay_ms'].mean()), 3),
            str(sub['country'].mode().iloc[0] if not sub['country'].empty else "UNKNOWN"),
            str(sub['node_type'].mode().iloc[0] if not sub['node_type'].empty else "UNKNOWN")
        ))
    conn.commit()

    # Save model
    joblib.dump(db, MODELS_DIR / "dbscan_model.joblib")
    print(f"Saved DBSCAN model to {MODELS_DIR}")

    cur.close()
    conn.close()
    print(">>> STEP 4 DBSCAN CLUSTERING COMPLETED SUCCESSFULLY! <<<")


if __name__ == "__main__":
    train_dbscan_clustering()
