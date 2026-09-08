"""
Step 3: Isolation Forest Anomaly Detection (SRS §22)
Trains Isolation Forest models on node features and transaction features.
Persists models and scalers to models/ directory.
Persists anomaly scores to analytics.node_anomalies and analytics.transaction_anomalies.
"""
import sys
import os
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
import joblib

# Add ingestion directory to path for database connection
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1
MODELS_DIR = Path(__file__).resolve().parents[1] / "models"
MODELS_DIR.mkdir(exist_ok=True)


def train_node_anomaly_detector():
    print("============================================================")
    print("   STEP 3: TRAIN ISOLATION FOREST - NODE ANOMALIES")
    print("============================================================\n")

    conn = get_connection()
    
    # Query node features
    query = """
        SELECT 
            node_id,
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
    print(f"Loaded {len(df_nodes)} node records.")

    feature_cols = [
        'peer_degree', 'outgoing_connections', 'incoming_connections', 'total_connections',
        'avg_connection_duration_ms', 'observations_as_observer', 'transactions_observed',
        'unique_peers_as_observer', 'avg_observer_delay_ms', 'propagations_as_peer',
        'transactions_propagated', 'unique_observers_as_peer', 'propagation_observation_ratio',
        'ip_diversity', 'degree_centrality', 'betweenness_centrality', 'closeness_centrality',
        'community_id'
    ]

    X = df_nodes[feature_cols].values.astype(float)

    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Train Isolation Forest
    clf = IsolationForest(contamination=0.08, random_state=42, n_estimators=100)
    clf.fit(X_scaled)

    # Raw score: higher = normal, lower = anomalous
    raw_scores = clf.decision_function(X_scaled)
    preds = clf.predict(X_scaled)  # -1 = anomaly, 1 = normal

    # Normalize anomaly score to [0, 1] range where 1 is highest anomaly
    # decision_function typically ranges from [-0.5, 0.5]
    min_s, max_s = raw_scores.min(), raw_scores.max()
    if max_s > min_s:
        normalized_anomaly_scores = 1.0 - ((raw_scores - min_s) / (max_s - min_s))
    else:
        normalized_anomaly_scores = np.zeros_like(raw_scores)

    df_nodes['raw_decision_score'] = raw_scores
    df_nodes['anomaly_score'] = normalized_anomaly_scores
    df_nodes['is_anomaly'] = preds == -1

    print(f"Node Anomaly Detection Summary:")
    print(f"  Detected {df_nodes['is_anomaly'].sum()} anomalous nodes out of {len(df_nodes)}")
    print(f"  Max anomaly score: {df_nodes['anomaly_score'].max():.4f}")
    print(f"  Mean anomaly score: {df_nodes['anomaly_score'].mean():.4f}")

    # Save scaler and model
    joblib.dump(scaler, MODELS_DIR / "node_scaler.joblib")
    joblib.dump(clf, MODELS_DIR / "node_isolation_forest.joblib")
    joblib.dump(feature_cols, MODELS_DIR / "node_feature_names.joblib")
    print(f"Saved node model & scaler to {MODELS_DIR}")

    # Persist to analytics.node_anomalies
    cur = conn.cursor()
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        DROP TABLE IF EXISTS analytics.node_anomalies CASCADE;
        CREATE TABLE analytics.node_anomalies (
            batch_id INTEGER NOT NULL,
            node_id VARCHAR(64) NOT NULL,
            raw_decision_score NUMERIC(10, 6) NOT NULL,
            anomaly_score NUMERIC(6, 4) NOT NULL,
            is_anomaly BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (batch_id, node_id)
        );
    """)
    conn.commit()

    insert_sql = """
        INSERT INTO analytics.node_anomalies (batch_id, node_id, raw_decision_score, anomaly_score, is_anomaly)
        VALUES (%s, %s, %s, %s, %s);
    """
    for _, row in df_nodes.iterrows():
        cur.execute(insert_sql, (
            BATCH_ID,
            row['node_id'],
            round(float(row['raw_decision_score']), 6),
            round(float(row['anomaly_score']), 4),
            bool(row['is_anomaly'])
        ))
    conn.commit()
    cur.close()
    conn.close()
    print("Node anomaly scores persisted to PostgreSQL.\n")


def train_transaction_anomaly_detector():
    print("============================================================")
    print("   STEP 3: TRAIN ISOLATION FOREST - TX ANOMALIES")
    print("============================================================\n")

    conn = get_connection()
    
    query = """
        SELECT 
            txid,
            fee,
            size,
            ratio_fee_size,
            rarity_score,
            inputs,
            outputs,
            total_observations,
            unique_observers,
            unique_peers,
            unique_observer_countries,
            unique_peer_countries,
            min_propagation_delay_ms,
            max_propagation_delay_ms,
            avg_propagation_delay_ms,
            median_propagation_delay_ms,
            observed_propagation_span_ms,
            creation_to_first_observation_ms,
            inv_ratio,
            tx_ratio
        FROM analytics.transaction_features
        WHERE batch_id = %s;
    """
    df_tx = pd.read_sql(query, conn, params=(BATCH_ID,))
    print(f"Loaded {len(df_tx)} transaction records.")

    feature_cols = [
        'fee', 'size', 'ratio_fee_size', 'rarity_score', 'inputs', 'outputs',
        'total_observations', 'unique_observers', 'unique_peers',
        'unique_observer_countries', 'unique_peer_countries',
        'min_propagation_delay_ms', 'max_propagation_delay_ms', 'avg_propagation_delay_ms',
        'median_propagation_delay_ms', 'observed_propagation_span_ms',
        'creation_to_first_observation_ms', 'inv_ratio', 'tx_ratio'
    ]

    X = df_tx[feature_cols].values.astype(float)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    clf = IsolationForest(contamination=0.05, random_state=42, n_estimators=100)
    clf.fit(X_scaled)

    raw_scores = clf.decision_function(X_scaled)
    preds = clf.predict(X_scaled)

    min_s, max_s = raw_scores.min(), raw_scores.max()
    if max_s > min_s:
        normalized_anomaly_scores = 1.0 - ((raw_scores - min_s) / (max_s - min_s))
    else:
        normalized_anomaly_scores = np.zeros_like(raw_scores)

    df_tx['raw_decision_score'] = raw_scores
    df_tx['anomaly_score'] = normalized_anomaly_scores
    df_tx['is_anomaly'] = preds == -1

    print(f"Transaction Anomaly Detection Summary:")
    print(f"  Detected {df_tx['is_anomaly'].sum()} anomalous transactions out of {len(df_tx)}")
    print(f"  Max anomaly score: {df_tx['anomaly_score'].max():.4f}")
    print(f"  Mean anomaly score: {df_tx['anomaly_score'].mean():.4f}")

    joblib.dump(scaler, MODELS_DIR / "tx_scaler.joblib")
    joblib.dump(clf, MODELS_DIR / "tx_isolation_forest.joblib")
    joblib.dump(feature_cols, MODELS_DIR / "tx_feature_names.joblib")
    print(f"Saved transaction model & scaler to {MODELS_DIR}")

    cur = conn.cursor()
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        DROP TABLE IF EXISTS analytics.transaction_anomalies CASCADE;
        CREATE TABLE analytics.transaction_anomalies (
            batch_id INTEGER NOT NULL,
            txid VARCHAR(64) NOT NULL,
            raw_decision_score NUMERIC(10, 6) NOT NULL,
            anomaly_score NUMERIC(6, 4) NOT NULL,
            is_anomaly BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (batch_id, txid)
        );
    """)
    conn.commit()

    insert_sql = """
        INSERT INTO analytics.transaction_anomalies (batch_id, txid, raw_decision_score, anomaly_score, is_anomaly)
        VALUES (%s, %s, %s, %s, %s);
    """
    for _, row in df_tx.iterrows():
        cur.execute(insert_sql, (
            BATCH_ID,
            row['txid'],
            round(float(row['raw_decision_score']), 6),
            round(float(row['anomaly_score']), 4),
            bool(row['is_anomaly'])
        ))
    conn.commit()
    cur.close()
    conn.close()
    print("Transaction anomaly scores persisted to PostgreSQL.\n")
    print(">>> STEP 3 ISOLATION FOREST MODELS TRAINED SUCCESSFULLY! <<<")


if __name__ == "__main__":
    train_node_anomaly_detector()
    train_transaction_anomaly_detector()
