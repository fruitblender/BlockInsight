"""
Step 7: Model Registry & Metadata Versioning (SRS §31)
Tracks model versions, algorithms, hyperparameters, training windows, and production status.
Supports model governance, lineage, and rollback capability.
Persists to analytics.model_registry in PostgreSQL.
"""
import sys
import os
import json
from pathlib import Path
import joblib

# Add ingestion directory to path for database connection
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

MODELS_DIR = Path(__file__).resolve().parents[1] / "models"


def register_models():
    print("============================================================")
    print("   STEP 7: MODEL REGISTRY & METADATA VERSIONING")
    print("============================================================\n")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        DROP TABLE IF EXISTS analytics.model_registry CASCADE;
        CREATE TABLE analytics.model_registry (
            model_id VARCHAR(64) NOT NULL PRIMARY KEY,
            model_name VARCHAR(100) NOT NULL,
            model_version VARCHAR(20) NOT NULL,
            algorithm VARCHAR(50) NOT NULL,
            target_entity VARCHAR(50) NOT NULL,
            hyperparameters JSONB NOT NULL,
            feature_names JSONB NOT NULL,
            metrics JSONB NOT NULL,
            artifact_path VARCHAR(255) NOT NULL,
            is_production BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    conn.commit()

    # Load feature names
    node_features = joblib.load(MODELS_DIR / "node_feature_names.joblib")
    tx_features = joblib.load(MODELS_DIR / "tx_feature_names.joblib")

    # 1. Node Isolation Forest Model
    node_model_entry = {
        "model_id": "MOD-ISO-NODE-v1.0",
        "model_name": "Node Behavioral Isolation Forest",
        "model_version": "v1.0.0",
        "algorithm": "IsolationForest",
        "target_entity": "NODE",
        "hyperparameters": {
            "n_estimators": 100,
            "contamination": 0.08,
            "random_state": 42,
            "bootstrap": False
        },
        "feature_names": node_features,
        "metrics": {
            "training_samples": 150,
            "anomalies_detected": 12,
            "contamination_rate": 0.08
        },
        "artifact_path": "models/node_isolation_forest.joblib",
        "is_production": True
    }

    # 2. Transaction Isolation Forest Model
    tx_model_entry = {
        "model_id": "MOD-ISO-TX-v1.0",
        "model_name": "Transaction Propagation Isolation Forest",
        "model_version": "v1.0.0",
        "algorithm": "IsolationForest",
        "target_entity": "TRANSACTION",
        "hyperparameters": {
            "n_estimators": 100,
            "contamination": 0.05,
            "random_state": 42,
            "bootstrap": False
        },
        "feature_names": tx_features,
        "metrics": {
            "training_samples": 498,
            "anomalies_detected": 25,
            "contamination_rate": 0.05
        },
        "artifact_path": "models/tx_isolation_forest.joblib",
        "is_production": True
    }

    # 3. Node DBSCAN Clustering Model
    dbscan_model_entry = {
        "model_id": "MOD-DBSCAN-NODE-v1.0",
        "model_name": "Node Behavioral DBSCAN Clusterer",
        "model_version": "v1.0.0",
        "algorithm": "DBSCAN",
        "target_entity": "NODE",
        "hyperparameters": {
            "eps": 2.5,
            "min_samples": 3,
            "metric": "euclidean"
        },
        "feature_names": node_features,
        "metrics": {
            "total_nodes": 150,
            "clusters_formed": 4,
            "noise_nodes": 8
        },
        "artifact_path": "models/dbscan_model.joblib",
        "is_production": True
    }

    insert_sql = """
        INSERT INTO analytics.model_registry (
            model_id, model_name, model_version, algorithm, target_entity,
            hyperparameters, feature_names, metrics, artifact_path, is_production
        ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s);
    """

    for entry in [node_model_entry, tx_model_entry, dbscan_model_entry]:
        cur.execute(insert_sql, (
            entry["model_id"],
            entry["model_name"],
            entry["model_version"],
            entry["algorithm"],
            entry["target_entity"],
            json.dumps(entry["hyperparameters"]),
            json.dumps(entry["feature_names"]),
            json.dumps(entry["metrics"]),
            entry["artifact_path"],
            entry["is_production"]
        ))
    conn.commit()

    cur.execute("SELECT model_id, model_name, model_version, algorithm FROM analytics.model_registry;")
    rows = cur.fetchall()
    print("Registered Models:")
    for r in rows:
        print(f"  [{r[0]}] {r[1]} ({r[2]}) - {r[3]}")

    cur.close()
    conn.close()
    print("\n>>> STEP 7 MODEL REGISTRY POPULATED SUCCESSFULLY! <<<")


if __name__ == "__main__":
    register_models()
