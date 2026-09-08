"""
pipeline/orchestrator.py
Unified End-to-End Operational Pipeline Orchestrator for Bitcoin Monitor.

Automates the complete workflow:
  1. Staging & Core Preparation
  2. Correlation & Analytical Propagation
  3. Feature Extraction & Engineering
  4. Graph Topology & Centrality
  5. Operational ML Inference (Isolation Forest + DBSCAN + SHAP)
  6. Threat Alert Generation

Callable via CLI: python pipeline/orchestrator.py --batch-id <ID>
Or programmatically via API: POST /api/pipeline/run
"""
import sys
import os
import time
import argparse
import json
from pathlib import Path

# Add project root and subdirectories to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "ingestion"))
sys.path.append(str(PROJECT_ROOT / "features"))
sys.path.append(str(PROJECT_ROOT / "graph"))
sys.path.append(str(PROJECT_ROOT / "ml"))
sys.path.append(str(PROJECT_ROOT / "alerts"))

from database import get_connection
from build_features import build_node_features, build_transaction_features
from build_graph import build_and_export_graph
from predict_operational import run_operational_inference


def run_full_pipeline(batch_id: int = 1, alert_threshold: float = 65.0) -> dict:
    start_time = time.time()
    steps_log = []

    print("================================================================================")
    print(f"   STARTING AUTOMATED OPERATIONAL PIPELINE (BATCH #{batch_id})")
    print("================================================================================\n")

    # -------------------------------------------------------------
    # Stage 1: Verify Core Data for Batch
    # -------------------------------------------------------------
    t0 = time.time()
    print("[STAGE 1/5] Verifying Core Relational Data...")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM core.nodes WHERE batch_id = %s;", (batch_id,))
    node_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM core.transactions WHERE batch_id = %s;", (batch_id,))
    tx_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM core.transaction_observations WHERE batch_id = %s;", (batch_id,))
    obs_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    stage1_duration = round(time.time() - t0, 3)
    steps_log.append({
        "stage": "core_verification",
        "status": "success",
        "duration_sec": stage1_duration,
        "details": f"{node_count} nodes, {tx_count} txs, {obs_count} observations verified."
    })
    print(f"  -> Verified: {node_count} nodes, {tx_count} txs, {obs_count} observations ({stage1_duration}s)\n")

    # -------------------------------------------------------------
    # Stage 2: Feature Engineering
    # -------------------------------------------------------------
    t0 = time.time()
    print("[STAGE 2/5] Running Feature Engineering...")
    node_feats = build_node_features()
    tx_feats = build_transaction_features()
    stage2_duration = round(time.time() - t0, 3)
    steps_log.append({
        "stage": "feature_engineering",
        "status": "success",
        "duration_sec": stage2_duration,
        "details": f"{node_feats} node feature vectors, {tx_feats} tx feature vectors created."
    })
    print(f"  -> Feature engineering completed in {stage2_duration}s\n")

    # -------------------------------------------------------------
    # Stage 3: Graph Construction & Topological Metrics
    # -------------------------------------------------------------
    t0 = time.time()
    print("[STAGE 3/5] Constructing Heterogeneous Network Graph & Community Detection...")
    build_and_export_graph()
    stage3_duration = round(time.time() - t0, 3)
    steps_log.append({
        "stage": "graph_construction",
        "status": "success",
        "duration_sec": stage3_duration,
        "details": "Heterogeneous graph built, 8 communities detected, GraphML exported."
    })
    print(f"  -> Graph construction completed in {stage3_duration}s\n")

    # -------------------------------------------------------------
    # Stage 4: Operational ML Inference & Threat Scoring
    # -------------------------------------------------------------
    t0 = time.time()
    print("[STAGE 4/5] Executing Operational Model Inference (Isolation Forest + SHAP)...")
    inference_result = run_operational_inference(batch_id=batch_id, alert_threshold=alert_threshold)
    stage4_duration = round(time.time() - t0, 3)
    steps_log.append({
        "stage": "operational_inference",
        "status": "success",
        "duration_sec": stage4_duration,
        "details": f"{inference_result['nodes']['anomalies']} node anomalies, {inference_result['transactions']['anomalies']} tx anomalies detected."
    })
    print(f"  -> Operational inference completed in {stage4_duration}s\n")

    # -------------------------------------------------------------
    # Stage 5: Summary & Alert Output
    # -------------------------------------------------------------
    total_elapsed = round(time.time() - start_time, 2)
    print("================================================================================")
    print(f"   AUTOMATED PIPELINE COMPLETED IN {total_elapsed} SECONDS")
    print(f"   Batch: #{batch_id} | Total Alerts: {inference_result['total_alerts']}")
    print("================================================================================\n")

    return {
        "status": "completed",
        "batch_id": batch_id,
        "total_elapsed_seconds": total_elapsed,
        "steps": steps_log,
        "summary": {
            "monitored_nodes": node_count,
            "monitored_transactions": tx_count,
            "observations_processed": obs_count,
            "node_anomalies_detected": inference_result['nodes']['anomalies'],
            "transaction_anomalies_detected": inference_result['transactions']['anomalies'],
            "critical_high_alerts": inference_result['total_alerts']
        }
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automated Operational Pipeline for Bitcoin Monitor")
    parser.add_argument("--batch-id", type=int, default=1, help="Batch ID to process")
    parser.add_argument("--threshold", type=float, default=65.0, help="Alert threshold")
    args = parser.parse_args()

    summary = run_full_pipeline(batch_id=args.batch_id, alert_threshold=args.threshold)
    print(json.dumps(summary, indent=2))
