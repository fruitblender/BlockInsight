"""
pipeline/run_full_pipeline.py
=============================================================================
UNIFIED END-TO-END OPERATIONAL PIPELINE for Bitcoin Monitor
=============================================================================

Automates the COMPLETE chain from raw data files to dashboard-ready predictions:

  Stage 0: Pre-Flight Checks (files + DB connectivity)
  Stage 1: Data Ingestion → staging.* tables
  Stage 2: Data Validation → validation.quarantine
  Stage 3: Validation Audit Report
  Stage 4: Core Transformation → core.* tables
  Stage 5: Transformation Verification
  Stage 6: Analytics Views → analytics.* views
  Stage 7: Feature Engineering → analytics.node_features, analytics.transaction_features
  Stage 8: Graph Construction → NetworkX, centrality, communities
  Stage 9: ML Inference + Threat Alerts → analytics.node_risk_scores, analytics.alerts

Usage:
  python pipeline/run_full_pipeline.py
  python pipeline/run_full_pipeline.py --data-dir data/incoming --threshold 65.0
  python pipeline/run_full_pipeline.py --clean   # wipe existing batch data before re-run

Can also be imported:
  from run_full_pipeline import run_full_pipeline
  result = run_full_pipeline(data_dir="data/incoming", threshold=65.0, clean=True)
"""
import sys
import os
import time
import argparse
import json
import subprocess
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Project root resolution
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Add required paths for direct Python imports (stages 7-9)
sys.path.insert(0, str(PROJECT_ROOT / "ingestion"))
sys.path.insert(0, str(PROJECT_ROOT / "features"))
sys.path.insert(0, str(PROJECT_ROOT / "graph"))
sys.path.insert(0, str(PROJECT_ROOT / "ml"))
sys.path.insert(0, str(PROJECT_ROOT / "alerts"))
sys.path.insert(0, str(PROJECT_ROOT / "analytics"))
sys.path.insert(0, str(PROJECT_ROOT / "correlation"))
sys.path.insert(0, str(PROJECT_ROOT / "transformation"))

from database import get_connection

# ---------------------------------------------------------------------------
# Expected raw data files
# ---------------------------------------------------------------------------
EXPECTED_FILES = [
    "nodes.csv",
    "peer_connections.csv",
    "transaction_observations.csv",
    "bitcoin_synthetic_whales.json",
    "network_events.csv",
]

# BATCH_ID used by all existing scripts
BATCH_ID = 1


# ===================================================================
# Helper: run a Python script via subprocess in a specific directory
# ===================================================================
def _run_script(script_path: str, cwd: str, label: str) -> bool:
    """
    Run a Python script via subprocess.
    Returns True on success, raises RuntimeError on failure.
    """
    # Use the same Python interpreter that's running this script
    python_exe = sys.executable
    full_script = os.path.join(cwd, script_path)

    if not os.path.isfile(full_script):
        raise FileNotFoundError(f"Script not found: {full_script}")

    result = subprocess.run(
        [python_exe, script_path],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=300,  # 5 minute timeout per script
    )

    if result.returncode != 0:
        print(f"\n  [STDERR] {result.stderr[:1000]}")
        raise RuntimeError(
            f"Stage '{label}' FAILED (exit code {result.returncode}): {script_path}\n"
            f"  stdout: {result.stdout[-500:]}\n"
            f"  stderr: {result.stderr[-500:]}"
        )

    # Print a condensed version of stdout (last few lines)
    lines = result.stdout.strip().split("\n")
    summary_lines = lines[-5:] if len(lines) > 5 else lines
    for line in summary_lines:
        print(f"    {line}")

    return True


# ===================================================================
# Stage 0: Pre-Flight Checks
# ===================================================================
def stage_0_preflight(data_dir: str) -> dict:
    """Verify all expected files exist and DB is reachable."""
    print("=" * 72)
    print("  STAGE 0/9: PRE-FLIGHT CHECKS")
    print("=" * 72)

    # Check files
    missing = []
    for fname in EXPECTED_FILES:
        fpath = os.path.join(data_dir, fname)
        if not os.path.isfile(fpath):
            missing.append(fname)
        else:
            size_kb = os.path.getsize(fpath) / 1024
            print(f"  ✓ {fname:50s} ({size_kb:.1f} KB)")

    if missing:
        raise FileNotFoundError(
            f"Missing data files in {data_dir}: {', '.join(missing)}"
        )

    # Check DB connectivity
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        print(f"  ✓ PostgreSQL connection verified")
    except Exception as e:
        raise ConnectionError(f"Database connection failed: {e}")

    print()
    return {"status": "passed", "data_dir": data_dir, "files_found": len(EXPECTED_FILES)}


# ===================================================================
# Stage CLEAN: Wipe existing batch data for a clean re-run
# ===================================================================
def stage_clean_batch(batch_id: int = 1):
    """Remove existing data for the batch to allow clean re-ingestion."""
    print("=" * 72)
    print(f"  CLEANING EXISTING DATA FOR BATCH #{batch_id}")
    print("=" * 72)

    conn = get_connection()
    cur = conn.cursor()

    # Order matters: analytics (dependent) → core → staging
    cleanup_tables = [
        # Analytics layer
        "analytics.alerts",
        "analytics.transaction_risk_scores",
        "analytics.node_risk_scores",
        "analytics.cluster_summaries",
        "analytics.node_clusters",
        "analytics.node_graph_metrics",
        "analytics.transaction_features",
        "analytics.node_features",
        # Core layer
        "core.network_events",
        "core.transaction_observations",
        "core.peer_connections",
        "core.transactions",
        "core.nodes",
        # Staging layer
        "staging.network_events_raw",
        "staging.transaction_observations_raw",
        "staging.peer_connections_raw",
        "staging.bitcoin_transactions_raw",
        "staging.nodes_raw",
        "ingestion_files",
    ]

    for table in cleanup_tables:
        try:
            cur.execute(f"DELETE FROM {table} WHERE batch_id = %s;", (batch_id,))
            deleted = cur.rowcount
            if deleted > 0:
                print(f"  Cleared {deleted:>6} rows from {table}")
        except Exception as e:
            # Table may not exist yet (first run)
            conn.rollback()
            print(f"  Skipped {table} (not found or error: {str(e)[:60]})")

    # Also clear quarantine records for this batch
    try:
        cur.execute("DELETE FROM validation.quarantine WHERE batch_id = %s;", (batch_id,))
        print(f"  Cleared {cur.rowcount} quarantine records")
    except Exception:
        conn.rollback()

    conn.commit()
    cur.close()
    conn.close()
    print()


# ===================================================================
# Stage 1: Data Ingestion → staging
# ===================================================================
def stage_1_ingestion(data_dir: str) -> dict:
    """Run all 5 loader scripts to ingest raw data into staging tables."""
    print("=" * 72)
    print("  STAGE 1/9: DATA INGESTION → staging.*")
    print("=" * 72)

    ingestion_dir = str(PROJECT_ROOT / "ingestion")
    loaders = [
        ("load_nodes.py", "Nodes"),
        ("load_peer_connections.py", "Peer Connections"),
        ("load_transaction_observations.py", "Transaction Observations"),
        ("load_bitcoin_transactions.py", "Bitcoin Transactions"),
        ("load_network_events.py", "Network Events"),
    ]

    for script, label in loaders:
        print(f"\n  [{label}]")
        _run_script(script, ingestion_dir, f"Ingestion: {label}")

    print()
    return {"status": "success", "datasets_loaded": len(loaders)}


# ===================================================================
# Stage 2: Validation
# ===================================================================
def stage_2_validation() -> dict:
    """Run all 5 validation scripts."""
    print("=" * 72)
    print("  STAGE 2/9: DATA VALIDATION → validation.quarantine")
    print("=" * 72)

    validation_dir = str(PROJECT_ROOT / "ingestion" / "validation")
    validators = [
        ("validate_nodes.py", "Nodes"),
        ("validate_peer_connections.py", "Peer Connections"),
        ("validate_transaction_observations.py", "Transaction Observations"),
        ("validate_bitcoin_transactions.py", "Bitcoin Transactions"),
        ("validate_network_events.py", "Network Events"),
    ]

    for script, label in validators:
        print(f"\n  [{label}]")
        _run_script(script, validation_dir, f"Validation: {label}")

    print()
    return {"status": "success", "validators_run": len(validators)}


# ===================================================================
# Stage 3: Validation Audit
# ===================================================================
def stage_3_audit() -> dict:
    """Run the comprehensive validation audit."""
    print("=" * 72)
    print("  STAGE 3/9: VALIDATION AUDIT")
    print("=" * 72)

    validation_dir = str(PROJECT_ROOT / "ingestion" / "validation")
    _run_script("audit_validation.py", validation_dir, "Validation Audit")

    # Check quarantine count
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM validation.quarantine WHERE batch_id = %s;", (BATCH_ID,))
    quarantined = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"\n  Quarantined records: {quarantined}")
    print()
    return {"status": "success", "quarantined_records": quarantined}


# ===================================================================
# Stage 4: Core Transformation
# ===================================================================
def stage_4_transformation() -> dict:
    """Transform staging data into core normalized tables."""
    print("=" * 72)
    print("  STAGE 4/9: CORE TRANSFORMATION → core.*")
    print("=" * 72)

    transform_dir = str(PROJECT_ROOT / "transformation")
    _run_script("run_transformation.py", transform_dir, "Core Transformation")
    print()
    return {"status": "success"}


# ===================================================================
# Stage 5: Transformation Verification
# ===================================================================
def stage_5_verify_transformation() -> dict:
    """Verify all core tables have correct row counts and referential integrity."""
    print("=" * 72)
    print("  STAGE 5/9: TRANSFORMATION VERIFICATION")
    print("=" * 72)

    transform_dir = str(PROJECT_ROOT / "transformation")
    _run_script("verify_transformation.py", transform_dir, "Transformation Verification")

    # Fetch counts
    conn = get_connection()
    cur = conn.cursor()
    counts = {}
    for table in ["core.nodes", "core.transactions", "core.peer_connections",
                   "core.transaction_observations", "core.network_events"]:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE batch_id = %s;", (BATCH_ID,))
        counts[table] = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"\n  Core table counts: {json.dumps(counts, indent=4)}")
    print()
    return {"status": "success", "core_counts": counts}


# ===================================================================
# Stage 6: Analytics Views
# ===================================================================
def stage_6_analytics_views() -> dict:
    """Create/refresh analytics materialized views."""
    print("=" * 72)
    print("  STAGE 6/9: ANALYTICS VIEWS → analytics.*")
    print("=" * 72)

    analytics_dir = str(PROJECT_ROOT / "analytics")

    print("\n  [Transaction Propagation View]")
    _run_script("create_transaction_propagation_view.py", analytics_dir,
                "Analytics: Transaction Propagation View")

    print("\n  [Transaction Propagation Summary]")
    _run_script("create_transaction_propagation_summary.py", analytics_dir,
                "Analytics: Transaction Propagation Summary")

    print("\n  [Node Behavior Summary]")
    _run_script("create_node_behavior_summary.py", analytics_dir,
                "Analytics: Node Behavior Summary")

    print()
    return {"status": "success"}


# ===================================================================
# Stage 7: Feature Engineering (direct Python import)
# ===================================================================
def stage_7_features() -> dict:
    """Build node and transaction feature tables."""
    print("=" * 72)
    print("  STAGE 7/9: FEATURE ENGINEERING → analytics.node_features, transaction_features")
    print("=" * 72)

    from build_features import build_node_features, build_transaction_features

    node_count = build_node_features()
    tx_count = build_transaction_features()

    print(f"\n  Node features: {node_count} rows")
    print(f"  Transaction features: {tx_count} rows")
    print()
    return {"status": "success", "node_features": node_count, "tx_features": tx_count}


# ===================================================================
# Stage 8: Graph Construction (direct Python import)
# ===================================================================
def stage_8_graph() -> dict:
    """Build heterogeneous graph, compute centrality & community detection."""
    print("=" * 72)
    print("  STAGE 8/9: GRAPH CONSTRUCTION → NetworkX + Louvain")
    print("=" * 72)

    from build_graph import build_and_export_graph
    build_and_export_graph()

    print()
    return {"status": "success"}


# ===================================================================
# Stage 9: ML Inference + Threat Alerts (direct Python import)
# ===================================================================
def stage_9_inference(batch_id: int = 1, threshold: float = 65.0) -> dict:
    """Run operational model inference and generate alerts."""
    print("=" * 72)
    print("  STAGE 9/9: ML INFERENCE + THREAT ALERT GENERATION")
    print("=" * 72)

    from predict_operational import run_operational_inference
    result = run_operational_inference(batch_id=batch_id, alert_threshold=threshold)

    print()
    return result


# ===================================================================
# UNIFIED PIPELINE ENTRY POINT
# ===================================================================
def run_full_pipeline(
    data_dir: str = None,
    batch_id: int = 1,
    threshold: float = 65.0,
    clean: bool = False,
) -> dict:
    """
    Execute the complete 10-stage pipeline from raw data to predictions.

    Args:
        data_dir:   Path to directory containing the 5 raw data files.
                    Defaults to <project_root>/data/incoming/
        batch_id:   Batch ID to process (default: 1)
        threshold:  Risk score threshold for alert generation (default: 65.0)
        clean:      If True, wipe existing batch data before re-ingestion

    Returns:
        dict with full pipeline execution summary
    """
    if data_dir is None:
        data_dir = str(PROJECT_ROOT / "data" / "incoming")

    pipeline_start = time.time()
    run_timestamp = datetime.now().isoformat()
    stages_log = []

    print()
    print("█" * 72)
    print("█" + " " * 70 + "█")
    print("█" + "  BITCOIN MONITOR — UNIFIED END-TO-END OPERATIONAL PIPELINE".center(70) + "█")
    print("█" + f"  Batch: #{batch_id}  |  Threshold: {threshold}  |  Clean: {clean}".center(70) + "█")
    print("█" + f"  Started: {run_timestamp}".center(70) + "█")
    print("█" + " " * 70 + "█")
    print("█" * 72)
    print()

    try:
        # -----------------------------------------------------------
        # Stage 0: Pre-flight
        # -----------------------------------------------------------
        t0 = time.time()
        result = stage_0_preflight(data_dir)
        stages_log.append({"stage": "0_preflight", "duration_sec": round(time.time() - t0, 3), **result})

        # -----------------------------------------------------------
        # Optional: Clean existing batch data
        # -----------------------------------------------------------
        if clean:
            t0 = time.time()
            stage_clean_batch(batch_id)
            stages_log.append({"stage": "clean", "duration_sec": round(time.time() - t0, 3), "status": "success"})

        # -----------------------------------------------------------
        # Stage 1: Ingestion
        # -----------------------------------------------------------
        t0 = time.time()
        result = stage_1_ingestion(data_dir)
        stages_log.append({"stage": "1_ingestion", "duration_sec": round(time.time() - t0, 3), **result})

        # -----------------------------------------------------------
        # Stage 2: Validation
        # -----------------------------------------------------------
        t0 = time.time()
        result = stage_2_validation()
        stages_log.append({"stage": "2_validation", "duration_sec": round(time.time() - t0, 3), **result})

        # -----------------------------------------------------------
        # Stage 3: Audit
        # -----------------------------------------------------------
        t0 = time.time()
        result = stage_3_audit()
        stages_log.append({"stage": "3_audit", "duration_sec": round(time.time() - t0, 3), **result})

        # -----------------------------------------------------------
        # Stage 4: Core Transformation
        # -----------------------------------------------------------
        t0 = time.time()
        result = stage_4_transformation()
        stages_log.append({"stage": "4_transformation", "duration_sec": round(time.time() - t0, 3), **result})

        # -----------------------------------------------------------
        # Stage 5: Verify Transformation
        # -----------------------------------------------------------
        t0 = time.time()
        result = stage_5_verify_transformation()
        stages_log.append({"stage": "5_verification", "duration_sec": round(time.time() - t0, 3), **result})

        # -----------------------------------------------------------
        # Stage 6: Analytics Views
        # -----------------------------------------------------------
        t0 = time.time()
        result = stage_6_analytics_views()
        stages_log.append({"stage": "6_analytics_views", "duration_sec": round(time.time() - t0, 3), **result})

        # -----------------------------------------------------------
        # Stage 7: Feature Engineering
        # -----------------------------------------------------------
        t0 = time.time()
        result = stage_7_features()
        stages_log.append({"stage": "7_features", "duration_sec": round(time.time() - t0, 3), **result})

        # -----------------------------------------------------------
        # Stage 8: Graph Construction
        # -----------------------------------------------------------
        t0 = time.time()
        result = stage_8_graph()
        stages_log.append({"stage": "8_graph", "duration_sec": round(time.time() - t0, 3), **result})

        # -----------------------------------------------------------
        # Stage 9: ML Inference + Alerts
        # -----------------------------------------------------------
        t0 = time.time()
        result = stage_9_inference(batch_id=batch_id, threshold=threshold)
        stages_log.append({"stage": "9_inference", "duration_sec": round(time.time() - t0, 3), **result})

    except Exception as e:
        total_elapsed = round(time.time() - pipeline_start, 2)
        print(f"\n{'!' * 72}")
        print(f"  PIPELINE FAILED after {total_elapsed}s: {str(e)}")
        print(f"{'!' * 72}\n")
        return {
            "status": "failed",
            "error": str(e),
            "batch_id": batch_id,
            "total_elapsed_seconds": total_elapsed,
            "stages_completed": stages_log,
        }

    # -----------------------------------------------------------
    # Final Summary
    # -----------------------------------------------------------
    total_elapsed = round(time.time() - pipeline_start, 2)

    # Fetch final dashboard-ready counts
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM core.nodes WHERE batch_id = %s;", (batch_id,))
    total_nodes = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM core.transactions WHERE batch_id = %s;", (batch_id,))
    total_txs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM analytics.alerts WHERE batch_id = %s;", (batch_id,))
    total_alerts = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM analytics.alerts
        WHERE batch_id = %s AND severity IN ('CRITICAL', 'HIGH');
    """, (batch_id,))
    critical_high = cur.fetchone()[0]

    cur.close()
    conn.close()

    print()
    print("█" * 72)
    print("█" + " " * 70 + "█")
    print("█" + "  ✅ UNIFIED PIPELINE COMPLETED SUCCESSFULLY".center(70) + "█")
    print("█" + f"  Total Time: {total_elapsed}s  |  Batch: #{batch_id}".center(70) + "█")
    print("█" + f"  Nodes: {total_nodes}  |  Transactions: {total_txs}  |  Alerts: {total_alerts}".center(70) + "█")
    print("█" + " " * 70 + "█")
    print("█" * 72)
    print()

    summary = {
        "status": "completed",
        "batch_id": batch_id,
        "timestamp": run_timestamp,
        "total_elapsed_seconds": total_elapsed,
        "stages": stages_log,
        "results": {
            "total_nodes": total_nodes,
            "total_transactions": total_txs,
            "total_alerts": total_alerts,
            "critical_high_alerts": critical_high,
        },
        "dashboard_url": "http://localhost:5173/",
        "api_url": "http://localhost:8000/docs",
    }
    return summary


# ===================================================================
# CLI Entry Point
# ===================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bitcoin Monitor — Unified End-to-End Operational Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline/run_full_pipeline.py
  python pipeline/run_full_pipeline.py --data-dir data/incoming --threshold 70
  python pipeline/run_full_pipeline.py --clean
        """,
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Path to directory containing the 5 raw data files (default: data/incoming/)",
    )
    parser.add_argument(
        "--batch-id",
        type=int,
        default=1,
        help="Batch ID to process (default: 1)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=65.0,
        help="Risk score threshold for alert generation (default: 65.0)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Wipe existing batch data before re-ingestion (for clean re-runs)",
    )

    args = parser.parse_args()

    result = run_full_pipeline(
        data_dir=args.data_dir,
        batch_id=args.batch_id,
        threshold=args.threshold,
        clean=args.clean,
    )

    print(json.dumps(result, indent=2, default=str))
