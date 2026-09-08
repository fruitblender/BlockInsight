"""
ml/predict_operational.py
Operational Inference Engine for Bitcoin Monitor (SRS §22, §24, §25, §26, §27)
Evaluates any incoming batch against trained production models without retraining.
Computes anomaly scores, risk ratings, SHAP factors, and persists alerts.
"""
import sys
import os
import json
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import joblib

try:
    import shap
    HAS_SHAP = True
except Exception:
    HAS_SHAP = False

# Add ingestion directory to path for database connection
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

MODELS_DIR = Path(__file__).resolve().parents[1] / "models"


def run_operational_inference(batch_id: int = 1, alert_threshold: float = 65.0) -> dict:
    print("============================================================")
    print(f"   OPERATIONAL INFERENCE ENGINE - BATCH #{batch_id}")
    print("============================================================\n")

    conn = get_connection()
    cur = conn.cursor()

    # -------------------------------------------------------------
    # 1. Load Pre-Trained Production Artifacts
    # -------------------------------------------------------------
    print("[1/4] Loading trained models & feature schemas from models/...")
    try:
        node_scaler = joblib.load(MODELS_DIR / "node_scaler.joblib")
        node_clf = joblib.load(MODELS_DIR / "node_isolation_forest.joblib")
        node_feat_cols = joblib.load(MODELS_DIR / "node_feature_names.joblib")

        tx_scaler = joblib.load(MODELS_DIR / "tx_scaler.joblib")
        tx_clf = joblib.load(MODELS_DIR / "tx_isolation_forest.joblib")
        tx_feat_cols = joblib.load(MODELS_DIR / "tx_feature_names.joblib")

        dbscan = joblib.load(MODELS_DIR / "dbscan_model.joblib")
    except Exception as e:
        raise RuntimeError(f"Error loading trained models: {e}. Ensure models were trained first.")

    # -------------------------------------------------------------
    # 2. Node Inference
    # -------------------------------------------------------------
    print(f"[2/4] Scoring nodes for batch #{batch_id}...")
    node_query = f"""
        SELECT 
            node_id, ip, country, asn, node_type,
            {', '.join(node_feat_cols)}
        FROM analytics.node_features
        WHERE batch_id = %s;
    """
    df_nodes = pd.read_sql(node_query, conn, params=(batch_id,))
    if df_nodes.empty:
        print(f"Warning: No node features found for batch #{batch_id} in analytics.node_features.")
        node_results = {"count": 0, "anomalies": 0, "critical": 0}
    else:
        X_nodes = df_nodes[node_feat_cols].values.astype(float)
        X_nodes_scaled = node_scaler.transform(X_nodes)

        # Anomaly scoring
        raw_dec = node_clf.decision_function(X_nodes_scaled)
        preds = node_clf.predict(X_nodes_scaled)  # -1 = anomaly, 1 = normal
        min_s, max_s = raw_dec.min(), raw_dec.max()
        norm_scores = 1.0 - ((raw_dec - min_s) / (max_s - min_s)) if max_s > min_s else np.zeros_like(raw_dec)

        # DBSCAN clustering
        cluster_labels = dbscan.fit_predict(X_nodes_scaled)
        is_noise = cluster_labels == -1

        # SHAP Explanations
        if HAS_SHAP:
            try:
                explainer = shap.TreeExplainer(node_clf)
                shap_vals = explainer.shap_values(X_nodes_scaled)
            except Exception:
                shap_vals = -(X_nodes_scaled - np.median(X_nodes_scaled, axis=0))
        else:
            shap_vals = -(X_nodes_scaled - np.median(X_nodes_scaled, axis=0))

        # Upsert Node Risk Scores
        upsert_node_sql = """
            INSERT INTO analytics.node_risk_scores (
                batch_id, node_id, risk_score, confidence, risk_level, explanation, shap_factors
            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (batch_id, node_id) DO UPDATE SET
                risk_score = EXCLUDED.risk_score,
                confidence = EXCLUDED.confidence,
                risk_level = EXCLUDED.risk_level,
                explanation = EXCLUDED.explanation,
                shap_factors = EXCLUDED.shap_factors,
                created_at = NOW();
        """
        critical_nodes = 0
        high_nodes = 0

        for idx, row in df_nodes.iterrows():
            base_score = float(norm_scores[idx]) * 65.0
            cluster_pen = 20.0 if is_noise[idx] else 0.0
            delay_pen = 10.0 if row.get('avg_observer_delay_ms', 0) > 300 else (5.0 if row.get('total_connections', 0) <= 1 else 0.0)
            total_risk = min(100.0, max(0.0, base_score + cluster_pen + delay_pen))
            
            sample_depth = row.get('total_connections', 0) + (row.get('transactions_observed', 0) / 50.0)
            conf = min(0.99, max(0.50, 0.60 + min(0.35, sample_depth * 0.05)))
            
            level = "CRITICAL" if total_risk >= 80 else ("HIGH" if total_risk >= 65 else ("MEDIUM" if total_risk >= 40 else "LOW"))
            if level == "CRITICAL": critical_nodes += 1
            if level == "HIGH": high_nodes += 1

            # Explanation
            top_indices = np.argsort(np.abs(shap_vals[idx]))[::-1][:3]
            top_factors = {node_feat_cols[i]: {"value": float(X_nodes[idx, i]), "impact": round(float(shap_vals[idx, i]), 4)} for i in top_indices}
            
            reasons = []
            if is_noise[idx]: reasons.append("Isolated network cluster (DBSCAN outlier)")
            if row.get('avg_observer_delay_ms', 0) > 200: reasons.append(f"High observer delay ({row['avg_observer_delay_ms']:.1f}ms)")
            if row.get('total_connections', 0) <= 2: reasons.append("Sparse peer connectivity")
            if not reasons: reasons.append("Operational metrics within normal baseline")
            expl = "; ".join(reasons[:2])

            cur.execute(upsert_node_sql, (
                batch_id,
                row['node_id'],
                round(total_risk, 2),
                round(conf, 3),
                level,
                expl,
                json.dumps(top_factors)
            ))

        conn.commit()
        node_results = {
            "count": len(df_nodes),
            "anomalies": int((preds == -1).sum()),
            "critical": critical_nodes,
            "high": high_nodes
        }
        print(f"  Nodes evaluated: {node_results['count']} | Anomalies: {node_results['anomalies']} | Critical/High: {critical_nodes + high_nodes}")

    # -------------------------------------------------------------
    # 3. Transaction Inference
    # -------------------------------------------------------------
    print(f"\n[3/4] Scoring transactions for batch #{batch_id}...")
    tx_query = f"""
        SELECT 
            txid,
            {', '.join(tx_feat_cols)}
        FROM analytics.transaction_features
        WHERE batch_id = %s;
    """
    df_tx = pd.read_sql(tx_query, conn, params=(batch_id,))
    if df_tx.empty:
        print(f"Warning: No transaction features found for batch #{batch_id} in analytics.transaction_features.")
        tx_results = {"count": 0, "anomalies": 0, "critical": 0}
    else:
        X_tx = df_tx[tx_feat_cols].values.astype(float)
        X_tx_scaled = tx_scaler.transform(X_tx)

        raw_dec_tx = tx_clf.decision_function(X_tx_scaled)
        preds_tx = tx_clf.predict(X_tx_scaled)
        min_s_t, max_s_t = raw_dec_tx.min(), raw_dec_tx.max()
        norm_scores_t = 1.0 - ((raw_dec_tx - min_s_t) / (max_s_t - min_s_t)) if max_s_t > min_s_t else np.zeros_like(raw_dec_tx)

        if HAS_SHAP:
            try:
                explainer_tx = shap.TreeExplainer(tx_clf)
                shap_vals_tx = explainer_tx.shap_values(X_tx_scaled)
            except Exception:
                shap_vals_tx = -(X_tx_scaled - np.median(X_tx_scaled, axis=0))
        else:
            shap_vals_tx = -(X_tx_scaled - np.median(X_tx_scaled, axis=0))

        upsert_tx_sql = """
            INSERT INTO analytics.transaction_risk_scores (
                batch_id, txid, risk_score, confidence, risk_level, explanation, shap_factors
            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (batch_id, txid) DO UPDATE SET
                risk_score = EXCLUDED.risk_score,
                confidence = EXCLUDED.confidence,
                risk_level = EXCLUDED.risk_level,
                explanation = EXCLUDED.explanation,
                shap_factors = EXCLUDED.shap_factors,
                created_at = NOW();
        """
        critical_txs = 0
        high_txs = 0

        for idx, row in df_tx.iterrows():
            base_score = float(norm_scores_t[idx]) * 60.0
            rarity_pen = min(25.0, float(row.get('rarity_score', 0)) * 25.0)
            prop_pen = 10.0 if row.get('total_observations', 0) < 3 else 0.0
            total_risk = min(100.0, max(0.0, base_score + rarity_pen + prop_pen))

            conf = min(0.99, max(0.55, 0.65 + min(0.30, row.get('total_observations', 0) * 0.03)))
            level = "CRITICAL" if total_risk >= 80 else ("HIGH" if total_risk >= 65 else ("MEDIUM" if total_risk >= 40 else "LOW"))
            if level == "CRITICAL": critical_txs += 1
            if level == "HIGH": high_txs += 1

            top_indices = np.argsort(np.abs(shap_vals_tx[idx]))[::-1][:3]
            top_factors = {tx_feat_cols[i]: {"value": float(X_tx[idx, i]), "impact": round(float(shap_vals_tx[idx, i]), 4)} for i in top_indices}

            reasons = []
            if row.get('rarity_score', 0) > 0.7: reasons.append(f"Whale/rare profile ({row['rarity_score']:.2f})")
            if row.get('fee', 0) > 0.01: reasons.append("Unusually high transaction fee")
            if row.get('total_observations', 0) < 3: reasons.append("Abnormally restricted propagation")
            if not reasons: reasons.append("Standard transaction fee and propagation baseline")
            expl = "; ".join(reasons[:2])

            cur.execute(upsert_tx_sql, (
                batch_id,
                row['txid'],
                round(total_risk, 2),
                round(conf, 3),
                level,
                expl,
                json.dumps(top_factors)
            ))

        conn.commit()
        tx_results = {
            "count": len(df_tx),
            "anomalies": int((preds_tx == -1).sum()),
            "critical": critical_txs,
            "high": high_txs
        }
        print(f"  Transactions evaluated: {tx_results['count']} | Anomalies: {tx_results['anomalies']} | Critical/High: {critical_txs + high_txs}")

    # -------------------------------------------------------------
    # 4. Trigger Alerts for High/Critical Risks
    # -------------------------------------------------------------
    print(f"\n[4/4] Generating threat alerts (threshold >= {alert_threshold})...")
    # Delete old alerts for this batch to refresh with latest state
    cur.execute("DELETE FROM analytics.alerts WHERE batch_id = %s;", (batch_id,))
    conn.commit()

    # Insert node alerts
    cur.execute("""
        INSERT INTO analytics.alerts (
            alert_id, batch_id, entity_type, entity_id, risk_score, confidence,
            severity, title, description, evidence, status
        )
        SELECT 
            'ALT-NODE-' || %s || '-' || nrs.node_id,
            %s,
            'NODE',
            nrs.node_id,
            nrs.risk_score,
            nrs.confidence,
            nrs.risk_level,
            nrs.risk_level || ' Risk Network Node: ' || nrs.node_id || ' (' || nf.ip || ')',
            'P2P node exhibits anomalous propagation behavior. ' || nrs.explanation,
            json_build_object(
                'node_id', nrs.node_id,
                'ip', nf.ip,
                'country', nf.country,
                'asn', nf.asn,
                'node_type', nf.node_type,
                'peer_degree', nf.peer_degree,
                'total_connections', nf.total_connections,
                'avg_observer_delay_ms', nf.avg_observer_delay_ms,
                'shap_factors', nrs.shap_factors
            )::jsonb,
            'OPEN'
        FROM analytics.node_risk_scores nrs
        JOIN analytics.node_features nf ON nrs.node_id = nf.node_id AND nrs.batch_id = nf.batch_id
        WHERE nrs.batch_id = %s AND nrs.risk_score >= %s;
    """, (batch_id, batch_id, batch_id, alert_threshold))

    # Insert transaction alerts
    cur.execute("""
        INSERT INTO analytics.alerts (
            alert_id, batch_id, entity_type, entity_id, risk_score, confidence,
            severity, title, description, evidence, status
        )
        SELECT 
            'ALT-TX-' || %s || '-' || SUBSTRING(trs.txid FROM 1 FOR 16),
            %s,
            'TRANSACTION',
            trs.txid,
            trs.risk_score,
            trs.confidence,
            trs.risk_level,
            trs.risk_level || ' Risk Transaction: ' || SUBSTRING(trs.txid FROM 1 FOR 12) || '...',
            'Transaction flagged with anomalous propagation signature. ' || trs.explanation,
            json_build_object(
                'txid', trs.txid,
                'fee', tf.fee,
                'size', tf.size,
                'rarity_score', tf.rarity_score,
                'total_observations', tf.total_observations,
                'avg_propagation_delay_ms', tf.avg_propagation_delay_ms,
                'shap_factors', trs.shap_factors
            )::jsonb,
            'OPEN'
        FROM analytics.transaction_risk_scores trs
        JOIN analytics.transaction_features tf ON trs.txid = tf.txid AND trs.batch_id = tf.batch_id
        WHERE trs.batch_id = %s AND trs.risk_score >= %s;
    """, (batch_id, batch_id, batch_id, alert_threshold))
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM analytics.alerts WHERE batch_id = %s;", (batch_id,))
    total_alerts = cur.fetchone()[0]
    print(f"  Generated {total_alerts} alerts for batch #{batch_id}.")

    cur.close()
    conn.close()

    result = {
        "status": "success",
        "batch_id": batch_id,
        "nodes": node_results,
        "transactions": tx_results,
        "total_alerts": total_alerts
    }
    print(f"\n>>> OPERATIONAL INFERENCE FOR BATCH #{batch_id} COMPLETED SUCCESSFULLY! <<<")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Operational Inference Engine for Bitcoin Monitor")
    parser.add_argument("--batch-id", type=int, default=1, help="Ingestion batch ID to score")
    parser.add_argument("--threshold", type=float, default=65.0, help="Alert risk score threshold")
    args = parser.parse_args()

    res = run_operational_inference(batch_id=args.batch_id, alert_threshold=args.threshold)
    print(json.dumps(res, indent=2))
