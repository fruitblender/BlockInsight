"""
Step 5: Risk Scoring & SHAP Explainability (SRS §25, §26)
Computes composite risk score (0-100) and confidence (0-1) for nodes and transactions.
Extracts SHAP feature importance using TreeExplainer on Isolation Forest.
Generates human-readable, plain-language reason strings.
Persists scores and explanations to analytics.node_risk_scores and analytics.transaction_risk_scores.
"""
import sys
import os
import json
from pathlib import Path
import numpy as np
import pandas as pd
try:
    import shap
    HAS_SHAP = True
except Exception:
    HAS_SHAP = False
import joblib

# Add ingestion directory to path for database connection
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1
MODELS_DIR = Path(__file__).resolve().parents[1] / "models"


def score_and_explain_nodes():
    print("============================================================")
    print("   STEP 5: RISK SCORING & SHAP EXPLAINABILITY - NODES")
    print("============================================================\n")

    conn = get_connection()

    query = """
        SELECT 
            nf.node_id,
            nf.ip,
            nf.country,
            nf.asn,
            nf.node_type,
            nf.peer_degree,
            nf.outgoing_connections,
            nf.incoming_connections,
            nf.total_connections,
            nf.avg_connection_duration_ms,
            nf.observations_as_observer,
            nf.transactions_observed,
            nf.unique_peers_as_observer,
            nf.avg_observer_delay_ms,
            nf.propagations_as_peer,
            nf.transactions_propagated,
            nf.unique_observers_as_peer,
            nf.propagation_observation_ratio,
            nf.ip_diversity,
            COALESCE(nf.degree_centrality, 0.0) AS degree_centrality,
            COALESCE(nf.betweenness_centrality, 0.0) AS betweenness_centrality,
            COALESCE(nf.closeness_centrality, 0.0) AS closeness_centrality,
            COALESCE(nf.community_id, 0) AS community_id,
            na.anomaly_score,
            na.is_anomaly,
            nc.cluster_id,
            nc.is_noise
        FROM analytics.node_features nf
        JOIN analytics.node_anomalies na ON nf.node_id = na.node_id AND nf.batch_id = na.batch_id
        JOIN analytics.node_clusters nc ON nf.node_id = nc.node_id AND nf.batch_id = nc.batch_id
        WHERE nf.batch_id = %s;
    """
    df = pd.read_sql(query, conn, params=(BATCH_ID,))
    print(f"Loaded {len(df)} nodes for risk scoring and SHAP analysis.")

    scaler = joblib.load(MODELS_DIR / "node_scaler.joblib")
    clf = joblib.load(MODELS_DIR / "node_isolation_forest.joblib")
    feature_cols = joblib.load(MODELS_DIR / "node_feature_names.joblib")

    X = df[feature_cols].values.astype(float)
    X_scaled = scaler.transform(X)

    if HAS_SHAP:
        print("Computing SHAP values for nodes...")
        try:
            explainer = shap.TreeExplainer(clf)
            shap_values = explainer.shap_values(X_scaled)
        except Exception as e:
            print(f"TreeExplainer exception ({e}), using feature deviations")
            shap_values = -(X_scaled - np.median(X_scaled, axis=0))
    else:
        print("Using standardized feature deviations for SHAP explanation")
        shap_values = -(X_scaled - np.median(X_scaled, axis=0))

    # Compute composite risk score (0 - 100)
    # 1. Base anomaly component: 0 - 65
    # 2. Cluster noise component: +20 if DBSCAN outlier
    # 3. High delay or zero-observation suspicion: +15
    risk_scores = []
    confidence_scores = []
    reasons_list = []
    shap_json_list = []

    for idx, row in df.iterrows():
        base_score = float(row['anomaly_score']) * 65.0
        cluster_penalty = 20.0 if row['is_noise'] else 0.0
        
        delay_penalty = 0.0
        if row['avg_observer_delay_ms'] > 300:
            delay_penalty += 10.0
        elif row['total_connections'] <= 1:
            delay_penalty += 5.0

        total_risk = min(100.0, max(0.0, base_score + cluster_penalty + delay_penalty))
        risk_scores.append(round(total_risk, 2))

        # Confidence: higher if more observations & connections observed
        sample_depth = row['total_connections'] + (row['transactions_observed'] / 50.0)
        confidence = min(0.99, max(0.50, 0.60 + min(0.35, sample_depth * 0.05)))
        confidence_scores.append(round(confidence, 3))

        # SHAP feature impact
        # In TreeExplainer on Isolation Forest: negative SHAP values push towards anomalous
        node_shap = shap_values[idx]
        # Sort features by highest absolute magnitude
        top_indices = np.argsort(np.abs(node_shap))[::-1][:4]

        top_factors = {}
        text_reasons = []
        for feat_idx in top_indices:
            feat_name = feature_cols[feat_idx]
            impact = float(node_shap[feat_idx])
            val = float(X[idx, feat_idx])
            top_factors[feat_name] = {"value": val, "impact": round(impact, 4)}

            # Generate natural language reason
            if feat_name == 'avg_observer_delay_ms' and val > 200:
                text_reasons.append(f"Elevated propagation observation delay ({val:.1f}ms)")
            elif feat_name == 'total_connections' and val <= 2:
                text_reasons.append(f"Sparse network connectivity ({int(val)} total peers)")
            elif feat_name == 'peer_degree' and val > 15:
                text_reasons.append(f"Unusually high peer degree ({int(val)} connections)")
            elif feat_name == 'ip_diversity' and val < 2:
                text_reasons.append("Extremely low peer IP diversity")
            elif feat_name == 'degree_centrality' and val > 0.15:
                text_reasons.append("Hub-like topological centrality")
            elif feat_name == 'propagation_observation_ratio' and val > 1.5:
                text_reasons.append("Abnormal peer propagation to observation ratio")

        if row['is_noise']:
            text_reasons.insert(0, "Identified as network behavioral outlier (DBSCAN cluster -1)")

        if not text_reasons:
            text_reasons.append("Standard peer operational metrics within expected baseline")

        reasons_list.append("; ".join(text_reasons[:3]))
        shap_json_list.append(json.dumps(top_factors))

    df['risk_score'] = risk_scores
    df['confidence'] = confidence_scores
    df['explanation'] = reasons_list
    df['shap_factors'] = shap_json_list

    # Persist to analytics.node_risk_scores
    cur = conn.cursor()
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        DROP TABLE IF EXISTS analytics.node_risk_scores CASCADE;
        CREATE TABLE analytics.node_risk_scores (
            batch_id INTEGER NOT NULL,
            node_id VARCHAR(64) NOT NULL,
            risk_score NUMERIC(6, 2) NOT NULL,
            confidence NUMERIC(5, 3) NOT NULL,
            risk_level VARCHAR(20) NOT NULL,
            explanation TEXT NOT NULL,
            shap_factors JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (batch_id, node_id)
        );
    """)
    conn.commit()

    insert_sql = """
        INSERT INTO analytics.node_risk_scores (
            batch_id, node_id, risk_score, confidence, risk_level, explanation, shap_factors
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb);
    """
    for _, row in df.iterrows():
        r = row['risk_score']
        level = "CRITICAL" if r >= 80 else ("HIGH" if r >= 65 else ("MEDIUM" if r >= 40 else "LOW"))
        cur.execute(insert_sql, (
            BATCH_ID,
            row['node_id'],
            r,
            row['confidence'],
            level,
            row['explanation'],
            row['shap_factors']
        ))
    conn.commit()

    print(f"Node Risk Scoring Complete:")
    print(f"  CRITICAL (>=80): {(df['risk_score'] >= 80).sum()}")
    print(f"  HIGH (65-79):    {((df['risk_score'] >= 65) & (df['risk_score'] < 80)).sum()}")
    print(f"  MEDIUM (40-64):  {((df['risk_score'] >= 40) & (df['risk_score'] < 65)).sum()}")
    print(f"  LOW (<40):       {(df['risk_score'] < 40).sum()}\n")

    cur.close()
    conn.close()


def score_and_explain_transactions():
    print("============================================================")
    print("   STEP 5: RISK SCORING & SHAP EXPLAINABILITY - TRANSACTIONS")
    print("============================================================\n")

    conn = get_connection()

    query = """
        SELECT 
            tf.txid,
            tf.fee,
            tf.size,
            tf.ratio_fee_size,
            tf.rarity_score,
            tf.inputs,
            tf.outputs,
            tf.total_observations,
            tf.unique_observers,
            tf.unique_peers,
            tf.unique_observer_countries,
            tf.unique_peer_countries,
            tf.min_propagation_delay_ms,
            tf.max_propagation_delay_ms,
            tf.avg_propagation_delay_ms,
            tf.median_propagation_delay_ms,
            tf.observed_propagation_span_ms,
            tf.creation_to_first_observation_ms,
            tf.inv_ratio,
            tf.tx_ratio,
            ta.anomaly_score,
            ta.is_anomaly
        FROM analytics.transaction_features tf
        JOIN analytics.transaction_anomalies ta ON tf.txid = ta.txid AND tf.batch_id = ta.batch_id
        WHERE tf.batch_id = %s;
    """
    df = pd.read_sql(query, conn, params=(BATCH_ID,))
    print(f"Loaded {len(df)} transactions for risk scoring and SHAP analysis.")

    scaler = joblib.load(MODELS_DIR / "tx_scaler.joblib")
    clf = joblib.load(MODELS_DIR / "tx_isolation_forest.joblib")
    feature_cols = joblib.load(MODELS_DIR / "tx_feature_names.joblib")

    X = df[feature_cols].values.astype(float)
    X_scaled = scaler.transform(X)

    if HAS_SHAP:
        print("Computing SHAP values for transactions...")
        try:
            explainer = shap.TreeExplainer(clf)
            shap_values = explainer.shap_values(X_scaled)
        except Exception as e:
            print(f"TreeExplainer exception ({e}), using feature deviations")
            shap_values = -(X_scaled - np.median(X_scaled, axis=0))
    else:
        print("Using standardized feature deviations for SHAP explanation")
        shap_values = -(X_scaled - np.median(X_scaled, axis=0))

    risk_scores = []
    confidence_scores = []
    reasons_list = []
    shap_json_list = []

    for idx, row in df.iterrows():
        # Composite risk for transactions:
        # 1. Base anomaly from Isolation Forest: up to 60
        # 2. Rarity score from synthetic whale dataset: up to 25
        # 3. Propagation anomaly (low observations or slow spread): up to 15
        base_score = float(row['anomaly_score']) * 60.0
        rarity_component = min(25.0, float(row['rarity_score']) * 25.0)
        
        prop_anomaly = 0.0
        if row['total_observations'] < 3:
            prop_anomaly += 10.0
        if row['avg_propagation_delay_ms'] > 400:
            prop_anomaly += 5.0

        total_risk = min(100.0, max(0.0, base_score + rarity_component + prop_anomaly))
        risk_scores.append(round(total_risk, 2))

        # Confidence: based on observation sample size
        confidence = min(0.99, max(0.55, 0.65 + min(0.30, row['total_observations'] * 0.03)))
        confidence_scores.append(round(confidence, 3))

        tx_shap = shap_values[idx]
        top_indices = np.argsort(np.abs(tx_shap))[::-1][:4]

        top_factors = {}
        text_reasons = []
        for feat_idx in top_indices:
            feat_name = feature_cols[feat_idx]
            impact = float(tx_shap[feat_idx])
            val = float(X[idx, feat_idx])
            top_factors[feat_name] = {"value": val, "impact": round(impact, 4)}

            if feat_name == 'rarity_score' and val > 0.7:
                text_reasons.append(f"Whale/rare transaction profile (rarity {val:.2f})")
            elif feat_name == 'fee' and val > 0.01:
                text_reasons.append(f"Unusually high transaction fee ({val:.4f} BTC)")
            elif feat_name == 'size' and val > 2000:
                text_reasons.append(f"Large transaction payload size ({int(val)} bytes)")
            elif feat_name == 'total_observations' and val < 4:
                text_reasons.append(f"Restricted propagation visibility ({int(val)} observations)")
            elif feat_name == 'avg_propagation_delay_ms' and val > 300:
                text_reasons.append(f"Slow network broadcast latency ({val:.1f}ms)")
            elif feat_name == 'ratio_fee_size' and val > 0.00001:
                text_reasons.append("High fee-to-size density")

        if not text_reasons:
            text_reasons.append("Standard transaction propagation and fee baseline")

        reasons_list.append("; ".join(text_reasons[:3]))
        shap_json_list.append(json.dumps(top_factors))

    df['risk_score'] = risk_scores
    df['confidence'] = confidence_scores
    df['explanation'] = reasons_list
    df['shap_factors'] = shap_json_list

    cur = conn.cursor()
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        DROP TABLE IF EXISTS analytics.transaction_risk_scores CASCADE;
        CREATE TABLE analytics.transaction_risk_scores (
            batch_id INTEGER NOT NULL,
            txid VARCHAR(64) NOT NULL,
            risk_score NUMERIC(6, 2) NOT NULL,
            confidence NUMERIC(5, 3) NOT NULL,
            risk_level VARCHAR(20) NOT NULL,
            explanation TEXT NOT NULL,
            shap_factors JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (batch_id, txid)
        );
    """)
    conn.commit()

    insert_sql = """
        INSERT INTO analytics.transaction_risk_scores (
            batch_id, txid, risk_score, confidence, risk_level, explanation, shap_factors
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb);
    """
    for _, row in df.iterrows():
        r = row['risk_score']
        level = "CRITICAL" if r >= 80 else ("HIGH" if r >= 65 else ("MEDIUM" if r >= 40 else "LOW"))
        cur.execute(insert_sql, (
            BATCH_ID,
            row['txid'],
            r,
            row['confidence'],
            level,
            row['explanation'],
            row['shap_factors']
        ))
    conn.commit()

    print(f"Transaction Risk Scoring Complete:")
    print(f"  CRITICAL (>=80): {(df['risk_score'] >= 80).sum()}")
    print(f"  HIGH (65-79):    {((df['risk_score'] >= 65) & (df['risk_score'] < 80)).sum()}")
    print(f"  MEDIUM (40-64):  {((df['risk_score'] >= 40) & (df['risk_score'] < 65)).sum()}")
    print(f"  LOW (<40):       {(df['risk_score'] < 40).sum()}\n")

    cur.close()
    conn.close()
    print(">>> STEP 5 RISK SCORING & SHAP EXPLANATIONS COMPLETED! <<<")


if __name__ == "__main__":
    score_and_explain_nodes()
    score_and_explain_transactions()
