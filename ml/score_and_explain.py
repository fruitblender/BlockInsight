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

ANOMALY_WEIGHT = 0.70
DBSCAN_NOISE_WEIGHT = 0.30
LOW_THRESHOLD = 0.30
MEDIUM_THRESHOLD = 0.60
HIGH_THRESHOLD = 0.80


def _table_columns(cur, table_name):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'analytics' AND table_name = %s;
        """,
        (table_name,),
    )
    return {row[0] for row in cur.fetchall()}


def risk_level_for_score(score):
    if score >= HIGH_THRESHOLD:
        return "CRITICAL"
    if score >= MEDIUM_THRESHOLD:
        return "HIGH"
    if score >= LOW_THRESHOLD:
        return "MEDIUM"
    return "LOW"


def _load_node_ml_results(conn):
    with conn.cursor() as cur:
        anomaly_columns = _table_columns(cur, "node_anomalies")
        cluster_columns = _table_columns(cur, "node_clusters")

    anomaly_required = {"batch_id", "node_id", "anomaly_score", "is_anomaly"}
    cluster_required = {"batch_id", "node_id", "cluster_id", "is_noise"}
    missing_anomaly_columns = sorted(anomaly_required - anomaly_columns)
    missing_cluster_columns = sorted(cluster_required - cluster_columns)
    if missing_anomaly_columns or missing_cluster_columns:
        problems = []
        if missing_anomaly_columns:
            problems.append(
                "node_anomalies missing " + ", ".join(missing_anomaly_columns)
            )
        if missing_cluster_columns:
            problems.append(
                "node_clusters missing " + ", ".join(missing_cluster_columns)
            )
        raise ValueError("; ".join(problems))

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

    if anomalies.empty or clusters.empty:
        raise ValueError(
            f"Both node_anomalies and node_clusters must contain rows for batch {BATCH_ID}."
        )

    if anomalies["node_id"].duplicated().any():
        raise ValueError("Duplicate node IDs found in analytics.node_anomalies.")
    if clusters["node_id"].duplicated().any():
        raise ValueError("Duplicate node IDs found in analytics.node_clusters.")

    anomaly_ids = set(anomalies["node_id"])
    cluster_ids = set(clusters["node_id"])
    missing_anomaly_matches = cluster_ids - anomaly_ids
    missing_cluster_matches = anomaly_ids - cluster_ids
    matched_count = len(anomaly_ids & cluster_ids)

    print(f"Nodes in anomaly results: {len(anomalies)}")
    print(f"Nodes in DBSCAN results: {len(clusters)}")
    print(f"Matched nodes: {matched_count}")
    print(f"Missing anomaly matches: {len(missing_anomaly_matches)}")
    print(f"Missing cluster matches: {len(missing_cluster_matches)}")

    if missing_anomaly_matches or missing_cluster_matches:
        raise ValueError(
            "Node anomaly and DBSCAN entity sets do not match; risk scores were not persisted."
        )

    anomalies["anomaly_score"] = pd.to_numeric(
        anomalies["anomaly_score"], errors="coerce"
    )
    invalid_scores = anomalies["anomaly_score"].isna() | ~anomalies[
        "anomaly_score"
    ].between(0.0, 1.0)
    if invalid_scores.any():
        raise ValueError("node_anomalies contains NULL or out-of-range anomaly_score values.")

    return anomalies.merge(clusters, on="node_id", how="inner", validate="one_to_one")


def _persist_node_risk_scores(conn, scores):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS analytics;
            DROP TABLE IF EXISTS analytics.node_risk_scores CASCADE;
            CREATE TABLE analytics.node_risk_scores (
                batch_id INTEGER NOT NULL,
                node_id VARCHAR(64) NOT NULL,
                anomaly_score NUMERIC(6, 4) NOT NULL,
                is_anomaly BOOLEAN NOT NULL,
                cluster_id INTEGER NOT NULL,
                is_noise BOOLEAN NOT NULL,
                combined_risk_score NUMERIC(6, 4) NOT NULL,
                risk_level VARCHAR(20) NOT NULL,
                risk_rank INTEGER NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (batch_id, node_id)
            );
            """
        )
        cur.executemany(
            """
            INSERT INTO analytics.node_risk_scores (
                batch_id, node_id, anomaly_score, is_anomaly, cluster_id, is_noise,
                combined_risk_score, risk_level, risk_rank
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            [
                (
                    BATCH_ID,
                    row.node_id,
                    float(row.anomaly_score),
                    bool(row.is_anomaly),
                    int(row.cluster_id),
                    bool(row.is_noise),
                    float(row.combined_risk_score),
                    row.risk_level,
                    int(row.risk_rank),
                )
                for row in scores.itertuples(index=False)
            ],
        )
    conn.commit()


def score_node_risk():
    print("============================================================")
    print("   STEP 5: NODE RISK SCORING")
    print("============================================================\n")
    print(f"ANOMALY_WEIGHT: {ANOMALY_WEIGHT}")
    print(f"DBSCAN_NOISE_WEIGHT: {DBSCAN_NOISE_WEIGHT}")
    print(f"Weight sum: {ANOMALY_WEIGHT + DBSCAN_NOISE_WEIGHT:.1f}\n")

    conn = get_connection()
    try:
        scores = _load_node_ml_results(conn)
        scores["dbscan_noise_score"] = scores["is_noise"].astype(float)
        scores["combined_risk_score"] = (
            ANOMALY_WEIGHT * scores["anomaly_score"]
            + DBSCAN_NOISE_WEIGHT * scores["dbscan_noise_score"]
        ).clip(0.0, 1.0)
        scores["risk_level"] = scores["combined_risk_score"].apply(risk_level_for_score)
        scores["risk_rank"] = (
            scores["combined_risk_score"]
            .rank(method="first", ascending=False)
            .astype(int)
        )

        _persist_node_risk_scores(conn, scores)
    finally:
        conn.close()

    print("\nRisk distribution:")
    for level in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
        print(f"  {level}: {(scores['risk_level'] == level).sum()}")
    print(f"Minimum combined risk: {scores['combined_risk_score'].min():.4f}")
    print(f"Maximum combined risk: {scores['combined_risk_score'].max():.4f}")
    print(f"Average combined risk: {scores['combined_risk_score'].mean():.4f}")
    print("Top 10 highest-risk nodes:")
    for row in scores.nsmallest(10, "risk_rank").itertuples(index=False):
        print(
            f"  {row.node_id} | {row.combined_risk_score:.4f} | "
            f"{row.risk_level} | anomaly={row.anomaly_score:.4f} | "
            f"is_noise={row.is_noise}"
        )


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
    score_node_risk()

    def explain_node_isolation_forest():
        print("============================================================")
        print("   STEP 5B: SHAP EXPLAINABILITY - NODES")
        print("============================================================\n")
        if not HAS_SHAP:
            raise ImportError("The 'shap' package is required but not installed.")

        conn = get_connection()
        try:
            feature_cols = joblib.load(MODELS_DIR / "node_feature_names.joblib")

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'analytics' AND table_name = 'node_features';"
                )
                available_cols = {r[0] for r in cur.fetchall()}

            missing = [f for f in feature_cols if f not in available_cols]
            if missing:
                raise ValueError(f"analytics.node_features is missing required training features: {missing}")

            imputer = joblib.load(MODELS_DIR / "node_imputer.joblib")
            scaler = joblib.load(MODELS_DIR / "node_scaler.joblib")
            clf = joblib.load(MODELS_DIR / "node_isolation_forest.joblib")

            select_cols = ", ".join(feature_cols)
            query = f"SELECT node_id, {select_cols} FROM analytics.node_features WHERE batch_id = %s;"
            df = pd.read_sql(query, conn, params=(BATCH_ID,))

            if df.empty:
                raise ValueError(f"No rows found in analytics.node_features for batch {BATCH_ID}.")

            numeric_data = df[feature_cols].apply(pd.to_numeric, errors="coerce")
            numeric_data = numeric_data.replace([np.inf, -np.inf], np.nan)

            imputed_data = imputer.transform(numeric_data)
            X_scaled = scaler.transform(imputed_data)

            if X_scaled.shape[1] != clf.n_features_in_:
                raise ValueError(f"Feature dimension mismatch: {X_scaled.shape[1]} vs expected {clf.n_features_in_}")

            print("Computing SHAP values for nodes...")
            explainer = shap.TreeExplainer(clf)
            shap_values = explainer.shap_values(X_scaled)

            explanations = []
            for idx, row in df.iterrows():
                node_id = row["node_id"]
                node_shap = shap_values[idx]
                abs_shap = np.abs(node_shap)

                ranks = pd.Series(abs_shap).rank(method="first", ascending=False).astype(int).values

                for feat_idx, feat_name in enumerate(feature_cols):
                    feat_val = float(imputed_data[idx, feat_idx])
                    s_val = float(node_shap[feat_idx])
                    abs_s_val = float(abs_shap[feat_idx])
                    rank = int(ranks[feat_idx])

                    explanations.append((
                        BATCH_ID,
                        node_id,
                        feat_name,
                        feat_val,
                        s_val,
                        abs_s_val,
                        rank
                    ))

            with conn.cursor() as cur:
                cur.execute("""
                    CREATE SCHEMA IF NOT EXISTS analytics;
                    CREATE TABLE IF NOT EXISTS analytics.node_explanations (
                        batch_id INTEGER NOT NULL,
                        node_id VARCHAR(64) NOT NULL,
                        feature_name VARCHAR(128) NOT NULL,
                        feature_value NUMERIC NOT NULL,
                        shap_value NUMERIC(20, 10) NOT NULL,
                        absolute_shap_value NUMERIC(20, 10) NOT NULL,
                        feature_rank INTEGER NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (batch_id, node_id, feature_name)
                    );
                    DELETE FROM analytics.node_explanations WHERE batch_id = %s;
                """, (BATCH_ID,))

                insert_sql = """
                    INSERT INTO analytics.node_explanations (
                        batch_id, node_id, feature_name, feature_value,
                        shap_value, absolute_shap_value, feature_rank
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                """
                cur.executemany(insert_sql, explanations)

            conn.commit()
            print(f"Persisted {len(explanations)} SHAP explanations for batch {BATCH_ID}.")

        finally:
            conn.close()

    explain_node_isolation_forest()
