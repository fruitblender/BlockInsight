"""
Step 6: Alert Generation & Evidence Linkage (SRS §27)
Generates structured, ranked, traceable alert objects for high-risk entities (risk_score >= 65).
Links supporting evidence (observations, peer connections, SHAP explanations).
Persists to analytics.alerts table in PostgreSQL.
"""
import sys
import os
import json
import uuid
from pathlib import Path
import pandas as pd

# Add ingestion directory to path for database connection
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1
ALERT_THRESHOLD = 65.0  # High or Critical risk


def generate_alerts():
    print("============================================================")
    print(f"   STEP 6: ALERT GENERATION (THRESHOLD >= {ALERT_THRESHOLD})")
    print("============================================================\n")

    conn = get_connection()
    cur = conn.cursor()

    # Create alerts table
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        DROP TABLE IF EXISTS analytics.alerts CASCADE;
        CREATE TABLE analytics.alerts (
            alert_id VARCHAR(64) NOT NULL PRIMARY KEY,
            batch_id INTEGER NOT NULL,
            entity_type VARCHAR(20) NOT NULL, -- 'NODE' or 'TRANSACTION'
            entity_id VARCHAR(64) NOT NULL,
            risk_score NUMERIC(6, 2) NOT NULL,
            confidence NUMERIC(5, 3) NOT NULL,
            severity VARCHAR(20) NOT NULL, -- 'CRITICAL', 'HIGH'
            title VARCHAR(255) NOT NULL,
            description TEXT NOT NULL,
            evidence JSONB NOT NULL,
            status VARCHAR(30) NOT NULL DEFAULT 'OPEN', -- 'OPEN', 'INVESTIGATING', 'RESOLVED'
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_alerts_risk ON analytics.alerts (risk_score DESC, confidence DESC);
        CREATE INDEX IF NOT EXISTS idx_alerts_entity ON analytics.alerts (entity_type, entity_id);
    """)
    conn.commit()

    # 1. Fetch Node Alerts
    node_query = """
        SELECT 
            nrs.node_id,
            nrs.risk_score,
            nrs.confidence,
            nrs.risk_level,
            nrs.explanation,
            nrs.shap_factors,
            nf.ip,
            nf.country,
            nf.asn,
            nf.node_type,
            nf.peer_degree,
            nf.total_connections,
            nf.observations_as_observer,
            nf.propagations_as_peer,
            nf.avg_observer_delay_ms
        FROM analytics.node_risk_scores nrs
        JOIN analytics.node_features nf ON nrs.node_id = nf.node_id AND nrs.batch_id = nf.batch_id
        WHERE nrs.batch_id = %s AND nrs.risk_score >= %s
        ORDER BY nrs.risk_score DESC;
    """
    df_node_alerts = pd.read_sql(node_query, conn, params=(BATCH_ID, ALERT_THRESHOLD))

    # 2. Fetch Transaction Alerts
    tx_query = """
        SELECT 
            trs.txid,
            trs.risk_score,
            trs.confidence,
            trs.risk_level,
            trs.explanation,
            trs.shap_factors,
            tf.fee,
            tf.size,
            tf.rarity_score,
            tf.total_observations,
            tf.unique_observers,
            tf.unique_peers,
            tf.avg_propagation_delay_ms
        FROM analytics.transaction_risk_scores trs
        JOIN analytics.transaction_features tf ON trs.txid = tf.txid AND trs.batch_id = tf.batch_id
        WHERE trs.batch_id = %s AND trs.risk_score >= %s
        ORDER BY trs.risk_score DESC;
    """
    df_tx_alerts = pd.read_sql(tx_query, conn, params=(BATCH_ID, ALERT_THRESHOLD))

    insert_alert_sql = """
        INSERT INTO analytics.alerts (
            alert_id, batch_id, entity_type, entity_id, risk_score, confidence,
            severity, title, description, evidence, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'OPEN');
    """

    alert_count = 0

    # Insert Node Alerts
    for _, row in df_node_alerts.iterrows():
        alert_id = f"ALT-NODE-{BATCH_ID}-{row['node_id']}"
        title = f"{row['risk_level']} Risk Network Node: {row['node_id']} ({row['ip']})"
        desc = f"P2P node exhibits anomalous propagation behavior. {row['explanation']}"
        
        evidence = {
            "node_id": row['node_id'],
            "ip": row['ip'],
            "country": row['country'],
            "asn": row['asn'],
            "node_type": row['node_type'],
            "peer_degree": int(row['peer_degree']),
            "total_connections": int(row['total_connections']),
            "observations_as_observer": int(row['observations_as_observer']),
            "propagations_as_peer": int(row['propagations_as_peer']),
            "avg_observer_delay_ms": float(row['avg_observer_delay_ms']),
            "shap_factors": row['shap_factors'] if isinstance(row['shap_factors'], dict) else json.loads(row['shap_factors'] or '{}')
        }

        cur.execute(insert_alert_sql, (
            alert_id,
            BATCH_ID,
            "NODE",
            row['node_id'],
            float(row['risk_score']),
            float(row['confidence']),
            row['risk_level'],
            title,
            desc,
            json.dumps(evidence)
        ))
        alert_count += 1

    # Insert Transaction Alerts
    for _, row in df_tx_alerts.iterrows():
        short_txid = row['txid'][:12] + "..." + row['txid'][-8:]
        alert_id = f"ALT-TX-{BATCH_ID}-{row['txid'][:16]}"
        title = f"{row['risk_level']} Risk Transaction: {short_txid}"
        desc = f"Transaction flagged with anomalous propagation or payload signature. {row['explanation']}"

        evidence = {
            "txid": row['txid'],
            "fee": float(row['fee']),
            "size": int(row['size']),
            "rarity_score": float(row['rarity_score']),
            "total_observations": int(row['total_observations']),
            "unique_observers": int(row['unique_observers']),
            "unique_peers": int(row['unique_peers']),
            "avg_propagation_delay_ms": float(row['avg_propagation_delay_ms']),
            "shap_factors": row['shap_factors'] if isinstance(row['shap_factors'], dict) else json.loads(row['shap_factors'] or '{}')
        }

        cur.execute(insert_alert_sql, (
            alert_id,
            BATCH_ID,
            "TRANSACTION",
            row['txid'],
            float(row['risk_score']),
            float(row['confidence']),
            row['risk_level'],
            title,
            desc,
            json.dumps(evidence)
        ))
        alert_count += 1

    conn.commit()

    print(f"Generated {alert_count} Alerts:")
    print(f"  Node alerts:        {len(df_node_alerts)}")
    print(f"  Transaction alerts: {len(df_tx_alerts)}")

    cur.close()
    conn.close()
    print(">>> STEP 6 ALERT GENERATION COMPLETED SUCCESSFULLY! <<<")


if __name__ == "__main__":
    generate_alerts()
