"""
Step 1: Feature Engineering
Builds numeric feature tables for Nodes (150 rows) and Transactions (498 rows).
Persists to analytics.node_features and analytics.transaction_features in PostgreSQL.
"""
import sys
import os
from pathlib import Path

# Add ingestion directory to path for database connection
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1


def build_node_features():
    print("============================================================")
    print("   STEP 1: FEATURE ENGINEERING - NODE FEATURES")
    print("============================================================\n")

    conn = get_connection()
    cur = conn.cursor()

    # Create table for node features
    create_table_sql = """
    CREATE SCHEMA IF NOT EXISTS analytics;

    DROP TABLE IF EXISTS analytics.node_features CASCADE;
    CREATE TABLE analytics.node_features (
        batch_id INTEGER NOT NULL,
        node_id VARCHAR(64) NOT NULL,
        ip VARCHAR(45) NOT NULL,
        country VARCHAR(10),
        asn VARCHAR(20),
        node_type VARCHAR(20),
        peer_degree INTEGER NOT NULL DEFAULT 0,
        outgoing_connections INTEGER NOT NULL DEFAULT 0,
        incoming_connections INTEGER NOT NULL DEFAULT 0,
        total_connections INTEGER NOT NULL DEFAULT 0,
        avg_connection_duration_ms NUMERIC(12, 3) NOT NULL DEFAULT 0.0,
        observations_as_observer INTEGER NOT NULL DEFAULT 0,
        transactions_observed INTEGER NOT NULL DEFAULT 0,
        unique_peers_as_observer INTEGER NOT NULL DEFAULT 0,
        avg_observer_delay_ms NUMERIC(12, 3) NOT NULL DEFAULT 0.0,
        propagations_as_peer INTEGER NOT NULL DEFAULT 0,
        transactions_propagated INTEGER NOT NULL DEFAULT 0,
        unique_observers_as_peer INTEGER NOT NULL DEFAULT 0,
        propagation_observation_ratio NUMERIC(10, 4) NOT NULL DEFAULT 0.0,
        ip_diversity INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (batch_id, node_id)
    );
    """
    cur.execute(create_table_sql)
    conn.commit()

    # Query node behavior summary combined with IP diversity calculation
    populate_sql = """
    WITH ip_div AS (
        -- Calculate IP diversity: count of distinct peer IPs encountered
        SELECT 
            node_id, 
            COUNT(DISTINCT peer_ip) AS ip_diversity
        FROM (
            SELECT src_node_id AS node_id, dst_ip AS peer_ip FROM core.peer_connections WHERE batch_id = %(batch_id)s
            UNION ALL
            SELECT dst_node_id AS node_id, src_ip AS peer_ip FROM core.peer_connections WHERE batch_id = %(batch_id)s
            UNION ALL
            SELECT observer_id AS node_id, peer_ip FROM analytics.transaction_propagation WHERE batch_id = %(batch_id)s AND peer_ip IS NOT NULL
        ) all_interactions
        GROUP BY node_id
    )
    INSERT INTO analytics.node_features (
        batch_id,
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
        ip_diversity
    )
    SELECT
        nbs.batch_id,
        nbs.node_id,
        nbs.ip,
        nbs.country,
        nbs.asn,
        nbs.node_type,
        nbs.peer_degree,
        nbs.outgoing_connections,
        nbs.incoming_connections,
        nbs.total_connections,
        COALESCE(nbs.avg_connection_duration_ms, 0.0),
        nbs.observations_as_observer,
        nbs.transactions_observed,
        nbs.unique_peers_as_observer,
        COALESCE(nbs.avg_observer_delay_ms, 0.0),
        nbs.propagations_as_peer,
        nbs.transactions_propagated,
        nbs.unique_observers_as_peer,
        COALESCE(nbs.propagation_to_observation_ratio, 0.0),
        COALESCE(div.ip_diversity, 0)
    FROM analytics.node_behavior_summary nbs
    LEFT JOIN ip_div div ON nbs.node_id = div.node_id
    WHERE nbs.batch_id = %(batch_id)s;
    """
    cur.execute(populate_sql, {"batch_id": BATCH_ID})
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM analytics.node_features WHERE batch_id = %s;", (BATCH_ID,))
    node_feat_count = cur.fetchone()[0]
    print(f"  analytics.node_features row count: {node_feat_count} (Expected: 150)")

    cur.close()
    conn.close()
    return node_feat_count


def build_transaction_features():
    print("\n============================================================")
    print("   STEP 1: FEATURE ENGINEERING - TRANSACTION FEATURES")
    print("============================================================\n")

    conn = get_connection()
    cur = conn.cursor()

    create_table_sql = """
    DROP TABLE IF EXISTS analytics.transaction_features CASCADE;
    CREATE TABLE analytics.transaction_features (
        batch_id INTEGER NOT NULL,
        txid VARCHAR(64) NOT NULL,
        fee NUMERIC(16, 8) NOT NULL,
        size INTEGER NOT NULL,
        ratio_fee_size NUMERIC(16, 8) NOT NULL,
        rarity_score NUMERIC(10, 4) NOT NULL,
        inputs INTEGER NOT NULL,
        outputs INTEGER NOT NULL,
        total_observations INTEGER NOT NULL,
        unique_observers INTEGER NOT NULL,
        unique_peers INTEGER NOT NULL,
        unique_observer_countries INTEGER NOT NULL,
        unique_peer_countries INTEGER NOT NULL,
        min_propagation_delay_ms NUMERIC(12, 3) NOT NULL DEFAULT 0.0,
        max_propagation_delay_ms NUMERIC(12, 3) NOT NULL DEFAULT 0.0,
        avg_propagation_delay_ms NUMERIC(12, 3) NOT NULL DEFAULT 0.0,
        median_propagation_delay_ms NUMERIC(12, 3) NOT NULL DEFAULT 0.0,
        observed_propagation_span_ms NUMERIC(12, 3) NOT NULL DEFAULT 0.0,
        creation_to_first_observation_ms NUMERIC(12, 3) NOT NULL DEFAULT 0.0,
        inv_ratio NUMERIC(6, 4) NOT NULL DEFAULT 0.0,
        tx_ratio NUMERIC(6, 4) NOT NULL DEFAULT 0.0,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (batch_id, txid)
    );
    """
    cur.execute(create_table_sql)
    conn.commit()

    populate_sql = """
    INSERT INTO analytics.transaction_features (
        batch_id,
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
    )
    SELECT
        tps.batch_id,
        tps.txid,
        t.fee,
        t.size,
        COALESCE(t.ratio_fee_size, CASE WHEN t.size > 0 THEN t.fee / t.size ELSE 0 END),
        t.rarity_score,
        t.inputs,
        t.outputs,
        tps.total_observations,
        tps.unique_observers,
        tps.unique_peers,
        tps.unique_observer_countries,
        tps.unique_peer_countries,
        COALESCE(tps.min_propagation_delay_ms, 0.0),
        COALESCE(tps.max_propagation_delay_ms, 0.0),
        COALESCE(tps.avg_propagation_delay_ms, 0.0),
        COALESCE(tps.median_propagation_delay_ms, 0.0),
        COALESCE(tps.observed_propagation_span_ms, 0.0),
        COALESCE(tps.creation_to_first_observation_ms, 0.0),
        COALESCE(tps.inv_ratio, 0.0),
        COALESCE(tps.tx_ratio, 0.0)
    FROM analytics.transaction_propagation_summary tps
    JOIN core.transactions t ON tps.txid = t.txid AND tps.batch_id = t.batch_id
    WHERE tps.batch_id = %(batch_id)s;
    """
    cur.execute(populate_sql, {"batch_id": BATCH_ID})
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM analytics.transaction_features WHERE batch_id = %s;", (BATCH_ID,))
    tx_feat_count = cur.fetchone()[0]
    print(f"  analytics.transaction_features row count: {tx_feat_count} (Expected: 498)")

    cur.close()
    conn.close()
    return tx_feat_count


if __name__ == "__main__":
    node_count = build_node_features()
    tx_count = build_transaction_features()
    print("\n--- Summary ---")
    print(f"Node features created: {node_count} rows")
    print(f"Transaction features created: {tx_count} rows")
    if node_count == 150 and tx_count == 498:
        print(">>> STEP 1 FEATURE ENGINEERING PASSED SUCCESSFULLY! <<<")
    else:
        print(">>> STEP 1 VERIFICATION FAILED! <<<")
