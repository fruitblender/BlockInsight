"""
ChainWatch - Step 1: Feature Engineering

Builds numeric feature tables for:

    1. Network Nodes
       -> analytics.node_features

    2. Transactions
       -> analytics.transaction_features

Important schema note:
The current core tables DO NOT contain batch_id.

Verified core tables:
    core.nodes
    core.peer_connections
    core.transaction_observations
    core.transactions

Therefore batch_id is used only with analytics summary tables
that were created specifically with batch information.
"""

import sys
from pathlib import Path

# ---------------------------------------------------------------------
# Allow importing database.py from ingestion/
# ---------------------------------------------------------------------

sys.path.append(
    str(Path(__file__).resolve().parents[1] / "ingestion")
)

from database import get_connection


BATCH_ID = 1


# =====================================================================
# NODE FEATURES
# =====================================================================

def build_node_features():

    print("============================================================")
    print("   STEP 1: FEATURE ENGINEERING - NODE FEATURES")
    print("============================================================\n")

    conn = get_connection()
    cur = conn.cursor()

    # -----------------------------------------------------------------
    # 1. Create analytics schema and node feature table
    # -----------------------------------------------------------------

    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;

        DROP TABLE IF EXISTS analytics.node_features CASCADE;

        CREATE TABLE analytics.node_features (

            batch_id INTEGER NOT NULL,

            node_id VARCHAR(64) NOT NULL,

            ip VARCHAR(45) NOT NULL,

            country VARCHAR(10),

            asn VARCHAR(20),

            node_type VARCHAR(30),

            peer_degree INTEGER NOT NULL DEFAULT 0,

            outgoing_connections INTEGER NOT NULL DEFAULT 0,

            incoming_connections INTEGER NOT NULL DEFAULT 0,

            total_connections INTEGER NOT NULL DEFAULT 0,

            avg_connection_duration_ms
                NUMERIC(12, 3) NOT NULL DEFAULT 0.0,

            observations_as_observer
                INTEGER NOT NULL DEFAULT 0,

            transactions_observed
                INTEGER NOT NULL DEFAULT 0,

            unique_peers_as_observer
                INTEGER NOT NULL DEFAULT 0,

            avg_observer_delay_ms
                NUMERIC(12, 3) NOT NULL DEFAULT 0.0,

            propagations_as_peer
                INTEGER NOT NULL DEFAULT 0,

            transactions_propagated
                INTEGER NOT NULL DEFAULT 0,

            unique_observers_as_peer
                INTEGER NOT NULL DEFAULT 0,

            propagation_observation_ratio
                NUMERIC(10, 4) NOT NULL DEFAULT 0.0,

            ip_diversity
                INTEGER NOT NULL DEFAULT 0,

            degree_centrality
                DOUBLE PRECISION NOT NULL DEFAULT 0.0,

            betweenness_centrality
                DOUBLE PRECISION NOT NULL DEFAULT 0.0,

            closeness_centrality
                DOUBLE PRECISION NOT NULL DEFAULT 0.0,

            community_id
                INTEGER NOT NULL DEFAULT 0,

            created_at TIMESTAMPTZ DEFAULT NOW(),

            PRIMARY KEY (batch_id, node_id)
        );
    """)

    conn.commit()

    # -----------------------------------------------------------------
    # 2. Calculate IP diversity
    #
    # A node's IP diversity is the number of distinct IP addresses
    # belonging to nodes it interacts with through peer connections
    # or transaction observations.
    #
    # No batch_id is used here because the verified core tables
    # do not contain batch_id.
    # -----------------------------------------------------------------

    populate_sql = """

        WITH ip_diversity AS (

            SELECT
                node_id,
                COUNT(DISTINCT peer_ip) AS ip_diversity

            FROM (

                -- Peer connection: source -> destination
                SELECT
                    pc.src_node_id AS node_id,
                    pc.dst_ip AS peer_ip
                FROM core.peer_connections pc
                WHERE pc.src_node_id IS NOT NULL
                  AND pc.dst_ip IS NOT NULL

                UNION

                -- Peer connection: destination -> source
                SELECT
                    pc.dst_node_id AS node_id,
                    pc.src_ip AS peer_ip
                FROM core.peer_connections pc
                WHERE pc.dst_node_id IS NOT NULL
                  AND pc.src_ip IS NOT NULL

                UNION

                -- Observation: observer -> peer
                SELECT
                    o.observer_id AS node_id,
                    peer.ip AS peer_ip
                FROM core.transaction_observations o
                JOIN core.nodes peer
                    ON o.peer_id = peer.node_id
                WHERE o.observer_id IS NOT NULL
                  AND peer.ip IS NOT NULL

            ) interactions

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

            n.ip,

            n.country,

            n.asn,

            n.node_type,

            COALESCE(nbs.peer_degree, 0),

            COALESCE(nbs.outgoing_connections, 0),

            COALESCE(nbs.incoming_connections, 0),

            COALESCE(nbs.total_connections, 0),

            COALESCE(
                nbs.avg_connection_duration_ms,
                0.0
            ),

            COALESCE(
                nbs.observations_as_observer,
                0
            ),

            COALESCE(
                nbs.transactions_observed,
                0
            ),

            COALESCE(
                nbs.unique_peers_as_observer,
                0
            ),

            COALESCE(
                nbs.avg_observer_delay_ms,
                0.0
            ),

            COALESCE(
                nbs.propagations_as_peer,
                0
            ),

            COALESCE(
                nbs.transactions_propagated,
                0
            ),

            COALESCE(
                nbs.unique_observers_as_peer,
                0
            ),

            COALESCE(
                nbs.propagation_to_observation_ratio,
                0.0
            ),

            COALESCE(
                div.ip_diversity,
                0
            )

        FROM analytics.node_behavior_summary nbs

        JOIN core.nodes n
            ON n.node_id = nbs.node_id

        LEFT JOIN ip_diversity div
            ON div.node_id = nbs.node_id

        WHERE nbs.batch_id = %(batch_id)s;

    """

    cur.execute(
        populate_sql,
        {"batch_id": BATCH_ID}
    )

    conn.commit()

    # -----------------------------------------------------------------
    # 3. Verify node feature count
    # -----------------------------------------------------------------

    cur.execute("""
        SELECT COUNT(*)
        FROM analytics.node_features
        WHERE batch_id = %s;
    """, (BATCH_ID,))

    node_count = cur.fetchone()[0]

    print(
        f"  analytics.node_features row count: "
        f"{node_count} (Expected: 150)"
    )

    # -----------------------------------------------------------------
    # 4. Basic feature verification
    # -----------------------------------------------------------------

    cur.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT node_id),
            COUNT(DISTINCT ip)
        FROM analytics.node_features
        WHERE batch_id = %s;
    """, (BATCH_ID,))

    total_rows, unique_nodes, unique_ips = cur.fetchone()

    print(
        f"  Unique node IDs                  : {unique_nodes}"
    )

    print(
        f"  Unique IP addresses              : {unique_ips}"
    )

    cur.close()
    conn.close()

    return node_count


# =====================================================================
# TRANSACTION FEATURES
# =====================================================================

def build_transaction_features():

    print("\n============================================================")
    print("   STEP 1: FEATURE ENGINEERING - TRANSACTION FEATURES")
    print("============================================================\n")

    conn = get_connection()
    cur = conn.cursor()

    # -----------------------------------------------------------------
    # 1. Create transaction feature table
    # -----------------------------------------------------------------

    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;

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

            min_propagation_delay_ms
                NUMERIC(12, 3) NOT NULL DEFAULT 0.0,

            max_propagation_delay_ms
                NUMERIC(12, 3) NOT NULL DEFAULT 0.0,

            avg_propagation_delay_ms
                NUMERIC(12, 3) NOT NULL DEFAULT 0.0,

            median_propagation_delay_ms
                NUMERIC(12, 3) NOT NULL DEFAULT 0.0,

            observed_propagation_span_ms
                NUMERIC(12, 3) NOT NULL DEFAULT 0.0,

            creation_to_first_observation_ms
                NUMERIC(12, 3) NOT NULL DEFAULT 0.0,

            inv_ratio
                NUMERIC(6, 4) NOT NULL DEFAULT 0.0,

            tx_ratio
                NUMERIC(6, 4) NOT NULL DEFAULT 0.0,

            created_at TIMESTAMPTZ DEFAULT NOW(),

            PRIMARY KEY (batch_id, txid)
        );
    """)

    conn.commit()

    # -----------------------------------------------------------------
    # 2. Populate transaction features
    #
    # IMPORTANT:
    #
    # core.transactions DOES NOT contain batch_id.
    #
    # Therefore the transaction summary's batch_id is used for the
    # feature batch, but transactions are joined only on txid.
    # -----------------------------------------------------------------

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

            COALESCE(t.fee, 0.0),

            COALESCE(t.size, 0),

            COALESCE(
                t.ratio_fee_size,
                CASE
                    WHEN t.size > 0
                    THEN t.fee / t.size
                    ELSE 0
                END
            ),

            COALESCE(t.rarity_score, 0.0),

            COALESCE(t.input_count, 0),

            COALESCE(t.output_count, 0),

            COALESCE(tps.total_observations, 0),

            COALESCE(tps.unique_observers, 0),

            COALESCE(tps.unique_peers, 0),

            COALESCE(
                tps.unique_observer_countries,
                0
            ),

            COALESCE(
                tps.unique_peer_countries,
                0
            ),

            COALESCE(
                tps.min_propagation_delay_ms,
                0.0
            ),

            COALESCE(
                tps.max_propagation_delay_ms,
                0.0
            ),

            COALESCE(
                tps.avg_propagation_delay_ms,
                0.0
            ),

            COALESCE(
                tps.median_propagation_delay_ms,
                0.0
            ),

            COALESCE(
                tps.observed_propagation_span_ms,
                0.0
            ),

            COALESCE(
                tps.creation_to_first_observation_ms,
                0.0
            ),

            COALESCE(
                tps.inv_ratio,
                0.0
            ),

            COALESCE(
                tps.tx_ratio,
                0.0
            )

        FROM analytics.transaction_propagation_summary tps

        JOIN core.transactions t
            ON t.txid = tps.txid

        WHERE tps.batch_id = %(batch_id)s;

    """

    cur.execute(
        populate_sql,
        {"batch_id": BATCH_ID}
    )

    conn.commit()

    # -----------------------------------------------------------------
    # 3. Verify transaction feature count
    # -----------------------------------------------------------------

    cur.execute("""
        SELECT COUNT(*)
        FROM analytics.transaction_features
        WHERE batch_id = %s;
    """, (BATCH_ID,))

    tx_count = cur.fetchone()[0]

    print(
        f"  analytics.transaction_features row count: "
        f"{tx_count} (Expected: 498)"
    )

    # -----------------------------------------------------------------
    # 4. Basic verification
    # -----------------------------------------------------------------

    cur.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT txid)
        FROM analytics.transaction_features
        WHERE batch_id = %s;
    """, (BATCH_ID,))

    total_rows, unique_txids = cur.fetchone()

    print(
        f"  Unique transaction IDs           : {unique_txids}"
    )

    cur.close()
    conn.close()

    return tx_count


# =====================================================================
# MAIN
# =====================================================================

if __name__ == "__main__":

    node_count = build_node_features()

    tx_count = build_transaction_features()

    print("\n============================================================")
    print("                    STEP 1 SUMMARY")
    print("============================================================")

    print(
        f"Node features created        : {node_count} rows"
    )

    print(
        f"Transaction features created : {tx_count} rows"
    )

    print()

    if node_count == 150 and tx_count == 498:

        print(
            ">>> STEP 1 FEATURE ENGINEERING "
            "PASSED SUCCESSFULLY! <<<"
        )

    else:

        print(
            ">>> STEP 1 VERIFICATION FAILED! <<<"
        )

        if node_count != 150:
            print(
                f"    Expected 150 node features, "
                f"got {node_count}"
            )

        if tx_count != 498:
            print(
                f"    Expected 498 transaction features, "
                f"got {tx_count}"
            )