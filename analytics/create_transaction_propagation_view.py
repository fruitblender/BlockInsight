"""
Phase 5 - Step 2: Create analytics.transaction_propagation

Grain:
    ONE ROW PER TRANSACTION OBSERVATION

Purpose:
    Enrich each network observation with:
    - transaction metadata
    - observer node metadata
    - propagating peer metadata
    - propagation timing

IMPORTANT:
    core.transaction_observations does NOT contain batch_id.
    core.nodes does NOT contain batch_id.

    Therefore BATCH_ID is represented as a constant for the
    current dataset, while joins use the actual keys:
        observation.txid       -> transactions.txid
        observation.observer_id -> nodes.node_id
        observation.peer_id      -> nodes.node_id
"""

import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parents[1] / "ingestion")
)

from database import get_connection


BATCH_ID = 1


def create_and_verify_view():

    conn = get_connection()
    cur = conn.cursor()

    print("=" * 60)
    print("   PHASE 5 - STEP 2: CREATE transaction_propagation")
    print("=" * 60)
    print()

    # ------------------------------------------------------------
    # 1. Create analytics schema
    # ------------------------------------------------------------

    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
    """)

    conn.commit()

    print("  Created/Verified schema 'analytics'.")
    print()

    # ------------------------------------------------------------
    # 2. Create view
    # ------------------------------------------------------------

    view_sql = f"""
    CREATE OR REPLACE VIEW analytics.transaction_propagation AS

    SELECT
        {BATCH_ID}::INTEGER AS batch_id,

        o.txid,
        o.observation_id,
        o.sequence_number,

        -- --------------------------------------------------------
        -- Transaction metadata
        -- --------------------------------------------------------

        t.timestamp AS transaction_timestamp,

        o.timestamp AS observation_timestamp,

        o.propagation_delay_ms,

        ROUND(
            (
                EXTRACT(
                    EPOCH FROM (o.timestamp - t.timestamp)
                ) * 1000
            )::numeric,
            3
        ) AS creation_to_observation_ms,

        t.fee,
        t.size,
        t.rarity_score,
        t.tema,

        -- --------------------------------------------------------
        -- Observation metadata
        -- --------------------------------------------------------

        o.message_type,
        o.direction AS observation_direction,

        -- --------------------------------------------------------
        -- Observer node
        -- --------------------------------------------------------

        o.observer_id,

        obs.ip AS observer_ip,
        obs.country AS observer_country,
        obs.asn AS observer_asn,
        obs.node_type AS observer_node_type,

        -- --------------------------------------------------------
        -- Propagating peer
        -- --------------------------------------------------------

        o.peer_id,

        peer.ip AS peer_ip,
        peer.country AS peer_country,
        peer.asn AS peer_asn,
        peer.node_type AS peer_node_type,

        -- --------------------------------------------------------
        -- Wire-level network fields
        -- --------------------------------------------------------

        o.src_ip AS wire_src_ip,
        o.src_port AS wire_src_port,
        o.dst_ip AS wire_dst_ip,
        o.dst_port AS wire_dst_port,

        -- Existing connection reference
        o.connection_id

    FROM core.transaction_observations o

    JOIN core.transactions t
        ON o.txid = t.txid

    JOIN core.nodes obs
        ON o.observer_id = obs.node_id

    JOIN core.nodes peer
        ON o.peer_id = peer.node_id;
    """

    cur.execute(view_sql)
    conn.commit()

    print("  Created/Updated view 'analytics.transaction_propagation'.")
    print()

    # ------------------------------------------------------------
    # 3. Verification
    # ------------------------------------------------------------

    cur.execute("""
        SELECT COUNT(*)
        FROM analytics.transaction_propagation
        WHERE batch_id = %s;
    """, (BATCH_ID,))

    view_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM core.transaction_observations;
    """)

    core_count = cur.fetchone()[0]

    print("--- Verification Check ---")

    print(
        f"  core.transaction_observations count : {core_count}"
    )

    print(
        f"  analytics.transaction_propagation count: {view_count}"
    )

    if view_count == core_count:
        print(
            "  Status: PASSED "
            "(100% of observations represented in analytics view)"
        )
    else:
        print(
            f"  Status: MISMATCH "
            f"(view={view_count}, core={core_count})"
        )

    print()

    # ------------------------------------------------------------
    # 4. Verify transaction coverage
    # ------------------------------------------------------------

    cur.execute("""
        SELECT COUNT(DISTINCT txid)
        FROM analytics.transaction_propagation
        WHERE batch_id = %s;
    """, (BATCH_ID,))

    propagated_tx_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(DISTINCT txid)
        FROM core.transaction_observations;
    """)

    observed_tx_count = cur.fetchone()[0]

    print("--- Transaction Coverage ---")

    print(
        f"  Distinct observed transactions : {observed_tx_count}"
    )

    print(
        f"  Transactions represented       : {propagated_tx_count}"
    )

    if propagated_tx_count == observed_tx_count:
        print("  Status: PASSED")
    else:
        print("  Status: MISMATCH")

    print()

    # ------------------------------------------------------------
    # 5. Sample records
    # ------------------------------------------------------------

    print("--- Sample Records ---")

    cur.execute("""
        SELECT
            txid,
            sequence_number,
            observation_timestamp,
            propagation_delay_ms,
            creation_to_observation_ms,
            observer_id,
            observer_country,
            peer_id,
            peer_country,
            peer_node_type,
            message_type
        FROM analytics.transaction_propagation
        WHERE batch_id = %s
        ORDER BY txid, sequence_number
        LIMIT 5;
    """, (BATCH_ID,))

    rows = cur.fetchall()

    for row in rows:

        (
            txid,
            sequence_number,
            observation_timestamp,
            propagation_delay,
            creation_diff,
            observer_id,
            observer_country,
            peer_id,
            peer_country,
            peer_node_type,
            message_type
        ) = row

        print(
            f"  TX: {txid[:12]}... | "
            f"Seq: #{sequence_number:2d} | "
            f"Delay: {propagation_delay}ms | "
            f"Creation Diff: {creation_diff}ms"
        )

        print(
            f"    Observer: {observer_id} "
            f"({observer_country}) <- "
            f"Peer: {peer_id} "
            f"({peer_country}, {peer_node_type}) | "
            f"Msg: {message_type}"
        )

        print()

    print("=" * 60)
    print("             STEP 2 CREATION & AUDIT COMPLETE")
    print("=" * 60)

    cur.close()
    conn.close()


if __name__ == "__main__":
    create_and_verify_view()