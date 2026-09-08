"""
Phase 5 - Step 2: Create Schema 'analytics' and View 'analytics.transaction_propagation'
Grain: ONE ROW PER OBSERVATION
Enriches each observation with observer node, peer node, and transaction metadata.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def create_and_verify_view():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print("   PHASE 5 - STEP 2: CREATE analytics.transaction_propagation")
    print("============================================================\n")

    # 1. Create Schema
    cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
    print("  Created/Verified schema 'analytics'.")

    # 2. Create or Replace View
    view_sql = """
    CREATE OR REPLACE VIEW analytics.transaction_propagation AS
    SELECT
        o.batch_id,
        o.txid,
        o.observation_id,
        o.sequence_number,
        t.timestamp AS transaction_timestamp,
        o.timestamp AS observation_timestamp,
        o.propagation_delay_ms,
        ROUND((EXTRACT(EPOCH FROM (o.timestamp - t.timestamp)) * 1000)::numeric, 3) AS creation_to_observation_ms,
        o.message_type,
        o.direction AS observation_direction,

        -- Observer Node Attributes
        o.observer_id,
        obs.ip AS observer_ip,
        obs.country AS observer_country,
        obs.asn AS observer_asn,
        obs.node_type AS observer_node_type,

        -- Propagating Peer Node Attributes
        o.peer_id,
        peer.ip AS peer_ip,
        peer.country AS peer_country,
        peer.asn AS peer_asn,
        peer.node_type AS peer_node_type,

        -- Network observation endpoint fields (as recorded on wire)
        o.src_ip AS wire_src_ip,
        o.src_port AS wire_src_port,
        o.dst_ip AS wire_dst_ip,
        o.dst_port AS wire_dst_port

    FROM core.transaction_observations o
    JOIN core.transactions t
        ON o.txid = t.txid AND o.batch_id = t.batch_id
    JOIN core.nodes obs
        ON o.observer_id = obs.node_id AND o.batch_id = obs.batch_id
    JOIN core.nodes peer
        ON o.peer_id = peer.node_id AND o.batch_id = peer.batch_id;
    """

    cur.execute(view_sql)
    conn.commit()
    print("  Created/Updated view 'analytics.transaction_propagation'.\n")

    # 3. Verification: Row count comparison
    cur.execute("SELECT COUNT(*) FROM analytics.transaction_propagation WHERE batch_id = %s;", (BATCH_ID,))
    view_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM core.transaction_observations WHERE batch_id = %s;", (BATCH_ID,))
    core_count = cur.fetchone()[0]

    print("--- Verification Check ---")
    print(f"  core.transaction_observations count : {core_count}")
    print(f"  analytics.transaction_propagation count: {view_count}")
    if view_count == core_count == 3940:
        print("  Status: PASSED (100% of observations cleanly represented in view)")
    else:
        print(f"  Status: MISMATCH (view={view_count}, core={core_count})")
    print()

    # 4. Sample Rows from the View
    print("--- Sample Records from analytics.transaction_propagation ---")
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

    for r in cur.fetchall():
        print(f"  TX: {r[0][:12]}... | Seq: #{r[1]:2d} | Delay: {r[3]}ms (Creation Diff: {r[4]}ms)")
        print(f"    Observer: {r[5]} ({r[6]}) <- Peer: {r[7]} ({r[8]}, {r[9]}) | Msg: {r[10]}")
        print()

    print("============================================================")
    print("             STEP 2 CREATION & AUDIT COMPLETE               ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    create_and_verify_view()
