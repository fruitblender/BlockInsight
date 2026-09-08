"""
Correlation Stage C: Verify Observation -> Peer Node Link
Confirms that core.transaction_observations correctly connects both observer_id and peer_id to core.nodes.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def run_stage_c():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print(f"     CORRELATION STAGE C: OBSERVATION -> PEER NODE (BATCH {BATCH_ID})")
    print("============================================================\n")

    # 1. Total counts in the 3-way joined view
    cur.execute("""
        SELECT
            COUNT(*) AS total_joined_observations,
            COUNT(DISTINCT o.observer_id) AS distinct_observers,
            COUNT(DISTINCT o.peer_id) AS distinct_peers
        FROM core.transaction_observations o
        JOIN core.nodes observer
            ON o.observer_id = observer.node_id
        JOIN core.nodes peer
            ON o.peer_id = peer.node_id
        WHERE o.batch_id = %s;
    """, (BATCH_ID,))
    joined_obs, distinct_obs, distinct_peers = cur.fetchone()

    print(f"Total observations joined (Obs -> Observer & Peer): {joined_obs} / 3940")
    print(f"Distinct observer nodes                              : {distinct_obs}")
    print(f"Distinct propagating peer nodes                      : {distinct_peers}")
    print()

    # 2. Node types of the propagating peers
    print("--- Peer Node Types ---")
    cur.execute("""
        SELECT
            peer.node_type,
            COUNT(DISTINCT o.peer_id) AS unique_peers,
            COUNT(*) AS total_propagations
        FROM core.transaction_observations o
        JOIN core.nodes peer
            ON o.peer_id = peer.node_id
        WHERE o.batch_id = %s
        GROUP BY peer.node_type
        ORDER BY total_propagations DESC;
    """, (BATCH_ID,))

    for r in cur.fetchall():
        print(f"  Type: {r[0]:20s} | Unique Peers: {r[1]:3d} | Observations Sent: {r[2]:4d}")
    print()

    # 3. Top 5 propagating peers
    print("--- Top 5 Propagating Peers ---")
    cur.execute("""
        SELECT
            o.peer_id,
            peer.ip,
            peer.country,
            peer.asn,
            peer.node_type,
            COUNT(*) AS count
        FROM core.transaction_observations o
        JOIN core.nodes peer
            ON o.peer_id = peer.node_id
        WHERE o.batch_id = %s
        GROUP BY o.peer_id, peer.ip, peer.country, peer.asn, peer.node_type
        ORDER BY count DESC
        LIMIT 5;
    """, (BATCH_ID,))

    for r in cur.fetchall():
        print(f"  Peer: {r[0]} | IP: {r[1]:15s} | Country: {r[2]} | ASN: {r[3]:10s} | Type: {r[4]:12s} | Sent: {r[5]}")

    print("\n--- Sample Complete 3-Way Correlation Record ---")
    cur.execute("""
        SELECT
            o.txid,
            o.sequence_number,
            o.timestamp AS obs_time,
            o.observer_id,
            obs.country AS obs_country,
            o.peer_id,
            peer.country AS peer_country,
            peer.node_type AS peer_type,
            o.message_type,
            o.propagation_delay_ms
        FROM core.transaction_observations o
        JOIN core.nodes obs
            ON o.observer_id = obs.node_id
        JOIN core.nodes peer
            ON o.peer_id = peer.node_id
        WHERE o.batch_id = %s
        ORDER BY o.txid, o.sequence_number
        LIMIT 3;
    """, (BATCH_ID,))

    for r in cur.fetchall():
        print(f"  TX: {r[0][:12]}... | Seq: {r[1]} | Delay: {r[9]}ms | Observer: {r[3]} ({r[4]}) <- Peer: {r[5]} ({r[6]}, {r[7]}) via {r[8]}")

    print("\n============================================================")
    print("               STAGE C VERIFICATION COMPLETE                ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_stage_c()
