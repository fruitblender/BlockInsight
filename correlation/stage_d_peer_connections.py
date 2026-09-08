"""
Correlation Stage D: Connect Observations to Peer Connection Topology & Timing
Checks whether direct edges exist between observer and peer in core.peer_connections,
and evaluates temporal alignment between observation timestamps and connection windows.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def run_stage_d():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print(f"  CORRELATION STAGE D: TOPOLOGY & PEER CONNECTIONS (BATCH {BATCH_ID})")
    print("============================================================\n")

    # 1. Observer and Peer participation in peer_connections
    cur.execute("""
        SELECT COUNT(DISTINCT o.observer_id)
        FROM core.transaction_observations o
        WHERE o.batch_id = %s
          AND o.observer_id IN (
              SELECT src_node_id FROM core.peer_connections WHERE batch_id = %s
              UNION
              SELECT dst_node_id FROM core.peer_connections WHERE batch_id = %s
          );
    """, (BATCH_ID, BATCH_ID, BATCH_ID))
    connected_observers = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(DISTINCT o.peer_id)
        FROM core.transaction_observations o
        WHERE o.batch_id = %s
          AND o.peer_id IN (
              SELECT src_node_id FROM core.peer_connections WHERE batch_id = %s
              UNION
              SELECT dst_node_id FROM core.peer_connections WHERE batch_id = %s
          );
    """, (BATCH_ID, BATCH_ID, BATCH_ID))
    connected_peers = cur.fetchone()[0]

    print(f"Active observers in peer_connections : {connected_observers} / 10 observers")
    print(f"Active peers in peer_connections     : {connected_peers} / 43 peers")
    print()

    # 2. Direct connection between observer and peer
    cur.execute("""
        SELECT
            COUNT(*) AS total_observations,
            COUNT(pc.connection_id) AS with_direct_connection
        FROM core.transaction_observations o
        LEFT JOIN core.peer_connections pc
            ON (
                (pc.src_node_id = o.observer_id AND pc.dst_node_id = o.peer_id)
                OR
                (pc.src_node_id = o.peer_id AND pc.dst_node_id = o.observer_id)
            )
        WHERE o.batch_id = %s;
    """, (BATCH_ID,))
    total_obs, direct_conns = cur.fetchone()

    print(f"Total observations                               : {total_obs}")
    print(f"Observations with direct observer-peer connection: {direct_conns}")
    print()

    # 3. Temporal alignment inspection
    print("--- Temporal Alignment Analysis ---")
    cur.execute("""
        SELECT
            CASE
                WHEN o.timestamp >= pc.timestamp_start AND o.timestamp <= pc.timestamp_end THEN 'DURING_CONNECTION'
                WHEN o.timestamp < pc.timestamp_start THEN 'BEFORE_CONNECTION'
                ELSE 'AFTER_CONNECTION'
            END AS window_status,
            COUNT(*) AS count
        FROM core.transaction_observations o
        JOIN core.peer_connections pc
            ON (
                (pc.src_node_id = o.observer_id AND pc.dst_node_id = o.peer_id)
                OR
                (pc.src_node_id = o.peer_id AND pc.dst_node_id = o.observer_id)
            )
        WHERE o.batch_id = %s
        GROUP BY window_status
        ORDER BY count DESC;
    """, (BATCH_ID,))

    for r in cur.fetchall():
        print(f"  Observation occurred: {r[0]:20s} | Count: {r[1]}")
    print()

    # 4. Sample joined topology record
    print("--- Sample Observation + Peer Connection Topology ---")
    cur.execute("""
        SELECT
            o.txid,
            o.sequence_number,
            o.timestamp AS obs_time,
            o.observer_id,
            o.peer_id,
            pc.connection_id,
            pc.timestamp_start,
            pc.timestamp_end,
            pc.direction
        FROM core.transaction_observations o
        JOIN core.peer_connections pc
            ON (
                (pc.src_node_id = o.observer_id AND pc.dst_node_id = o.peer_id)
                OR
                (pc.src_node_id = o.peer_id AND pc.dst_node_id = o.observer_id)
            )
        WHERE o.batch_id = %s
        ORDER BY o.txid, o.sequence_number
        LIMIT 3;
    """, (BATCH_ID,))

    for r in cur.fetchall():
        print(f"  TX: {r[0][:12]}... | Seq: {r[1]} | Obs: {r[2]}")
        print(f"    Observer: {r[3]} <-> Peer: {r[4]} | Conn: {r[5]} ({r[8]})")
        print(f"    Conn Window: {r[6]} to {r[7]}")
        print()

    print("============================================================")
    print("               STAGE D VERIFICATION COMPLETE                ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_stage_d()
