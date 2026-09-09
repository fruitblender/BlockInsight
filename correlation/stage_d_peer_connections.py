"""
Correlation Stage D: Connect Observations to Peer Connection Topology & Timing

Checks:
1. Whether observer nodes participate in the peer connection topology.
2. Whether peer nodes participate in the peer connection topology.
3. Whether a direct connection exists between observer and peer.
4. Whether the observation occurred during the connection window.
"""

import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from database import get_connection


def run_stage_d():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print("  CORRELATION STAGE D: TOPOLOGY & PEER CONNECTIONS")
    print("============================================================\n")

    # ----------------------------------------------------------
    # 1. Observer participation in peer connections
    # ----------------------------------------------------------

    cur.execute("""
        SELECT COUNT(DISTINCT o.observer_id)
        FROM core.transaction_observations o
        WHERE o.observer_id IN (
            SELECT src_node_id
            FROM core.peer_connections

            UNION

            SELECT dst_node_id
            FROM core.peer_connections
        );
    """)

    connected_observers = cur.fetchone()[0]

    # Total distinct observers
    cur.execute("""
        SELECT COUNT(DISTINCT observer_id)
        FROM core.transaction_observations;
    """)

    total_observers = cur.fetchone()[0]

    # ----------------------------------------------------------
    # 2. Peer participation in peer connections
    # ----------------------------------------------------------

    cur.execute("""
        SELECT COUNT(DISTINCT o.peer_id)
        FROM core.transaction_observations o
        WHERE o.peer_id IN (
            SELECT src_node_id
            FROM core.peer_connections

            UNION

            SELECT dst_node_id
            FROM core.peer_connections
        );
    """)

    connected_peers = cur.fetchone()[0]

    # Total distinct peers
    cur.execute("""
        SELECT COUNT(DISTINCT peer_id)
        FROM core.transaction_observations;
    """)

    total_peers = cur.fetchone()[0]

    print(
        f"Active observers in peer_connections : "
        f"{connected_observers} / {total_observers} observers"
    )

    print(
        f"Active peers in peer_connections     : "
        f"{connected_peers} / {total_peers} peers"
    )

    print()

    # ----------------------------------------------------------
    # 3. Direct connection between observer and peer
    # ----------------------------------------------------------

    cur.execute("""
        SELECT
            COUNT(*) AS total_observations,
            COUNT(pc.connection_id) AS with_direct_connection
        FROM core.transaction_observations o
        LEFT JOIN core.peer_connections pc
            ON (
                (
                    pc.src_node_id = o.observer_id
                    AND
                    pc.dst_node_id = o.peer_id
                )
                OR
                (
                    pc.src_node_id = o.peer_id
                    AND
                    pc.dst_node_id = o.observer_id
                )
            );
    """)

    total_obs, direct_conns = cur.fetchone()

    print(f"Total observations                                : {total_obs}")
    print(
        f"Observations with direct observer-peer connection: "
        f"{direct_conns}"
    )

    if total_obs > 0:
        percentage = (direct_conns / total_obs) * 100
    else:
        percentage = 0

    print(f"Direct connection coverage                         : {percentage:.2f}%")
    print()

    # ----------------------------------------------------------
    # 4. Temporal alignment
    # ----------------------------------------------------------

    print("--- Temporal Alignment Analysis ---")

    cur.execute("""
        SELECT
            CASE
                WHEN o.timestamp >= pc.timestamp_start
                 AND o.timestamp <= pc.timestamp_end
                    THEN 'DURING_CONNECTION'

                WHEN o.timestamp < pc.timestamp_start
                    THEN 'BEFORE_CONNECTION'

                ELSE 'AFTER_CONNECTION'
            END AS window_status,

            COUNT(*) AS count

        FROM core.transaction_observations o

        JOIN core.peer_connections pc
            ON (
                (
                    pc.src_node_id = o.observer_id
                    AND
                    pc.dst_node_id = o.peer_id
                )
                OR
                (
                    pc.src_node_id = o.peer_id
                    AND
                    pc.dst_node_id = o.observer_id
                )
            )

        GROUP BY window_status
        ORDER BY count DESC;
    """)

    temporal_rows = cur.fetchall()

    if temporal_rows:
        for status, count in temporal_rows:
            print(
                f"  Observation occurred: "
                f"{status:20s} | Count: {count}"
            )
    else:
        print("  No direct observer-peer connections found.")

    print()

    # ----------------------------------------------------------
    # 5. Sample joined topology records
    # ----------------------------------------------------------

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
                (
                    pc.src_node_id = o.observer_id
                    AND
                    pc.dst_node_id = o.peer_id
                )
                OR
                (
                    pc.src_node_id = o.peer_id
                    AND
                    pc.dst_node_id = o.observer_id
                )
            )

        ORDER BY o.txid, o.sequence_number
        LIMIT 3;
    """)

    sample_rows = cur.fetchall()

    if sample_rows:
        for r in sample_rows:
            print(
                f"  TX: {r[0][:12]}... | "
                f"Seq: {r[1]} | "
                f"Obs: {r[2]}"
            )

            print(
                f"    Observer: {r[3]} <-> Peer: {r[4]} | "
                f"Conn: {r[5]} ({r[8]})"
            )

            print(
                f"    Conn Window: {r[6]} to {r[7]}"
            )

            print()
    else:
        print("  No joined observation-peer connection records found.")

    # ----------------------------------------------------------
    # 6. Final verification summary
    # ----------------------------------------------------------

    print("============================================================")
    print("               STAGE D VERIFICATION COMPLETE")
    print("============================================================")

    cur.close()
    conn.close()


if __name__ == "__main__":
    run_stage_d()