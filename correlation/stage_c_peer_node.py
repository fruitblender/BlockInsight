"""
Correlation Stage C:
Verify the observation -> peer node relationship.

Confirms that transaction_observations.peer_id
correctly connects to nodes.node_id.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from database import get_connection


def run_stage_c():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            print("=" * 60)
            print("CORRELATION STAGE C: OBSERVATION -> PEER NODE")
            print("=" * 60)

            # 1. Total observations
            cur.execute("""
                SELECT COUNT(*)
                FROM core.transaction_observations;
            """)
            total_observations = cur.fetchone()[0]

            # 2. Observations with a valid peer
            cur.execute("""
                SELECT COUNT(*)
                FROM core.transaction_observations o
                JOIN core.nodes peer
                    ON o.peer_id = peer.node_id;
            """)
            linked_observations = cur.fetchone()[0]

            # 3. Observations with NULL peer_id
            cur.execute("""
                SELECT COUNT(*)
                FROM core.transaction_observations
                WHERE peer_id IS NULL;
            """)
            null_peers = cur.fetchone()[0]

            # 4. Distinct peer nodes
            cur.execute("""
                SELECT COUNT(DISTINCT o.peer_id)
                FROM core.transaction_observations o
                JOIN core.nodes peer
                    ON o.peer_id = peer.node_id;
            """)
            distinct_peers = cur.fetchone()[0]

            print()
            print(f"Total observations          : {total_observations}")
            print(f"Linked to valid peer nodes  : {linked_observations}")
            print(f"NULL peer IDs               : {null_peers}")
            print(f"Distinct peer nodes         : {distinct_peers}")

            # 5. Verification
            print()

            if total_observations == linked_observations:
                print("Peer Node Join: PASSED")
                print("100% of observations have a valid peer node.")
            else:
                print("Peer Node Join: NOT COMPLETE")
                print(
                    f"{total_observations - linked_observations} "
                    "observations do not have a valid peer node."
                )

            # 6. Peer node types
            print()
            print("--- Peer Node Types ---")

            cur.execute("""
                SELECT
                    peer.node_type,
                    COUNT(DISTINCT o.peer_id) AS unique_peers,
                    COUNT(*) AS observation_count
                FROM core.transaction_observations o
                JOIN core.nodes peer
                    ON o.peer_id = peer.node_id
                GROUP BY peer.node_type
                ORDER BY observation_count DESC;
            """)

            for row in cur.fetchall():
                print(
                    f"Type={row[0]} | "
                    f"Unique peers={row[1]} | "
                    f"Observations={row[2]}"
                )

            # 7. Top peer nodes
            print()
            print("--- Top 5 Peer Nodes ---")

            cur.execute("""
                SELECT
                    o.peer_id,
                    peer.ip,
                    peer.country,
                    peer.asn,
                    peer.node_type,
                    COUNT(*) AS observation_count
                FROM core.transaction_observations o
                JOIN core.nodes peer
                    ON o.peer_id = peer.node_id
                GROUP BY
                    o.peer_id,
                    peer.ip,
                    peer.country,
                    peer.asn,
                    peer.node_type
                ORDER BY observation_count DESC
                LIMIT 5;
            """)

            for row in cur.fetchall():
                print(
                    f"Peer={row[0]} | "
                    f"IP={row[1]} | "
                    f"Country={row[2]} | "
                    f"ASN={row[3]} | "
                    f"Type={row[4]} | "
                    f"Observations={row[5]}"
                )

            # 8. Sample observer -> peer relationship
            print()
            print("--- Sample Observer -> Peer Records ---")

            cur.execute("""
                SELECT
                    o.txid,
                    o.sequence_number,
                    o.observer_id,
                    observer.country,
                    o.peer_id,
                    peer.country,
                    peer.node_type,
                    o.message_type,
                    o.propagation_delay_ms
                FROM core.transaction_observations o
                JOIN core.nodes observer
                    ON o.observer_id = observer.node_id
                JOIN core.nodes peer
                    ON o.peer_id = peer.node_id
                ORDER BY o.txid, o.sequence_number
                LIMIT 5;
            """)

            for row in cur.fetchall():
                print(
                    f"TX={row[0][:12]}... | "
                    f"Seq={row[1]} | "
                    f"Observer={row[2]} ({row[3]}) | "
                    f"Peer={row[4]} ({row[5]}, {row[6]}) | "
                    f"Message={row[7]} | "
                    f"Delay={row[8]}ms"
                )

            print()
            print("=" * 60)
            print("STAGE C COMPLETE")
            print("=" * 60)

    finally:
        conn.close()


if __name__ == "__main__":
    run_stage_c()
