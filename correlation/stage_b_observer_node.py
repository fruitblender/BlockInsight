"""
Correlation Stage B:
Verify the observation -> observer node relationship.

Confirms that transaction_observations.observer_id
correctly connects to nodes.node_id.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from database import get_connection


def run_stage_b():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            print("=" * 60)
            print("CORRELATION STAGE B: OBSERVATION -> OBSERVER NODE")
            print("=" * 60)

            # 1. Total observations
            cur.execute("""
                SELECT COUNT(*)
                FROM core.transaction_observations;
            """)
            total_observations = cur.fetchone()[0]

            # 2. Observations linked to valid nodes
            cur.execute("""
                SELECT COUNT(*)
                FROM core.transaction_observations o
                JOIN core.nodes n
                    ON o.observer_id = n.node_id;
            """)
            linked_observations = cur.fetchone()[0]

            # 3. Distinct observer nodes
            cur.execute("""
                SELECT COUNT(DISTINCT o.observer_id)
                FROM core.transaction_observations o
                JOIN core.nodes n
                    ON o.observer_id = n.node_id;
            """)
            distinct_observers = cur.fetchone()[0]

            # 4. Total nodes
            cur.execute("""
                SELECT COUNT(*)
                FROM core.nodes;
            """)
            total_nodes = cur.fetchone()[0]

            print()
            print(f"Total observations          : {total_observations}")
            print(f"Linked to valid nodes       : {linked_observations}")
            print(f"Distinct observer nodes     : {distinct_observers}")
            print(f"Total nodes                 : {total_nodes}")

            # 5. Verification
            print()

            if total_observations == linked_observations:
                print("Observer Join: PASSED")
                print("100% of observations have a valid observer node.")
            else:
                print("Observer Join: FAILED")
                print(
                    f"{total_observations - linked_observations} "
                    "observations have no matching observer node."
                )

            # 6. Top observer nodes
            print()
            print("--- Top 5 Observer Nodes ---")

            cur.execute("""
                SELECT
                    o.observer_id,
                    n.ip,
                    n.country,
                    n.asn,
                    n.node_type,
                    COUNT(*) AS observation_count
                FROM core.transaction_observations o
                JOIN core.nodes n
                    ON o.observer_id = n.node_id
                GROUP BY
                    o.observer_id,
                    n.ip,
                    n.country,
                    n.asn,
                    n.node_type
                ORDER BY observation_count DESC
                LIMIT 5;
            """)

            for row in cur.fetchall():
                print(
                    f"Node={row[0]} | "
                    f"IP={row[1]} | "
                    f"Country={row[2]} | "
                    f"ASN={row[3]} | "
                    f"Type={row[4]} | "
                    f"Observations={row[5]}"
                )

            # 7. Geographic distribution
            print()
            print("--- Geographic Distribution ---")

            cur.execute("""
                SELECT
                    n.country,
                    COUNT(DISTINCT o.observer_id) AS observer_count,
                    COUNT(*) AS observation_count
                FROM core.transaction_observations o
                JOIN core.nodes n
                    ON o.observer_id = n.node_id
                GROUP BY n.country
                ORDER BY observation_count DESC;
            """)

            for row in cur.fetchall():
                print(
                    f"Country={row[0]} | "
                    f"Observers={row[1]} | "
                    f"Observations={row[2]}"
                )

            print()
            print("=" * 60)
            print("STAGE B COMPLETE")
            print("=" * 60)

    finally:
        conn.close()


if __name__ == "__main__":
    run_stage_b()
