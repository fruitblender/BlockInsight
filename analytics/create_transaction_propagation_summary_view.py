import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parents[1] / "ingestion")
)

from database import get_connection

BATCH_ID = 1


def create_transaction_propagation_summary():

    print("=" * 60)
    print(" CREATING analytics.transaction_propagation_summary")
    print("=" * 60)

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS analytics;
        """)

        cur.execute("""
            DROP VIEW IF EXISTS analytics.transaction_propagation_summary;
        """)

        cur.execute("""
            CREATE VIEW analytics.transaction_propagation_summary AS

            SELECT
                1::INTEGER AS batch_id,

                t.txid,

                COUNT(o.txid) AS total_observations,

                COUNT(DISTINCT o.observer_id) AS unique_observers,

                COUNT(DISTINCT o.peer_id) AS unique_peers,

                COUNT(DISTINCT observer_node.country)
                    FILTER (WHERE observer_node.country IS NOT NULL)
                    AS unique_observer_countries,

                COUNT(DISTINCT peer_node.country)
                    FILTER (WHERE peer_node.country IS NOT NULL)
                    AS unique_peer_countries,

                MIN(o.propagation_delay_ms)
                    AS min_propagation_delay_ms,

                MAX(o.propagation_delay_ms)
                    AS max_propagation_delay_ms,

                AVG(o.propagation_delay_ms)
                    AS avg_propagation_delay_ms,

                PERCENTILE_CONT(0.5)
                    WITHIN GROUP (
                        ORDER BY o.propagation_delay_ms
                    )
                    AS median_propagation_delay_ms,

                MAX(o.propagation_delay_ms)
                    - MIN(o.propagation_delay_ms)
                    AS observed_propagation_span_ms,

                MIN(o.propagation_delay_ms)
                    AS creation_to_first_observation_ms,

                AVG(
                    CASE
                        WHEN LOWER(o.message_type) = 'inv'
                        THEN 1.0
                        ELSE 0.0
                    END
                ) AS inv_ratio,

                AVG(
                    CASE
                        WHEN LOWER(o.message_type) = 'tx'
                        THEN 1.0
                        ELSE 0.0
                    END
                ) AS tx_ratio

            FROM core.transactions t

            LEFT JOIN core.transaction_observations o
                ON t.txid = o.txid

            LEFT JOIN core.nodes observer_node
                ON observer_node.node_id = o.observer_id

            LEFT JOIN core.nodes peer_node
                ON peer_node.node_id = o.peer_id

            GROUP BY t.txid;
        """)

        conn.commit()

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.transaction_propagation_summary
            WHERE batch_id = %s;
        """, (BATCH_ID,))

        count = cur.fetchone()[0]

        print(f"Successfully created view with {count} transactions.")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    create_transaction_propagation_summary()
