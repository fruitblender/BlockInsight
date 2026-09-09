"""
ChainWatch
Phase 5 - Step 2
Construct analytics.node_behavior_summary

IMPORTANT:
The current core PostgreSQL tables do NOT contain batch_id.

Actual core tables:
    core.nodes
    core.peer_connections
    core.transaction_observations

Therefore:
    - core tables are treated as the current analysis dataset
    - BATCH_ID = 1 is added only in the analytics layer

Grain:
    ONE ROW PER (batch_id, node_id)

Base table:
    core.nodes

Independent aggregations:
    1. Connection topology
    2. Observer behavior
    3. Peer propagation behavior

The independent aggregations prevent row multiplication.
"""

import sys
from pathlib import Path

# ------------------------------------------------------------
# Allow importing database.py from ingestion/
# ------------------------------------------------------------

sys.path.append(
    str(Path(__file__).resolve().parents[1] / "ingestion")
)

from database import get_connection


# ------------------------------------------------------------
# Logical batch identifier.
# This does NOT exist in the core tables.
# ------------------------------------------------------------

BATCH_ID = 1


def create_and_verify_node_summary():

    conn = get_connection()
    cur = conn.cursor()

    try:

        print("=" * 60)
        print(" PHASE 5 - STEP 2: CONSTRUCT analytics.node_behavior_summary")
        print("=" * 60)
        print()

        # ========================================================
        # 1. VERIFY CORE DATASET
        # ========================================================

        print("[1/5] Verifying core dataset...")

        cur.execute("""
            SELECT COUNT(*)
            FROM core.nodes;
        """)

        node_count = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*)
            FROM core.peer_connections;
        """)

        connection_count = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*)
            FROM core.transaction_observations;
        """)

        observation_count = cur.fetchone()[0]

        print(f"  Nodes                   : {node_count}")
        print(f"  Peer connections        : {connection_count}")
        print(f"  Transaction observations: {observation_count}")
        print()

        if node_count != 150:
            print(
                f"  WARNING: Expected 150 nodes, found {node_count}"
            )

        if connection_count != 441:
            print(
                f"  WARNING: Expected 441 connections, found {connection_count}"
            )

        if observation_count != 3940:
            print(
                f"  WARNING: Expected 3940 observations, found {observation_count}"
            )

        # ========================================================
        # 2. TEST CONNECTION AGGREGATION
        # ========================================================

        print("[2/5] Testing connection aggregation...")

        cur.execute("""
            WITH conn_edges AS (

                SELECT
                    src_node_id AS node_id,
                    dst_node_id AS peer_id,

                    EXTRACT(
                        EPOCH FROM (
                            timestamp_end - timestamp_start
                        )
                    ) * 1000.0 AS duration_ms,

                    1 AS is_outgoing,
                    0 AS is_incoming

                FROM core.peer_connections

                UNION ALL

                SELECT
                    dst_node_id AS node_id,
                    src_node_id AS peer_id,

                    EXTRACT(
                        EPOCH FROM (
                            timestamp_end - timestamp_start
                        )
                    ) * 1000.0 AS duration_ms,

                    0 AS is_outgoing,
                    1 AS is_incoming

                FROM core.peer_connections
            )

            SELECT
                COUNT(DISTINCT node_id),
                COALESCE(SUM(is_outgoing), 0),
                COALESCE(SUM(is_incoming), 0),
                COUNT(*)

            FROM conn_edges;
        """)

        c_nodes, c_out, c_in, c_total = cur.fetchone()

        print(f"  Nodes with connections : {c_nodes}")
        print(f"  Outgoing endpoints     : {c_out}")
        print(f"  Incoming endpoints     : {c_in}")
        print(f"  Total endpoints        : {c_total}")

        if c_out == connection_count and c_in == connection_count:
            print("  -> conn_agg test PASSED.")
        else:
            print("  -> conn_agg test FAILED.")

        print()

        # ========================================================
        # 3. TEST OBSERVER AGGREGATION
        # ========================================================

        print("[3/5] Testing observer aggregation...")

        cur.execute("""
            SELECT
                COUNT(DISTINCT observer_id),
                COUNT(*)
            FROM core.transaction_observations
            WHERE observer_id IS NOT NULL;
        """)

        observer_count, observer_observations = cur.fetchone()

        print(
            f"  Distinct observer nodes : {observer_count}"
        )

        print(
            f"  Total observations      : {observer_observations}"
        )

        if observer_observations == observation_count:
            print("  -> obs_agg test PASSED.")
        else:
            print("  -> obs_agg test FAILED.")

        print()

        # ========================================================
        # 4. TEST PEER AGGREGATION
        # ========================================================

        print("[4/5] Testing peer aggregation...")

        cur.execute("""
            SELECT
                COUNT(DISTINCT peer_id),
                COUNT(*)
            FROM core.transaction_observations
            WHERE peer_id IS NOT NULL;
        """)

        peer_count, peer_observations = cur.fetchone()

        print(
            f"  Distinct peer nodes : {peer_count}"
        )

        print(
            f"  Total propagations  : {peer_observations}"
        )

        if peer_observations == observation_count:
            print("  -> peer_agg test PASSED.")
        else:
            print("  -> peer_agg test FAILED.")

        print()

        # ========================================================
        # 5. CREATE ANALYTICS VIEW
        # ========================================================

        print("[5/5] Creating analytics.node_behavior_summary...")

        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS analytics;
        """)

        # DROP is intentional because the previous broken version
        # may have a different column structure.
        cur.execute("""
            DROP VIEW IF EXISTS
                analytics.node_behavior_summary
            CASCADE;
        """)

        # --------------------------------------------------------
        # IMPORTANT:
        # No batch_id is read from core tables.
        #
        # BATCH_ID is inserted as a literal constant.
        # --------------------------------------------------------

        view_sql = f"""
        CREATE VIEW analytics.node_behavior_summary AS

        WITH

        -- ======================================================
        -- CONNECTION AGGREGATION
        -- ======================================================

        conn_edges AS (

            SELECT
                src_node_id AS node_id,
                dst_node_id AS peer_id,

                EXTRACT(
                    EPOCH FROM (
                        timestamp_end - timestamp_start
                    )
                ) * 1000.0 AS duration_ms,

                1 AS is_outgoing,
                0 AS is_incoming

            FROM core.peer_connections

            UNION ALL

            SELECT
                dst_node_id AS node_id,
                src_node_id AS peer_id,

                EXTRACT(
                    EPOCH FROM (
                        timestamp_end - timestamp_start
                    )
                ) * 1000.0 AS duration_ms,

                0 AS is_outgoing,
                1 AS is_incoming

            FROM core.peer_connections
        ),

        conn_agg AS (

            SELECT

                node_id,

                SUM(is_outgoing)::integer
                    AS outgoing_connections,

                SUM(is_incoming)::integer
                    AS incoming_connections,

                COUNT(*)::integer
                    AS total_connections,

                COUNT(DISTINCT peer_id)::integer
                    AS peer_degree,

                ROUND(
                    AVG(duration_ms)::numeric,
                    3
                ) AS avg_connection_duration_ms

            FROM conn_edges

            GROUP BY node_id
        ),

        -- ======================================================
        -- OBSERVER AGGREGATION
        -- ======================================================

        obs_agg AS (

            SELECT

                observer_id AS node_id,

                COUNT(*)::integer
                    AS observations_as_observer,

                COUNT(DISTINCT txid)::integer
                    AS transactions_observed,

                COUNT(DISTINCT peer_id)::integer
                    AS unique_peers_as_observer,

                ROUND(
                    AVG(propagation_delay_ms)::numeric,
                    3
                ) AS avg_observer_delay_ms,

                ROUND(
                    MIN(propagation_delay_ms)::numeric,
                    3
                ) AS min_observer_delay_ms,

                ROUND(
                    MAX(propagation_delay_ms)::numeric,
                    3
                ) AS max_observer_delay_ms

            FROM core.transaction_observations

            WHERE observer_id IS NOT NULL

            GROUP BY observer_id
        ),

        -- ======================================================
        -- PEER AGGREGATION
        -- ======================================================

        peer_agg AS (

            SELECT

                peer_id AS node_id,

                COUNT(*)::integer
                    AS propagations_as_peer,

                COUNT(DISTINCT txid)::integer
                    AS transactions_propagated,

                COUNT(DISTINCT observer_id)::integer
                    AS unique_observers_as_peer

            FROM core.transaction_observations

            WHERE peer_id IS NOT NULL

            GROUP BY peer_id
        )

        -- ======================================================
        -- FINAL NODE-LEVEL SUMMARY
        -- ======================================================

        SELECT

            {BATCH_ID}::integer
                AS batch_id,

            n.node_id,
            n.ip,
            n.port,
            n.country,
            n.asn,
            n.node_type,

            -- --------------------------------------------------
            -- Topology
            -- --------------------------------------------------

            COALESCE(
                c.peer_degree,
                0
            )::integer
                AS peer_degree,

            COALESCE(
                c.outgoing_connections,
                0
            )::integer
                AS outgoing_connections,

            COALESCE(
                c.incoming_connections,
                0
            )::integer
                AS incoming_connections,

            COALESCE(
                c.total_connections,
                0
            )::integer
                AS total_connections,

            COALESCE(
                c.avg_connection_duration_ms,
                0
            )::numeric
                AS avg_connection_duration_ms,

            -- --------------------------------------------------
            -- Observer behavior
            -- --------------------------------------------------

            COALESCE(
                o.observations_as_observer,
                0
            )::integer
                AS observations_as_observer,

            COALESCE(
                o.transactions_observed,
                0
            )::integer
                AS transactions_observed,

            COALESCE(
                o.unique_peers_as_observer,
                0
            )::integer
                AS unique_peers_as_observer,

            o.avg_observer_delay_ms,

            o.min_observer_delay_ms,

            o.max_observer_delay_ms,

            -- --------------------------------------------------
            -- Peer behavior
            -- --------------------------------------------------

            COALESCE(
                p.propagations_as_peer,
                0
            )::integer
                AS propagations_as_peer,

            COALESCE(
                p.transactions_propagated,
                0
            )::integer
                AS transactions_propagated,

            COALESCE(
                p.unique_observers_as_peer,
                0
            )::integer
                AS unique_observers_as_peer,

            -- --------------------------------------------------
            -- Propagation / observation ratio
            --
            -- NULL when the node has no observations as observer.
            -- This avoids falsely assigning a zero ratio.
            -- --------------------------------------------------

            CASE

                WHEN COALESCE(
                    o.observations_as_observer,
                    0
                ) > 0

                THEN ROUND(
                    (
                        COALESCE(
                            p.propagations_as_peer,
                            0
                        )::numeric
                        /
                        o.observations_as_observer::numeric
                    ),
                    4
                )

                ELSE NULL

            END AS propagation_to_observation_ratio

        FROM core.nodes n

        LEFT JOIN conn_agg c
            ON n.node_id = c.node_id

        LEFT JOIN obs_agg o
            ON n.node_id = o.node_id

        LEFT JOIN peer_agg p
            ON n.node_id = p.node_id;
        """

        cur.execute(view_sql)

        conn.commit()

        print("  View created successfully.")
        print()

        # ========================================================
        # VERIFICATION
        # ========================================================

        print("--- Verification Audit ---")

        all_passed = True

        # --------------------------------------------------------
        # Check 1: Row count
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary;
        """)

        r1 = cur.fetchone()[0]

        p1 = (r1 == node_count)

        print(
            f"Check 1  - Summary row count       : "
            f"{r1} (Expected {node_count}) -> "
            f"{'PASSED' if p1 else 'FAILED'}"
        )

        all_passed = all_passed and p1

        # --------------------------------------------------------
        # Check 2: Batch ID
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s;
        """, (BATCH_ID,))

        r2 = cur.fetchone()[0]

        p2 = (r2 == node_count)

        print(
            f"Check 2  - Batch {BATCH_ID} rows          : "
            f"{r2} -> "
            f"{'PASSED' if p2 else 'FAILED'}"
        )

        all_passed = all_passed and p2

        # --------------------------------------------------------
        # Check 3: Duplicate node IDs
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT
                    batch_id,
                    node_id,
                    COUNT(*) AS cnt
                FROM analytics.node_behavior_summary
                GROUP BY batch_id, node_id
                HAVING COUNT(*) > 1
            ) duplicates;
        """)

        r3 = cur.fetchone()[0]

        p3 = (r3 == 0)

        print(
            f"Check 3  - Duplicate (batch,node) : "
            f"{r3} duplicates -> "
            f"{'PASSED' if p3 else 'FAILED'}"
        )

        all_passed = all_passed and p3

        # --------------------------------------------------------
        # Check 4: Node coverage
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM core.nodes n
            LEFT JOIN analytics.node_behavior_summary s
                ON n.node_id = s.node_id
               AND s.batch_id = %s
            WHERE s.node_id IS NULL;
        """, (BATCH_ID,))

        r4 = cur.fetchone()[0]

        p4 = (r4 == 0)

        print(
            f"Check 4  - Missing nodes from core : "
            f"{r4} missing -> "
            f"{'PASSED' if p4 else 'FAILED'}"
        )

        all_passed = all_passed and p4

        # --------------------------------------------------------
        # Check 5: Observation reconciliation
        # --------------------------------------------------------

        cur.execute("""
            SELECT
                COALESCE(
                    SUM(observations_as_observer),
                    0
                )
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s;
        """, (BATCH_ID,))

        r5 = cur.fetchone()[0]

        p5 = (r5 == observation_count)

        print(
            f"Check 5  - Observation reconciliation: "
            f"{r5} (Expected {observation_count}) -> "
            f"{'PASSED' if p5 else 'FAILED'}"
        )

        all_passed = all_passed and p5

        # --------------------------------------------------------
        # Check 6: Distinct transaction logic
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
              AND transactions_observed >
                  observations_as_observer;
        """, (BATCH_ID,))

        r6 = cur.fetchone()[0]

        p6 = (r6 == 0)

        print(
            f"Check 6  - Tx observed > observations: "
            f"{r6} invalid rows -> "
            f"{'PASSED' if p6 else 'FAILED'}"
        )

        all_passed = all_passed and p6

        # --------------------------------------------------------
        # Check 7: Propagation reconciliation
        # --------------------------------------------------------

        cur.execute("""
            SELECT
                COALESCE(
                    SUM(propagations_as_peer),
                    0
                )
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s;
        """, (BATCH_ID,))

        r7 = cur.fetchone()[0]

        p7 = (r7 == observation_count)

        print(
            f"Check 7  - Propagation reconciliation: "
            f"{r7} (Expected {observation_count}) -> "
            f"{'PASSED' if p7 else 'FAILED'}"
        )

        all_passed = all_passed and p7

        # --------------------------------------------------------
        # Check 8: Connection reconciliation
        # --------------------------------------------------------

        cur.execute("""
            SELECT
                COALESCE(SUM(outgoing_connections), 0),
                COALESCE(SUM(incoming_connections), 0)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s;
        """, (BATCH_ID,))

        r8_out, r8_in = cur.fetchone()

        p8 = (
            r8_out == connection_count
            and
            r8_in == connection_count
        )

        print(
            f"Check 8  - Connections reconciled   : "
            f"out={r8_out}, in={r8_in} "
            f"(Expected {connection_count} each) -> "
            f"{'PASSED' if p8 else 'FAILED'}"
        )

        all_passed = all_passed and p8

        # --------------------------------------------------------
        # Check 9: Degree consistency
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
              AND peer_degree > total_connections;
        """, (BATCH_ID,))

        r9 = cur.fetchone()[0]

        p9 = (r9 == 0)

        print(
            f"Check 9  - Degree > total connections: "
            f"{r9} violations -> "
            f"{'PASSED' if p9 else 'FAILED'}"
        )

        all_passed = all_passed and p9

        # --------------------------------------------------------
        # Check 10: Delay sanity
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
              AND observations_as_observer > 0
              AND (
                    min_observer_delay_ms >
                    avg_observer_delay_ms

                    OR

                    avg_observer_delay_ms >
                    max_observer_delay_ms
                  );
        """, (BATCH_ID,))

        r10 = cur.fetchone()[0]

        p10 = (r10 == 0)

        print(
            f"Check 10 - Min <= Avg <= Max delay : "
            f"{r10} violations -> "
            f"{'PASSED' if p10 else 'FAILED'}"
        )

        all_passed = all_passed and p10

        # --------------------------------------------------------
        # Check 11: Non-negative values
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
              AND (
                    min_observer_delay_ms < 0

                    OR

                    avg_connection_duration_ms < 0
                  );
        """, (BATCH_ID,))

        r11 = cur.fetchone()[0]

        p11 = (r11 == 0)

        print(
            f"Check 11 - Negative delay/duration : "
            f"{r11} negative rows -> "
            f"{'PASSED' if p11 else 'FAILED'}"
        )

        all_passed = all_passed and p11

        # --------------------------------------------------------
        # Check 12: NULL semantics
        #
        # Nodes with zero observations should have NULL
        # observer-delay statistics.
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
              AND observations_as_observer = 0
              AND (
                    avg_observer_delay_ms IS NOT NULL
                    OR
                    min_observer_delay_ms IS NOT NULL
                    OR
                    max_observer_delay_ms IS NOT NULL
                  );
        """, (BATCH_ID,))

        r12 = cur.fetchone()[0]

        p12 = (r12 == 0)

        print(
            f"Check 12 - NULL semantics           : "
            f"{r12} violations -> "
            f"{'PASSED' if p12 else 'FAILED'}"
        )

        all_passed = all_passed and p12

        # --------------------------------------------------------
        # Check 13: Ratio safety
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
              AND observations_as_observer = 0
              AND propagation_to_observation_ratio IS NOT NULL;
        """, (BATCH_ID,))

        r13 = cur.fetchone()[0]

        p13 = (r13 == 0)

        print(
            f"Check 13 - Ratio safety             : "
            f"{r13} violations -> "
            f"{'PASSED' if p13 else 'FAILED'}"
        )

        all_passed = all_passed and p13

        # ========================================================
        # SAMPLE RECORDS
        # ========================================================

        print()
        print("--- Sample Node Summaries ---")

        cur.execute("""
            SELECT
                node_id,
                node_type,
                country,
                peer_degree,
                total_connections,
                observations_as_observer,
                transactions_observed,
                avg_observer_delay_ms,
                propagations_as_peer,
                propagation_to_observation_ratio
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
            ORDER BY
                observations_as_observer DESC,
                propagations_as_peer DESC,
                node_id
            LIMIT 6;
        """, (BATCH_ID,))

        rows = cur.fetchall()

        print(
            "  NODE  | TYPE                 | CTRY | "
            "DEGREE | CONNS | OBS_CNT | TX_OBS | "
            "AVG_DELAY | PROP_CNT | PROP/OBS"
        )

        print("  " + "-" * 105)

        for row in rows:

            (
                node_id,
                node_type,
                country,
                peer_degree,
                total_connections,
                observations,
                transactions,
                avg_delay,
                propagations,
                ratio
            ) = row

            node_type = node_type or "NULL"
            country = country or "NULL"

            delay_text = (
                f"{avg_delay:.2f}ms"
                if avg_delay is not None
                else "NULL"
            )

            ratio_text = (
                f"{ratio:.4f}"
                if ratio is not None
                else "NULL"
            )

            print(
                f"  {node_id:5s} | "
                f"{node_type:20s} | "
                f"{country:4s} | "
                f"{peer_degree:6d} | "
                f"{total_connections:5d} | "
                f"{observations:7d} | "
                f"{transactions:6d} | "
                f"{delay_text:9s} | "
                f"{propagations:8d} | "
                f"{ratio_text}"
            )

        # ========================================================
        # FINAL RESULT
        # ========================================================

        print()
        print("=" * 60)

        if all_passed:
            print(
                " VERIFICATION AUDIT RESULT: "
                "ALL 13 CHECKS PASSED"
            )
        else:
            print(
                " VERIFICATION AUDIT RESULT: "
                "SOME CHECKS FAILED"
            )

        print("=" * 60)

    finally:

        cur.close()
        conn.close()


if __name__ == "__main__":
    create_and_verify_node_summary()
