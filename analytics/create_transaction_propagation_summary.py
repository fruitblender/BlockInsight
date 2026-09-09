"""
ChainWatch - Node Behavior Summary

Builds analytics.node_behavior_summary from the CURRENT core dataset.

IMPORTANT:
The current core PostgreSQL schema does NOT contain batch_id.

Therefore:
    - core.nodes
    - core.peer_connections
    - core.transaction_observations

are treated as the current dataset.

BATCH_ID = 1 is assigned only to the analytics output.

Grain:
    ONE ROW PER (batch_id, node_id)

Expected:
    150 nodes
"""

import sys
from pathlib import Path

# ------------------------------------------------------------
# Database connection
# ------------------------------------------------------------

sys.path.append(
    str(Path(__file__).resolve().parents[1] / "ingestion")
)

from database import get_connection


BATCH_ID = 1


# ============================================================
# MAIN
# ============================================================

def create_and_verify_node_summary():

    print("=" * 60)
    print(" PHASE 5 - STEP 2: CONSTRUCT analytics.node_behavior_summary")
    print("=" * 60)
    print()

    conn = get_connection()
    cur = conn.cursor()

    try:

        # ========================================================
        # 1. CREATE ANALYTICS SCHEMA
        # ========================================================

        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS analytics;
        """)

        # ========================================================
        # 2. DROP OLD VIEW
        # ========================================================

        cur.execute("""
            DROP VIEW IF EXISTS analytics.node_behavior_summary;
        """)

        # ========================================================
        # 3. CREATE NODE BEHAVIOR SUMMARY VIEW
        # ========================================================
        #
        # IMPORTANT:
        # There is NO batch_id in the core tables.
        #
        # We generate:
        #
        #     1 AS batch_id
        #
        # for the analytics layer.
        #
        # ========================================================

        view_sql = """
        CREATE VIEW analytics.node_behavior_summary AS

        WITH

        -- ======================================================
        -- CONNECTION AGGREGATION
        -- ======================================================

        conn_agg AS (

            SELECT
                node_id,

                COUNT(*) AS total_connections,

                COUNT(*) FILTER (
                    WHERE direction = 'outbound'
                ) AS outgoing_connections,

                COUNT(*) FILTER (
                    WHERE direction = 'inbound'
                ) AS incoming_connections,

                COUNT(DISTINCT peer_id) AS peer_degree,

                AVG(
                    EXTRACT(
                        EPOCH FROM (
                            timestamp_end - timestamp_start
                        )
                    ) * 1000.0
                ) AS avg_connection_duration_ms,

                MIN(
                    EXTRACT(
                        EPOCH FROM (
                            timestamp_end - timestamp_start
                        )
                    ) * 1000.0
                ) AS min_connection_duration_ms,

                MAX(
                    EXTRACT(
                        EPOCH FROM (
                            timestamp_end - timestamp_start
                        )
                    ) * 1000.0
                ) AS max_connection_duration_ms

            FROM (

                -- Node is source
                SELECT
                    src_node_id AS node_id,
                    dst_node_id AS peer_id,
                    direction,
                    timestamp_start,
                    timestamp_end
                FROM core.peer_connections

                UNION ALL

                -- Node is destination
                SELECT
                    dst_node_id AS node_id,
                    src_node_id AS peer_id,
                    CASE
                        WHEN direction = 'outbound'
                            THEN 'inbound'
                        WHEN direction = 'inbound'
                            THEN 'outbound'
                        ELSE direction
                    END AS direction,
                    timestamp_start,
                    timestamp_end
                FROM core.peer_connections

            ) connections

            GROUP BY node_id
        ),


        -- ======================================================
        -- OBSERVER ACTIVITY
        -- ======================================================

        obs_agg AS (

            SELECT
                observer_id AS node_id,

                COUNT(*) AS observations_as_observer,

                COUNT(DISTINCT txid) AS transactions_observed,

                COUNT(DISTINCT peer_id) AS unique_peers_as_observer,

                AVG(propagation_delay_ms)
                    AS avg_observer_delay_ms,

                MIN(propagation_delay_ms)
                    AS min_observer_delay_ms,

                MAX(propagation_delay_ms)
                    AS max_observer_delay_ms

            FROM core.transaction_observations

            GROUP BY observer_id
        ),


        -- ======================================================
        -- PEER PROPAGATION ACTIVITY
        -- ======================================================

        peer_agg AS (

            SELECT
                peer_id AS node_id,

                COUNT(*) AS propagations_as_peer,

                COUNT(DISTINCT txid) AS transactions_propagated,

                COUNT(DISTINCT observer_id)
                    AS unique_observers_as_peer

            FROM core.transaction_observations

            WHERE peer_id IS NOT NULL

            GROUP BY peer_id
        )


        -- ======================================================
        -- FINAL NODE SUMMARY
        -- ======================================================

        SELECT

            -- Analytics batch identifier.
            -- NOT read from core.
            %(batch_id)s::INTEGER AS batch_id,

            -- Node identity
            n.node_id,
            n.ip,
            n.country,
            n.asn,
            n.node_type,

            -- --------------------------------------------------
            -- Connection metrics
            -- --------------------------------------------------

            COALESCE(
                c.total_connections,
                0
            ) AS total_connections,

            COALESCE(
                c.outgoing_connections,
                0
            ) AS outgoing_connections,

            COALESCE(
                c.incoming_connections,
                0
            ) AS incoming_connections,

            COALESCE(
                c.peer_degree,
                0
            ) AS peer_degree,

            c.avg_connection_duration_ms,
            c.min_connection_duration_ms,
            c.max_connection_duration_ms,

            -- --------------------------------------------------
            -- Observer metrics
            -- --------------------------------------------------

            COALESCE(
                o.observations_as_observer,
                0
            ) AS observations_as_observer,

            COALESCE(
                o.transactions_observed,
                0
            ) AS transactions_observed,

            COALESCE(
                o.unique_peers_as_observer,
                0
            ) AS unique_peers_as_observer,

            o.avg_observer_delay_ms,
            o.min_observer_delay_ms,
            o.max_observer_delay_ms,

            -- --------------------------------------------------
            -- Peer propagation metrics
            -- --------------------------------------------------

            COALESCE(
                p.propagations_as_peer,
                0
            ) AS propagations_as_peer,

            COALESCE(
                p.transactions_propagated,
                0
            ) AS transactions_propagated,

            COALESCE(
                p.unique_observers_as_peer,
                0
            ) AS unique_observers_as_peer,

            -- --------------------------------------------------
            -- Propagation / observation ratio
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
                        )::NUMERIC
                        /
                        o.observations_as_observer
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
            ON n.node_id = p.node_id
        ;
        """

        # --------------------------------------------------------
        # PostgreSQL views cannot directly contain a psycopg
        # parameter placeholder.
        #
        # Therefore replace the batch expression safely with the
        # constant configured by this script.
        # --------------------------------------------------------

        view_sql = view_sql.replace(
            "%(batch_id)s",
            str(BATCH_ID)
        )

        cur.execute(view_sql)

        conn.commit()

        print(
            "  -> View analytics.node_behavior_summary "
            "created successfully."
        )
        print()


        # ========================================================
        # 4. BASIC VERIFICATION
        # ========================================================

        print("=" * 60)
        print("        RUNNING NODE SUMMARY VERIFICATION")
        print("=" * 60)
        print()

        all_passed = True


        # --------------------------------------------------------
        # Check 1: Row count
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s;
        """, (BATCH_ID,))

        row_count = cur.fetchone()[0]

        passed = row_count == 150

        print(
            f"Check 1  - Total Rows              : "
            f"{row_count} (Expected 150) -> "
            f"{'PASSED' if passed else 'FAILED'}"
        )

        all_passed = all_passed and passed


        # --------------------------------------------------------
        # Check 2: Distinct nodes
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(DISTINCT node_id)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s;
        """, (BATCH_ID,))

        distinct_nodes = cur.fetchone()[0]

        passed = distinct_nodes == 150

        print(
            f"Check 2  - Distinct node_ids       : "
            f"{distinct_nodes} (Expected 150) -> "
            f"{'PASSED' if passed else 'FAILED'}"
        )

        all_passed = all_passed and passed


        # --------------------------------------------------------
        # Check 3: Duplicate grain
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT
                    batch_id,
                    node_id,
                    COUNT(*) AS cnt
                FROM analytics.node_behavior_summary
                WHERE batch_id = %s
                GROUP BY batch_id, node_id
                HAVING COUNT(*) > 1
            ) duplicates;
        """, (BATCH_ID,))

        duplicate_count = cur.fetchone()[0]

        passed = duplicate_count == 0

        print(
            f"Check 3  - Duplicate "
            f"(batch_id,node_id)     : "
            f"{duplicate_count} -> "
            f"{'PASSED' if passed else 'FAILED'}"
        )

        all_passed = all_passed and passed


        # --------------------------------------------------------
        # Check 4: Every core node represented
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM core.nodes n
            LEFT JOIN analytics.node_behavior_summary s
                ON n.node_id = s.node_id
               AND s.batch_id = %s
            WHERE s.node_id IS NULL;
        """, (BATCH_ID,))

        missing_nodes = cur.fetchone()[0]

        passed = missing_nodes == 0

        print(
            f"Check 4  - Missing core nodes      : "
            f"{missing_nodes} -> "
            f"{'PASSED' if passed else 'FAILED'}"
        )

        all_passed = all_passed and passed


        # --------------------------------------------------------
        # Check 5: Connection count consistency
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
              AND total_connections < 0;
        """, (BATCH_ID,))

        invalid_connections = cur.fetchone()[0]

        passed = invalid_connections == 0

        print(
            f"Check 5  - Negative connections    : "
            f"{invalid_connections} -> "
            f"{'PASSED' if passed else 'FAILED'}"
        )

        all_passed = all_passed and passed


        # --------------------------------------------------------
        # Check 6: Degree <= connections
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
              AND peer_degree > total_connections;
        """, (BATCH_ID,))

        invalid_degree = cur.fetchone()[0]

        passed = invalid_degree == 0

        print(
            f"Check 6  - Degree <= connections   : "
            f"{invalid_degree} violations -> "
            f"{'PASSED' if passed else 'FAILED'}"
        )

        all_passed = all_passed and passed


        # --------------------------------------------------------
        # Check 7: Delay ordering
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

        invalid_delay_order = cur.fetchone()[0]

        passed = invalid_delay_order == 0

        print(
            f"Check 7  - Min <= Avg <= Max       : "
            f"{invalid_delay_order} violations -> "
            f"{'PASSED' if passed else 'FAILED'}"
        )

        all_passed = all_passed and passed


        # --------------------------------------------------------
        # Check 8: No negative propagation delays
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
              AND (
                    min_observer_delay_ms < 0
                    OR
                    avg_observer_delay_ms < 0
                    OR
                    max_observer_delay_ms < 0
                  );
        """, (BATCH_ID,))

        negative_delays = cur.fetchone()[0]

        passed = negative_delays == 0

        print(
            f"Check 8  - Negative delays         : "
            f"{negative_delays} -> "
            f"{'PASSED' if passed else 'FAILED'}"
        )

        all_passed = all_passed and passed


        # --------------------------------------------------------
        # Check 9: Ratio safety
        # --------------------------------------------------------

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
              AND observations_as_observer = 0
              AND propagation_to_observation_ratio IS NOT NULL;
        """, (BATCH_ID,))

        invalid_ratio = cur.fetchone()[0]

        passed = invalid_ratio == 0

        print(
            f"Check 9  - Division-by-zero safety  : "
            f"{invalid_ratio} violations -> "
            f"{'PASSED' if passed else 'FAILED'}"
        )

        all_passed = all_passed and passed


        # ========================================================
        # 5. GLOBAL METRICS
        # ========================================================

        print()
        print("--- Global Node Metrics ---")

        cur.execute("""
            SELECT
                COUNT(*),
                COALESCE(SUM(total_connections), 0),
                COALESCE(SUM(observations_as_observer), 0),
                COALESCE(SUM(propagations_as_peer), 0),
                COUNT(*) FILTER (
                    WHERE observations_as_observer > 0
                ),
                COUNT(*) FILTER (
                    WHERE propagations_as_peer > 0
                )
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s;
        """, (BATCH_ID,))

        (
            total_nodes,
            total_connections,
            total_observations,
            total_propagations,
            observer_nodes,
            peer_nodes
        ) = cur.fetchone()

        print(
            f"  Total nodes                  : {total_nodes}"
        )

        print(
            f"  Total node connection records: "
            f"{total_connections}"
        )

        print(
            f"  Total observations           : "
            f"{total_observations}"
        )

        print(
            f"  Total peer propagations      : "
            f"{total_propagations}"
        )

        print(
            f"  Nodes acting as observers    : "
            f"{observer_nodes}"
        )

        print(
            f"  Nodes acting as peers        : "
            f"{peer_nodes}"
        )


        # ========================================================
        # 6. SAMPLE NODE SUMMARIES
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
                propagations_as_peer DESC
            LIMIT 6;
        """, (BATCH_ID,))

        rows = cur.fetchall()

        print(
            "  NODE  | TYPE                 | CTRY | "
            "DEGREE | CONNS | OBS_CNT | TX_OBS | "
            "AVG_DELAY | PROP_CNT | PROP/OBS"
        )

        print(
            "  " + "-" * 105
        )

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

            node_type = node_type or "unknown"
            country = country or "--"

            delay_text = (
                f"{float(avg_delay):.2f}ms"
                if avg_delay is not None
                else "NULL"
            )

            ratio_text = (
                f"{float(ratio):.4f}"
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
        # 7. FINAL RESULT
        # ========================================================

        print()
        print("=" * 60)

        if all_passed:
            print(
                " VERIFICATION AUDIT RESULT: "
                "ALL CHECKS PASSED"
            )
        else:
            print(
                " VERIFICATION AUDIT RESULT: "
                "SOME CHECKS FAILED"
            )

        print("=" * 60)

    except Exception:

        conn.rollback()
        raise

    finally:

        cur.close()
        conn.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    create_and_verify_node_summary()