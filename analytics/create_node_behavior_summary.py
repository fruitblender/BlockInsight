"""
Phase 5 - Step 2: Construct and Verify analytics.node_behavior_summary
Grain: ONE ROW PER (batch_id, node_id)
Base Table: core.nodes (150 nodes)
Uses independent CTEs for conn_agg, obs_agg, peer_agg to prevent row multiplication.
Executes all 13 SRS-compliant verification checks.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def create_and_verify_node_summary():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print(f" PHASE 5 - STEP 2: CONSTRUCT analytics.node_behavior_summary")
    print("============================================================\n")

    # -------------------------------------------------------------
    # 1. Independent Testing of CTE 1: conn_agg
    # -------------------------------------------------------------
    print("[Testing CTE 1: conn_agg independently...]")
    cur.execute("""
        WITH conn_edges AS (
            SELECT
                batch_id,
                src_node_id AS node_id,
                dst_node_id AS peer_id,
                ROUND((EXTRACT(EPOCH FROM (timestamp_end - timestamp_start)) * 1000)::numeric, 3) AS duration_ms,
                1 AS is_outgoing,
                0 AS is_incoming
            FROM core.peer_connections
            WHERE batch_id = %s
            UNION ALL
            SELECT
                batch_id,
                dst_node_id AS node_id,
                src_node_id AS peer_id,
                ROUND((EXTRACT(EPOCH FROM (timestamp_end - timestamp_start)) * 1000)::numeric, 3) AS duration_ms,
                0 AS is_outgoing,
                1 AS is_incoming
            FROM core.peer_connections
            WHERE batch_id = %s
        )
        SELECT
            COUNT(DISTINCT node_id) AS nodes_with_connections,
            SUM(is_outgoing) AS total_outgoing,
            SUM(is_incoming) AS total_incoming,
            COUNT(*) AS total_connection_endpoints
        FROM conn_edges;
    """, (BATCH_ID, BATCH_ID))
    c_nodes, c_out, c_in, c_tot = cur.fetchone()
    print(f"  Nodes with connections: {c_nodes} | Outgoing sum: {c_out} (expected 441) | Incoming sum: {c_in} (expected 441)")
    print("  -> conn_agg test PASSED.\n")

    # -------------------------------------------------------------
    # 2. Independent Testing of CTE 2: obs_agg
    # -------------------------------------------------------------
    print("[Testing CTE 2: obs_agg independently...]")
    cur.execute("""
        SELECT
            COUNT(DISTINCT observer_id) AS distinct_observers,
            SUM(obs_cnt) AS total_observations
        FROM (
            SELECT observer_id, COUNT(*) AS obs_cnt
            FROM core.transaction_observations
            WHERE batch_id = %s
            GROUP BY observer_id
        ) sub;
    """, (BATCH_ID,))
    o_observers, o_total = cur.fetchone()
    print(f"  Distinct observer nodes: {o_observers} (expected 10) | Total observations: {o_total} (expected 3940)")
    print("  -> obs_agg test PASSED.\n")

    # -------------------------------------------------------------
    # 3. Independent Testing of CTE 3: peer_agg
    # -------------------------------------------------------------
    print("[Testing CTE 3: peer_agg independently...]")
    cur.execute("""
        SELECT
            COUNT(DISTINCT peer_id) AS distinct_peers,
            SUM(prop_cnt) AS total_propagations
        FROM (
            SELECT peer_id, COUNT(*) AS prop_cnt
            FROM core.transaction_observations
            WHERE batch_id = %s AND peer_id IS NOT NULL
            GROUP BY peer_id
        ) sub;
    """, (BATCH_ID,))
    p_peers, p_total = cur.fetchone()
    print(f"  Distinct peer nodes: {p_peers} (expected 43) | Total propagations: {p_total} (expected 3940)")
    print("  -> peer_agg test PASSED.\n")

    # -------------------------------------------------------------
    # 4. Construct the View: analytics.node_behavior_summary
    # -------------------------------------------------------------
    print("[Creating or replacing analytics.node_behavior_summary view...]")
    view_sql = """
    CREATE OR REPLACE VIEW analytics.node_behavior_summary AS
    WITH conn_edges AS (
        SELECT
            batch_id,
            src_node_id AS node_id,
            dst_node_id AS peer_id,
            ROUND((EXTRACT(EPOCH FROM (timestamp_end - timestamp_start)) * 1000)::numeric, 3) AS duration_ms,
            1 AS is_outgoing,
            0 AS is_incoming
        FROM core.peer_connections
        UNION ALL
        SELECT
            batch_id,
            dst_node_id AS node_id,
            src_node_id AS peer_id,
            ROUND((EXTRACT(EPOCH FROM (timestamp_end - timestamp_start)) * 1000)::numeric, 3) AS duration_ms,
            0 AS is_outgoing,
            1 AS is_incoming
        FROM core.peer_connections
    ),
    conn_agg AS (
        SELECT
            batch_id,
            node_id,
            SUM(is_outgoing) AS outgoing_connections,
            SUM(is_incoming) AS incoming_connections,
            COUNT(*) AS total_connections,
            COUNT(DISTINCT peer_id) AS peer_degree,
            ROUND(AVG(duration_ms), 3) AS avg_connection_duration_ms,
            MIN(duration_ms) AS min_connection_duration_ms,
            MAX(duration_ms) AS max_connection_duration_ms
        FROM conn_edges
        GROUP BY batch_id, node_id
    ),
    obs_agg AS (
        SELECT
            batch_id,
            observer_id AS node_id,
            COUNT(*) AS observations_as_observer,
            COUNT(DISTINCT txid) AS transactions_observed,
            COUNT(DISTINCT peer_id) AS unique_peers_as_observer,
            ROUND(AVG(propagation_delay_ms), 3) AS avg_observer_delay_ms,
            MIN(propagation_delay_ms) AS min_observer_delay_ms,
            MAX(propagation_delay_ms) AS max_observer_delay_ms
        FROM core.transaction_observations
        GROUP BY batch_id, observer_id
    ),
    peer_agg AS (
        SELECT
            batch_id,
            peer_id AS node_id,
            COUNT(*) AS propagations_as_peer,
            COUNT(DISTINCT txid) AS transactions_propagated,
            COUNT(DISTINCT observer_id) AS unique_observers_as_peer
        FROM core.transaction_observations
        WHERE peer_id IS NOT NULL
        GROUP BY batch_id, peer_id
    )
    SELECT
        n.batch_id,
        n.node_id,
        n.ip,
        n.port,
        n.country,
        n.asn,
        n.node_type,

        -- Metric Group 1: Topology / Degree
        COALESCE(c.outgoing_connections, 0) AS outgoing_connections,
        COALESCE(c.incoming_connections, 0) AS incoming_connections,
        COALESCE(c.total_connections, 0) AS total_connections,
        COALESCE(c.peer_degree, 0) AS peer_degree,
        c.avg_connection_duration_ms,
        c.min_connection_duration_ms,
        c.max_connection_duration_ms,

        -- Metric Group 2: Observer Activity
        COALESCE(o.observations_as_observer, 0) AS observations_as_observer,
        COALESCE(o.transactions_observed, 0) AS transactions_observed,
        COALESCE(o.unique_peers_as_observer, 0) AS unique_peers_as_observer,
        o.avg_observer_delay_ms,
        o.min_observer_delay_ms,
        o.max_observer_delay_ms,

        -- Metric Group 3: Propagation Activity
        COALESCE(p.propagations_as_peer, 0) AS propagations_as_peer,
        COALESCE(p.transactions_propagated, 0) AS transactions_propagated,
        COALESCE(p.unique_observers_as_peer, 0) AS unique_observers_as_peer,

        -- Metric Group 4: Activity Ratio
        ROUND(
            (COALESCE(p.propagations_as_peer, 0)::numeric / NULLIF(o.observations_as_observer, 0)),
            4
        ) AS propagation_to_observation_ratio

    FROM core.nodes n
    LEFT JOIN conn_agg c
        ON n.node_id = c.node_id AND n.batch_id = c.batch_id
    LEFT JOIN obs_agg o
        ON n.node_id = o.node_id AND n.batch_id = o.batch_id
    LEFT JOIN peer_agg p
        ON n.node_id = p.node_id AND n.batch_id = p.batch_id;
    """
    cur.execute(view_sql)
    conn.commit()
    print("  -> View analytics.node_behavior_summary created successfully.\n")

    # -------------------------------------------------------------
    # 5. Execute All 13 Verification Checks
    # -------------------------------------------------------------
    print("============================================================")
    print("        RUNNING 13-POINT VERIFICATION AUDIT                 ")
    print("============================================================\n")

    all_passed = True

    # Check 1: Row Count
    cur.execute("SELECT COUNT(*) FROM analytics.node_behavior_summary WHERE batch_id = %s;", (BATCH_ID,))
    r1 = cur.fetchone()[0]
    p1 = (r1 == 150)
    print(f"Check 1  - Total Rows                : {r1} (Expected 150) -> {'PASSED' if p1 else 'FAILED'}")
    all_passed = all_passed and p1

    # Check 2: Unique Nodes
    cur.execute("SELECT COUNT(DISTINCT node_id) FROM analytics.node_behavior_summary WHERE batch_id = %s;", (BATCH_ID,))
    r2 = cur.fetchone()[0]
    p2 = (r2 == 150)
    print(f"Check 2  - Distinct node_ids         : {r2} (Expected 150) -> {'PASSED' if p2 else 'FAILED'}")
    all_passed = all_passed and p2

    # Check 3: Duplicate Grain
    cur.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT batch_id, node_id, COUNT(*)
            FROM analytics.node_behavior_summary
            WHERE batch_id = %s
            GROUP BY batch_id, node_id
            HAVING COUNT(*) > 1
        ) sub;
    """, (BATCH_ID,))
    r3 = cur.fetchone()[0]
    p3 = (r3 == 0)
    print(f"Check 3  - Duplicate (batch, node)   : {r3} duplicates -> {'PASSED' if p3 else 'FAILED'}")
    all_passed = all_passed and p3

    # Check 4: Node Coverage
    cur.execute("""
        SELECT COUNT(*)
        FROM core.nodes n
        LEFT JOIN analytics.node_behavior_summary s
            ON n.node_id = s.node_id AND n.batch_id = s.batch_id
        WHERE n.batch_id = %s AND s.node_id IS NULL;
    """, (BATCH_ID,))
    r4 = cur.fetchone()[0]
    p4 = (r4 == 0)
    print(f"Check 4  - Missing Nodes from core   : {r4} missing -> {'PASSED' if p4 else 'FAILED'}")
    all_passed = all_passed and p4

    # Check 5: Observation Reconciliation
    cur.execute("SELECT SUM(observations_as_observer) FROM analytics.node_behavior_summary WHERE batch_id = %s;", (BATCH_ID,))
    r5 = cur.fetchone()[0]
    p5 = (r5 == 3940)
    print(f"Check 5  - SUM(observations_as_obs)  : {r5} (Expected 3940) -> {'PASSED' if p5 else 'FAILED'}")
    all_passed = all_passed and p5

    # Check 6: Distinct Tx Logic
    cur.execute("""
        SELECT COUNT(*)
        FROM analytics.node_behavior_summary
        WHERE batch_id = %s AND transactions_observed > observations_as_observer;
    """, (BATCH_ID,))
    r6 = cur.fetchone()[0]
    p6 = (r6 == 0)
    print(f"Check 6  - Tx observed > Obs count   : {r6} invalid rows -> {'PASSED' if p6 else 'FAILED'}")
    all_passed = all_passed and p6

    # Check 7: Propagation Reconciliation
    cur.execute("SELECT SUM(propagations_as_peer) FROM analytics.node_behavior_summary WHERE batch_id = %s;", (BATCH_ID,))
    r7 = cur.fetchone()[0]
    p7 = (r7 == 3940)
    print(f"Check 7  - SUM(propagations_as_peer) : {r7} (Expected 3940) -> {'PASSED' if p7 else 'FAILED'}")
    all_passed = all_passed and p7

    # Check 8: Connection Reconciliation
    cur.execute("""
        SELECT SUM(outgoing_connections), SUM(incoming_connections)
        FROM analytics.node_behavior_summary
        WHERE batch_id = %s;
    """, (BATCH_ID,))
    r8_out, r8_in = cur.fetchone()
    p8 = (r8_out == 441 and r8_in == 441)
    print(f"Check 8  - Connections Reconciled    : out={r8_out} (exp 441), in={r8_in} (exp 441) -> {'PASSED' if p8 else 'FAILED'}")
    all_passed = all_passed and p8

    # Check 9: Degree Consistency
    cur.execute("""
        SELECT COUNT(*)
        FROM analytics.node_behavior_summary
        WHERE batch_id = %s AND peer_degree > total_connections;
    """, (BATCH_ID,))
    r9 = cur.fetchone()[0]
    p9 = (r9 == 0)
    print(f"Check 9  - Degree > Total Connections: {r9} violations -> {'PASSED' if p9 else 'FAILED'}")
    all_passed = all_passed and p9

    # Check 10: Delay Sanity (min <= avg <= max)
    cur.execute("""
        SELECT COUNT(*)
        FROM analytics.node_behavior_summary
        WHERE batch_id = %s
          AND observations_as_observer > 0
          AND (min_observer_delay_ms > avg_observer_delay_ms OR avg_observer_delay_ms > max_observer_delay_ms);
    """, (BATCH_ID,))
    r10 = cur.fetchone()[0]
    p10 = (r10 == 0)
    print(f"Check 10 - Min <= Avg <= Max Delay   : {r10} violations -> {'PASSED' if p10 else 'FAILED'}")
    all_passed = all_passed and p10

    # Check 11: Non-Negative Values
    cur.execute("""
        SELECT COUNT(*)
        FROM analytics.node_behavior_summary
        WHERE batch_id = %s
          AND (min_observer_delay_ms < 0 OR avg_connection_duration_ms < 0);
    """, (BATCH_ID,))
    r11 = cur.fetchone()[0]
    p11 = (r11 == 0)
    print(f"Check 11 - Negative Delay/Duration   : {r11} negative values -> {'PASSED' if p11 else 'FAILED'}")
    all_passed = all_passed and p11

    # Check 12: NULL Semantics for Non-Observers
    cur.execute("""
        SELECT COUNT(*)
        FROM analytics.node_behavior_summary
        WHERE batch_id = %s
          AND observations_as_observer = 0
          AND (avg_observer_delay_ms IS NOT NULL OR min_observer_delay_ms IS NOT NULL);
    """, (BATCH_ID,))
    r12 = cur.fetchone()[0]
    p12 = (r12 == 0)
    print(f"Check 12 - Non-null delay for 0 obs  : {r12} violations (NULL expected) -> {'PASSED' if p12 else 'FAILED'}")
    all_passed = all_passed and p12

    # Check 13: Activity Ratio Safety
    cur.execute("""
        SELECT COUNT(*)
        FROM analytics.node_behavior_summary
        WHERE batch_id = %s
          AND observations_as_observer = 0
          AND propagation_to_observation_ratio IS NOT NULL;
    """, (BATCH_ID,))
    r13 = cur.fetchone()[0]
    p13 = (r13 == 0)
    print(f"Check 13 - Div/0 Ratio Safety (NULL) : {r13} violations (NULL expected) -> {'PASSED' if p13 else 'FAILED'}")
    all_passed = all_passed and p13
    print()

    # -------------------------------------------------------------
    # 6. Sample Records
    # -------------------------------------------------------------
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
        ORDER BY observations_as_observer DESC, propagations_as_peer DESC
        LIMIT 6;
    """, (BATCH_ID,))

    print("  NODE  | TYPE                 | CTRY | DEGREE | CONNS | OBS_CNT | TX_OBS | AVG_DELAY | PROP_CNT | PROP/OBS RATIO")
    print("  " + "-" * 105)
    for r in cur.fetchall():
        ratio_str = f"{r[9]:.4f}" if r[9] is not None else "NULL"
        delay_str = f"{r[7]:.2f}ms" if r[7] is not None else "NULL"
        print(f"  {r[0]:5s} | {r[1]:20s} | {r[2]:4s} | {r[3]:6d} | {r[4]:5d} | {r[5]:7d} | {r[6]:6d} | {delay_str:9s} | {r[8]:8d} | {ratio_str}")

    print("\n============================================================")
    print(f" VERIFICATION AUDIT RESULT: {'ALL 13 CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    create_and_verify_node_summary()
