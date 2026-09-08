"""
Phase 5 - Step 3: Create View 'analytics.transaction_propagation_summary'
Grain: ONE ROW PER TRANSACTION PER BATCH
Aggregates observation counts, propagation spans, delays, sequence, and peer characteristics.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def create_and_verify_summary():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print("   PHASE 5 - STEP 3: CREATE transaction_propagation_summary")
    print("============================================================\n")

    summary_view_sql = """
    CREATE OR REPLACE VIEW analytics.transaction_propagation_summary AS
    WITH ranked_observations AS (
        SELECT
            tp.batch_id,
            tp.txid,
            tp.observation_id,
            tp.observer_id,
            tp.peer_id,
            tp.peer_node_type,
            ROW_NUMBER() OVER (
                PARTITION BY tp.batch_id, tp.txid
                ORDER BY tp.sequence_number, tp.observation_timestamp
            ) AS rn_first
        FROM analytics.transaction_propagation tp
    )
    SELECT
        tp.batch_id,
        tp.txid,

        -- Transaction Layer Metadata
        t.timestamp AS transaction_timestamp,
        t.fee,
        t.size,
        t.rarity_score,
        t.tema,

        -- Network Observation Counts
        COUNT(*) AS total_observations,
        COUNT(DISTINCT tp.observer_id) AS unique_observers,
        COUNT(DISTINCT tp.peer_id) AS unique_peers,
        COUNT(DISTINCT tp.observer_country) AS unique_observer_countries,
        COUNT(DISTINCT tp.peer_country) AS unique_peer_countries,

        -- Initial Observer & Peer (Observed Sequence #1)
        MAX(CASE WHEN ro.rn_first = 1 THEN ro.observer_id END) AS first_observer_id,
        MAX(CASE WHEN ro.rn_first = 1 THEN ro.peer_id END) AS first_peer_id,
        MAX(CASE WHEN ro.rn_first = 1 THEN ro.peer_node_type END) AS first_peer_type,

        -- Chronological Timing Windows
        MIN(tp.observation_timestamp) AS first_observation_time,
        MAX(tp.observation_timestamp) AS last_observation_time,

        -- Time from TX creation to first network detection (ms)
        ROUND((EXTRACT(EPOCH FROM (MIN(tp.observation_timestamp) - t.timestamp)) * 1000)::numeric, 3)
            AS creation_to_first_observation_ms,

        -- Observed propagation span across the monitoring nodes (ms)
        ROUND((EXTRACT(EPOCH FROM (MAX(tp.observation_timestamp) - MIN(tp.observation_timestamp))) * 1000)::numeric, 3)
            AS observed_propagation_span_ms,

        -- Propagation Delay Metrics (ms)
        MIN(tp.propagation_delay_ms) AS min_propagation_delay_ms,
        MAX(tp.propagation_delay_ms) AS max_propagation_delay_ms,
        ROUND(AVG(tp.propagation_delay_ms), 3) AS avg_propagation_delay_ms,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tp.propagation_delay_ms))::numeric, 3)
            AS median_propagation_delay_ms,

        -- Sequence Metrics
        MIN(tp.sequence_number) AS min_sequence,
        MAX(tp.sequence_number) AS max_sequence,

        -- Message Type Ratios
        COUNT(*) FILTER (WHERE tp.message_type = 'inv') AS inv_count,
        COUNT(*) FILTER (WHERE tp.message_type = 'tx') AS tx_count,
        ROUND(COUNT(*) FILTER (WHERE tp.message_type = 'inv')::numeric / COUNT(*), 4) AS inv_ratio,
        ROUND(COUNT(*) FILTER (WHERE tp.message_type = 'tx')::numeric / COUNT(*), 4) AS tx_ratio

    FROM analytics.transaction_propagation tp
    JOIN core.transactions t
        ON tp.txid = t.txid AND tp.batch_id = t.batch_id
    LEFT JOIN ranked_observations ro
        ON tp.observation_id = ro.observation_id AND tp.batch_id = ro.batch_id
    GROUP BY
        tp.batch_id,
        tp.txid,
        t.timestamp,
        t.fee,
        t.size,
        t.rarity_score,
        t.tema;
    """

    cur.execute(summary_view_sql)
    conn.commit()
    print("  Created/Updated view 'analytics.transaction_propagation_summary'.\n")

    # Verification 1: Row count
    cur.execute("SELECT COUNT(*) FROM analytics.transaction_propagation_summary WHERE batch_id = %s;", (BATCH_ID,))
    summary_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT txid) FROM core.transaction_observations WHERE batch_id = %s;", (BATCH_ID,))
    expected_count = cur.fetchone()[0]

    print("--- Row Count Verification ---")
    print(f"  Distinct txids in core.transaction_observations : {expected_count}")
    print(f"  Rows in analytics.transaction_propagation_summary: {summary_count}")
    print(f"  Status: {'PASSED (Exactly 1 row per observed transaction)' if summary_count == expected_count else 'FAILED'}")
    print()

    # Verification 2: Reconcile with Stage E Global Metrics
    print("--- Reconciliation with Python Stage E Results ---")
    cur.execute("""
        SELECT
            COUNT(*),
            ROUND(AVG(total_observations), 2),
            MIN(min_propagation_delay_ms),
            MAX(max_propagation_delay_ms),
            ROUND(AVG(avg_propagation_delay_ms), 2),
            SUM(inv_count),
            SUM(tx_count),
            ROUND(AVG(observed_propagation_span_ms), 3)
        FROM analytics.transaction_propagation_summary
        WHERE batch_id = %s;
    """, (BATCH_ID,))
    r = cur.fetchone()

    print(f"  Transactions Count      : {r[0]} (Expected: 498)")
    print(f"  Avg Obs per Transaction : {r[1]} (Expected: 7.91)")
    print(f"  Min Propagation Delay   : {r[2]} ms (Expected: 5.0 ms)")
    print(f"  Max Propagation Delay   : {r[3]} ms (Expected: 373.921 ms)")
    print(f"  Avg Propagation Delay   : {r[4]} ms (Expected: 102.61 ms)")
    print(f"  Total inv Announcements : {r[5]} (Expected: 3917)")
    print(f"  Total tx Messages       : {r[6]} (Expected: 23)")
    print(f"  Avg Propagation Span    : {r[7]} ms")
    print()

    # Verification 3: Reconcile with Example Transaction
    sample_txid = "0032371456e60b5050e79097b1ec75dd764e620aad2f0c422807e81a1ed165e4"
    cur.execute("""
        SELECT
            txid,
            total_observations,
            unique_observers,
            unique_peers,
            first_observer_id,
            first_peer_id,
            first_peer_type,
            creation_to_first_observation_ms,
            observed_propagation_span_ms,
            min_propagation_delay_ms,
            max_propagation_delay_ms,
            median_propagation_delay_ms,
            inv_count,
            tx_count
        FROM analytics.transaction_propagation_summary
        WHERE batch_id = %s AND txid = %s;
    """, (BATCH_ID, sample_txid))

    ex = cur.fetchone()
    print(f"--- Reconciling Example Transaction ({sample_txid[:12]}...) ---")
    print(f"  Observations / Observers / Peers : {ex[1]} / {ex[2]} / {ex[3]}")
    print(f"  First Observer <- First Peer    : {ex[4]} <- {ex[5]} ({ex[6]})")
    print(f"  Creation -> First Observation   : {ex[7]} ms (Expected: 77.795 ms)")
    print(f"  Observed Propagation Span       : {ex[8]} ms (Expected: 72.663 ms)")
    print(f"  Min / Max / Median Delays       : {ex[9]} / {ex[10]} / {ex[11]} ms")
    print(f"  Message counts                  : inv={ex[12]}, tx={ex[13]}")
    print()

    print("============================================================")
    print("             STEP 3 CREATION & AUDIT COMPLETE               ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    create_and_verify_summary()
