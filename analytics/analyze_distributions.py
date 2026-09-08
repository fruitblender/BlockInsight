"""
Phase 5 - Step 3: Statistical Distribution Analysis & Baseline Profiling
========================================================================
Calculates empirical distributions (min, max, mean, stddev, P50, P75, P90, P95, P99)
across both node behavior metrics (150 nodes) and transaction propagation metrics (498 txs).
"""

import sys
from decimal import Decimal
from pathlib import Path
from psycopg.rows import dict_row

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection


def format_val(val, decimals=2):
    if val is None:
        return "N/A"
    if isinstance(val, (int, float, Decimal)):
        return f"{float(val):.{decimals}f}"
    return str(val)


def run_distribution_analysis():
    conn = get_connection()
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            print("=" * 82)
            print(" PHASE 5 - STEP 3: STATISTICAL DISTRIBUTION ANALYSIS & BASELINE PROFILING")
            print("=" * 82)

            # -------------------------------------------------------------
            # 1. NODE BEHAVIOR DISTRIBUTIONS (OVERALL 150 NODES)
            # -------------------------------------------------------------
            print("\n[1] Node Behavior Metric Distributions (All 150 Nodes in Batch 1)")
            print("-" * 82)

            node_metrics = [
                ("peer_degree", "Peer Degree (Unique Peers)"),
                ("total_connections", "Total Peer Connections"),
                ("observations_as_observer", "Observations as Observer"),
                ("transactions_observed", "Transactions Observed"),
                ("propagations_as_peer", "Propagations as Peer"),
                ("transactions_propagated", "Transactions Propagated"),
                ("avg_observer_delay_ms", "Avg Observer Delay (ms)"),
            ]

            header = f"{'METRIC':<32} | {'MIN':>6} | {'AVG':>8} | {'P50':>6} | {'P75':>6} | {'P90':>6} | {'P95':>6} | {'P99':>6} | {'MAX':>6}"
            print(header)
            print("-" * len(header))

            for col, label in node_metrics:
                cur.execute(f"""
                    SELECT 
                        MIN({col}) AS min_val,
                        AVG({col}) AS avg_val,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {col}) AS p50,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col}) AS p75,
                        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY {col}) AS p90,
                        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {col}) AS p95,
                        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY {col}) AS p99,
                        MAX({col}) AS max_val
                    FROM analytics.node_behavior_summary
                    WHERE batch_id = 1;
                """)
                row = cur.fetchone()
                print(
                    f"{label:<32} | "
                    f"{format_val(row['min_val'], 1):>6} | "
                    f"{format_val(row['avg_val'], 1):>8} | "
                    f"{format_val(row['p50'], 1):>6} | "
                    f"{format_val(row['p75'], 1):>6} | "
                    f"{format_val(row['p90'], 1):>6} | "
                    f"{format_val(row['p95'], 1):>6} | "
                    f"{format_val(row['p99'], 1):>6} | "
                    f"{format_val(row['max_val'], 1):>6}"
                )

            # -------------------------------------------------------------
            # 2. NODE BEHAVIOR BY NODE TYPE
            # -------------------------------------------------------------
            print("\n[2] Node Behavior Breakdown by Node Type")
            print("-" * 82)

            cur.execute("""
                SELECT 
                    node_type,
                    COUNT(*) AS node_count,
                    ROUND(AVG(peer_degree), 1) AS avg_degree,
                    ROUND(AVG(total_connections), 1) AS avg_conns,
                    ROUND(AVG(observations_as_observer), 1) AS avg_obs,
                    ROUND(AVG(propagations_as_peer), 1) AS avg_props,
                    ROUND(AVG(avg_observer_delay_ms), 2) AS avg_delay_ms
                FROM analytics.node_behavior_summary
                WHERE batch_id = 1
                GROUP BY node_type
                ORDER BY node_count DESC;
            """)
            type_rows = cur.fetchall()
            t_header = f"{'NODE TYPE':<22} | {'COUNT':>5} | {'AVG DEG':>7} | {'AVG CONNS':>9} | {'AVG OBS':>8} | {'AVG PROPS':>9} | {'AVG DELAY (ms)':>14}"
            print(t_header)
            print("-" * len(t_header))
            for r in type_rows:
                print(
                    f"{r['node_type']:<22} | "
                    f"{r['node_count']:>5} | "
                    f"{format_val(r['avg_degree'], 1):>7} | "
                    f"{format_val(r['avg_conns'], 1):>9} | "
                    f"{format_val(r['avg_obs'], 1):>8} | "
                    f"{format_val(r['avg_props'], 1):>9} | "
                    f"{format_val(r['avg_delay_ms'], 2):>14}"
                )

            # -------------------------------------------------------------
            # 3. TRANSACTION PROPAGATION DISTRIBUTIONS (498 TRANSACTIONS)
            # -------------------------------------------------------------
            print("\n[3] Transaction Propagation Metric Distributions (498 Transactions)")
            print("-" * 82)

            tx_metrics = [
                ("total_observations", "Observations per Tx"),
                ("unique_observers", "Distinct Observers per Tx"),
                ("unique_peers", "Distinct Propagating Peers"),
                ("creation_to_first_observation_ms", "Creation to 1st Obs (ms)"),
                ("min_propagation_delay_ms", "Min Propagation Delay (ms)"),
                ("max_propagation_delay_ms", "Max Propagation Delay (ms)"),
                ("avg_propagation_delay_ms", "Avg Propagation Delay (ms)"),
                ("observed_propagation_span_ms", "Full Propagation Span (ms)"),
                ("inv_ratio", "Inventory ('inv') Msg Ratio"),
            ]

            print(header)
            print("-" * len(header))
            for col, label in tx_metrics:
                cur.execute(f"""
                    SELECT 
                        MIN({col}) AS min_val,
                        AVG({col}) AS avg_val,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {col}) AS p50,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col}) AS p75,
                        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY {col}) AS p90,
                        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {col}) AS p95,
                        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY {col}) AS p99,
                        MAX({col}) AS max_val
                    FROM analytics.transaction_propagation_summary
                    WHERE batch_id = 1;
                """)
                row = cur.fetchone()
                print(
                    f"{label:<32} | "
                    f"{format_val(row['min_val'], 2):>6} | "
                    f"{format_val(row['avg_val'], 2):>8} | "
                    f"{format_val(row['p50'], 2):>6} | "
                    f"{format_val(row['p75'], 2):>6} | "
                    f"{format_val(row['p90'], 2):>6} | "
                    f"{format_val(row['p95'], 2):>6} | "
                    f"{format_val(row['p99'], 2):>6} | "
                    f"{format_val(row['max_val'], 2):>6}"
                )

            # -------------------------------------------------------------
            # 4. OBSERVATION-LEVEL DELAY DISTRIBUTION (3,940 OBSERVATIONS)
            # -------------------------------------------------------------
            print("\n[4] Observation-Level Delay Percentiles (All 3,940 Observations)")
            print("-" * 82)

            cur.execute("""
                SELECT 
                    MIN(propagation_delay_ms) AS min_delay,
                    AVG(propagation_delay_ms) AS avg_delay,
                    STDDEV(propagation_delay_ms) AS std_delay,
                    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY propagation_delay_ms) AS p05,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY propagation_delay_ms) AS p25,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY propagation_delay_ms) AS p50,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY propagation_delay_ms) AS p75,
                    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY propagation_delay_ms) AS p90,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY propagation_delay_ms) AS p95,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY propagation_delay_ms) AS p99,
                    MAX(propagation_delay_ms) AS max_delay
                FROM analytics.transaction_propagation
                WHERE batch_id = 1;
            """)
            obs_row = cur.fetchone()

            print(f"  Min Delay   : {format_val(obs_row['min_delay'], 2):>8} ms")
            print(f"  P05 Delay   : {format_val(obs_row['p05'], 2):>8} ms")
            print(f"  P25 Delay   : {format_val(obs_row['p25'], 2):>8} ms")
            print(f"  Median (P50): {format_val(obs_row['p50'], 2):>8} ms")
            print(f"  Mean Delay  : {format_val(obs_row['avg_delay'], 2):>8} ms  (StdDev: {format_val(obs_row['std_delay'], 2)} ms)")
            print(f"  P75 Delay   : {format_val(obs_row['p75'], 2):>8} ms")
            print(f"  P90 Delay   : {format_val(obs_row['p90'], 2):>8} ms")
            print(f"  P95 Delay   : {format_val(obs_row['p95'], 2):>8} ms")
            print(f"  P99 Delay   : {format_val(obs_row['p99'], 2):>8} ms")
            print(f"  Max Delay   : {format_val(obs_row['max_delay'], 2):>8} ms")

            print("\n" + "=" * 82)
            print(" STATISTICAL DISTRIBUTION ANALYSIS COMPLETE")
            print("=" * 82)

    finally:
        conn.close()


if __name__ == "__main__":
    run_distribution_analysis()
