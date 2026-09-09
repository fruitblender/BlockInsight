"""
Correlation Stage E: Transaction Propagation Timeline and Propagation Features

Generates:
- transaction-level propagation statistics
- observation/message statistics
- detailed propagation timeline for a sample transaction
- propagation duration across observations
"""

import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from database import get_connection


def run_stage_e():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print("  CORRELATION STAGE E: TRANSACTION PROPAGATION TIMELINE")
    print("============================================================\n")

    # ----------------------------------------------------------
    # 1. Dataset-wide propagation summary
    # ----------------------------------------------------------

    cur.execute("""
        SELECT
            COUNT(*) AS total_tx_analyzed,
            ROUND(AVG(total_observations), 2) AS avg_obs_per_tx,
            MIN(total_observations) AS min_obs_per_tx,
            MAX(total_observations) AS max_obs_per_tx,
            ROUND(AVG(avg_delay_ms)::numeric, 2) AS overall_avg_delay_ms,
            MIN(min_delay_ms) AS overall_min_delay_ms,
            MAX(max_delay_ms) AS overall_max_delay_ms,
            SUM(inv_count) AS total_inv,
            SUM(tx_count) AS total_tx_msgs
        FROM (
            SELECT
                txid,
                COUNT(*) AS total_observations,
                MIN(propagation_delay_ms) AS min_delay_ms,
                MAX(propagation_delay_ms) AS max_delay_ms,
                AVG(propagation_delay_ms) AS avg_delay_ms,

                COUNT(*) FILTER (
                    WHERE message_type = 'inv'
                ) AS inv_count,

                COUNT(*) FILTER (
                    WHERE message_type = 'tx'
                ) AS tx_count

            FROM core.transaction_observations

            GROUP BY txid
        ) sub;
    """)

    row = cur.fetchone()

    print("--- Propagation Dataset Overview ---")
    print(f"  Transactions analyzed in network  : {row[0]}")
    print(
        f"  Observations per transaction      : "
        f"avg={row[1]}, min={row[2]}, max={row[3]}"
    )
    print(
        f"  Propagation delay across network  : "
        f"min={row[5]}ms, max={row[6]}ms, avg={row[4]}ms"
    )
    print(
        f"  Message distribution across batch : "
        f"{row[7]} 'inv' announcements, {row[8]} 'tx' messages"
    )
    print()

    # ----------------------------------------------------------
    # 2. Select a sample transaction
    # ----------------------------------------------------------

    cur.execute("""
        SELECT txid
        FROM core.transaction_observations
        GROUP BY txid
        HAVING COUNT(*) >= 8
        ORDER BY txid
        LIMIT 1;
    """)

    sample_result = cur.fetchone()

    if sample_result is None:
        print("No transaction has at least 8 observations.")
        print("\n============================================================")
        print("               STAGE E VERIFICATION COMPLETE")
        print("============================================================")

        cur.close()
        conn.close()
        return

    sample_txid = sample_result[0]

    # ----------------------------------------------------------
    # 3. Fetch transaction metadata
    # ----------------------------------------------------------

    cur.execute("""
        SELECT
            txid,
            timestamp,
            fee,
            size,
            rarity_score,
            tema
        FROM core.transactions
        WHERE txid = %s;
    """, (sample_txid,))

    tx_meta = cur.fetchone()

    if tx_meta is None:
        print("Sample transaction was not found in core.transactions.")

        cur.close()
        conn.close()
        return

    print("--- Detailed Propagation Timeline for Transaction ---")
    print(f"  TXID        : {tx_meta[0]}")
    print(f"  Created At  : {tx_meta[1]}")
    print(f"  Fee / Size  : {tx_meta[2]} BTC / {tx_meta[3]} bytes")
    print(f"  Theme/Score : {tx_meta[5]} (rarity: {tx_meta[4]})")
    print()

    # ----------------------------------------------------------
    # 4. Fetch propagation timeline
    # ----------------------------------------------------------

    cur.execute("""
        SELECT
            o.sequence_number,
            o.timestamp,
            o.propagation_delay_ms,
            o.observer_id,
            obs.country AS obs_country,
            obs.node_type AS obs_type,
            o.peer_id,
            peer.country AS peer_country,
            peer.node_type AS peer_type,
            o.message_type

        FROM core.transaction_observations o

        JOIN core.nodes obs
            ON o.observer_id = obs.node_id

        JOIN core.nodes peer
            ON o.peer_id = peer.node_id

        WHERE o.txid = %s

        ORDER BY o.sequence_number, o.timestamp;
    """, (sample_txid,))

    timeline_rows = cur.fetchall()

    print(
        "  SEQUENCE | OBS TIME                    | DELAY (ms) | "
        "OBSERVER NODE        | PROPAGATING PEER            | MSG"
    )

    print("  " + "-" * 95)

    for (
        seq,
        timestamp,
        delay,
        obs_id,
        obs_country,
        obs_type,
        peer_id,
        peer_country,
        peer_type,
        message_type
    ) in timeline_rows:

        delay_value = delay if delay is not None else 0.0

        print(
            f"  #{seq:2d}      | {timestamp} | "
            f"{delay_value:9.3f}  | "
            f"{obs_id} ({obs_country:2s})         | "
            f"{peer_id} ({peer_country:2s}, "
            f"{peer_type:17s}) | "
            f"{message_type}"
        )

    print()

    # ----------------------------------------------------------
    # 5. Propagation span
    # ----------------------------------------------------------

    if timeline_rows:

        t_first = timeline_rows[0][1]
        t_last = timeline_rows[-1][1]

        span_ms = (
            t_last - t_first
        ).total_seconds() * 1000.0

        print(f"  Propagation start (first seen): {t_first}")
        print(f"  Propagation end   (last seen) : {t_last}")
        print(
            f"  Total observed propagation duration: "
            f"{span_ms:.3f} ms across {len(timeline_rows)} observations"
        )

    # ----------------------------------------------------------
    # 6. Verification
    # ----------------------------------------------------------

    print()
    print("--- Stage E Verification ---")

    cur.execute("""
        SELECT COUNT(*)
        FROM core.transaction_observations;
    """)

    total_observations = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(DISTINCT txid)
        FROM core.transaction_observations;
    """)

    distinct_transactions = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM core.transactions;
    """)

    total_transactions = cur.fetchone()[0]

    print(f"  Total observations              : {total_observations}")
    print(f"  Distinct observed transactions : {distinct_transactions}")
    print(f"  Transactions in core           : {total_transactions}")

    if distinct_transactions <= total_transactions:
        print("  Transaction coverage: PASSED")
    else:
        print("  Transaction coverage: FAILED")

    print("\n============================================================")
    print("               STAGE E VERIFICATION COMPLETE")
    print("============================================================")

    cur.close()
    conn.close()


if __name__ == "__main__":
    run_stage_e()
