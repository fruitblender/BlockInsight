"""
Correlation Stage E: Transaction Propagation Timeline and Propagation Features
Generates timeline sequences and propagation duration/delay statistics.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def run_stage_e():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print(f"  CORRELATION STAGE E: TRANSACTION PROPAGATION TIMELINE (BATCH {BATCH_ID})")
    print("============================================================\n")

    # 1. Dataset-Wide Propagation Summary Across 498 Observed Transactions
    cur.execute("""
        SELECT
            COUNT(*) AS total_tx_analyzed,
            ROUND(AVG(total_observations), 2) AS avg_obs_per_tx,
            MIN(total_observations) AS min_obs_per_tx,
            MAX(total_observations) AS max_obs_per_tx,
            ROUND(AVG(avg_delay_ms), 2) AS overall_avg_delay_ms,
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
                COUNT(*) FILTER (WHERE message_type = 'inv') AS inv_count,
                COUNT(*) FILTER (WHERE message_type = 'tx') AS tx_count
            FROM core.transaction_observations
            WHERE batch_id = %s
            GROUP BY txid
        ) sub;
    """, (BATCH_ID,))

    row = cur.fetchone()
    print("--- Propagation Dataset Overview ---")
    print(f"  Transactions analyzed in network  : {row[0]}")
    print(f"  Observations per transaction      : avg={row[1]}, min={row[2]}, max={row[3]}")
    print(f"  Propagation delay across network  : min={row[5]}ms, max={row[6]}ms, avg={row[4]}ms")
    print(f"  Message distribution across batch : {row[7]} 'inv' announcements, {row[8]} 'tx' messages")
    print()

    # 2. Pick a sample transaction to view its complete propagation timeline
    cur.execute("""
        SELECT txid
        FROM core.transaction_observations
        WHERE batch_id = %s
        GROUP BY txid
        HAVING COUNT(*) >= 8
        ORDER BY txid
        LIMIT 1;
    """, (BATCH_ID,))
    sample_txid = cur.fetchone()[0]

    # Fetch transaction metadata
    cur.execute("""
        SELECT txid, timestamp, fee, size, rarity_score, tema
        FROM core.transactions
        WHERE txid = %s;
    """, (sample_txid,))
    tx_meta = cur.fetchone()

    print(f"--- Detailed Propagation Timeline for Transaction ---")
    print(f"  TXID        : {tx_meta[0]}")
    print(f"  Created At  : {tx_meta[1]}")
    print(f"  Fee / Size  : {tx_meta[2]} BTC / {tx_meta[3]} bytes")
    print(f"  Theme/Score : {tx_meta[5]} (rarity: {tx_meta[4]})")
    print()

    # Fetch timeline observations
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
        JOIN core.nodes obs ON o.observer_id = obs.node_id
        JOIN core.nodes peer ON o.peer_id = peer.node_id
        WHERE o.batch_id = %s AND o.txid = %s
        ORDER BY o.sequence_number, o.timestamp;
    """, (BATCH_ID, sample_txid))

    timeline_rows = cur.fetchall()
    print("  SEQUENCE | OBS TIME                    | DELAY (ms) | OBSERVER NODE        | PROPAGATING PEER            | MSG")
    print("  " + "-" * 95)
    for seq, t, delay, obs_id, obs_ctry, obs_type, peer_id, peer_ctry, peer_type, msg in timeline_rows:
        print(f"  #{seq:2d}      | {t} | {delay:9.3f}  | {obs_id} ({obs_ctry:2s})         | {peer_id} ({peer_ctry:2s}, {peer_type:17s}) | {msg}")
    print()

    # Propagation span for this transaction
    t_first = timeline_rows[0][1]
    t_last = timeline_rows[-1][1]
    span_ms = (t_last - t_first).total_seconds() * 1000.0
    print(f"  Propagation start (first seen): {t_first}")
    print(f"  Propagation end   (last seen) : {t_last}")
    print(f"  Total observed propagation duration: {span_ms:.3f} ms across {len(timeline_rows)} nodes")

    print("\n============================================================")
    print("               STAGE E VERIFICATION COMPLETE                ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_stage_e()
