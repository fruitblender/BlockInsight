"""
Correlation Stage A: Verify Transaction -> Observations Link
Confirms that core.transaction_observations correctly connects to core.transactions on txid.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def run_stage_a():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print(f"   CORRELATION STAGE A: TRANSACTION -> OBSERVATIONS (BATCH {BATCH_ID})")
    print("============================================================\n")

    # 1. Total counts in the joined view
    cur.execute("""
        SELECT
            COUNT(*) AS total_joined_observations,
            COUNT(DISTINCT o.txid) AS distinct_transactions
        FROM core.transaction_observations o
        JOIN core.transactions t
            ON o.txid = t.txid
        WHERE o.batch_id = %s;
    """, (BATCH_ID,))
    joined_obs, distinct_tx = cur.fetchone()

    # Total counts in core.transaction_observations
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT txid) FROM core.transaction_observations WHERE batch_id = %s;", (BATCH_ID,))
    total_obs, total_obs_tx = cur.fetchone()

    # Total counts in core.transactions
    cur.execute("SELECT COUNT(*) FROM core.transactions WHERE batch_id = %s;", (BATCH_ID,))
    total_core_tx = cur.fetchone()[0]

    print(f"Total observations in core.transaction_observations : {total_obs}")
    print(f"Total joined observations (o.txid = t.txid)         : {joined_obs}")
    print(f"Distinct transactions in observations               : {distinct_tx}")
    print(f"Total transactions in core.transactions             : {total_core_tx}")
    print()

    match_obs = (total_obs == joined_obs)
    print(f"Observation Join Completeness: {'PASSED (100% of observations linked)' if match_obs else 'FAILED'}")
    print(f"Transaction Coverage        : {distinct_tx} / {total_core_tx} transactions observed in network")
    print()

    # 2. Sample joined records
    print("--- Sample Joined Records (first 5 by txid, sequence_number) ---")
    cur.execute("""
        SELECT
            o.observation_id,
            o.txid,
            o.timestamp AS obs_time,
            t.timestamp AS tx_time,
            o.message_type,
            o.propagation_delay_ms,
            o.sequence_number
        FROM core.transaction_observations o
        JOIN core.transactions t
            ON o.txid = t.txid
        WHERE o.batch_id = %s
        ORDER BY o.txid, o.sequence_number
        LIMIT 5;
    """, (BATCH_ID,))

    rows = cur.fetchall()
    for r in rows:
        print(f"  obs_id: {r[0]} | txid: {r[1][:16]}... | obs_time: {r[2]} | tx_time: {r[3]} | msg: {r[4]:3s} | delay: {r[5]}ms | seq: {r[6]}")

    print("\n============================================================")
    print("               STAGE A VERIFICATION COMPLETE                ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_stage_a()
