"""
Correlation Stage A:
Verify the transaction -> observation relationship using TXID.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from database import get_connection


def run_stage_a():

    conn = get_connection()

    try:
        with conn.cursor() as cur:

            print("=" * 60)
            print("CORRELATION STAGE A: TRANSACTION -> OBSERVATIONS")
            print("=" * 60)

            # 1. Total observations
            cur.execute("""
                SELECT COUNT(*)
                FROM core.transaction_observations;
            """)
            total_observations = cur.fetchone()[0]

            # 2. Observations with a valid transaction
            cur.execute("""
                SELECT COUNT(*)
                FROM core.transaction_observations o
                JOIN core.transactions t
                    ON o.txid = t.txid;
            """)
            linked_observations = cur.fetchone()[0]

            # 3. Distinct transactions observed
            cur.execute("""
                SELECT COUNT(DISTINCT o.txid)
                FROM core.transaction_observations o
                JOIN core.transactions t
                    ON o.txid = t.txid;
            """)
            observed_transactions = cur.fetchone()[0]

            # 4. Total transactions
            cur.execute("""
                SELECT COUNT(*)
                FROM core.transactions;
            """)
            total_transactions = cur.fetchone()[0]

            print()
            print(f"Total observations        : {total_observations}")
            print(f"Linked observations       : {linked_observations}")
            print(f"Observed transactions     : {observed_transactions}")
            print(f"Total transactions        : {total_transactions}")

            # 5. Verification
            print()

            if total_observations == linked_observations:
                print("Observation Join: PASSED")
                print("100% of observations link to a transaction.")
            else:
                print("Observation Join: FAILED")
                print(
                    f"{total_observations - linked_observations} "
                    "observations have no matching transaction."
                )

            print()
            print("--- Sample Correlated Records ---")

            cur.execute("""
                SELECT
                    o.observation_id,
                    o.txid,
                    t.timestamp AS transaction_time,
                    o.timestamp AS observation_time,
                    o.observer_id,
                    o.message_type,
                    o.propagation_delay_ms,
                    o.sequence_number
                FROM core.transaction_observations o
                JOIN core.transactions t
                    ON o.txid = t.txid
                ORDER BY o.txid, o.sequence_number
                LIMIT 5;
            """)

            rows = cur.fetchall()

            for row in rows:
                print(
                    f"obs={row[0]} | "
                    f"txid={row[1][:16]}... | "
                    f"tx_time={row[2]} | "
                    f"obs_time={row[3]} | "
                    f"observer={row[4]} | "
                    f"msg={row[5]} | "
                    f"delay={row[6]}ms | "
                    f"seq={row[7]}"
                )

            print()
            print("=" * 60)
            print("STAGE A COMPLETE")
            print("=" * 60)

    finally:
        conn.close()


if __name__ == "__main__":
    run_stage_a()
