"""
Transformation: staging.bitcoin_transactions_raw -> core.transactions
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def transform_transactions():
    conn = get_connection()
    cur = conn.cursor()

    print(f"Transforming transactions for batch {BATCH_ID}...")

    # 1. Execute idempotent SQL transformation in PostgreSQL
    cur.execute("""
        INSERT INTO core.transactions (
            txid,
            timestamp,
            inputs,
            outputs,
            fee,
            size,
            ratio_fee_size,
            rarity_score,
            tema,
            description_ia,
            batch_id
        )
        SELECT
            txid,
            timestamp::timestamp,
            inputs::integer,
            outputs::integer,
            fee::numeric,
            size::integer,
            ratio_fee_size::numeric,
            rarity_score::numeric,
            tema,
            description_ia,
            batch_id
        FROM staging.bitcoin_transactions_raw
        WHERE batch_id = %s
        ON CONFLICT (txid) DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            inputs = EXCLUDED.inputs,
            outputs = EXCLUDED.outputs,
            fee = EXCLUDED.fee,
            size = EXCLUDED.size,
            ratio_fee_size = EXCLUDED.ratio_fee_size,
            rarity_score = EXCLUDED.rarity_score,
            tema = EXCLUDED.tema,
            description_ia = EXCLUDED.description_ia,
            batch_id = EXCLUDED.batch_id;
    """, (BATCH_ID,))

    conn.commit()

    # 2. Verification: Compare counts
    cur.execute("SELECT COUNT(*) FROM staging.bitcoin_transactions_raw WHERE batch_id = %s", (BATCH_ID,))
    staging_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM core.transactions WHERE batch_id = %s", (BATCH_ID,))
    core_count = cur.fetchone()[0]

    print(f"  staging.bitcoin_transactions_raw rows: {staging_count}")
    print(f"  core.transactions rows:                {core_count}")

    if staging_count == core_count:
        print("  Status: SUCCESS - Counts match perfectly.")
    else:
        print(f"  Status: MISMATCH - staging: {staging_count}, core: {core_count}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    transform_transactions()
