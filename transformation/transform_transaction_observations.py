"""
Transformation: staging.transaction_observations_raw -> core.transaction_observations
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def transform_transaction_observations():
    conn = get_connection()
    cur = conn.cursor()

    print(f"Transforming transaction observations for batch {BATCH_ID}...")

    # 1. Execute idempotent SQL transformation in PostgreSQL
    cur.execute("""
        INSERT INTO core.transaction_observations (
            observation_id,
            timestamp,
            observer_id,
            src_ip,
            src_port,
            dst_ip,
            dst_port,
            txid,
            message_type,
            peer_id,
            connection_id,
            propagation_delay_ms,
            direction,
            sequence_number,
            batch_id
        )
        SELECT
            observation_id,
            timestamp::timestamp,
            observer_id,
            src_ip::inet,
            src_port::integer,
            dst_ip::inet,
            dst_port::integer,
            txid,
            message_type,
            peer_id,
            NULLIF(TRIM(connection_id), ''),
            propagation_delay_ms::numeric,
            direction,
            sequence_number::integer,
            batch_id
        FROM staging.transaction_observations_raw
        WHERE batch_id = %s
        ON CONFLICT (observation_id) DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            observer_id = EXCLUDED.observer_id,
            src_ip = EXCLUDED.src_ip,
            src_port = EXCLUDED.src_port,
            dst_ip = EXCLUDED.dst_ip,
            dst_port = EXCLUDED.dst_port,
            txid = EXCLUDED.txid,
            message_type = EXCLUDED.message_type,
            peer_id = EXCLUDED.peer_id,
            connection_id = EXCLUDED.connection_id,
            propagation_delay_ms = EXCLUDED.propagation_delay_ms,
            direction = EXCLUDED.direction,
            sequence_number = EXCLUDED.sequence_number,
            batch_id = EXCLUDED.batch_id;
    """, (BATCH_ID,))

    conn.commit()

    # 2. Verification: Compare counts
    cur.execute("SELECT COUNT(*) FROM staging.transaction_observations_raw WHERE batch_id = %s", (BATCH_ID,))
    staging_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM core.transaction_observations WHERE batch_id = %s", (BATCH_ID,))
    core_count = cur.fetchone()[0]

    print(f"  staging.transaction_observations_raw rows: {staging_count}")
    print(f"  core.transaction_observations rows:        {core_count}")

    if staging_count == core_count:
        print("  Status: SUCCESS - Counts match perfectly.")
    else:
        print(f"  Status: MISMATCH - staging: {staging_count}, core: {core_count}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    transform_transaction_observations()
