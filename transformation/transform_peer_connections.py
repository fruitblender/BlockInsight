"""
Transformation: staging.peer_connections_raw -> core.peer_connections
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def transform_peer_connections():
    conn = get_connection()
    cur = conn.cursor()

    print(f"Transforming peer connections for batch {BATCH_ID}...")

    # 1. Execute idempotent SQL transformation in PostgreSQL
    cur.execute("""
        INSERT INTO core.peer_connections (
            connection_id,
            timestamp_start,
            timestamp_end,
            src_node_id,
            dst_node_id,
            src_ip,
            dst_ip,
            src_port,
            dst_port,
            direction,
            batch_id
        )
        SELECT
            connection_id,
            timestamp_start::timestamp,
            timestamp_end::timestamp,
            src_node_id,
            dst_node_id,
            src_ip::inet,
            dst_ip::inet,
            src_port::integer,
            dst_port::integer,
            direction,
            batch_id
        FROM staging.peer_connections_raw
        WHERE batch_id = %s
        ON CONFLICT (connection_id) DO UPDATE SET
            timestamp_start = EXCLUDED.timestamp_start,
            timestamp_end = EXCLUDED.timestamp_end,
            src_node_id = EXCLUDED.src_node_id,
            dst_node_id = EXCLUDED.dst_node_id,
            src_ip = EXCLUDED.src_ip,
            dst_ip = EXCLUDED.dst_ip,
            src_port = EXCLUDED.src_port,
            dst_port = EXCLUDED.dst_port,
            direction = EXCLUDED.direction,
            batch_id = EXCLUDED.batch_id;
    """, (BATCH_ID,))

    conn.commit()

    # 2. Verification: Compare counts
    cur.execute("SELECT COUNT(*) FROM staging.peer_connections_raw WHERE batch_id = %s", (BATCH_ID,))
    staging_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM core.peer_connections WHERE batch_id = %s", (BATCH_ID,))
    core_count = cur.fetchone()[0]

    print(f"  staging.peer_connections_raw rows: {staging_count}")
    print(f"  core.peer_connections rows:        {core_count}")

    if staging_count == core_count:
        print("  Status: SUCCESS - Counts match perfectly.")
    else:
        print(f"  Status: MISMATCH - staging: {staging_count}, core: {core_count}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    transform_peer_connections()
