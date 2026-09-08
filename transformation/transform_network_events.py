"""
Transformation: staging.network_events_raw -> core.network_events
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def transform_network_events():
    conn = get_connection()
    cur = conn.cursor()

    print(f"Transforming network events for batch {BATCH_ID}...")

    # 1. Execute idempotent SQL transformation in PostgreSQL
    cur.execute("""
        INSERT INTO core.network_events (
            event_id,
            timestamp,
            event_type,
            node_id,
            peer_id,
            connection_id,
            batch_id
        )
        SELECT
            event_id,
            timestamp::timestamp,
            event_type,
            node_id,
            peer_id,
            connection_id,
            batch_id
        FROM staging.network_events_raw
        WHERE batch_id = %s
        ON CONFLICT (event_id) DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            event_type = EXCLUDED.event_type,
            node_id = EXCLUDED.node_id,
            peer_id = EXCLUDED.peer_id,
            connection_id = EXCLUDED.connection_id,
            batch_id = EXCLUDED.batch_id;
    """, (BATCH_ID,))

    conn.commit()

    # 2. Verification: Compare counts
    cur.execute("SELECT COUNT(*) FROM staging.network_events_raw WHERE batch_id = %s", (BATCH_ID,))
    staging_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM core.network_events WHERE batch_id = %s", (BATCH_ID,))
    core_count = cur.fetchone()[0]

    print(f"  staging.network_events_raw rows: {staging_count}")
    print(f"  core.network_events rows:        {core_count}")

    if staging_count == core_count:
        print("  Status: SUCCESS - Counts match perfectly.")
    else:
        print(f"  Status: MISMATCH - staging: {staging_count}, core: {core_count}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    transform_network_events()
