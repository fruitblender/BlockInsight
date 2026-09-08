"""
Transformation: staging.nodes_raw -> core.nodes
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def transform_nodes():
    conn = get_connection()
    cur = conn.cursor()

    print(f"Transforming nodes for batch {BATCH_ID}...")

    # 1. Execute idempotent transformation
    cur.execute("""
        INSERT INTO core.nodes (
            node_id,
            ip,
            port,
            country,
            asn,
            node_type,
            first_seen,
            last_seen,
            batch_id
        )
        SELECT
            node_id,
            ip::inet,
            port::integer,
            country,
            asn,
            node_type,
            first_seen::timestamp,
            last_seen::timestamp,
            batch_id
        FROM staging.nodes_raw
        WHERE batch_id = %s
        ON CONFLICT (node_id) DO UPDATE SET
            ip = EXCLUDED.ip,
            port = EXCLUDED.port,
            country = EXCLUDED.country,
            asn = EXCLUDED.asn,
            node_type = EXCLUDED.node_type,
            first_seen = EXCLUDED.first_seen,
            last_seen = EXCLUDED.last_seen,
            batch_id = EXCLUDED.batch_id;
    """, (BATCH_ID,))

    conn.commit()

    # 2. Verification / Audit: Compare counts
    cur.execute("SELECT COUNT(*) FROM staging.nodes_raw WHERE batch_id = %s", (BATCH_ID,))
    staging_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM core.nodes WHERE batch_id = %s", (BATCH_ID,))
    core_count = cur.fetchone()[0]

    print(f"  staging.nodes_raw rows: {staging_count}")
    print(f"  core.nodes rows:        {core_count}")

    if staging_count == core_count:
        print("  Status: SUCCESS - Counts match perfectly.")
    else:
        print(f"  Status: MISMATCH - staging: {staging_count}, core: {core_count}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    transform_nodes()
