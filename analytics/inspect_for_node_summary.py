"""
Step 1: Database and Schema Inspection for Node Behavior Analysis
Inspects core.nodes, core.peer_connections, core.transaction_observations, and analytics objects.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

def inspect_for_node_summary():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print(" STEP 1: DATABASE & SCHEMA INSPECTION FOR NODE SUMMARY      ")
    print("============================================================\n")

    # 1. Inspect existing analytics objects
    cur.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'analytics'
        ORDER BY table_name;
    """)
    analytics_objs = cur.fetchall()
    print(f"1. EXISTING OBJECTS IN 'analytics' SCHEMA ({len(analytics_objs)}):")
    for s, name, ttype in analytics_objs:
        print(f"   - {s}.{name:35s} [{ttype}]")
    print()

    # 2. Inspect core.nodes columns and row count
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT node_id) FROM core.nodes WHERE batch_id = 1;")
    nodes_cnt, unique_nodes = cur.fetchone()
    print(f"2. core.nodes (Batch 1): {nodes_cnt} rows | {unique_nodes} unique node_ids")
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'nodes'
        ORDER BY ordinal_position;
    """)
    for col, dt, nl in cur.fetchall():
        print(f"     {col:25s} {dt:22s} {'(NULL)' if nl == 'YES' else 'NOT NULL'}")
    print()

    # 3. Inspect core.peer_connections columns and row count
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT connection_id) FROM core.peer_connections WHERE batch_id = 1;")
    pc_cnt, unique_pc = cur.fetchone()
    print(f"3. core.peer_connections (Batch 1): {pc_cnt} rows | {unique_pc} unique connection_ids")
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'peer_connections'
        ORDER BY ordinal_position;
    """)
    for col, dt, nl in cur.fetchall():
        print(f"     {col:25s} {dt:22s} {'(NULL)' if nl == 'YES' else 'NOT NULL'}")
    print()

    # 4. Inspect core.transaction_observations columns and row count
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT observation_id) FROM core.transaction_observations WHERE batch_id = 1;")
    obs_cnt, unique_obs = cur.fetchone()
    print(f"4. core.transaction_observations (Batch 1): {obs_cnt} rows | {unique_obs} unique observation_ids")
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'transaction_observations'
        ORDER BY ordinal_position;
    """)
    for col, dt, nl in cur.fetchall():
        print(f"     {col:25s} {dt:22s} {'(NULL)' if nl == 'YES' else 'NOT NULL'}")
    print()

    # 5. Check indexes relevant to node joins
    cur.execute("""
        SELECT tablename, indexname
        FROM pg_indexes
        WHERE schemaname = 'core'
          AND tablename IN ('nodes', 'peer_connections', 'transaction_observations')
        ORDER BY tablename, indexname;
    """)
    print("5. EXISTING INDEXES FOR NODE JOINS:")
    for tbl, idx in cur.fetchall():
        print(f"   - core.{tbl:26s} | {idx}")
    print()

    # 6. Check peer_connections connection duration support (sample non-null check)
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE timestamp_start IS NOT NULL AND timestamp_end IS NOT NULL) AS valid_duration_rows,
            MIN(EXTRACT(EPOCH FROM (timestamp_end - timestamp_start)) * 1000) AS min_duration_ms,
            MAX(EXTRACT(EPOCH FROM (timestamp_end - timestamp_start)) * 1000) AS max_duration_ms,
            AVG(EXTRACT(EPOCH FROM (timestamp_end - timestamp_start)) * 1000) AS avg_duration_ms
        FROM core.peer_connections
        WHERE batch_id = 1;
    """)
    dur_rows, min_d, max_d, avg_d = cur.fetchone()
    print("6. CONNECTION DURATION CHECK IN core.peer_connections:")
    print(f"   - Rows with both timestamp_start and timestamp_end: {dur_rows} / {pc_cnt}")
    print(f"   - Duration range: min={min_d:.2f} ms | max={max_d:.2f} ms | avg={avg_d:.2f} ms")
    print()

    print("============================================================")
    print("             STEP 1 INSPECTION COMPLETED                    ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_for_node_summary()
