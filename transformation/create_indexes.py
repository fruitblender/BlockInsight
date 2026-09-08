"""
Phase 3: Indexing and Database Quality
Creates targeted indexes on core.* tables to support correlation, graph building, and timeline queries.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

INDEXES = [
    # 1. core.transactions
    (
        "idx_transactions_timestamp",
        "core.transactions",
        "CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON core.transactions(timestamp);"
    ),

    # 2. core.nodes
    (
        "idx_nodes_ip",
        "core.nodes",
        "CREATE INDEX IF NOT EXISTS idx_nodes_ip ON core.nodes(ip);"
    ),

    # 3. core.peer_connections
    (
        "idx_peer_connections_src",
        "core.peer_connections",
        "CREATE INDEX IF NOT EXISTS idx_peer_connections_src ON core.peer_connections(src_node_id);"
    ),
    (
        "idx_peer_connections_dst",
        "core.peer_connections",
        "CREATE INDEX IF NOT EXISTS idx_peer_connections_dst ON core.peer_connections(dst_node_id);"
    ),
    (
        "idx_peer_connections_times",
        "core.peer_connections",
        "CREATE INDEX IF NOT EXISTS idx_peer_connections_times ON core.peer_connections(timestamp_start, timestamp_end);"
    ),

    # 4. core.transaction_observations
    (
        "idx_observations_txid",
        "core.transaction_observations",
        "CREATE INDEX IF NOT EXISTS idx_observations_txid ON core.transaction_observations(txid);"
    ),
    (
        "idx_observations_observer",
        "core.transaction_observations",
        "CREATE INDEX IF NOT EXISTS idx_observations_observer ON core.transaction_observations(observer_id);"
    ),
    (
        "idx_observations_peer",
        "core.transaction_observations",
        "CREATE INDEX IF NOT EXISTS idx_observations_peer ON core.transaction_observations(peer_id);"
    ),
    (
        "idx_observations_timestamp",
        "core.transaction_observations",
        "CREATE INDEX IF NOT EXISTS idx_observations_timestamp ON core.transaction_observations(timestamp);"
    ),
    (
        "idx_observations_delay",
        "core.transaction_observations",
        "CREATE INDEX IF NOT EXISTS idx_observations_delay ON core.transaction_observations(propagation_delay_ms);"
    ),

    # 5. core.network_events
    (
        "idx_network_events_node",
        "core.network_events",
        "CREATE INDEX IF NOT EXISTS idx_network_events_node ON core.network_events(node_id);"
    ),
    (
        "idx_network_events_conn",
        "core.network_events",
        "CREATE INDEX IF NOT EXISTS idx_network_events_conn ON core.network_events(connection_id);"
    )
]

def create_indexes():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print("      CREATING PERFORMANCE INDEXES ON CORE SCHEMA           ")
    print("============================================================\n")

    for idx_name, table, ddl in INDEXES:
        print(f"Creating {idx_name} on {table}...")
        cur.execute(ddl)

    conn.commit()

    print("\n--- Verifying Indexes in PostgreSQL ---")
    cur.execute("""
        SELECT
            tablename,
            indexname,
            indexdef
        FROM pg_indexes
        WHERE schemaname = 'core'
        ORDER BY tablename, indexname;
    """)

    rows = cur.fetchall()
    for tablename, indexname, indexdef in rows:
        print(f"  core.{tablename:26s} | {indexname}")

    print("\n============================================================")
    print(f"  INDEXING COMPLETED: {len(rows)} TOTAL INDEXES IN CORE")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    create_indexes()
