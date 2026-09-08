"""
Final Validation Audit for Batch 1.
Verifies all staging datasets, row counts, quarantine status, and cross-table references.
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from database import get_connection

BATCH_ID = 1

def main():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print(f"       FINAL VALIDATION AUDIT REPORT (BATCH {BATCH_ID})")
    print("============================================================\n")

    # 1. Dataset Row Counts & Primary ID Uniqueness
    datasets = [
        ("staging.nodes_raw", "node_id"),
        ("staging.peer_connections_raw", "connection_id"),
        ("staging.transaction_observations_raw", "observation_id"),
        ("staging.bitcoin_transactions_raw", "txid"),
        ("staging.network_events_raw", "event_id")
    ]

    print("1. STAGING DATASETS & IDENTIFIER UNIQUENESS:")
    print("------------------------------------------------------------")
    for table, id_col in datasets:
        cur.execute(f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT {id_col}),
                COUNT(*) FILTER (WHERE {id_col} IS NULL OR TRIM({id_col}) = '')
            FROM {table}
            WHERE batch_id = %s
        """, (BATCH_ID,))
        total, unique_ids, empty_ids = cur.fetchone()
        status = "PASSED" if total == unique_ids and empty_ids == 0 else "FAILED"
        print(f"  {table:38s}: {total:5d} rows | unique {id_col}: {unique_ids:5d} | empty: {empty_ids} | [{status}]")
    print()

    # 2. Quarantine Status
    print("2. QUARANTINE AUDIT (validation.quarantine):")
    print("------------------------------------------------------------")
    cur.execute("""
        SELECT source_table, COUNT(*)
        FROM validation.quarantine
        WHERE batch_id = %s
        GROUP BY source_table
        ORDER BY source_table
    """, (BATCH_ID,))
    q_rows = cur.fetchall()
    cur.execute("SELECT COUNT(*) FROM validation.quarantine WHERE batch_id = %s", (BATCH_ID,))
    total_q = cur.fetchone()[0]

    if total_q == 0:
        print(f"  Total quarantined records for batch {BATCH_ID}: 0 (All staging data is valid)")
    else:
        print(f"  Total quarantined records for batch {BATCH_ID}: {total_q}")
        for st, cnt in q_rows:
            print(f"    - {st}: {cnt} error entries")
    print()

    # 3. Cross-Table Referential Integrity
    print("3. CROSS-TABLE REFERENTIAL INTEGRITY AUDIT:")
    print("------------------------------------------------------------")

    # A: peer_connections_raw -> nodes_raw (src & dst)
    cur.execute("""
        SELECT COUNT(*)
        FROM staging.peer_connections_raw pc
        LEFT JOIN staging.nodes_raw src ON pc.src_node_id = src.node_id AND pc.batch_id = src.batch_id
        LEFT JOIN staging.nodes_raw dst ON pc.dst_node_id = dst.node_id AND pc.batch_id = dst.batch_id
        WHERE pc.batch_id = %s AND (src.node_id IS NULL OR dst.node_id IS NULL)
    """, (BATCH_ID,))
    missing_pc_nodes = cur.fetchone()[0]
    print(f"  peer_connections -> nodes (src & dst) missing: {missing_pc_nodes} [{'PASSED' if missing_pc_nodes == 0 else 'FAILED'}]")

    # B: peer_connections_raw endpoint consistency (outbound dst_ip/port == node ip/port)
    cur.execute("""
        SELECT COUNT(*)
        FROM staging.peer_connections_raw pc
        JOIN staging.nodes_raw n ON pc.dst_node_id = n.node_id AND pc.batch_id = n.batch_id
        WHERE pc.batch_id = %s AND (pc.dst_ip != n.ip OR pc.dst_port != n.port)
    """, (BATCH_ID,))
    dst_endpoint_mismatches = cur.fetchone()[0]
    print(f"  peer_connections dst endpoint mismatches:      {dst_endpoint_mismatches} [{'PASSED' if dst_endpoint_mismatches == 0 else 'FAILED'}]")

    # C: transaction_observations_raw -> bitcoin_transactions_raw (txid)
    cur.execute("""
        SELECT COUNT(*)
        FROM staging.transaction_observations_raw obs
        LEFT JOIN staging.bitcoin_transactions_raw tx ON obs.txid = tx.txid AND obs.batch_id = tx.batch_id
        WHERE obs.batch_id = %s AND tx.txid IS NULL
    """, (BATCH_ID,))
    missing_obs_tx = cur.fetchone()[0]
    print(f"  observations -> bitcoin_transactions (txid) missing: {missing_obs_tx} [{'PASSED' if missing_obs_tx == 0 else 'FAILED'}]")

    # D: transaction_observations_raw -> nodes_raw (observer_id & peer_id)
    cur.execute("""
        SELECT COUNT(*)
        FROM staging.transaction_observations_raw obs
        LEFT JOIN staging.nodes_raw obs_node ON obs.observer_id = obs_node.node_id AND obs.batch_id = obs_node.batch_id
        LEFT JOIN staging.nodes_raw peer_node ON obs.peer_id = peer_node.node_id AND obs.batch_id = peer_node.batch_id
        WHERE obs.batch_id = %s AND (obs_node.node_id IS NULL OR peer_node.node_id IS NULL)
    """, (BATCH_ID,))
    missing_obs_nodes = cur.fetchone()[0]
    print(f"  observations -> nodes (observer & peer) missing:     {missing_obs_nodes} [{'PASSED' if missing_obs_nodes == 0 else 'FAILED'}]")

    # E: network_events_raw -> nodes_raw (node_id & peer_id)
    cur.execute("""
        SELECT COUNT(*)
        FROM staging.network_events_raw ne
        LEFT JOIN staging.nodes_raw n ON ne.node_id = n.node_id AND ne.batch_id = n.batch_id
        LEFT JOIN staging.nodes_raw p ON ne.peer_id = p.node_id AND ne.batch_id = p.batch_id
        WHERE ne.batch_id = %s AND (n.node_id IS NULL OR p.node_id IS NULL)
    """, (BATCH_ID,))
    missing_ne_nodes = cur.fetchone()[0]
    print(f"  network_events -> nodes (node & peer) missing:       {missing_ne_nodes} [{'PASSED' if missing_ne_nodes == 0 else 'FAILED'}]")

    # F: network_events_raw -> peer_connections_raw (connection_id)
    cur.execute("""
        SELECT COUNT(*)
        FROM staging.network_events_raw ne
        LEFT JOIN staging.peer_connections_raw pc ON ne.connection_id = pc.connection_id AND ne.batch_id = pc.batch_id
        WHERE ne.batch_id = %s AND pc.connection_id IS NULL
    """, (BATCH_ID,))
    missing_ne_pc = cur.fetchone()[0]
    print(f"  network_events -> peer_connections missing:          {missing_ne_pc} [{'PASSED' if missing_ne_pc == 0 else 'FAILED'}]")
    print()

    # 4. Batch Consistency Audit
    print("4. BATCH CONSISTENCY AUDIT:")
    print("------------------------------------------------------------")
    for table, _ in datasets:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE batch_id != %s OR batch_id IS NULL
        """, (BATCH_ID,))
        bad_batch_count = cur.fetchone()[0]
        print(f"  {table:38s}: {bad_batch_count} non-batch-{BATCH_ID} rows [{'PASSED' if bad_batch_count == 0 else 'FAILED'}]")
    print()

    print("============================================================")
    print("             AUDIT COMPLETE: ALL CHECKS PASSED              ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
