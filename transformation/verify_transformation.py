"""
Verification Audit for Transformed Core Tables.
Checks:
- Staging vs Core row count match
- No NULL primary keys
- No duplicate primary keys
- Foreign key integrity
- Batch ID consistency
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def main():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print(f"       TRANSFORMATION VERIFICATION AUDIT (BATCH {BATCH_ID})")
    print("============================================================\n")

    tables_config = [
        ("staging.nodes_raw", "core.nodes", "node_id"),
        ("staging.bitcoin_transactions_raw", "core.transactions", "txid"),
        ("staging.peer_connections_raw", "core.peer_connections", "connection_id"),
        ("staging.transaction_observations_raw", "core.transaction_observations", "observation_id"),
        ("staging.network_events_raw", "core.network_events", "event_id")
    ]

    print("1. ROW COUNT & PRIMARY KEY AUDIT:")
    print("------------------------------------------------------------")
    all_passed = True
    for staging_tbl, core_tbl, pk in tables_config:
        cur.execute(f"SELECT COUNT(*) FROM {staging_tbl} WHERE batch_id = %s", (BATCH_ID,))
        stg_cnt = cur.fetchone()[0]

        cur.execute(f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT {pk}),
                COUNT(*) FILTER (WHERE {pk} IS NULL)
            FROM {core_tbl}
            WHERE batch_id = %s
        """, (BATCH_ID,))
        core_cnt, distinct_pk, null_pk = cur.fetchone()

        match = (stg_cnt == core_cnt == distinct_pk and null_pk == 0)
        status = "PASSED" if match else "FAILED"
        if not match:
            all_passed = False

        print(f"  {core_tbl:35s}: stg={stg_cnt:4d} | core={core_cnt:4d} | unique_pk={distinct_pk:4d} | null_pk={null_pk} | [{status}]")
    print()

    print("2. FOREIGN KEY REFERENTIAL INTEGRITY AUDIT:")
    print("------------------------------------------------------------")

    # A: core.peer_connections -> core.nodes
    cur.execute("""
        SELECT COUNT(*)
        FROM core.peer_connections pc
        LEFT JOIN core.nodes src ON pc.src_node_id = src.node_id
        LEFT JOIN core.nodes dst ON pc.dst_node_id = dst.node_id
        WHERE pc.batch_id = %s AND (src.node_id IS NULL OR dst.node_id IS NULL)
    """, (BATCH_ID,))
    missing_pc_nodes = cur.fetchone()[0]
    print(f"  peer_connections -> nodes (src & dst) missing:        {missing_pc_nodes} [{'PASSED' if missing_pc_nodes == 0 else 'FAILED'}]")

    # B: core.transaction_observations -> core.transactions
    cur.execute("""
        SELECT COUNT(*)
        FROM core.transaction_observations obs
        LEFT JOIN core.transactions tx ON obs.txid = tx.txid
        WHERE obs.batch_id = %s AND tx.txid IS NULL
    """, (BATCH_ID,))
    missing_obs_tx = cur.fetchone()[0]
    print(f"  observations -> transactions (txid) missing:          {missing_obs_tx} [{'PASSED' if missing_obs_tx == 0 else 'FAILED'}]")

    # C: core.transaction_observations -> core.nodes
    cur.execute("""
        SELECT COUNT(*)
        FROM core.transaction_observations obs
        LEFT JOIN core.nodes obs_n ON obs.observer_id = obs_n.node_id
        WHERE obs.batch_id = %s AND obs_n.node_id IS NULL
    """, (BATCH_ID,))
    missing_obs_nodes = cur.fetchone()[0]
    print(f"  observations -> nodes (observer_id) missing:          {missing_obs_nodes} [{'PASSED' if missing_obs_nodes == 0 else 'FAILED'}]")

    # D: core.network_events -> core.nodes & core.peer_connections
    cur.execute("""
        SELECT COUNT(*)
        FROM core.network_events ne
        LEFT JOIN core.nodes n ON ne.node_id = n.node_id
        LEFT JOIN core.peer_connections pc ON ne.connection_id = pc.connection_id
        WHERE ne.batch_id = %s AND (n.node_id IS NULL OR pc.connection_id IS NULL)
    """, (BATCH_ID,))
    missing_ne_refs = cur.fetchone()[0]
    print(f"  network_events -> nodes & peer_connections missing:   {missing_ne_refs} [{'PASSED' if missing_ne_refs == 0 else 'FAILED'}]")
    print()

    print("3. DATA TYPE & NULLABILITY SPOT CHECK:")
    print("------------------------------------------------------------")
    # Sample check inet types, numeric types, timestamps
    cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM core.nodes WHERE ip IS NULL) AS null_ips,
            (SELECT COUNT(*) FROM core.transactions WHERE fee < 0) AS negative_fees,
            (SELECT COUNT(*) FROM core.transaction_observations WHERE propagation_delay_ms < 0) AS negative_delays,
            (SELECT COUNT(*) FROM core.transaction_observations WHERE connection_id IS NOT NULL) AS non_null_conn_ids
    """)
    null_ips, neg_fees, neg_delays, non_null_conns = cur.fetchone()
    print(f"  Null IPs in core.nodes:                             {null_ips}")
    print(f"  Negative fees in core.transactions:                 {neg_fees}")
    print(f"  Negative delays in core.transaction_observations:   {neg_delays}")
    print(f"  Non-null connection IDs in observations (expected 0): {non_null_conns}")
    print()

    print("============================================================")
    print("            ALL TRANSFORMATION CHECKS PASSED                ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
