"""
Correlation Stage B: Verify Observation -> Observer Node Link
Confirms that core.transaction_observations correctly connects to core.nodes on observer_id.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

def run_stage_b():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print(f"   CORRELATION STAGE B: OBSERVATION -> OBSERVER NODE (BATCH {BATCH_ID})")
    print("============================================================\n")

    # 1. Total counts in the joined view
    cur.execute("""
        SELECT
            COUNT(*) AS total_joined_observations,
            COUNT(DISTINCT o.observer_id) AS distinct_observers
        FROM core.transaction_observations o
        JOIN core.nodes n
            ON o.observer_id = n.node_id
        WHERE o.batch_id = %s;
    """, (BATCH_ID,))
    joined_obs, distinct_observers = cur.fetchone()

    # Total nodes in core.nodes
    cur.execute("SELECT COUNT(*) FROM core.nodes WHERE batch_id = %s;", (BATCH_ID,))
    total_nodes = cur.fetchone()[0]

    print(f"Total observations joined to observer node: {joined_obs} / 3940")
    print(f"Distinct observer nodes active             : {distinct_observers} / {total_nodes} total nodes")
    print()

    # 2. Top observer nodes by volume
    print("--- Top 5 Observer Nodes by Observation Volume ---")
    cur.execute("""
        SELECT
            o.observer_id,
            n.ip,
            n.country,
            n.asn,
            n.node_type,
            COUNT(*) AS obs_count
        FROM core.transaction_observations o
        JOIN core.nodes n
            ON o.observer_id = n.node_id
        WHERE o.batch_id = %s
        GROUP BY o.observer_id, n.ip, n.country, n.asn, n.node_type
        ORDER BY obs_count DESC
        LIMIT 5;
    """, (BATCH_ID,))

    for r in cur.fetchall():
        print(f"  Node: {r[0]} | IP: {r[1]:15s} | Country: {r[2]} | ASN: {r[3]:10s} | Type: {r[4]:10s} | Observations: {r[5]}")

    print("\n--- Geographic Distribution of Observers ---")
    cur.execute("""
        SELECT
            n.country,
            COUNT(DISTINCT o.observer_id) AS observer_count,
            COUNT(*) AS observation_count
        FROM core.transaction_observations o
        JOIN core.nodes n
            ON o.observer_id = n.node_id
        WHERE o.batch_id = %s
        GROUP BY n.country
        ORDER BY observation_count DESC;
    """, (BATCH_ID,))

    for r in cur.fetchall():
        print(f"  Country: {r[0]:4s} | Observers: {r[1]:3d} | Total Observations: {r[2]:4d}")

    print("\n============================================================")
    print("               STAGE B VERIFICATION COMPLETE                ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_stage_b()
