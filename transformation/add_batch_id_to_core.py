"""
Add batch_id column to existing core.* tables.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

CORE_TABLES = [
    "nodes",
    "transactions",
    "peer_connections",
    "transaction_observations",
    "network_events"
]

def main():
    conn = get_connection()
    cur = conn.cursor()

    print("Adding batch_id to core tables...")
    for table in CORE_TABLES:
        cur.execute(f"""
            ALTER TABLE core.{table}
            ADD COLUMN IF NOT EXISTS batch_id bigint NOT NULL;
        """)
        print(f"  core.{table:25s} -> batch_id added/verified")

    conn.commit()
    cur.close()
    conn.close()
    print("Schema update complete.")

if __name__ == "__main__":
    main()
