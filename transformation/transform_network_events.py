"""
Transform staging.network_events -> core.network_events
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from database import get_connection


def transform_network_events():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            print("Transforming network events...")

            cur.execute("""
                INSERT INTO core.network_events (
                    event_id,
                    timestamp,
                    event_type,
                    node_id,
                    peer_id,
                    connection_id
                )
                SELECT
                    event_id,
                    timestamp,
                    event_type,
                    node_id,
                    peer_id,
                    connection_id
                FROM staging.network_events
                ON CONFLICT (event_id) DO UPDATE SET
                    timestamp = EXCLUDED.timestamp,
                    event_type = EXCLUDED.event_type,
                    node_id = EXCLUDED.node_id,
                    peer_id = EXCLUDED.peer_id,
                    connection_id = EXCLUDED.connection_id;
            """)

            conn.commit()

            cur.execute("""
                SELECT COUNT(*)
                FROM staging.network_events
            """)
            staging_count = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM core.network_events
            """)
            core_count = cur.fetchone()[0]

            print(f"staging network events: {staging_count}")
            print(f"core network events:    {core_count}")

            if staging_count == core_count:
                print("SUCCESS - Network event transformation complete.")
            else:
                print(
                    f"ERROR - Count mismatch: "
                    f"staging={staging_count}, core={core_count}"
                )

    finally:
        conn.close()


if __name__ == "__main__":
    transform_network_events()
