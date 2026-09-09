"""
Transform staging.peer_connections -> core.peer_connections
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from database import get_connection


def transform_peer_connections():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            print("Transforming peer connections...")

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
                    direction
                )
                SELECT
                    connection_id,
                    timestamp_start,
                    timestamp_end,
                    src_node_id,
                    dst_node_id,
                    src_ip,
                    dst_ip,
                    src_port,
                    dst_port,
                    direction
                FROM staging.peer_connections
                ON CONFLICT (connection_id) DO UPDATE SET
                    timestamp_start = EXCLUDED.timestamp_start,
                    timestamp_end = EXCLUDED.timestamp_end,
                    src_node_id = EXCLUDED.src_node_id,
                    dst_node_id = EXCLUDED.dst_node_id,
                    src_ip = EXCLUDED.src_ip,
                    dst_ip = EXCLUDED.dst_ip,
                    src_port = EXCLUDED.src_port,
                    dst_port = EXCLUDED.dst_port,
                    direction = EXCLUDED.direction;
            """)

            conn.commit()

            cur.execute(
                "SELECT COUNT(*) FROM staging.peer_connections"
            )
            staging_count = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM core.peer_connections"
            )
            core_count = cur.fetchone()[0]

            print(f"staging.peer_connections rows: {staging_count}")
            print(f"core.peer_connections rows:    {core_count}")

            if staging_count == core_count:
                print("SUCCESS - Peer connection transformation complete.")
            else:
                print(
                    f"ERROR - Count mismatch: "
                    f"staging={staging_count}, core={core_count}"
                )

    finally:
        conn.close()


if __name__ == "__main__":
    transform_peer_connections()
