"""
Transform staging.nodes -> core.nodes
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from database import get_connection


def transform_nodes():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            print("Transforming nodes...")

            cur.execute("""
                INSERT INTO core.nodes (
                    node_id,
                    ip,
                    port,
                    country,
                    asn,
                    node_type,
                    first_seen,
                    last_seen
                )
                SELECT
                    node_id,
                    ip,
                    port,
                    country,
                    asn,
                    node_type,
                    first_seen,
                    last_seen
                FROM staging.nodes
                ON CONFLICT (node_id) DO UPDATE SET
                    ip = EXCLUDED.ip,
                    port = EXCLUDED.port,
                    country = EXCLUDED.country,
                    asn = EXCLUDED.asn,
                    node_type = EXCLUDED.node_type,
                    first_seen = EXCLUDED.first_seen,
                    last_seen = EXCLUDED.last_seen;
            """)

            conn.commit()

            cur.execute("SELECT COUNT(*) FROM staging.nodes")
            staging_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM core.nodes")
            core_count = cur.fetchone()[0]

            print(f"staging.nodes rows: {staging_count}")
            print(f"core.nodes rows:    {core_count}")

            if staging_count == core_count:
                print("SUCCESS - Node transformation complete.")
            else:
                print(
                    f"ERROR - Count mismatch: "
                    f"staging={staging_count}, core={core_count}"
                )

    finally:
        conn.close()


if __name__ == "__main__":
    transform_nodes()
