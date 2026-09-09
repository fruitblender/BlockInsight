"""
Transform staging.transaction_observations -> core.transaction_observations
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from database import get_connection


def transform_transaction_observations():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            print("Transforming transaction observations...")

            cur.execute("""
                INSERT INTO core.transaction_observations (
                    observation_id,
                    timestamp,
                    observer_id,
                    src_ip,
                    src_port,
                    dst_ip,
                    dst_port,
                    txid,
                    message_type,
                    peer_id,
                    connection_id,
                    propagation_delay_ms,
                    direction,
                    sequence_number
                )
                SELECT
                    observation_id,
                    timestamp,
                    observer_id,
                    src_ip,
                    src_port,
                    dst_ip,
                    dst_port,
                    txid,
                    message_type,
                    peer_id,
                    NULLIF(TRIM(connection_id), ''),
                    propagation_delay_ms,
                    direction,
                    sequence_number
                FROM staging.transaction_observations
                ON CONFLICT (observation_id) DO UPDATE SET
                    timestamp = EXCLUDED.timestamp,
                    observer_id = EXCLUDED.observer_id,
                    src_ip = EXCLUDED.src_ip,
                    src_port = EXCLUDED.src_port,
                    dst_ip = EXCLUDED.dst_ip,
                    dst_port = EXCLUDED.dst_port,
                    txid = EXCLUDED.txid,
                    message_type = EXCLUDED.message_type,
                    peer_id = EXCLUDED.peer_id,
                    connection_id = EXCLUDED.connection_id,
                    propagation_delay_ms = EXCLUDED.propagation_delay_ms,
                    direction = EXCLUDED.direction,
                    sequence_number = EXCLUDED.sequence_number;
            """)

            conn.commit()

            cur.execute("""
                SELECT COUNT(*)
                FROM staging.transaction_observations
            """)
            staging_count = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM core.transaction_observations
            """)
            core_count = cur.fetchone()[0]

            print(f"staging observations: {staging_count}")
            print(f"core observations:    {core_count}")

            if staging_count == core_count:
                print(
                    "SUCCESS - Transaction observation transformation complete."
                )
            else:
                print(
                    f"ERROR - Count mismatch: "
                    f"staging={staging_count}, core={core_count}"
                )

    finally:
        conn.close()


if __name__ == "__main__":
    transform_transaction_observations()
