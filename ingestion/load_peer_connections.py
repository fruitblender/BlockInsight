import csv
from database import get_connection

CSV_FILE = "../data/incoming/peer_connections.csv"
BATCH_ID = 1


def load_peer_connections():
    conn = None

    try:
        conn = get_connection()

        # Register this file under the existing batch
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_files
                    (batch_id, dataset_name, file_name, status)
                VALUES
                    (%s, %s, %s, %s)
                RETURNING file_id;
                """,
                (
                    BATCH_ID,
                    "peer_connections",
                    "peer_connections.csv",
                    "PROCESSING"
                )
            )

            file_id = cur.fetchone()[0]

        print(f"Registered file: {file_id}")
        print(f"Using batch: {BATCH_ID}")

        # Read CSV
        with open(CSV_FILE, "r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            rows = [
                (
                    row["connection_id"],
                    row["timestamp_start"],
                    row["timestamp_end"],
                    row["src_node_id"],
                    row["dst_node_id"],
                    row["src_ip"],
                    row["dst_ip"],
                    row["src_port"],
                    row["dst_port"],
                    row["direction"],
                    BATCH_ID
                )
                for row in reader
            ]

        # Insert into staging
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.peer_connections_raw
                (
                    connection_id,
                    timestamp_start,
                    timestamp_end,
                    src_node_id,
                    dst_node_id,
                    src_ip,
                    dst_ip,
                    src_port,
                    dst_port,
                    direction,
                    batch_id
                )
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                rows
            )

            # Mark file completed
            cur.execute(
                """
                UPDATE ingestion_files
                SET
                    status = 'COMPLETED',
                    completed_at = CURRENT_TIMESTAMP,
                    row_count = %s
                WHERE file_id = %s;
                """,
                (len(rows), file_id)
            )

        conn.commit()

        print(f"Successfully loaded {len(rows)} peer connections.")
        print(f"File completed under batch {BATCH_ID}.")

    except Exception as e:

        if conn:
            conn.rollback()

        print("Peer connection ingestion failed.")
        print(e)

    finally:

        if conn:
            conn.close()


if __name__ == "__main__":
    load_peer_connections()
