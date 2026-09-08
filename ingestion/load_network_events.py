import csv
from database import get_connection

CSV_FILE = "../data/incoming/network_events.csv"
BATCH_ID = 1


def load_network_events():
    conn = None

    try:
        conn = get_connection()

        # Register file under existing Batch 1
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
                    "network_events",
                    "network_events.csv",
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
                    row["event_id"],
                    row["timestamp"],
                    row["event_type"],
                    row["node_id"],
                    row["peer_id"],
                    row["connection_id"],
                    BATCH_ID
                )
                for row in reader
            ]

        # Insert into staging
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.network_events_raw
                (
                    event_id,
                    timestamp,
                    event_type,
                    node_id,
                    peer_id,
                    connection_id,
                    batch_id
                )
                VALUES
                (%s, %s, %s, %s, %s, %s, %s);
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

        print(f"Successfully loaded {len(rows)} network events.")
        print(f"File completed under batch {BATCH_ID}.")

    except Exception as e:

        if conn:
            conn.rollback()

        print("Network event ingestion failed.")
        print(e)

    finally:

        if conn:
            conn.close()


if __name__ == "__main__":
    load_network_events()
