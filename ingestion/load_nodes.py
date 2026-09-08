import csv
from database import get_connection

CSV_FILE = "../data/incoming/nodes.csv"
BATCH_ID = 1


def load_nodes():
    conn = None

    try:
        conn = get_connection()

        # --------------------------------------------------
        # 1. Ensure ingestion batch exists
        # --------------------------------------------------
        batch_id = BATCH_ID
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_batches
                    (batch_id, dataset_name, file_name, status)
                VALUES
                    (%s, %s, %s, %s)
                ON CONFLICT (batch_id) DO UPDATE
                SET status = 'PROCESSING', started_at = CURRENT_TIMESTAMP;
                """,
                (batch_id, "nodes", "nodes.csv", "PROCESSING")
            )

        print(f"Using batch: {batch_id}")

        # --------------------------------------------------
        # 2. Read CSV
        # --------------------------------------------------
        with open(CSV_FILE, "r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            rows = [
                (
                    row["node_id"],
                    row["ip"],
                    row["port"],
                    row["country"],
                    row["asn"],
                    row["node_type"],
                    row["first_seen"],
                    row["last_seen"],
                    batch_id
                )
                for row in reader
            ]

        # --------------------------------------------------
        # 3. Insert into staging
        # --------------------------------------------------
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.nodes_raw
                (
                    node_id,
                    ip,
                    port,
                    country,
                    asn,
                    node_type,
                    first_seen,
                    last_seen,
                    batch_id
                )
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                rows
            )

            # --------------------------------------------------
            # 4. Mark batch completed
            # --------------------------------------------------
            cur.execute(
                """
                UPDATE ingestion_batches
                SET
                    status = 'COMPLETED',
                    completed_at = CURRENT_TIMESTAMP,
                    row_count = %s
                WHERE batch_id = %s;
                """,
                (len(rows), batch_id)
            )

        conn.commit()

        print(f"Successfully loaded {len(rows)} nodes.")
        print(f"Batch {batch_id} completed.")

    except Exception as e:

        if conn:
            conn.rollback()

        print("Node ingestion failed.")
        print(e)

    finally:

        if conn:
            conn.close()


if __name__ == "__main__":
    load_nodes()
