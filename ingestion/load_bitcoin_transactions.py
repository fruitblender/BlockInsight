import json
from database import get_connection

JSON_FILE = "../data/incoming/bitcoin_synthetic_whales.json"
BATCH_ID = 1


def load_bitcoin_transactions():
    conn = None

    try:
        conn = get_connection()

        # Register file under Batch 1
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
                    "bitcoin_transactions",
                    "bitcoin_synthetic_whales.json",
                    "PROCESSING"
                )
            )

            file_id = cur.fetchone()[0]

        print(f"Registered file: {file_id}")
        print(f"Using batch: {BATCH_ID}")

        # Read JSON
        with open(JSON_FILE, "r", encoding="utf-8") as file:
            transactions = json.load(file)

        if not isinstance(transactions, list):
            raise ValueError("Expected JSON file to contain a list.")

        rows = [
            (
                tx.get("txid"),
                tx.get("timestamp"),
                tx.get("inputs"),
                tx.get("outputs"),
                tx.get("fee"),
                tx.get("size"),
                tx.get("ratio_fee_size"),
                tx.get("rarity_score"),
                tx.get("tema"),
                tx.get("description_ia"),
                BATCH_ID
            )
            for tx in transactions
        ]

        # Insert into staging
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.bitcoin_transactions_raw
                (
                    txid,
                    timestamp,
                    inputs,
                    outputs,
                    fee,
                    size,
                    ratio_fee_size,
                    rarity_score,
                    tema,
                    description_ia,
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

        print(f"Successfully loaded {len(rows)} Bitcoin transactions.")
        print(f"File completed under batch {BATCH_ID}.")

    except Exception as e:

        if conn:
            conn.rollback()

        print("Bitcoin transaction ingestion failed.")
        print(e)

    finally:

        if conn:
            conn.close()


if __name__ == "__main__":
    load_bitcoin_transactions()
