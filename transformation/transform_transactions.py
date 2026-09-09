"""
Transform staging.bitcoin_transactions_raw -> core.transactions
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from database import get_connection


def transform_transactions():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            print("Transforming transactions...")

            cur.execute("""
                INSERT INTO core.transactions (
                    txid,
                    timestamp,
                    input_count,
                    output_count,
                    fee,
                    size,
                    rarity_score,
                    ratio_fee_size,
                    tema,
                    description_ia
                )
                SELECT
                    raw_data->>'txid',
                    (raw_data->>'timestamp')::timestamp,
                    (raw_data->>'inputs')::integer,
                    (raw_data->>'outputs')::integer,
                    (raw_data->>'fee')::double precision,
                    (raw_data->>'size')::integer,
                    (raw_data->>'rarity_score')::double precision,
                    (raw_data->>'ratio_fee_size')::double precision,
                    raw_data->>'tema',
                    raw_data->>'description_ia'
                FROM staging.bitcoin_transactions_raw
                ON CONFLICT (txid) DO UPDATE SET
                    timestamp = EXCLUDED.timestamp,
                    input_count = EXCLUDED.input_count,
                    output_count = EXCLUDED.output_count,
                    fee = EXCLUDED.fee,
                    size = EXCLUDED.size,
                    rarity_score = EXCLUDED.rarity_score,
                    ratio_fee_size = EXCLUDED.ratio_fee_size,
                    tema = EXCLUDED.tema,
                    description_ia = EXCLUDED.description_ia;
            """)

            conn.commit()

            cur.execute("""
                SELECT COUNT(*)
                FROM staging.bitcoin_transactions_raw
            """)
            staging_count = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM core.transactions
            """)
            core_count = cur.fetchone()[0]

            print(f"staging transactions: {staging_count}")
            print(f"core transactions:    {core_count}")

            if staging_count == core_count:
                print("SUCCESS - Transaction transformation complete.")
            else:
                print(
                    f"ERROR - Count mismatch: "
                    f"staging={staging_count}, core={core_count}"
                )

    finally:
        conn.close()


if __name__ == "__main__":
    transform_transactions()
