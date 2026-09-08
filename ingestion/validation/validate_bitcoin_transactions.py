import re
import sys
from pathlib import Path

from common import (
    is_empty,
    parse_timestamp,
    parse_integer,
    parse_float,
    add_error,
    quarantine_record
)

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1]))

from database import get_connection


BATCH_ID = 1
SOURCE_TABLE = "staging.bitcoin_transactions_raw"

# Bitcoin txid is a 64-character lowercase or uppercase hexadecimal string
TXID_REGEX = re.compile(r"^[0-9a-fA-F]{64}$")


def validate_transaction(row):
    """
    Validate one Bitcoin transaction record.

    Returns:
        List of validation errors.
    """
    errors = []

    txid = row["txid"]
    timestamp = row["timestamp"]
    inputs = row["inputs"]
    outputs = row["outputs"]
    fee = row["fee"]
    size = row["size"]
    ratio_fee_size = row["ratio_fee_size"]
    rarity_score = row["rarity_score"]
    tema = row["tema"]
    description_ia = row["description_ia"]
    batch_id = row["batch_id"]

    # -------------------------------------------------
    # 1. Required fields
    # -------------------------------------------------
    required_fields = {
        "txid": txid,
        "timestamp": timestamp,
        "inputs": inputs,
        "outputs": outputs,
        "fee": fee,
        "size": size,
        "ratio_fee_size": ratio_fee_size,
        "rarity_score": rarity_score,
        "tema": tema,
        "description_ia": description_ia,
        "batch_id": batch_id
    }

    for field, value in required_fields.items():
        if is_empty(value):
            add_error(
                errors,
                "REQUIRED_FIELD",
                f"{field} is required"
            )

    # -------------------------------------------------
    # 2. txid format validation (64-char hex)
    # -------------------------------------------------
    if not is_empty(txid):
        clean_txid = str(txid).strip()
        if not TXID_REGEX.match(clean_txid):
            add_error(
                errors,
                "INVALID_TXID_FORMAT",
                f"txid must be a 64-character hexadecimal hash: {txid}"
            )

    # -------------------------------------------------
    # 3. Timestamp validation
    # -------------------------------------------------
    timestamp_value = parse_timestamp(timestamp)
    if not is_empty(timestamp) and timestamp_value is None:
        add_error(
            errors,
            "INVALID_TIMESTAMP",
            f"Invalid timestamp: {timestamp}"
        )

    # -------------------------------------------------
    # 4. Inputs validation (integer >= 1)
    # -------------------------------------------------
    inputs_value = parse_integer(inputs)
    if not is_empty(inputs) and inputs_value is None:
        add_error(
            errors,
            "INVALID_INPUTS_TYPE",
            f"inputs must be an integer: {inputs}"
        )
    elif inputs_value is not None:
        if inputs_value < 1:
            add_error(
                errors,
                "INVALID_INPUTS_RANGE",
                f"inputs must be at least 1: {inputs_value}"
            )

    # -------------------------------------------------
    # 5. Outputs validation (integer >= 1)
    # -------------------------------------------------
    outputs_value = parse_integer(outputs)
    if not is_empty(outputs) and outputs_value is None:
        add_error(
            errors,
            "INVALID_OUTPUTS_TYPE",
            f"outputs must be an integer: {outputs}"
        )
    elif outputs_value is not None:
        if outputs_value < 1:
            add_error(
                errors,
                "INVALID_OUTPUTS_RANGE",
                f"outputs must be at least 1: {outputs_value}"
            )

    # -------------------------------------------------
    # 6. Fee validation (float/numeric >= 0)
    # -------------------------------------------------
    fee_value = parse_float(fee)
    if not is_empty(fee) and fee_value is None:
        add_error(
            errors,
            "INVALID_FEE_TYPE",
            f"fee must be numeric: {fee}"
        )
    elif fee_value is not None:
        if fee_value < 0:
            add_error(
                errors,
                "INVALID_FEE_RANGE",
                f"fee cannot be negative: {fee_value}"
            )

    # -------------------------------------------------
    # 7. Size validation (integer > 0)
    # -------------------------------------------------
    size_value = parse_integer(size)
    if not is_empty(size) and size_value is None:
        add_error(
            errors,
            "INVALID_SIZE_TYPE",
            f"size must be an integer: {size}"
        )
    elif size_value is not None:
        if size_value < 1:
            add_error(
                errors,
                "INVALID_SIZE_RANGE",
                f"size must be greater than 0: {size_value}"
            )

    # -------------------------------------------------
    # 8. Ratio fee/size validation (numeric >= 0)
    # -------------------------------------------------
    ratio_value = parse_float(ratio_fee_size)
    if not is_empty(ratio_fee_size) and ratio_value is None:
        add_error(
            errors,
            "INVALID_RATIO_FEE_SIZE_TYPE",
            f"ratio_fee_size must be numeric: {ratio_fee_size}"
        )
    elif ratio_value is not None:
        if ratio_value < 0:
            add_error(
                errors,
                "INVALID_RATIO_FEE_SIZE_RANGE",
                f"ratio_fee_size cannot be negative: {ratio_value}"
            )

    # -------------------------------------------------
    # 9. Rarity score validation (numeric in 0.0 .. 1.0)
    # -------------------------------------------------
    rarity_value = parse_float(rarity_score)
    if not is_empty(rarity_score) and rarity_value is None:
        add_error(
            errors,
            "INVALID_RARITY_SCORE_TYPE",
            f"rarity_score must be numeric: {rarity_score}"
        )
    elif rarity_value is not None:
        if rarity_value < 0.0 or rarity_value > 1.0:
            add_error(
                errors,
                "INVALID_RARITY_SCORE_RANGE",
                f"rarity_score must be between 0.0 and 1.0: {rarity_value}"
            )

    # -------------------------------------------------
    # 10. Batch consistency
    # -------------------------------------------------
    try:
        row_batch_id = int(batch_id)
        if row_batch_id != BATCH_ID:
            add_error(
                errors,
                "BATCH_MISMATCH",
                f"Expected batch_id {BATCH_ID}, found {row_batch_id}"
            )
    except (ValueError, TypeError):
        add_error(
            errors,
            "INVALID_BATCH_ID",
            f"Invalid batch_id: {batch_id}"
        )

    return errors


# =============================================================
# DATASET-LEVEL VALIDATION
# =============================================================


def validate_duplicate_txids(connection):
    """
    Find duplicate txid values within the current batch.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            txid,
            COUNT(*) AS occurrence_count
        FROM staging.bitcoin_transactions_raw
        WHERE batch_id = %s
        GROUP BY txid
        HAVING COUNT(*) > 1
        """,
        (BATCH_ID,)
    )

    duplicates = cursor.fetchall()
    print(f"Duplicate txids found: {len(duplicates)}")

    for txid, count in duplicates:
        cursor.execute(
            """
            SELECT
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
            FROM staging.bitcoin_transactions_raw
            WHERE batch_id = %s
              AND txid = %s
            """,
            (BATCH_ID, txid)
        )

        duplicate_rows = cursor.fetchall()

        for row in duplicate_rows:
            record = {
                "txid": row[0],
                "timestamp": row[1],
                "inputs": row[2],
                "outputs": row[3],
                "fee": row[4],
                "size": row[5],
                "ratio_fee_size": row[6],
                "rarity_score": row[7],
                "tema": row[8],
                "description_ia": row[9],
                "batch_id": row[10]
            }

            errors = [
                {
                    "rule": "DUPLICATE_TXID",
                    "message": f"txid {txid} appears {count} times in batch {BATCH_ID}"
                }
            ]

            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=txid,
                errors=errors,
                raw_record=record
            )

    cursor.close()


# =============================================================
# MAIN
# =============================================================


def main():
    connection = get_connection()
    cursor = connection.cursor()

    # -------------------------------------------------
    # Fetch all transactions for this batch
    # -------------------------------------------------
    cursor.execute(
        """
        SELECT
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
        FROM staging.bitcoin_transactions_raw
        WHERE batch_id = %s
        """,
        (BATCH_ID,)
    )

    rows = cursor.fetchall()

    print(f"Found {len(rows)} Bitcoin transactions for batch {BATCH_ID}")

    valid_count = 0
    invalid_count = 0

    # -------------------------------------------------
    # Row-level validation
    # -------------------------------------------------
    for row in rows:
        record = {
            "txid": row[0],
            "timestamp": row[1],
            "inputs": row[2],
            "outputs": row[3],
            "fee": row[4],
            "size": row[5],
            "ratio_fee_size": row[6],
            "rarity_score": row[7],
            "tema": row[8],
            "description_ia": row[9],
            "batch_id": row[10]
        }

        errors = validate_transaction(record)

        if errors:
            invalid_count += 1
            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=record["txid"],
                errors=errors,
                raw_record=record
            )
        else:
            valid_count += 1

    # -------------------------------------------------
    # Dataset-level validation
    # -------------------------------------------------
    validate_duplicate_txids(connection)

    connection.commit()

    cursor.close()
    connection.close()

    print()
    print("Bitcoin transaction validation complete.")
    print(f"Valid records:   {valid_count}")
    print(f"Invalid records: {invalid_count}")


if __name__ == "__main__":
    main()
