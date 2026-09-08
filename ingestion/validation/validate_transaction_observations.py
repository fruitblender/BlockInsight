from common import (
    is_empty,
    parse_timestamp,
    parse_integer,
    parse_float,
    validate_ip,
    add_error,
    quarantine_record
)

import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1]))

from database import get_connection


BATCH_ID = 1
SOURCE_TABLE = "staging.transaction_observations_raw"

# Allowed values based on observed Batch 1 data
VALID_MESSAGE_TYPES = {"inv", "tx"}
VALID_DIRECTIONS = {"inbound"}


def validate_observation(row):
    """
    Validate one transaction observation record.

    Returns:
        List of validation errors.
    """

    errors = []

    observation_id = row["observation_id"]
    timestamp = row["timestamp"]
    observer_id = row["observer_id"]
    src_ip = row["src_ip"]
    src_port = row["src_port"]
    dst_ip = row["dst_ip"]
    dst_port = row["dst_port"]
    txid = row["txid"]
    message_type = row["message_type"]
    peer_id = row["peer_id"]
    connection_id = row["connection_id"]
    propagation_delay_ms = row["propagation_delay_ms"]
    direction = row["direction"]
    sequence_number = row["sequence_number"]
    batch_id = row["batch_id"]

    # -------------------------------------------------
    # 1. Required fields
    #
    # connection_id is deliberately NOT required.
    # All 3940 Batch 1 observations have empty
    # connection_id, which is intentional.
    # -------------------------------------------------

    required_fields = {
        "observation_id": observation_id,
        "timestamp": timestamp,
        "observer_id": observer_id,
        "src_ip": src_ip,
        "src_port": src_port,
        "dst_ip": dst_ip,
        "dst_port": dst_port,
        "txid": txid,
        "message_type": message_type,
        "peer_id": peer_id,
        "propagation_delay_ms": propagation_delay_ms,
        "direction": direction,
        "sequence_number": sequence_number,
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
    # 2. Source IP validation
    # -------------------------------------------------

    if not is_empty(src_ip) and not validate_ip(src_ip):

        add_error(
            errors,
            "INVALID_SRC_IP",
            f"Invalid source IP address: {src_ip}"
        )

    # -------------------------------------------------
    # 3. Destination IP validation
    # -------------------------------------------------

    if not is_empty(dst_ip) and not validate_ip(dst_ip):

        add_error(
            errors,
            "INVALID_DST_IP",
            f"Invalid destination IP address: {dst_ip}"
        )

    # -------------------------------------------------
    # 4. Source port validation
    # -------------------------------------------------

    src_port_value = parse_integer(src_port)

    if not is_empty(src_port) and src_port_value is None:

        add_error(
            errors,
            "INVALID_SRC_PORT_TYPE",
            f"Source port must be an integer: {src_port}"
        )

    elif src_port_value is not None:

        if src_port_value < 1 or src_port_value > 65535:

            add_error(
                errors,
                "INVALID_SRC_PORT_RANGE",
                f"Source port must be between 1 and 65535: {src_port_value}"
            )

    # -------------------------------------------------
    # 5. Destination port validation
    # -------------------------------------------------

    dst_port_value = parse_integer(dst_port)

    if not is_empty(dst_port) and dst_port_value is None:

        add_error(
            errors,
            "INVALID_DST_PORT_TYPE",
            f"Destination port must be an integer: {dst_port}"
        )

    elif dst_port_value is not None:

        if dst_port_value < 1 or dst_port_value > 65535:

            add_error(
                errors,
                "INVALID_DST_PORT_RANGE",
                f"Destination port must be between 1 and 65535: {dst_port_value}"
            )

    # -------------------------------------------------
    # 6. Timestamp validation
    # -------------------------------------------------

    timestamp_value = parse_timestamp(timestamp)

    if not is_empty(timestamp) and timestamp_value is None:

        add_error(
            errors,
            "INVALID_TIMESTAMP",
            f"Invalid timestamp: {timestamp}"
        )

    # -------------------------------------------------
    # 7. Message type validation
    #
    # Observed Batch 1 values: inv, tx
    # -------------------------------------------------

    if not is_empty(message_type):

        message_type_value = str(message_type).strip().lower()

        if message_type_value not in VALID_MESSAGE_TYPES:

            add_error(
                errors,
                "INVALID_MESSAGE_TYPE",
                f"Expected message_type in {VALID_MESSAGE_TYPES}, "
                f"found: {message_type}"
            )

    # -------------------------------------------------
    # 8. Direction validation
    #
    # Observed Batch 1 value: inbound
    # -------------------------------------------------

    if not is_empty(direction):

        direction_value = str(direction).strip().lower()

        if direction_value not in VALID_DIRECTIONS:

            add_error(
                errors,
                "INVALID_DIRECTION",
                f"Expected direction in {VALID_DIRECTIONS}, "
                f"found: {direction}"
            )

    # -------------------------------------------------
    # 9. Propagation delay validation
    #
    # Must be numeric and non-negative.
    # Observed Batch 1 range: 5 to 373.921 ms
    # We enforce >= 0 as a physical constraint
    # but do NOT hardcode a max.
    # -------------------------------------------------

    delay_value = parse_float(propagation_delay_ms)

    if not is_empty(propagation_delay_ms) and delay_value is None:

        add_error(
            errors,
            "INVALID_PROPAGATION_DELAY_TYPE",
            f"propagation_delay_ms must be numeric: "
            f"{propagation_delay_ms}"
        )

    elif delay_value is not None:

        if delay_value < 0:

            add_error(
                errors,
                "INVALID_PROPAGATION_DELAY_RANGE",
                f"propagation_delay_ms cannot be negative: "
                f"{delay_value}"
            )

    # -------------------------------------------------
    # 10. Sequence number validation
    #
    # Must be a positive integer.
    # Observed Batch 1 range: 1 to 12.
    # We enforce >= 1 but do NOT hardcode a max.
    # -------------------------------------------------

    sequence_value = parse_integer(sequence_number)

    if not is_empty(sequence_number) and sequence_value is None:

        add_error(
            errors,
            "INVALID_SEQUENCE_NUMBER_TYPE",
            f"sequence_number must be an integer: {sequence_number}"
        )

    elif sequence_value is not None:

        if sequence_value < 1:

            add_error(
                errors,
                "INVALID_SEQUENCE_NUMBER_RANGE",
                f"sequence_number must be >= 1: {sequence_value}"
            )

    # -------------------------------------------------
    # 11. Batch validation
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


def validate_duplicate_observation_ids(connection):
    """
    Find duplicate observation_id values within the current batch.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            observation_id,
            COUNT(*) AS occurrence_count
        FROM staging.transaction_observations_raw
        WHERE batch_id = %s
        GROUP BY observation_id
        HAVING COUNT(*) > 1
        """,
        (BATCH_ID,)
    )

    duplicates = cursor.fetchall()

    print(f"Duplicate observation IDs found: {len(duplicates)}")

    for observation_id, count in duplicates:

        cursor.execute(
            """
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
                connection_id,
                propagation_delay_ms,
                direction,
                sequence_number,
                batch_id
            FROM staging.transaction_observations_raw
            WHERE batch_id = %s
              AND observation_id = %s
            """,
            (BATCH_ID, observation_id)
        )

        duplicate_rows = cursor.fetchall()

        for row in duplicate_rows:

            record = {
                "observation_id": row[0],
                "timestamp": row[1],
                "observer_id": row[2],
                "src_ip": row[3],
                "src_port": row[4],
                "dst_ip": row[5],
                "dst_port": row[6],
                "txid": row[7],
                "message_type": row[8],
                "peer_id": row[9],
                "connection_id": row[10],
                "propagation_delay_ms": row[11],
                "direction": row[12],
                "sequence_number": row[13],
                "batch_id": row[14]
            }

            errors = [
                {
                    "rule": "DUPLICATE_OBSERVATION_ID",
                    "message": (
                        f"observation_id {observation_id} appears "
                        f"{count} times in batch {BATCH_ID}"
                    )
                }
            ]

            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=observation_id,
                errors=errors,
                raw_record=record
            )

    cursor.close()


def validate_txid_references(connection):
    """
    Check that every txid references an existing record
    in staging.bitcoin_transactions_raw for the same batch.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            obs.observation_id,
            obs.timestamp,
            obs.observer_id,
            obs.src_ip,
            obs.src_port,
            obs.dst_ip,
            obs.dst_port,
            obs.txid,
            obs.message_type,
            obs.peer_id,
            obs.connection_id,
            obs.propagation_delay_ms,
            obs.direction,
            obs.sequence_number,
            obs.batch_id
        FROM staging.transaction_observations_raw obs
        LEFT JOIN staging.bitcoin_transactions_raw tx
            ON obs.txid = tx.txid
           AND obs.batch_id = tx.batch_id
        WHERE obs.batch_id = %s
          AND tx.txid IS NULL
        """,
        (BATCH_ID,)
    )

    invalid_rows = cursor.fetchall()

    print(
        f"Observations with invalid txid references: "
        f"{len(invalid_rows)}"
    )

    for row in invalid_rows:

        record = {
            "observation_id": row[0],
            "timestamp": row[1],
            "observer_id": row[2],
            "src_ip": row[3],
            "src_port": row[4],
            "dst_ip": row[5],
            "dst_port": row[6],
            "txid": row[7],
            "message_type": row[8],
            "peer_id": row[9],
            "connection_id": row[10],
            "propagation_delay_ms": row[11],
            "direction": row[12],
            "sequence_number": row[13],
            "batch_id": row[14]
        }

        errors = [
            {
                "rule": "INVALID_TXID_REFERENCE",
                "message": (
                    f"txid {row[7]} does not exist "
                    f"in bitcoin_transactions_raw for batch {BATCH_ID}"
                )
            }
        ]

        quarantine_record(
            connection=connection,
            batch_id=BATCH_ID,
            source_table=SOURCE_TABLE,
            record_id=record["observation_id"],
            errors=errors,
            raw_record=record
        )

    cursor.close()


def validate_peer_references(connection):
    """
    Check that every peer_id references an existing node
    in staging.nodes_raw for the same batch.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            obs.observation_id,
            obs.timestamp,
            obs.observer_id,
            obs.src_ip,
            obs.src_port,
            obs.dst_ip,
            obs.dst_port,
            obs.txid,
            obs.message_type,
            obs.peer_id,
            obs.connection_id,
            obs.propagation_delay_ms,
            obs.direction,
            obs.sequence_number,
            obs.batch_id
        FROM staging.transaction_observations_raw obs
        LEFT JOIN staging.nodes_raw n
            ON obs.peer_id = n.node_id
           AND obs.batch_id = n.batch_id
        WHERE obs.batch_id = %s
          AND n.node_id IS NULL
        """,
        (BATCH_ID,)
    )

    invalid_rows = cursor.fetchall()

    print(
        f"Observations with invalid peer_id references: "
        f"{len(invalid_rows)}"
    )

    for row in invalid_rows:

        record = {
            "observation_id": row[0],
            "timestamp": row[1],
            "observer_id": row[2],
            "src_ip": row[3],
            "src_port": row[4],
            "dst_ip": row[5],
            "dst_port": row[6],
            "txid": row[7],
            "message_type": row[8],
            "peer_id": row[9],
            "connection_id": row[10],
            "propagation_delay_ms": row[11],
            "direction": row[12],
            "sequence_number": row[13],
            "batch_id": row[14]
        }

        errors = [
            {
                "rule": "INVALID_PEER_REFERENCE",
                "message": (
                    f"peer_id {row[9]} does not exist "
                    f"in nodes_raw for batch {BATCH_ID}"
                )
            }
        ]

        quarantine_record(
            connection=connection,
            batch_id=BATCH_ID,
            source_table=SOURCE_TABLE,
            record_id=record["observation_id"],
            errors=errors,
            raw_record=record
        )

    cursor.close()


def validate_observer_references(connection):
    """
    Check that every observer_id references an existing node
    in staging.nodes_raw for the same batch.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            obs.observation_id,
            obs.timestamp,
            obs.observer_id,
            obs.src_ip,
            obs.src_port,
            obs.dst_ip,
            obs.dst_port,
            obs.txid,
            obs.message_type,
            obs.peer_id,
            obs.connection_id,
            obs.propagation_delay_ms,
            obs.direction,
            obs.sequence_number,
            obs.batch_id
        FROM staging.transaction_observations_raw obs
        LEFT JOIN staging.nodes_raw n
            ON obs.observer_id = n.node_id
           AND obs.batch_id = n.batch_id
        WHERE obs.batch_id = %s
          AND n.node_id IS NULL
        """,
        (BATCH_ID,)
    )

    invalid_rows = cursor.fetchall()

    print(
        f"Observations with invalid observer_id references: "
        f"{len(invalid_rows)}"
    )

    for row in invalid_rows:

        record = {
            "observation_id": row[0],
            "timestamp": row[1],
            "observer_id": row[2],
            "src_ip": row[3],
            "src_port": row[4],
            "dst_ip": row[5],
            "dst_port": row[6],
            "txid": row[7],
            "message_type": row[8],
            "peer_id": row[9],
            "connection_id": row[10],
            "propagation_delay_ms": row[11],
            "direction": row[12],
            "sequence_number": row[13],
            "batch_id": row[14]
        }

        errors = [
            {
                "rule": "INVALID_OBSERVER_REFERENCE",
                "message": (
                    f"observer_id {row[2]} does not exist "
                    f"in nodes_raw for batch {BATCH_ID}"
                )
            }
        ]

        quarantine_record(
            connection=connection,
            batch_id=BATCH_ID,
            source_table=SOURCE_TABLE,
            record_id=record["observation_id"],
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
    # Fetch all observations for this batch
    # -------------------------------------------------

    cursor.execute(
        """
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
            connection_id,
            propagation_delay_ms,
            direction,
            sequence_number,
            batch_id
        FROM staging.transaction_observations_raw
        WHERE batch_id = %s
        """,
        (BATCH_ID,)
    )

    rows = cursor.fetchall()

    print(
        f"Found {len(rows)} transaction observations "
        f"for batch {BATCH_ID}"
    )

    valid_count = 0
    invalid_count = 0

    # -------------------------------------------------
    # Row-level validation
    # -------------------------------------------------

    for row in rows:

        record = {
            "observation_id": row[0],
            "timestamp": row[1],
            "observer_id": row[2],
            "src_ip": row[3],
            "src_port": row[4],
            "dst_ip": row[5],
            "dst_port": row[6],
            "txid": row[7],
            "message_type": row[8],
            "peer_id": row[9],
            "connection_id": row[10],
            "propagation_delay_ms": row[11],
            "direction": row[12],
            "sequence_number": row[13],
            "batch_id": row[14]
        }

        errors = validate_observation(record)

        if errors:

            invalid_count += 1

            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=record["observation_id"],
                errors=errors,
                raw_record=record
            )

        else:

            valid_count += 1

    # -------------------------------------------------
    # Dataset-level validation
    # -------------------------------------------------

    validate_duplicate_observation_ids(connection)
    validate_txid_references(connection)
    validate_peer_references(connection)
    validate_observer_references(connection)

    connection.commit()

    cursor.close()
    connection.close()

    print()
    print("Transaction observation validation complete.")
    print(f"Valid records:   {valid_count}")
    print(f"Invalid records: {invalid_count}")


if __name__ == "__main__":
    main()
