import sys
from pathlib import Path

from common import (
    is_empty,
    parse_timestamp,
    add_error,
    quarantine_record
)

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1]))

from database import get_connection


BATCH_ID = 1
SOURCE_TABLE = "staging.network_events_raw"

VALID_EVENT_TYPES = {"peer_connect"}


def validate_network_event(row):
    """
    Validate one network event record.

    Returns:
        List of validation errors.
    """
    errors = []

    event_id = row["event_id"]
    timestamp = row["timestamp"]
    event_type = row["event_type"]
    node_id = row["node_id"]
    peer_id = row["peer_id"]
    connection_id = row["connection_id"]
    batch_id = row["batch_id"]

    # -------------------------------------------------
    # 1. Required fields
    # -------------------------------------------------
    required_fields = {
        "event_id": event_id,
        "timestamp": timestamp,
        "event_type": event_type,
        "node_id": node_id,
        "peer_id": peer_id,
        "connection_id": connection_id,
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
    # 2. Timestamp validation
    # -------------------------------------------------
    timestamp_value = parse_timestamp(timestamp)
    if not is_empty(timestamp) and timestamp_value is None:
        add_error(
            errors,
            "INVALID_TIMESTAMP",
            f"Invalid timestamp: {timestamp}"
        )

    # -------------------------------------------------
    # 3. Event type validation
    # -------------------------------------------------
    if not is_empty(event_type):
        clean_event_type = str(event_type).strip().lower()
        if clean_event_type not in VALID_EVENT_TYPES:
            add_error(
                errors,
                "INVALID_EVENT_TYPE",
                f"Expected event_type in {VALID_EVENT_TYPES}, found: {event_type}"
            )

    # -------------------------------------------------
    # 4. Batch validation
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


def validate_duplicate_event_ids(connection):
    """
    Find duplicate event_id values within the current batch.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            event_id,
            COUNT(*) AS occurrence_count
        FROM staging.network_events_raw
        WHERE batch_id = %s
        GROUP BY event_id
        HAVING COUNT(*) > 1
        """,
        (BATCH_ID,)
    )

    duplicates = cursor.fetchall()
    print(f"Duplicate event IDs found: {len(duplicates)}")

    for event_id, count in duplicates:
        cursor.execute(
            """
            SELECT
                event_id,
                timestamp,
                event_type,
                node_id,
                peer_id,
                connection_id,
                batch_id
            FROM staging.network_events_raw
            WHERE batch_id = %s
              AND event_id = %s
            """,
            (BATCH_ID, event_id)
        )

        duplicate_rows = cursor.fetchall()

        for row in duplicate_rows:
            record = {
                "event_id": row[0],
                "timestamp": row[1],
                "event_type": row[2],
                "node_id": row[3],
                "peer_id": row[4],
                "connection_id": row[5],
                "batch_id": row[6]
            }

            errors = [
                {
                    "rule": "DUPLICATE_EVENT_ID",
                    "message": f"event_id {event_id} appears {count} times in batch {BATCH_ID}"
                }
            ]

            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=event_id,
                errors=errors,
                raw_record=record
            )

    cursor.close()


def validate_node_and_peer_references(connection):
    """
    Verify that node_id and peer_id both exist in staging.nodes_raw for this batch.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            ne.event_id,
            ne.timestamp,
            ne.event_type,
            ne.node_id,
            ne.peer_id,
            ne.connection_id,
            ne.batch_id
        FROM staging.network_events_raw ne
        LEFT JOIN staging.nodes_raw n
            ON ne.node_id = n.node_id AND ne.batch_id = n.batch_id
        LEFT JOIN staging.nodes_raw p
            ON ne.peer_id = p.node_id AND ne.batch_id = p.batch_id
        WHERE ne.batch_id = %s
          AND (n.node_id IS NULL OR p.node_id IS NULL)
        """,
        (BATCH_ID,)
    )

    invalid_rows = cursor.fetchall()
    print(f"Network events with invalid node/peer references: {len(invalid_rows)}")

    for row in invalid_rows:
        record = {
            "event_id": row[0],
            "timestamp": row[1],
            "event_type": row[2],
            "node_id": row[3],
            "peer_id": row[4],
            "connection_id": row[5],
            "batch_id": row[6]
        }

        errors = [
            {
                "rule": "INVALID_NODE_OR_PEER_REFERENCE",
                "message": f"node_id {row[3]} or peer_id {row[4]} does not exist in nodes_raw for batch {BATCH_ID}"
            }
        ]

        quarantine_record(
            connection=connection,
            batch_id=BATCH_ID,
            source_table=SOURCE_TABLE,
            record_id=record["event_id"],
            errors=errors,
            raw_record=record
        )

    cursor.close()


def validate_connection_references(connection):
    """
    Verify that connection_id exists in staging.peer_connections_raw for this batch.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            ne.event_id,
            ne.timestamp,
            ne.event_type,
            ne.node_id,
            ne.peer_id,
            ne.connection_id,
            ne.batch_id
        FROM staging.network_events_raw ne
        LEFT JOIN staging.peer_connections_raw pc
            ON ne.connection_id = pc.connection_id AND ne.batch_id = pc.batch_id
        WHERE ne.batch_id = %s
          AND pc.connection_id IS NULL
        """,
        (BATCH_ID,)
    )

    invalid_rows = cursor.fetchall()
    print(f"Network events with invalid connection references: {len(invalid_rows)}")

    for row in invalid_rows:
        record = {
            "event_id": row[0],
            "timestamp": row[1],
            "event_type": row[2],
            "node_id": row[3],
            "peer_id": row[4],
            "connection_id": row[5],
            "batch_id": row[6]
        }

        errors = [
            {
                "rule": "INVALID_CONNECTION_REFERENCE",
                "message": f"connection_id {row[5]} does not exist in peer_connections_raw for batch {BATCH_ID}"
            }
        ]

        quarantine_record(
            connection=connection,
            batch_id=BATCH_ID,
            source_table=SOURCE_TABLE,
            record_id=record["event_id"],
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
    # Fetch all network events for this batch
    # -------------------------------------------------
    cursor.execute(
        """
        SELECT
            event_id,
            timestamp,
            event_type,
            node_id,
            peer_id,
            connection_id,
            batch_id
        FROM staging.network_events_raw
        WHERE batch_id = %s
        """,
        (BATCH_ID,)
    )

    rows = cursor.fetchall()
    print(f"Found {len(rows)} network events for batch {BATCH_ID}")

    valid_count = 0
    invalid_count = 0

    # -------------------------------------------------
    # Row-level validation
    # -------------------------------------------------
    for row in rows:
        record = {
            "event_id": row[0],
            "timestamp": row[1],
            "event_type": row[2],
            "node_id": row[3],
            "peer_id": row[4],
            "connection_id": row[5],
            "batch_id": row[6]
        }

        errors = validate_network_event(record)

        if errors:
            invalid_count += 1
            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=record["event_id"],
                errors=errors,
                raw_record=record
            )
        else:
            valid_count += 1

    # -------------------------------------------------
    # Dataset-level validation
    # -------------------------------------------------
    validate_duplicate_event_ids(connection)
    validate_node_and_peer_references(connection)
    validate_connection_references(connection)

    connection.commit()

    cursor.close()
    connection.close()

    print()
    print("Network event validation complete.")
    print(f"Valid records:   {valid_count}")
    print(f"Invalid records: {invalid_count}")


if __name__ == "__main__":
    main()
