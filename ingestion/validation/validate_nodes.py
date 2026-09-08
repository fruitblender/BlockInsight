from common import (
    is_empty,
    parse_timestamp,
    parse_integer,
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
SOURCE_TABLE = "staging.nodes_raw"


def validate_node(row):
    """
    Validate one node record.

    Returns:
        List of validation errors.
    """

    errors = []

    node_id = row["node_id"]
    ip = row["ip"]
    port = row["port"]
    country = row["country"]
    asn = row["asn"]
    node_type = row["node_type"]
    first_seen = row["first_seen"]
    last_seen = row["last_seen"]
    batch_id = row["batch_id"]

    # -------------------------------------------------
    # 1. Required fields / empty strings
    # -------------------------------------------------

    required_fields = {
        "node_id": node_id,
        "ip": ip,
        "port": port,
        "country": country,
        "asn": asn,
        "node_type": node_type,
        "first_seen": first_seen,
        "last_seen": last_seen,
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
    # 2. IP validation
    # -------------------------------------------------

    if not is_empty(ip) and not validate_ip(ip):
        add_error(
            errors,
            "INVALID_IP",
            f"Invalid IP address: {ip}"
        )

    # -------------------------------------------------
    # 3. Port validation
    # -------------------------------------------------

    port_value = parse_integer(port)

    if not is_empty(port) and port_value is None:
        add_error(
            errors,
            "INVALID_PORT_TYPE",
            f"Port must be an integer: {port}"
        )

    elif port_value is not None:
        if port_value < 1 or port_value > 65535:
            add_error(
                errors,
                "INVALID_PORT_RANGE",
                f"Port must be between 1 and 65535: {port_value}"
            )

    # -------------------------------------------------
    # 4. Timestamp validation
    # -------------------------------------------------

    first_seen_value = parse_timestamp(first_seen)
    last_seen_value = parse_timestamp(last_seen)

    if not is_empty(first_seen) and first_seen_value is None:
        add_error(
            errors,
            "INVALID_FIRST_SEEN",
            f"Invalid first_seen timestamp: {first_seen}"
        )

    if not is_empty(last_seen) and last_seen_value is None:
        add_error(
            errors,
            "INVALID_LAST_SEEN",
            f"Invalid last_seen timestamp: {last_seen}"
        )

    # -------------------------------------------------
    # 5. Timestamp consistency
    # -------------------------------------------------

    if (
        first_seen_value is not None
        and last_seen_value is not None
        and last_seen_value < first_seen_value
    ):
        add_error(
            errors,
            "INVALID_TIME_RANGE",
            "last_seen cannot be earlier than first_seen"
        )

    # -------------------------------------------------
    # 6. Batch consistency
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


def validate_duplicate_node_ids(connection):
    """
    Find duplicate node_id values within the current batch.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            node_id,
            COUNT(*) AS occurrence_count
        FROM staging.nodes_raw
        WHERE batch_id = %s
        GROUP BY node_id
        HAVING COUNT(*) > 1
        """,
        (BATCH_ID,)
    )

    duplicates = cursor.fetchall()

    print(f"Duplicate node IDs found: {len(duplicates)}")

    for node_id, count in duplicates:

        cursor.execute(
            """
            SELECT
                node_id,
                ip,
                port,
                country,
                asn,
                node_type,
                first_seen,
                last_seen,
                batch_id
            FROM staging.nodes_raw
            WHERE batch_id = %s
              AND node_id = %s
            """,
            (BATCH_ID, node_id)
        )

        duplicate_rows = cursor.fetchall()

        for row in duplicate_rows:

            record = {
                "node_id": row[0],
                "ip": row[1],
                "port": row[2],
                "country": row[3],
                "asn": row[4],
                "node_type": row[5],
                "first_seen": row[6],
                "last_seen": row[7],
                "batch_id": row[8]
            }

            errors = [
                {
                    "rule": "DUPLICATE_NODE_ID",
                    "message": (
                        f"node_id {node_id} appears "
                        f"{count} times in batch {BATCH_ID}"
                    )
                }
            ]

            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=node_id,
                errors=errors,
                raw_record=record
            )

    cursor.close()

def main():

    connection = get_connection()

    cursor = connection.cursor()

    # -------------------------------------------------
    # Fetch all nodes for this batch
    # -------------------------------------------------

    cursor.execute(
        """
        SELECT
            node_id,
            ip,
            port,
            country,
            asn,
            node_type,
            first_seen,
            last_seen,
            batch_id
        FROM staging.nodes_raw
        WHERE batch_id = %s
        """,
        (BATCH_ID,)
    )

    rows = cursor.fetchall()

    print(f"Found {len(rows)} node records for batch {BATCH_ID}")

    valid_count = 0
    invalid_count = 0

    # -------------------------------------------------
    # Validate each record
    # -------------------------------------------------

    for row in rows:

        record = {
            "node_id": row[0],
            "ip": row[1],
            "port": row[2],
            "country": row[3],
            "asn": row[4],
            "node_type": row[5],
            "first_seen": row[6],
            "last_seen": row[7],
            "batch_id": row[8]
        }

        errors = validate_node(record)

        if errors:

            invalid_count += 1

            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=record["node_id"],
                errors=errors,
                raw_record=record
            )

        else:
            valid_count += 1

        
    validate_duplicate_node_ids(connection)
    connection.commit()

    cursor.close()
    connection.close()

    print()
    print("Node validation complete.")
    print(f"Valid records:   {valid_count}")
    print(f"Invalid records: {invalid_count}")


if __name__ == "__main__":
    main()