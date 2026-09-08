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
SOURCE_TABLE = "staging.peer_connections_raw"


def validate_peer_connection(row):
    """
    Validate one peer connection record.

    Returns:
        List of validation errors.
    """

    errors = []

    connection_id = row["connection_id"]
    timestamp_start = row["timestamp_start"]
    timestamp_end = row["timestamp_end"]
    src_node_id = row["src_node_id"]
    dst_node_id = row["dst_node_id"]
    src_ip = row["src_ip"]
    dst_ip = row["dst_ip"]
    src_port = row["src_port"]
    dst_port = row["dst_port"]
    direction = row["direction"]
    batch_id = row["batch_id"]

    # -------------------------------------------------
    # 1. Required fields
    # -------------------------------------------------

    required_fields = {
        "connection_id": connection_id,
        "timestamp_start": timestamp_start,
        "timestamp_end": timestamp_end,
        "src_node_id": src_node_id,
        "dst_node_id": dst_node_id,
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "src_port": src_port,
        "dst_port": dst_port,
        "direction": direction,
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

    if not is_empty(src_ip) and not validate_ip(src_ip):

        add_error(
            errors,
            "INVALID_SRC_IP",
            f"Invalid source IP address: {src_ip}"
        )

    if not is_empty(dst_ip) and not validate_ip(dst_ip):

        add_error(
            errors,
            "INVALID_DST_IP",
            f"Invalid destination IP address: {dst_ip}"
        )

    # -------------------------------------------------
    # 3. Source port validation
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
    # 4. Destination port validation
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
    # 5. Timestamp validation
    # -------------------------------------------------

    start_time = parse_timestamp(timestamp_start)
    end_time = parse_timestamp(timestamp_end)

    if not is_empty(timestamp_start) and start_time is None:

        add_error(
            errors,
            "INVALID_TIMESTAMP_START",
            f"Invalid timestamp_start: {timestamp_start}"
        )

    if not is_empty(timestamp_end) and end_time is None:

        add_error(
            errors,
            "INVALID_TIMESTAMP_END",
            f"Invalid timestamp_end: {timestamp_end}"
        )

    # -------------------------------------------------
    # 6. Timestamp consistency
    # -------------------------------------------------

    if (
        start_time is not None
        and end_time is not None
        and end_time < start_time
    ):

        add_error(
            errors,
            "INVALID_TIME_RANGE",
            "timestamp_end cannot be earlier than timestamp_start"
        )

    # -------------------------------------------------
    # 7. Direction validation
    # -------------------------------------------------

    if not is_empty(direction):

        direction_value = str(direction).strip().lower()

        if direction_value != "outbound":

            add_error(
                errors,
                "INVALID_DIRECTION",
                f"Expected direction 'outbound', found: {direction}"
            )

    # -------------------------------------------------
    # 8. Batch validation
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


def validate_duplicate_connection_ids(connection):
    """
    Find duplicate connection_id values within the current batch.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            connection_id,
            COUNT(*) AS occurrence_count
        FROM staging.peer_connections_raw
        WHERE batch_id = %s
        GROUP BY connection_id
        HAVING COUNT(*) > 1
        """,
        (BATCH_ID,)
    )

    duplicates = cursor.fetchall()

    print(f"Duplicate connection IDs found: {len(duplicates)}")

    for connection_id, count in duplicates:

        cursor.execute(
            """
            SELECT
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
            FROM staging.peer_connections_raw
            WHERE batch_id = %s
              AND connection_id = %s
            """,
            (BATCH_ID, connection_id)
        )

        duplicate_rows = cursor.fetchall()

        for row in duplicate_rows:

            record = {
                "connection_id": row[0],
                "timestamp_start": row[1],
                "timestamp_end": row[2],
                "src_node_id": row[3],
                "dst_node_id": row[4],
                "src_ip": row[5],
                "dst_ip": row[6],
                "src_port": row[7],
                "dst_port": row[8],
                "direction": row[9],
                "batch_id": row[10]
            }

            errors = [
                {
                    "rule": "DUPLICATE_CONNECTION_ID",
                    "message": (
                        f"connection_id {connection_id} appears "
                        f"{count} times in batch {BATCH_ID}"
                    )
                }
            ]

            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=connection_id,
                errors=errors,
                raw_record=record
            )

    cursor.close()

def validate_node_references(connection):
    """
    Check that src_node_id and dst_node_id exist
    in staging.nodes_raw for the same batch.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            pc.connection_id,
            pc.src_node_id,
            pc.dst_node_id,
            pc.src_ip,
            pc.dst_ip,
            pc.src_port,
            pc.dst_port,
            pc.timestamp_start,
            pc.timestamp_end,
            pc.direction,
            pc.batch_id
        FROM staging.peer_connections_raw pc
        LEFT JOIN staging.nodes_raw src
            ON pc.src_node_id = src.node_id
           AND pc.batch_id = src.batch_id
        LEFT JOIN staging.nodes_raw dst
            ON pc.dst_node_id = dst.node_id
           AND pc.batch_id = dst.batch_id
        WHERE pc.batch_id = %s
          AND (
              src.node_id IS NULL
              OR dst.node_id IS NULL
          )
        """,
        (BATCH_ID,)
    )

    invalid_rows = cursor.fetchall()

    print(f"Connections with invalid node references: {len(invalid_rows)}")

    for row in invalid_rows:

        record = {
            "connection_id": row[0],
            "src_node_id": row[1],
            "dst_node_id": row[2],
            "src_ip": row[3],
            "dst_ip": row[4],
            "src_port": row[5],
            "dst_port": row[6],
            "timestamp_start": row[7],
            "timestamp_end": row[8],
            "direction": row[9],
            "batch_id": row[10]
        }

        errors = []

        if row[1] is None or str(row[1]).strip() == "":
            errors.append({
                "rule": "MISSING_SRC_NODE_REFERENCE",
                "message": "Source node reference is missing"
            })

        else:
            cursor.execute(
                """
                SELECT 1
                FROM staging.nodes_raw
                WHERE node_id = %s
                  AND batch_id = %s
                """,
                (row[1], BATCH_ID)
            )

            if cursor.fetchone() is None:
                errors.append({
                    "rule": "INVALID_SRC_NODE_REFERENCE",
                    "message": (
                        f"Source node {row[1]} "
                        f"does not exist in batch {BATCH_ID}"
                    )
                })

        if row[2] is None or str(row[2]).strip() == "":
            errors.append({
                "rule": "MISSING_DST_NODE_REFERENCE",
                "message": "Destination node reference is missing"
            })

        else:
            cursor.execute(
                """
                SELECT 1
                FROM staging.nodes_raw
                WHERE node_id = %s
                  AND batch_id = %s
                """,
                (row[2], BATCH_ID)
            )

            if cursor.fetchone() is None:
                errors.append({
                    "rule": "INVALID_DST_NODE_REFERENCE",
                    "message": (
                        f"Destination node {row[2]} "
                        f"does not exist in batch {BATCH_ID}"
                    )
                })

        if errors:

            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=record["connection_id"],
                errors=errors,
                raw_record=record
            )

    cursor.close()

def validate_destination_endpoints(connection):
    """
    Verify that the destination IP and port of each
    outbound connection match the referenced destination node.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            pc.connection_id,
            pc.dst_node_id,
            pc.dst_ip,
            pc.dst_port,
            n.ip AS node_ip,
            n.port AS node_port,
            pc.batch_id
        FROM staging.peer_connections_raw pc
        JOIN staging.nodes_raw n
            ON pc.dst_node_id = n.node_id
           AND pc.batch_id = n.batch_id
        WHERE pc.batch_id = %s
          AND (
              pc.dst_ip <> n.ip
              OR pc.dst_port <> n.port
          )
        """,
        (BATCH_ID,)
    )

    invalid_rows = cursor.fetchall()

    print(
        f"Connections with invalid destination endpoints: "
        f"{len(invalid_rows)}"
    )

    for row in invalid_rows:

        record = {
            "connection_id": row[0],
            "dst_node_id": row[1],
            "dst_ip": row[2],
            "dst_port": row[3],
            "node_ip": row[4],
            "node_port": row[5],
            "batch_id": row[6]
        }

        errors = []

        if row[2] != row[4]:

            errors.append({
                "rule": "DST_IP_MISMATCH",
                "message": (
                    f"Connection destination IP {row[2]} "
                    f"does not match node {row[1]} IP {row[4]}"
                )
            })

        if row[3] != row[5]:

            errors.append({
                "rule": "DST_PORT_MISMATCH",
                "message": (
                    f"Connection destination port {row[3]} "
                    f"does not match node {row[1]} port {row[5]}"
                )
            })

        if errors:

            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=record["connection_id"],
                errors=errors,
                raw_record=record
            )

    cursor.close()

def main():

    connection = get_connection()
    cursor = connection.cursor()

    # -------------------------------------------------
    # Fetch Batch 1 connections
    # -------------------------------------------------

    cursor.execute(
        """
        SELECT
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
        FROM staging.peer_connections_raw
        WHERE batch_id = %s
        """,
        (BATCH_ID,)
    )

    rows = cursor.fetchall()

    print(
        f"Found {len(rows)} peer connections "
        f"for batch {BATCH_ID}"
    )

    valid_count = 0
    invalid_count = 0

    # -------------------------------------------------
    # Validate individual records
    # -------------------------------------------------

    for row in rows:

        record = {
            "connection_id": row[0],
            "timestamp_start": row[1],
            "timestamp_end": row[2],
            "src_node_id": row[3],
            "dst_node_id": row[4],
            "src_ip": row[5],
            "dst_ip": row[6],
            "src_port": row[7],
            "dst_port": row[8],
            "direction": row[9],
            "batch_id": row[10]
        }

        errors = validate_peer_connection(record)

        if errors:

            invalid_count += 1

            quarantine_record(
                connection=connection,
                batch_id=BATCH_ID,
                source_table=SOURCE_TABLE,
                record_id=record["connection_id"],
                errors=errors,
                raw_record=record
            )

        else:

            valid_count += 1

    # -------------------------------------------------
    # Dataset-level duplicate validation
    # -------------------------------------------------

    validate_duplicate_connection_ids(connection)
    validate_node_references(connection)
    validate_destination_endpoints(connection)
    
    connection.commit()

    cursor.close()
    connection.close()

    print()
    print("Peer connection validation complete.")
    print(f"Valid records:   {valid_count}")
    print(f"Invalid records: {invalid_count}")


if __name__ == "__main__":
    main()