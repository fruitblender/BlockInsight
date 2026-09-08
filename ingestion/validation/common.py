import json
import ipaddress
from datetime import datetime


def is_empty(value):
    """
    Return True if a value is NULL or an empty/whitespace string.
    """
    return value is None or str(value).strip() == ""


def parse_timestamp(value):
    """
    Try to convert a raw timestamp into a Python datetime.

    Returns:
        datetime object if valid
        None if invalid
    """
    if is_empty(value):
        return None

    try:
        return datetime.fromisoformat(str(value).strip())
    except (ValueError, TypeError):
        return None


def parse_integer(value):
    """
    Try to convert a raw value into an integer.

    Returns:
        integer if valid
        None if invalid
    """
    if is_empty(value):
        return None

    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None


def parse_float(value):
    """
    Try to convert a raw value into a float.

    Returns:
        float if valid
        None if invalid
    """
    if is_empty(value):
        return None

    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None


def validate_ip(value):
    """
    Check whether a value is a valid IPv4 or IPv6 address.

    Returns:
        True if valid
        False otherwise
    """
    if is_empty(value):
        return False

    try:
        ipaddress.ip_address(str(value).strip())
        return True
    except ValueError:
        return False


def add_error(errors, rule, message):
    """
    Add a validation error to the current record.
    """
    errors.append({
        "rule": rule,
        "message": message
    })


def quarantine_record(
    connection,
    batch_id,
    source_table,
    record_id,
    errors,
    raw_record
):
    """
    Store validation errors in validation.quarantine.

    One row is created for each validation error.
    """

    cursor = connection.cursor()

    for error in errors:
        cursor.execute(
            """
            INSERT INTO validation.quarantine (
                batch_id,
                source_table,
                record_id,
                validation_rule,
                error_message,
                raw_record
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                batch_id,
                source_table,
                record_id,
                error["rule"],
                error["message"],
                json.dumps(raw_record)
            )
        )

    cursor.close()
    