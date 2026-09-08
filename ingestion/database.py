import psycopg


def get_connection():
    return psycopg.connect(
        host="172.25.240.1",
        port=5432,
        dbname="bitcoin_monitor",
        user="postgres",
        password="1305"
    )
