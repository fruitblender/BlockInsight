"""Inspect full database schema for SRS evaluation."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

conn = get_connection()
cur = conn.cursor()

tables_to_check = [
    ("core", "transactions"),
    ("core", "nodes"),
    ("core", "transaction_observations"),
    ("core", "peer_connections"),
    ("core", "network_events"),
]

for schema, table in tables_to_check:
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema=%s AND table_name=%s 
        ORDER BY ordinal_position;
    """, (schema, table))
    rows = cur.fetchall()
    print(f"\n=== {schema}.{table} ({len(rows)} columns) ===")
    for r in rows:
        print(f"  {r[0]:35s} {r[1]}")

# All schemas
cur.execute("""
    SELECT schema_name FROM information_schema.schemata 
    WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast') 
    ORDER BY 1;
""")
print("\n=== Database Schemas ===")
for r in cur.fetchall():
    print(f"  {r[0]}")

# All views in analytics
cur.execute("""
    SELECT table_name FROM information_schema.views 
    WHERE table_schema='analytics' ORDER BY 1;
""")
print("\n=== analytics views ===")
for r in cur.fetchall():
    print(f"  {r[0]}")

# Sample transaction row
cur.execute("SELECT * FROM core.transactions LIMIT 1;")
cols = [desc[0] for desc in cur.description]
row = cur.fetchone()
print("\n=== Sample core.transactions row ===")
for c, v in zip(cols, row):
    print(f"  {c:35s} = {v}")

# Sample node row
cur.execute("SELECT * FROM core.nodes LIMIT 1;")
cols = [desc[0] for desc in cur.description]
row = cur.fetchone()
print("\n=== Sample core.nodes row ===")
for c, v in zip(cols, row):
    print(f"  {c:35s} = {v}")

# Check if staging tables exist
cur.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema='staging' ORDER BY 1;
""")
print("\n=== staging tables ===")
for r in cur.fetchall():
    print(f"  {r[0]}")

# Check validation schema
cur.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema='validation' ORDER BY 1;
""")
print("\n=== validation tables ===")
for r in cur.fetchall():
    print(f"  {r[0]}")

conn.close()
