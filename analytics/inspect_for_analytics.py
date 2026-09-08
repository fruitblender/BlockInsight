"""
Phase 5 - Step 1: Database Inspection for Analytics
Inspects existing schemas, tables, views, columns, and indexes in PostgreSQL.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

def inspect_database():
    conn = get_connection()
    cur = conn.cursor()

    print("============================================================")
    print("      PHASE 5 - STEP 1: DATABASE INSPECTION FOR ANALYTICS   ")
    print("============================================================\n")

    # 1. Existing Schemas
    cur.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY schema_name;
    """)
    schemas = [r[0] for r in cur.fetchall()]
    print(f"1. NON-SYSTEM SCHEMAS FOUND ({len(schemas)}):")
    for s in schemas:
        print(f"   - {s}")
    print()

    # 2. Tables and Views in 'core' and 'analytics'
    cur.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema IN ('core', 'analytics')
        ORDER BY table_schema, table_name;
    """)
    objects = cur.fetchall()
    print(f"2. OBJECTS IN 'core' AND 'analytics' SCHEMAS ({len(objects)}):")
    if not objects:
        print("   (None found)")
    for s, name, ttype in objects:
        print(f"   - {s}.{name:30s} [{ttype}]")
    print()

    # 3. Exact Columns in core.* tables
    cur.execute("""
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'core'
        ORDER BY table_name, ordinal_position;
    """)
    cols = cur.fetchall()
    print("3. COLUMNS IN 'core' SCHEMA TABLES:")
    current_table = None
    for tbl, col, dt, nl in cols:
        if tbl != current_table:
            current_table = tbl
            print(f"\n   [core.{tbl}]")
        print(f"     {col:25s} {dt:22s} {'(NULL)' if nl == 'YES' else 'NOT NULL'}")
    print("\n")

    # 4. Primary Keys and Indexes in 'core'
    cur.execute("""
        SELECT tablename, indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'core'
        ORDER BY tablename, indexname;
    """)
    indexes = cur.fetchall()
    print(f"4. EXISTING INDEXES IN 'core' ({len(indexes)}):")
    for tbl, idx, defn in indexes:
        print(f"   - core.{tbl:26s} | {idx:32s}")
    print()

    print("============================================================")
    print("                STEP 1 INSPECTION COMPLETE                  ")
    print("============================================================")

    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_database()
