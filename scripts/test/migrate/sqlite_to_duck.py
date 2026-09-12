import duckdb
import time

db_path = "data/hk-market.duckdb"
sqlite_path = "data/hkex-market-breadth.db"

# Connect to target DuckDB file
con = duckdb.connect(db_path)

# Initialize extensions
con.execute("INSTALL sqlite; LOAD sqlite;")
con.execute(f"ATTACH '{sqlite_path}' AS sqlite_db (TYPE sqlite);")

# Discover all tables in the SQLite database
tables = con.execute("SHOW TABLES FROM sqlite_db").fetchall()

print(f"Found {len(tables)} tables to migrate...")

for (table_name,) in tables:
    start_time = time.time()
    print(f"Migrating {table_name}...", end="", flush=True)
    
    # Direct block copy using DuckDB engine
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM sqlite_db.{table_name};")
    
    elapsed = time.time() - start_time
    print(f" Done ({elapsed:.2f}s)")

print("\nAll tables migrated successfully!")
con.close()