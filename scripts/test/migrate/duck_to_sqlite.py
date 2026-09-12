import duckdb

duckdb_path = "data/us-market-duck.db"
sqlite_path = "data/us-market-sqlite.db"

conn = duckdb.connect(duckdb_path)

# 1. Install and load the SQLite extension in DuckDB
conn.execute("INSTALL sqlite; LOAD sqlite;")

# 2. Attach the SQLite database file
conn.execute(f"ATTACH '{sqlite_path}' AS sqlite_db (TYPE SQLITE);")

# 3. Export specific tables directly from DuckDB to SQLite
# Option A: Create new tables in SQLite from DuckDB
# conn.execute(
#    "CREATE TABLE sqlite_db.DAILY_MARKET_STATS AS SELECT * FROM main.DAILY_MARKET_STATS;"
#)
#conn.execute(
#    "CREATE TABLE sqlite_db.DAILY_SECTORS_STATS AS SELECT * FROM main.DAILY_SECTORS_STATS;"
#)
#conn.execute(
#    "CREATE TABLE sqlite_db.DAILY_STOCK_PRICE AS SELECT * FROM main.DAILY_STOCK_PRICE;"
#)
#conn.execute(
#    "CREATE TABLE sqlite_db.DAILY_STOCK_STATS AS SELECT * FROM main.DAILY_STOCK_STATS;"
#)
conn.execute(
    "CREATE TABLE sqlite_db.STOCK AS SELECT * FROM main.STOCK;"
)


# Option B: Insert into existing SQLite tables (if schemas are already created)
# conn.execute("INSERT INTO sqlite_db.DAILY_STOCK_STATS SELECT * FROM main.DAILY_STOCK_STATS;")

conn.close()
print("Migration completed successfully!")