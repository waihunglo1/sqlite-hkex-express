import duckdb
import yfinance as yf
import pandas as pd
from yahooquery import Ticker
import configparser
import sqlite3
import os
from pathlib import Path
import logging
from core import config, sqliteFile

def populate(conn, config, id, shouldTranspose):
    sql = config[id]['SQL']
    # Query the SQLite data using DuckDB's fast columnar engine
    # df = conn.execute("SELECT dt, COUNT(*) FROM sqlite_db.daily_stock_stats GROUP BY dt order by dt").df()
    # print(df)

    df = conn.execute(sql).df()

    if shouldTranspose:
        print(df.T)    
    else:
        print(df)

def initDuckDb(sqliteFile):
    # Install and load the SQLite extension inside DuckDB
    conn = duckdb.connect()
    conn.execute("INSTALL sqlite;")
    conn.execute("LOAD sqlite;")

    # Attach your SQLite database file
    command = f"ATTACH '{sqliteFile}' AS sqlite_db (TYPE sqlite);"
    logging.info(command)
    conn.execute(command)  
    return conn  

# Initialize the parser
logging.info(f"SQLITE : {sqliteFile}") 

conn = initDuckDb(sqliteFile)
# populate(conn, config, "MARKET-BREADTH-SQL", False)
populate(conn, config, "SECTOR-BREADTH-SQL", False)
