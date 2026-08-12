import duckdb
import yfinance as yf
import pandas as pd
from yahooquery import Ticker
import configparser
import sqlite3
import os
from pathlib import Path
import logging
import time
import random
import math

# 1. 設定日誌格式：包含 [時間] [層級] 檔案名稱:行數 - 訊息
logging.basicConfig(
    level=logging.INFO,  # 設定最低捕捉層級
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)04d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # 精簡時間格式
)

# Initialize the parser
config = configparser.ConfigParser()
config.read('config/analyst-data-hk.ini', encoding='utf-8')
sqliteFile = config['SQLITE']['FILE']
logging.info(f"SQLITE : {sqliteFile}") 

# Install and load the SQLite extension inside DuckDB
conn = duckdb.connect()
conn.execute("INSTALL sqlite;")
conn.execute("LOAD sqlite;")

# Attach your SQLite database file
command = f"ATTACH '{sqliteFile}' AS sqlite_db (TYPE sqlite);"
logging.info(command)
conn.execute(command)

# Query the SQLite data using DuckDB's fast columnar engine
df = conn.execute("SELECT dt, COUNT(*) FROM sqlite_db.daily_stock_stats GROUP BY dt order by dt").df()
print(df)