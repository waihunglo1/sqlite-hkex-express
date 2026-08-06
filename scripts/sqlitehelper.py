
import sqlite3
import logging
import sys
import yfinance as yf
import pandas as pd
from yahooquery import Ticker
import configparser
import sqlite3
import os
from pathlib import Path
import time
import random
import math

def insertOrReplaceStockInfo(sqliteFile, df):
    df["symbol"] = df["symbol"].str.replace("/", "-", regex=False)
    updated = 0
    try:
        with sqlite3.connect(sqliteFile, timeout=10) as conn:
            cursor = conn.cursor()
        
            # Use SQLite "INSERT OR REPLACE" logic row-by-row
            for _, row in df.iterrows():
                cursor.execute('''
                    INSERT INTO STOCK (symbol, name, sector, market_cap) 
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        sector = EXCLUDED.sector,
                        market_cap = EXCLUDED.market_cap
                ''', (row['symbol'], row['name'], row['industry'], row['marketCap']))
                updated += cursor.rowcount
            
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
        sys.exit()
    except Exception as e:
        logging.error(f"❌ ⚪ 未知錯誤: {e}")
        sys.exit()

    return updated

def fetchTickers(sqliteFile, whereClause):
    try:
        with sqlite3.connect(sqliteFile, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT symbol FROM stock WHERE " + whereClause + " order by symbol")

            # 4. Retrieve and print the results
            symbols = [row[0] for row in cursor.fetchall()]

            if not symbols:
                logging.info("沒有需要更新的股票。")
            return symbols
    except sqlite3.Error as e:
        logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
        sys.exit()
    except Exception as e:
        logging.error(f"❌ ⚪ 未知錯誤: {e}")
        sys.exit()

def fetch_and_populate(sqliteFile, query):
    # 1. 從 SQLite 查詢資料
    if not os.path.exists(sqliteFile):
        logging.errror(f"錯誤：找不到資料庫檔案 '{sqliteFile}'")
        return

    logging.info("正在從 SQLite 讀取資料...")
    conn = sqlite3.connect(sqliteFile)

    try:
        # --- PANDAS 優化：直接讀取為 DataFrame，保持真實的數據型態 (int, float, object) ---
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        logging.error(f"資料庫查詢失敗: {e}")
        return None
    finally:
        conn.close()

    if df.empty:
        logging.error("未找到任何資料。")
        return None    

    # 2. 資料清洗：將 SQLite 的 None (在 Pandas 中為 NaN) 轉成空字串 ""
    # 這樣既能保持數值欄位的真實數值型態，又不會在寫入 Google Sheets 時出錯
    processed_df = df.fillna("")
    return processed_df