
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
import utility as util

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

def fetch_and_populate(sqliteFile, query, funcNameConfig):
    if not os.path.exists(sqliteFile):
        logging.errror(f"錯誤：找不到資料庫檔案 '{sqliteFile}'")
        return

    try:
        logging.info("正在從 SQLite 讀取資料...")
        conn = sqlite3.connect(sqliteFile)

        # --- PANDAS 優化：直接讀取為 DataFrame，保持真實的數據型態 (int, float, object) ---
        df = pd.read_sql_query(query, conn)
        if df.empty:
            logging.error("未找到任何資料。")
            return None   

        current_module = globals()
        funcNames = util.splitStringToArray(funcNameConfig)

        for funcName in funcNames:
            if not funcName:
                continue
            if funcName in current_module:
                func_to_run = current_module[funcName]
                logging.info(f"從設定檔觸發函數: {funcName}")
                df = func_to_run(df, conn)
            else:
                logging.info(f"找不到名為 '{funcName}' 的函數。")

        # 2. 資料清洗：將 SQLite 的 None (在 Pandas 中為 NaN) 轉成空字串 ""
        # 這樣既能保持數值欄位的真實數值型態，又不會在寫入 Google Sheets 時出錯
        processed_df = df.fillna("")
        return processed_df  
    except Exception as e:
        logging.error(f"資料庫查詢失敗: {e}")
        return None
    finally:
        conn.close()

def fillStocksRelativeStrength(df, conn):
    logging.info("Extend to fill relative strength")        
    records = df.to_dict('records')
    logging.info("正在為每檔股票填充前 20 日的歷史 Normalize RS 數據...")
    for row in records:
        fill_historical_normalize_rs(row, conn)    

    # 4. 將擴充完（多了 20 個欄位）的字典列表，重新轉回 DataFrame
    extended_df = pd.DataFrame(records)  
    return extended_df  

def fillStocksSCTR(df, conn):
    logging.info("Extend to fill relative strength")        
    records = df.to_dict('records')
    logging.info("正在為每檔股票填充前 20 日的歷史 Normalize RS 數據...")
    for row in records:
        fill_historical_sctr(row, conn)    

    # 4. 將擴充完（多了 20 個欄位）的字典列表，重新轉回 DataFrame
    extended_df = pd.DataFrame(records)  
    return extended_df  

def fill_historical_sctr(daily_stat, conn):
    sql_sctr = "SELECT sctr FROM DAILY_STOCK_STATS WHERE symbol = ? ORDER BY dt DESC LIMIT 20"
    cursor = conn.cursor()
    cursor.execute(sql_sctr, (daily_stat["symbol"],))
    
    # 擷取資料，sctr_list 會是一個包含 tuple 的 list，例如 [(95.1,), (94.2,)]
    # 用 [row[0] for row in ...] 把它轉成純數字 list: [95.1, 94.2]
    sctr_list = [row[0] for row in cursor.fetchall()]
    
    if len(sctr_list) < 20:
        logging.info(f"Not enough SCTR data for {daily_stat['symbol']}. Only {len(sctr_list)} records found.")
        # 補足 0 到 20 個
        sctr_list += [0] * (20 - len(sctr_list))
        
    # 用一個迴圈直接動態寫入 daily_stat["sctr1"] 到 daily_stat["sctr20"]
    for i, val in enumerate(sctr_list, start=1):
        daily_stat[f"sctr{i}"] = val if val is not None else 0


def fill_historical_normalize_rs(daily_stat, conn):
    sql_rs = "SELECT normalise_rs FROM DAILY_STOCK_STATS WHERE symbol = ? ORDER BY dt DESC LIMIT 20"
    cursor = conn.cursor()
    cursor.execute(sql_rs, (daily_stat["symbol"],))
    
    rs_list = [row[0] for row in cursor.fetchall()]
    
    if len(rs_list) < 20:
        logging.info(f"Not enough rs data for {daily_stat['symbol']}. Only {len(rs_list)} records found.")
        rs_list += [0] * (20 - len(rs_list))
        
    # 動態寫入 daily_stat["normalise_rs1"] 到 daily_stat["normalise_rs20"]
    for i, val in enumerate(rs_list, start=1):
        daily_stat[f"normalise_rs{i}"] = val if val is not None else 0
