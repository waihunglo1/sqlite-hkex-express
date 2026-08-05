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

def tickersFromXls(hkexConfig):
    downloadPath = hkexConfig['DOWNLOAD_PATH']
    fileName = hkexConfig['listOfSecurities']
    current_dir = Path.cwd()
    targetPath = os.path.join(str(current_dir), downloadPath)
    save_path = os.path.join(targetPath, fileName)

    # Define converters to force the first 3 columns (by index 0, 1, 2) to string
    df = pd.read_excel(
        save_path,
        skiprows=2,
        usecols=[0, 1, 2],
        converters={0: str, 1: str, 2: str}
    )

    filterDf = df[df.iloc[:, 2].isin(['股本', '交易所買賣產品'])]

    # Clean and format the first column without lambda
    clean_codes = filterDf.iloc[:, 0].str.split('.').str[0].str.strip().str.lstrip('0')

    # Pad with leading zeros to 4 digits and add .HK
    filterDf.iloc[:, 0] = clean_codes.str.zfill(4) + '.HK'
    column_hashmap = dict(zip(filterDf.iloc[:, 0], filterDf.iloc[:, 1]))
    # tickerList = filterDf.iloc[:, 0].tolist()
    return column_hashmap

def yahooQuery(tickerBatch, errorRecords, tickerMap):
    tickers = Ticker(tickerBatch, country='hong kong')

    # 1. 獲取數據
    # Fetch the raw data dictionaries
    # summary = tickers.summary_detail
    profile_data = tickers.asset_profile
    quote = tickers.quotes
    # logging.info(len(profile_data), len(quote))

    records = []
    for symbol in tickerBatch:
        try:
            tickerName = tickerMap.get(symbol,'NONE')
            records.append({
                'symbol': symbol,
                'name'  : quote[symbol].get("longName",tickerName),
                'sector': profile_data[symbol].get("sector","NONE"),
                'industry': profile_data[symbol].get("industry","NONE"),
                'marketCap' : quote[symbol].get("marketCap","NONE")
            })
        except Exception as e:
            # logging.error(f"❌ [{symbol}] 未知錯誤: {e} / {profile[symbol]}")
            errorRecords.append(
                {
                    'symbol': symbol,
                    'error' : e,
                    'message' : profile_data[symbol]
                }
            )

    # update sqlite
    if len(records) > 0:
        updated = insertOrReplace(records)
        # logging.info(f"Changed Row: {updated} data size: {len(records)}")
        return updated

    return 0

def insertOrReplace(records):
    df = pd.DataFrame(records)
    # logging.info("\n" + df.to_markdown(index=False).rstrip)

    updated = 0
    try:
        with sqlite3.connect(sqliteFile, timeout=10) as conn:
            cursor = conn.cursor()
        
            # Use SQLite "INSERT OR REPLACE" logic row-by-row
            for _, row in df.iterrows():
                cursor.execute('''
                    REPLACE INTO STOCK (symbol,name,industry,sector,market_cap) VALUES (?, ?, ?, ?, ?)
                ''', (row['symbol'], row['name'], row['industry'], row['sector'], row['marketCap']))
                # 📜 獲取受影響的行數
                updated += cursor.rowcount
            
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
        exit
    except Exception as e:
        logging.error(f"❌ ⚪ 未知錯誤: {e}")
        exit

    return updated

def yahooQueryStockInfoToSqlite(tickerMap):
    # Split ticker_list into batches of 100 items
    errorRecords = []
    batch_size = 100
    updated = 0
    # Extract keys as a standard Python list
    tickerList = list(tickerMap.keys())

    for i in range(0, len(tickerList), batch_size):
        tickerBatch = tickerList[i : i + batch_size]
        updated += yahooQuery(tickerBatch,errorRecords,tickerMap)
        sleep = random.uniform(1, 10)
        logging.info(f"Updated : {updated} / {len(tickerList)} / Error Records : {len(errorRecords)} / sleep : {sleep:.2f}")
        time.sleep(sleep)

    return errorRecords

#
# Main Program
# 

# Initialize the parser
config = configparser.ConfigParser()
config.read('config/analyst-data-hk.ini', encoding='utf-8')
sqliteFile = config['SQLITE']['FILE']
logging.info(f"SQLITE : {sqliteFile}") 

# read xls
hkexConfig = config['HKEX']
tickerMap = tickersFromXls(hkexConfig)
logging.info(f"SIZE : {len(tickerMap)}")

# Split ticker_list into batches of items
errorRecords = yahooQueryStockInfoToSqlite(tickerMap)
if len(errorRecords) > 0:
    df = pd.DataFrame(errorRecords)
    logging.info("\n" + df.to_markdown(index=False).strip())  