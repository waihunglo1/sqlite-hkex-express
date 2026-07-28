import yfinance as yf
import pandas as pd
from yahooquery import Ticker
import configparser
import sqlite3
import os
import urllib.request
from pathlib import Path
import sys
from requests.exceptions import HTTPError
import time
import random

def usTickerFromGit(tickerConfig):
    # Use the raw URL to get plain text
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"

    try:
        with urllib.request.urlopen(url) as response:
            # Read, decode bytes to string, and split by newline
            tickers = response.read().decode('utf-8').splitlines()
            print(f"Successfully fetched {len(tickers)} tickers.")

    except Exception as e:
        print(f"Failed to download file: {e}")

    return tickers


def yahooHistPriceBatchQuery(tickerList):
    print(f"Requesting data for {len(tickerList)} tickers...")
    tickers_data = Ticker(tickerList, asynchronous=True) 
    hist = tickers_data.history(period='1y', interval='1d')
    hist = hist.reset_index()
    print(hist)

    if len(hist) > 0:
        insertOrReplaceHistPrice(hist)

def insertOrReplaceHistPrice(hist):    
    hist.rename(columns={'symbol': 'ticker'}, inplace=True)
    hist['date'] = (
        pd.to_datetime(hist['date'], utc=True)
        .dt.tz_convert('US/Eastern')
        .dt.strftime('%Y%m%d')
    )
    required_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'adjclose', 'volume']
    for col in required_columns:
        if col not in hist.columns:
            hist[col] = None
            
    hist_final = hist[required_columns]

    static_sql = """
        INSERT OR REPLACE INTO daily_stock_price 
        (symbol, period, dt, open, high, low, close, adj_close, volume, open_int) 
        VALUES (?, 'D', ?, ?, ?, ?, ?, ?, ?, 0)
    """

    try:
        with sqlite3.connect(sqliteFile, timeout=10) as conn:
            cursor = conn.cursor()
            data_tuples = list(hist_final.itertuples(index=False, name=None))
            cursor.executemany(static_sql, data_tuples)
            conn.commit()
            print(f"✅ 成功將 {len(hist_final)} 筆歷史數據以 100% 靜態安全語法更新至 SQLite 資料庫。")
    except Exception as e:
        conn.rollback()
        print(f"❌ 寫入資料庫時出錯: {e}")
    finally:
        conn.close()

def yahooStockInfoBatchQueryRetry(tickerBatch):
    time.sleep(random.uniform(1.0, 3.5))
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            tickers = Ticker(tickerBatch)
            profile_data = tickers.asset_profile
            quote = tickers.quotes

            if isinstance(profile_data, dict) and any('error' in str(v).lower() or 'timeout' in str(v).lower() for v in profile_data.values()):
                raise RuntimeError("偵測到 API 回傳內容包含 504/逾時錯誤資訊")
                
            saveStockData(tickerBatch, quote, profile_data)
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = random.uniform(2.0, 5.0) * (attempt + 1)
                print(f"發生錯誤: {e}。等待 {wait_time:.2f} 秒後進行第 {attempt + 2} 次重試...")
                time.sleep(wait_time)
            else:
                print(f"已達到最大重試次數，放棄擷取 {tickerBatch}")
                return None
            
def saveStockData(tickerBatch, quote, profile_data):
    records = []
    for symbol in tickerBatch:
        try:
            # 安全獲取各模組的字典，若無資料則給空字典
            sym_quote = quote.get(symbol, {}) if isinstance(quote, dict) else {}
            sym_profile = profile_data.get(symbol, {}) if isinstance(profile_data, dict) else {}
            
            # 如果對應的 dictionary 是字串（代表 yahooquery 回傳錯誤訊息），則跳過或視為空
            if isinstance(sym_quote, str): sym_quote = {}
            if isinstance(sym_profile, str): sym_profile = {}

            records.append({
                'symbol': symbol,
                'name'  : sym_quote.get('shortName') or sym_quote.get('longName') or 'N/A', # quote 通常用 shortName/longName
                'sector': sym_profile.get('sector', 'N/A'),
                'industry': sym_profile.get('industry', 'N/A'),
                'marketCap' : sym_quote.get('marketCap', None) # 從 summary_detail 獲取最安全的市值
            })
        except Exception as e:
            errorRecords.append(
                {
                    'symbol': symbol,
                    'exception' : e
                }
            )
            # print(f"{symbol}: 無法獲取資料")
     
    # update sqlite
    if len(records) > 0:
        updated = insertOrReplace(records)
        print(f"Changed Row: {updated} data size: {len(records)}")

def insertOrReplace(records,):
    df = pd.DataFrame(records)
    print(df)

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
        print(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
        exit
    except Exception as e:
        print(f"❌ ⚪ 未知錯誤: {e}")
        exit

    return updated
#
# Main Program
# 

# Initialize the parser
config = configparser.ConfigParser()
config.read('config/analyst-data-us.ini', encoding='utf-8')
sqliteFile = config['SQLITE']['FILE']
print(f"SQLITE : {sqliteFile}") 

# read xls
tickerConfig = config['TICKERS']
tickerList = usTickerFromGit(tickerConfig)
print(f"SIZE : {len(tickerList)}")

# Split ticker_list into batches of 100 items
errorRecords = []
batch_size = 100
for i in range(0, len(tickerList), batch_size):
    tickerBatch = tickerList[i : i + batch_size]
    # yahooHistPriceBatchQuery(tickerBatch)
    yahooStockInfoBatchQueryRetry(tickerBatch)

if len(errorRecords) > 0:
    df = pd.DataFrame(errorRecords)
    print(df.to_markdown(index=False))  