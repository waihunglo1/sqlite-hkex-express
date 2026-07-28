import yfinance as yf
import pandas as pd
from yahooquery import Ticker
import configparser
import sqlite3
import os
import urllib.request
from pathlib import Path
import sys

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
    required_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'adjclose', 'volume']
    for col in required_columns:
        if col not in hist.columns:
            hist[col] = None
            
    hist_final = hist[required_columns]

    static_sql = """
        INSERT OR REPLACE INTO stock_history 
        (ticker, date, open, high, low, close, adjclose, volume) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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

def yahooStockInfoBatchQuery(tickerBatch):
    tickers = Ticker(tickerBatch, asynchronous=True)

    # 1. 獲取數據
    # Fetch the raw data dictionaries
    # summary = tickers.summary_detail
    profile_data = tickers.asset_profile
    quote = tickers.quotes
    # print(len(profile_data), len(quote))

    records = []
    for symbol in tickerBatch:
        try:
            records.append({
                'symbol': symbol,
                'name'  : quote[symbol]['longName'],
                'sector': profile_data[symbol]['sector'],
                'industry': profile_data[symbol]['industry'],
                'marketCap' : quote[symbol]['marketCap']
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
    yahooHistPriceBatchQuery(tickerBatch)
    yahooStockInfoBatchQuery(tickerBatch)

if len(errorRecords) > 0:
    df = pd.DataFrame(errorRecords)
    print(df.to_markdown(index=False))  