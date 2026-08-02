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
import math
import logging
import sqliteutility as sqliteUtil
import utility as util

# 1. 設定日誌格式：包含 [時間] [層級] 檔案名稱:行數 - 訊息
logging.basicConfig(
    level=logging.INFO,  # 設定最低捕捉層級
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)04d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # 精簡時間格式
)

# config
total_size = 0
batch_size = 50

def usTickerFromGitAte329():
    df = pd.read_csv(
        "https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/all.csv"
    )

    if total_size > 0:
        dfToGet = df.head(total_size)
    else: 
        dfToGet = df
    
    rowUpdated = sqliteUtil.insertOrReplaceStockInfo(sqliteFile, dfToGet)        
    logging.info(f"Stock Info rows updated : {rowUpdated}")

def yahooHistPriceBatchQuery():
    tickers_list = sqliteUtil.fetchTickers(sqliteFile, "1 = 1")
    total_tickers = len(tickers_list)
    logging.info(f"No of Tickers to load from yahoo : {total_tickers}")

    updatedCount = 0
    errorRecords = []

    for i in range(0, total_tickers, batch_size):
        batch = tickers_list[i : i + batch_size]
        updatedCount += fillHistPriceByYahooQuery(batch, errorRecords)
        logging.info(f"正在處理第 {i//batch_size + 1} 批 / 共 {math.ceil(total_tickers/batch_size)} 批 / Update : {updatedCount} / Error : {len(errorRecords)}")

    util.dumpErrorRecord("Yahoo Histical Price", errorRecords)    

def fillHistPriceByYahooQuery(tickerList, errorRecords):
    updatedCount = 0

    try:
        logging.info(f"Requesting data for {len(tickerList)} tickers...")
        tickers_data = Ticker(tickerList, asynchronous=True) 
        hist = tickers_data.history(period='2y', interval='1d')
        hist = hist.reset_index()
        # logging.info(hist)

        if len(hist) > 0:
            updatedCount = insertOrReplaceHistPrice(hist)
    except Exception as e:
        logging.error(e)
        errorRecords.append(
            {
                'symbols': tickerList,
                'error' : e
            }
        )

    sleep = random.uniform(1, 10)
    time.sleep(sleep)
    return updatedCount 

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
            logging.info(f"✅ 成功將 {len(hist_final)} 筆歷史數據以 100% 靜態安全語法更新至 SQLite 資料庫。")

            return len(data_tuples)
    except Exception as e:
        conn.rollback()
        logging.error(f"❌ 寫入資料庫時出錯: {e}")

    return 0

def yahooStockInfoBatchQueryRetry():
    tickers_list = sqliteUtil.fetchTickers(sqliteFile, "(industry is null or industry = '')")
    total_tickers = len(tickers_list)
    logging.info(f"No of Tickers to load from yahoo : {total_tickers}")

    updateRecords = []
    errorRecords = []

    for i in range(0, total_tickers, batch_size):
        batch = tickers_list[i : i + batch_size]
        logging.info(f"正在處理第 {i//batch_size + 1} 批 / 共 {math.ceil(total_tickers/batch_size)} 批 / Error {len(errorRecords)}")
        fillSectorIndustryByYahooQuery(batch, updateRecords, errorRecords)

    # 3. 將結果對照回原本的 DataFrame
    if updateRecords:
        try:
            with sqlite3.connect(sqliteFile, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    'UPDATE STOCK SET sector = ?, industry = ? WHERE symbol = ?', 
                    updateRecords
                )
                conn.commit()  # 提交當前批次的更新
                logging.info(f"成功更新 {len(updateRecords)} 筆股票的產業資訊。")
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit()
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit()
      
    util.dumpErrorRecord("Yahoo Sector / industry Query", errorRecords)

def fillSectorIndustryByYahooQuery(tickers, updateRecords, errorRecords):
    t = Ticker(tickers, asynchronous=True)  # 啟用非同步加速
    profile = t.asset_profile

    for symbol in tickers:
        try:
            sector = profile[symbol].get("sector", "NONE")
            industry = profile[symbol].get("industry", "NONE")
            updateRecords.append((sector, industry, symbol))
        except Exception as e:
            # logging.error(f"❌ [{symbol}] 未知錯誤: {e} / {profile[symbol]}")
            errorRecords.append(
                {
                    'symbol': symbol,
                    'error' : e,
                    'message' : profile[symbol]
                }
            )
            # print(f"{symbol}: 無法獲取資料")    

    sleep = random.uniform(1, 10)
    logging.info(f"Update Records : {len(updateRecords)} / Error Records : {len(errorRecords)} / sleep : {sleep:.2f}")
    time.sleep(sleep)

#
# Main Program
# 

# Initialize the parser
config = configparser.ConfigParser()
config.read('config/analyst-data-us.ini', encoding='utf-8')
sqliteFile = config['SQLITE']['FILE']
logging.info(f"SQLITE : {sqliteFile}") 

# read xls
tickerConfig = config['TICKERS']
usTickerFromGitAte329()
# yahooStockInfoBatchQueryRetry()
yahooHistPriceBatchQuery()

