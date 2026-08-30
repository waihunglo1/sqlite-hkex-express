import yfinance as yf
import pandas as pd
import os
import logging
import time
import random
import math
import sys
from core import config, sqliteDbHelper, quoteParser
from core import utility as helper
from core import translator as translaterHelper

# Import the class from your utility file
from yahooquery import Ticker
from pathlib import Path

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

def doYahooQuery(sqliteDbHelper, tickerBatch, errorRecords, tickerMap):
    tickers = Ticker(tickerBatch, country="hong kong")

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
                'sector': translaterHelper.financial_term(profile_data[symbol].get("sector","NONE"), "sector", symbol),
                'industry': translaterHelper.financial_term(profile_data[symbol].get("industry","NONE"), "industry", symbol),
                'marketCap' : quote[symbol].get("marketCap",0)
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
        updated = sqliteDbHelper.insertOrReplaceStockInfo(records)
        # logging.info(f"Changed Row: {updated} data size: {len(records)}")
        return updated

    return 0

def yahooQueryStockInfo(sqliteDbHelper, tickerMap):
    # Split ticker_list into batches of 100 items
    errorRecords = []
    batch_size = 100
    updated = 0
    sleep = 0

    # Extract keys as a standard Python list
    tickerList = list(tickerMap.keys())
    logging.info(f"YahooQuery / DB-Updated : {updated} / {len(tickerList)} / Error : {len(errorRecords)} / sleep : {sleep:.2f}")

    for i in range(0, len(tickerList), batch_size):
        tickerBatch = tickerList[i : i + batch_size]
        updated += doYahooQuery(sqliteDbHelper, tickerBatch, errorRecords, tickerMap)
        sleep = random.uniform(1, 10)
        logging.info(f"YahooQuery / DB-Updated : {updated} / {len(tickerList)} / Error : {len(errorRecords)} / sleep : {sleep:.2f}")
        time.sleep(sleep)

    return errorRecords

def loadIndexDataByYahooFinance(sqliteDbHelper):
    indexes = ["^HSI", "^HSCE"]
    start_date = "2006-10-13"

    for index_symbol in indexes:
        df = yf.download(
            index_symbol,
            start=start_date,
            interval="1d",
            progress=False,
            multi_level_index=False,  # Forces 1D columns
        )

        if df.empty:
            continue

        df = df.reset_index()

        # Vectorized column mapping
        records = pd.DataFrame(
            {
                "symbol": index_symbol,
                "period": "D",
                "dt": df["Date"].dt.strftime("%Y%m%d"),
                "tm": "000000",
                "open": df["Open"],
                "high": df["High"],
                "low": df["Low"],
                "close": df["Close"],
                "volume": df["Volume"],
                "adj_close": df["Close"],
                "open_int": 0,
            }
        )

        logging.info("\n" + records.tail(5).to_string())

        rows = records.to_dict(orient="records")
        updatedRows = sqliteDbHelper.insertDailyStockPrice(rows)
        logging.info(f"Yahoo indexes Processed[{index_symbol}] : {len(rows)} / Updated : {updatedRows}")

#
# Main program
#
if __name__ == "__main__": 
    # read xls
    hkexConfig = config['HKEX']
    tickerMap = tickersFromXls(hkexConfig)
    logging.info(f"SIZE : {len(tickerMap)}")

    # load index data
    loadIndexDataByYahooFinance(sqliteDbHelper)

    # Split ticker_list into batches of items
    errorRecords = yahooQueryStockInfo(sqliteDbHelper, tickerMap)
    if len(errorRecords) > 0:
        df = pd.DataFrame(errorRecords)
        logging.info("\n" + df.to_markdown(index=False).strip())  