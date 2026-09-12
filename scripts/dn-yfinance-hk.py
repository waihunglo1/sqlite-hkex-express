import yfinance as yf
import pandas as pd
import os
import logging
import time
import random
import math
import sys
from basehk import config, sqliteDbHelper
from common import translator as translaterHelper
from common import utility as helper

# Import the class from your utility file
from yahooquery import Ticker
from pathlib import Path

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
    # Set explicit request headers to reduce blocking risk
    tickers = Ticker(
        tickerBatch,
        country="hong kong",
        asynchronous=False,  # Sequential mode prevents concurrent burst requests
        max_workers=4,  # Limits concurrent worker threads
    )

    # 1. 獲取數據
    # Fetch the raw data dictionaries
    # summary = tickers.summary_detail
    profile_data = tickers.asset_profile
    quote = tickers.quotes
    quote_type = tickers.quote_type
    # logging.info(len(profile_data), len(quote))

    records = []
    for symbol in tickerBatch:
        try:
            tickerName = tickerMap.get(symbol,'NONE')
            sector_en = profile_data[symbol].get("sector","UNKNOWN")
            industry_en = profile_data[symbol].get("industry","UNKNOWN")
            sector_zh = translaterHelper.financial_term(sector_en, "sector", symbol)
            industry_zh = translaterHelper.financial_term(industry_en, "industry", symbol)
            longName = quote[symbol].get("longName",tickerName)
            marketCap = quote[symbol].get("marketCap",0)
            quote_type_str = quote_type[symbol].get("quoteType")  # e.g., 'EQUITY', 'ETF', 'OPTION'

            records.append({
                'symbol': symbol,
                'name'  : longName,
                'sector_en' : sector_en,
                'industry_en' : industry_en,
                'sector': sector_zh,
                'industry': industry_zh,
                'marketCap' : marketCap,
                'quoteType' : quote_type_str
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

def dumpSectorStatistics(sqliteDbHelper):
    # price history row
    sectorSql = """
        SELECT sector, count(1) 
        FROM stock
        group by sector
        order by sector 
    """
    sectors = sqliteDbHelper.fetchAllRows(sectorSql)
    helper.prettyPrint(sectors)

def extractTickerNameSectors(config, sqliteDbHelper):
    # read xls
    hkexConfig = config['HKEX']
    tickerMap = tickersFromXls(hkexConfig)
    logging.info(f"TICKERS SIZE : {len(tickerMap)}")

    # Split ticker_list into batches of items
    errorRecords = yahooQueryStockInfo(sqliteDbHelper, tickerMap)
    if len(errorRecords) > 0:
        df = pd.DataFrame(errorRecords)
        logging.info("\n" + df.to_markdown(index=False).strip())      


#
# Main program
#
if __name__ == "__main__": 
    extractTickerNameSectors(config, sqliteDbHelper)
    dumpSectorStatistics(sqliteDbHelper)    