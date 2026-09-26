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
from baseus import config, duckDbHelper
from common import translator as translaterHelper

def usTickerFromGitAte329(tickerConfig):
    url = tickerConfig["URL"]
    df = pd.read_csv(url)
    logging.info("\n" + df.head(5).to_string())
    rowUpdated = duckDbHelper.insertOrReplaceStockInfo(df)
    logging.info(f"URL : {url}")  
    logging.info(f"Stock Info ({len(df)}) rows updated : {rowUpdated}")

    column_hashmap = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
    return column_hashmap

def yahooQueryStockInfo(tickerMap):
    # Split ticker_list into batches of 100 items
    errorRecords = []
    batch_size = 100
    updated = 0
    sleep = 0

    # Extract keys as a standard Python list
    tickerList = list(tickerMap.keys())
    logging.info(f"Start YahooQuery Product load : {len(tickerList)} / batch size : {batch_size}")

    for i in range(0, len(tickerList), batch_size):
        tickerBatch = tickerList[i : i + batch_size]
        start_time = time.perf_counter()
        updated += doYahooQuery(tickerBatch, errorRecords, tickerMap)
        elapsed_time = time.perf_counter() - start_time
        sleep = random.uniform(1, 10)
        logging.info(f"Updated:{updated} Error:{len(errorRecords)}. elapse : {elapsed_time:.2f} / sleep : {sleep:.2f}")
        time.sleep(sleep)

    return errorRecords    

def doYahooQuery(tickerBatch, errorRecords, tickerMap):
    tickers = Ticker(tickerBatch)

    profile_data = tickers.asset_profile
    quote = tickers.quotes
    quote_type = tickers.quote_type

    records = []
    for symbol in tickerBatch:
        try:
            tickerName = tickerMap.get(symbol,'NONE')
            sector_en = profile_data[symbol].get("sector","UNKNOWN")
            industry_en = profile_data[symbol].get("industry","UNKNOWN")
            sector_zh = translaterHelper.financial_term(sector_en, "sector", symbol)
            industry_zh = translaterHelper.financial_term(industry_en, "industry", symbol)
            quote_type_str = quote_type[symbol].get("quoteType")  # e.g., 'EQUITY', 'ETF', 'OPTION'
            longName = quote[symbol].get("longName",tickerName)
            marketCap = quote[symbol].get("marketCap",0)

            records.append(
                {
                'symbol': symbol,
                'name'  : longName,
                'sector_en' : sector_en,
                'industry_en' : industry_en,
                'sector': sector_zh,
                'industry': industry_zh,
                'marketCap' : marketCap,
                'quoteType' : quote_type_str
                }
            )
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
        updated = duckDbHelper.insertOrReplaceStockInfo2(records)
        # logging.info(f"Changed Row: {updated} data size: {len(records)}")
        return updated

    return 0    

def yahooHistPriceBatchQuery(historyConfig):
    selected_period = historyConfig["SELECTED_PERIOD"]
    batch_size = int(historyConfig["BATCH_SIZE"] or 50)
    tickers_list = duckDbHelper.fetchTickers("1 = 1")
    total_tickers = len(tickers_list)
    logging.info(f"No of Tickers to load from yahoo : {total_tickers} / batch size : {batch_size}")

    updatedCount = 0
    errorRecords = []

    for i in range(0, total_tickers, batch_size):
        batch = tickers_list[i : i + batch_size]
        updatedCount += fillHistPriceByYahooQuery(batch, errorRecords, selected_period)
        logging.info(f"正在處理第 {i//batch_size + 1} 批 / 共 {math.ceil(total_tickers/batch_size)} 批 / Update : {updatedCount} / Error : {len(errorRecords)}")

def retrieveLastTradingDate():
    # Download a short window of recent market data for a major ticker
    ticker = yf.Ticker("^GSPC") # S&P 500 Index
    recent_data = ticker.history(period="5d")

    # Extract the date of the very last row in the DataFrame
    last_trading_date = recent_data.index[-1].strftime('%Y-%m-%d')

    logging.info(f"The last US trading date was: {last_trading_date}")
    return last_trading_date

def fillHistPriceByYahooQuery(tickerList, errorRecords, selected_period='2y'):
    updatedCount = 0
    last_trading_date = retrieveLastTradingDate()

    try:
        sleep = random.uniform(1, 10)
        logging.info(f"Requesting data for {len(tickerList)} tickers. time to wait: {sleep:.2f}")
        time.sleep(sleep)
        tickers_data = Ticker(tickerList, asynchronous=True) 
        hist = tickers_data.history(period=selected_period, interval='1d', end=last_trading_date)
        hist = hist.reset_index()

        if len(hist) > 0:
            updatedCount = duckDbHelper.insertOrReplaceHistPrice(hist)
    except Exception as e:
        logging.error(e)
        errorRecords.append(
            {
                'symbols': tickerList,
                'error' : e
            }
        )

    return updatedCount 

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
if __name__ == "__main__": 
    tickerMap = usTickerFromGitAte329(config['TICKERS'])

    # Split ticker_list into batches of items
    errorRecords = yahooQueryStockInfo(tickerMap)
    if len(errorRecords) > 0:
        df = pd.DataFrame(errorRecords)
        logging.info("\n" + df.to_markdown(index=False).strip())  

    yahooHistPriceBatchQuery(config['YAHOO-FINANCE'])

