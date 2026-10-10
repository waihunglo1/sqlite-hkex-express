import logging
import pandas as pd
import yfinance as yf
import time
import random
import math
from yahooquery import Ticker
from . import basedbhelper as dbHelper
from common import gsheethelper as gsheetHelper
from . import utility as helper

class YahooQueryPriceFiller():
    def __init__(self, dbHelper):
        self.dbHelper = dbHelper

    def fillPriceByConfig(self, historyConfig):
        selected_period = historyConfig["SELECTED_PERIOD"]
        batch_size = int(historyConfig["BATCH_SIZE"] or 50)
        latest_trading_date = helper.retrieveLatestTradingDate(historyConfig)

        tickers_list = self.dbHelper.fetchTickers("1 = 1")
        total_tickers = len(tickers_list)
        logging.info(f"No of Tickers to load from yahoo : {total_tickers} / batch size : {batch_size}")
        updatedCount = 0
        errorRecords = []

        for i in range(0, total_tickers, batch_size):
            batch = tickers_list[i : i + batch_size]
            updatedCount += self.fillHistPriceByYahooQuery(batch, errorRecords, latest_trading_date, selected_period)
            logging.info(f"正在處理第 {i//batch_size + 1} 批 / 共 {math.ceil(total_tickers/batch_size)} 批 / Update : {updatedCount} / Error : {len(errorRecords)}")

    def fillHistPriceByYahooQuery(self, tickerList, errorRecords, latest_trading_date, selected_period='2y'):
        updatedCount = 0

        try:
            sleep = random.uniform(1, 10)
            logging.info(f"Requesting data for {len(tickerList)} tickers. time to wait: {sleep:.2f}s")
            time.sleep(sleep)

            # query historical data by yahooQuery
            tickers_data = Ticker(tickerList, asynchronous=True) 
            hist = tickers_data.history(period=selected_period, interval='1d', end=latest_trading_date)
            hist = hist.reset_index()

            if len(hist) > 0:
                updatedCount = self.dbHelper.insertOrReplaceHistPrice(hist)
        except Exception as e:
            logging.error(e)
            errorRecords.append(
                {
                    'symbols': tickerList,
                    'error' : e
                }
            )

        return updatedCount 
