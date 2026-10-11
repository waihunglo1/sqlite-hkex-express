import logging
import pandas as pd
import yfinance as yf
import time
import random
import math
from yahooquery import Ticker
from . import basedbhelper as dbHelper
from . import utility as helper
from . import translator as translatorHelper

class YahooQueryStockInfoFiller():
    def __init__(self, config, dbHelper):
        self.dbHelper = dbHelper
        self.country = config["YAHOO-FINANCE"]["TICKER_COUNTRY"]
        self.indexes = helper.splitStringToArray(config["YAHOO-FINANCE"]["INDEXES"])

    def fill(self, tickerMap):
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
            updated += self.doYahooQuery(tickerBatch, errorRecords, tickerMap)
            elapsed_time = time.perf_counter() - start_time
            sleep = random.uniform(1, 10)
            logging.info(f"YahooQuery Product load Updated:{updated} Error:{len(errorRecords)}. elapse : {elapsed_time:.2f} / sleep : {sleep:.2f}")
            time.sleep(sleep)

        return errorRecords    

    def doYahooQuery(self, tickerBatch, errorRecords, tickerMap):
        # Set explicit request headers to reduce blocking risk
        tickers = Ticker(
            tickerBatch,
            country = self.country,
            asynchronous = False,  # Sequential mode prevents concurrent burst requests
            max_workers = 4,  # Limits concurrent worker threads
        )        

        profile_data = tickers.asset_profile
        quote = tickers.quotes
        quote_type = tickers.quote_type

        records = []
        for symbol in tickerBatch:
            try:
                tickerName = tickerMap.get(symbol,'NONE')
                sector_en = profile_data[symbol].get("sector","UNKNOWN")
                industry_en = profile_data[symbol].get("industry","UNKNOWN")
                sector_zh = translatorHelper.financial_term_for_sector(sector_en, symbol, errorRecords)
                industry_zh = translatorHelper.financial_term_for_industry(industry_en, symbol, errorRecords)
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
            updated = self.dbHelper.insertOrReplaceStockInfo(records)
            # logging.info(f"Changed Row: {updated} data size: {len(records)}")
            return updated

        return 0

    def loadIndexDataByYahooFinance(self):
        start_date = "2006-10-13"

        for index_symbol in self.indexes:
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
            updatedRows = self.dbHelper.insertDailyStockPrice(rows)
            logging.info(f"Yahoo indexes Processed[{index_symbol}] : {len(rows)} / Updated : {updatedRows}")    