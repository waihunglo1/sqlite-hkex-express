import logging
import pandas as pd
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

    def _fillData(self, tickerMap):
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

    def fill(self, tickerMap):
        errorRecords = self._fillData(tickerMap)
        if len(errorRecords) > 0:
            df = pd.DataFrame(errorRecords)
            logging.info("\n" + df.to_markdown(index=False).strip())

        # pretty print sector
        self.dumpSectorStatistics()

    def dumpSectorStatistics(self):
        # price history row
        sql_query = """
            SELECT sector, count(1) 
            FROM stock
            group by sector
            order by sector 
        """
        sectors = self.dbHelper.fetchAllRows(sql_query)
        self.prettyPrint(sectors)        

    def prettyPrint(self, sectors):
        df = pd.DataFrame(sectors)
        # Clean up column names and nulls
        df.columns = ["Sector", "Count"]
        # df["Sector"] = df["Sector"].replace("", "UNASSIGNED").fillna("UNASSIGNED")

        # Print clean string representation
        log_table = df.to_markdown(index=False, tablefmt="plain")
        logging.info(f"\nSector Summary:\n{log_table}")      

