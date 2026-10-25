import pandas as pd
import os
import logging
from common import utility as helper
from common.yf_stock_info_filler import YahooQueryStockInfoFiller
from common.hkex_price_filler import HkexPriceFiller
from common import market_parameter as marketParameter
from common.ticker_extractor import TickerExtractor
from common.yf_price_filler import YahooQueryPriceFiller
from common.index_data_extractor import IndexDataExtractor

def yfFillStockInfo(tickerMap, yahooQueryStockInfoFiller):
    errorRecords = yahooQueryStockInfoFiller.fill(tickerMap)
    if len(errorRecords) > 0:
        df = pd.DataFrame(errorRecords)
        logging.info("\n" + df.to_markdown(index=False).strip())

    # pretty print sector
    dbHelper.dumpSectorStatistics()    

#
# Main program
#
if __name__ == "__main__": 
    config, dbHelper, avienUri = marketParameter.parse_argument()

    # load indexes
    if config['PARAMETER']['FILL_STOCK_INFO'] == "y":
        indexDataExtractor = IndexDataExtractor(config, dbHelper)
        indexDataExtractor.loadIndexDataByYahooFinance()

    # fill stock info from yahoo query
    if config['PARAMETER']['FILL_STOCK_INFO'] == "y":
        # extract Ticker from xls
        tickerExtractor = TickerExtractor(config, dbHelper)
        tickerMap = tickerExtractor.tickerMap()

        # fill stock info from yahoo query
        yahooQueryStockInfoFiller = YahooQueryStockInfoFiller(config, dbHelper)    
        yfFillStockInfo(tickerMap, yahooQueryStockInfoFiller)

    # fill daily price
    if config['PARAMETER']['MARKET'] == "us":
        priceFiller = YahooQueryPriceFiller(config, dbHelper=dbHelper)
    else:
        priceFiller = HkexPriceFiller(config, dbHelper=dbHelper)

    rowUpdate = priceFiller.fill() 
    priceFiller.dumpStatistics()

