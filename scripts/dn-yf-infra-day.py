import logging
import pandas as pd
import sys
from common.statistics_processor import StatisticsProcessor 
from common import market_parameter as marketParameter
from common import utility as helper
from common.yf_price_filler import YahooQueryPriceFiller
from common.daily_price_processor import DailyPriceProcessor

#
# Main program
# extract infra date
#
if __name__ == "__main__": 
    config, dbHelper, avienUri =  marketParameter.parse_argument()

    latestTradingDate = helper.retrieveLatestTradingDate2(config["YAHOO-FINANCE"], "%Y%m%d")
    logging.info(f"Trading date : {latestTradingDate}")

    # fill price
    priceFiller = YahooQueryPriceFiller(dbHelper=dbHelper)
    priceFiller.fillPriceByConfig(config['YAHOO-FINANCE'])

    # process price
    # where clause
    where_clause_str = helper.sqlClean(config['DAILY_PRICE_PROCESSOR']['WHERE_CLAUSE'])

    # Pass helper to your processor
    dpProcessor = DailyPriceProcessor(dbHelper=dbHelper, whereClause=where_clause_str)
    dpProcessor.process_data_local(latestTradingDate, "")    

    # generate statistics
    statProcessor = StatisticsProcessor(dbHelper=dbHelper, avienUri=avienUri)    
    statProcessor.doMarketAnalysis(config)
    statProcessor.doStockAnalysis(config)