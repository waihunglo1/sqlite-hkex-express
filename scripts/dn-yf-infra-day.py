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

    latestTradingDate = helper.retrieveLatestTradingDate(config["YAHOO-FINANCE"], "%Y%m%d")
    logging.info(f"Trading date : {latestTradingDate}")

    # fill price
    # priceFiller = YahooQueryPriceFiller(dbHelper=dbHelper)
    # priceFiller.fillPriceByConfig(config['YAHOO-FINANCE'])

    # process price
    # where clause
    where_clause_str = helper.sqlClean(config['DAILY_PRICE_PROCESSOR']['WHERE_CLAUSE'])

    # Pass helper to your processor
    #processor = DailyPriceProcessor(dbHelper=dbHelper, whereClause=where_clause_str)
    #processor.process_data_local(latestTradingDate, "")    

    # generate statistics
    #processor = StatisticsProcessor(dbHelper=dbHelper, avienUri=avienUri)    
    #processor.doMarketAnalysis(config)
    #processor.doStockAnalysis(config)    

    # send notification
    helper.send_execution_report(
        config, 
        subject="HKEX Market Analysis Completed",
        body_html="<p>The daily market statistics pipeline ran successfully.</p>",
    )    