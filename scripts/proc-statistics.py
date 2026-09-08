import logging
import pandas as pd
from common.statistics_processor import StatisticsProcessor 
from common import market_parameter as marketParameter
from common import utility as helper
#
# Main program
#
if __name__ == "__main__": 
    config, dbHelper, avienUri =  marketParameter.parse_argument()    

    # indexes data
    indexes_str = config['YAHOO-FINANCE']['INDEXES'] # ^GSPC, ^NDX
    indexes = helper.splitStringToArray(indexes_str)

    # processing
    processor = StatisticsProcessor(dbHelper=dbHelper, avienUri=avienUri, indexes=indexes)
    processor.loadIndexDataByYahooFinance()
    processor.populateSectorStatistics(config)
    processor.populateMarketStatistics(config)