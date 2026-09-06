import logging
import pandas as pd
from common.statistics_processor import StatisticsProcessor 
from common import market_parameter as marketParameter
from common import utility as helper
#
# Main program
#
if __name__ == "__main__": 
    config, dbHelper = marketParameter.parse_argument()    

    # indexes data
    indexes_str = config['YAHOO-FINANCE']['INDEXES'] # ^GSPC, ^NDX
    indexes = helper.splitStringToArray(indexes_str)

    # processing
    processor = StatisticsProcessor(dbHelper=dbHelper, indexes=indexes)
    processor.loadIndexDataByYahooFinance()
    processor.populateSectorStatistics(config)
    processor.populateMarketStatistics(config)