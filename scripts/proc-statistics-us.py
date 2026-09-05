import logging
import pandas as pd
from baseus import config, duckDbHelper
from common.statistics_processor import StatisticsProcessor 

#
# Main program
#
if __name__ == "__main__": 
    processor = StatisticsProcessor(dbHelper=duckDbHelper, indexes=["^GSPC", "^NDX"])

    processor.loadIndexDataByYahooFinance()
    processor.populateSectorStatistics(config)
    processor.populateMarketStatistics(config)