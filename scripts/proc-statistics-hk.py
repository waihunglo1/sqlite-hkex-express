import logging
import pandas as pd
from basehk import config, sqliteDbHelper
from common.statistics_processor import StatisticsProcessor

#
# Main program
#
if __name__ == "__main__": 
    processor = StatisticsProcessor(dbHelper=sqliteDbHelper, indexes=["^HSI", "^HSCE"])

    processor.loadIndexDataByYahooFinance()
    processor.populateSectorStatistics(config)
    processor.populateMarketStatistics(config)