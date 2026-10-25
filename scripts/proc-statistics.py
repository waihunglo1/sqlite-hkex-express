import logging
import pandas as pd
import sys
from common.statistics_processor import StatisticsProcessor 
from common import market_parameter as marketParameter
from common import utility as helper
#
# Main program
#
if __name__ == "__main__": 
    config, dbHelper, avienUri =  marketParameter.parse_argument()    

    # processing
    processor = StatisticsProcessor(dbHelper=dbHelper, avienUri=avienUri)

    if config['PARAMETER']['MARKET'] == "us":
        processor.populateStockChartsETFRelativeStrength(config)

    # sector and industry
    processor.doMarketAnalysis(config)
    processor.doIndustryStatistic01(config)
    processor.doIndustryStatistic02(config)

    # stock analysis
    processor.doStockAnalysis(config)
    processor.populateAvien(config)