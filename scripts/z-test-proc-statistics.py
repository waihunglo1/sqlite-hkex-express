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
    processor.doIndustryStatistic01(config)