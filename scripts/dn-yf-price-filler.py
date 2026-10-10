import logging
import pandas as pd
import sys
from common.statistics_processor import StatisticsProcessor 
from common import market_parameter as marketParameter
from common import utility as helper
from common.yf_price_filler import YahooQueryPriceFiller
from common.hkex_price_filler import HkexPriceFiller

#
# Main program
#
if __name__ == "__main__": 
    config, dbHelper, avienUri =  marketParameter.parse_argument()
    priceFiller = YahooQueryPriceFiller(dbHelper=dbHelper)
    priceFiller.fillPriceByConfig(config['YAHOO-FINANCE'])