from dataclasses import dataclass, field
import logging
import math
import time
import sys
from common.daily_price_processor import DailyPriceProcessor
from common import market_parameter as marketParameter
from common import utility as helper

# Run Configuration
QUERY_DATE = ""  # e.g., '20260730'
QUERY_SYMBOL = ""  # e.g., '2697.HK'

if __name__ == "__main__": 
    config, dbHelper, avienUri = marketParameter.parse_argument()

    try:
        retrieve_last_trade_date = config["PARAMETER"]["TRADE_DATE"]
        if retrieve_last_trade_date == 'y':
            QUERY_DATE = helper.retrieveLatestTradingDate2(config["YAHOO-FINANCE"], "%Y%m%d")
        logging.info(f"Trading date : {QUERY_DATE or 'NONE'}")
        
        # where clause
        where_clause_str = helper.sqlClean(config['DAILY_PRICE_PROCESSOR']['WHERE_CLAUSE'])

        # Pass helper to your processor
        processor = DailyPriceProcessor(dbHelper=dbHelper, whereClause=where_clause_str)
        processor.process_data_local(QUERY_DATE, QUERY_SYMBOL)
    except Exception as e:
        logging.error(f"❌ ⚪ 未知錯誤: {e}")
        sys.exit(-1)

