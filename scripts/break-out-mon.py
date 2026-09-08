import os
from datetime import date, timedelta
import logging
import gspread
import os
import certifi
import logging
import pandas as pd

from tenacity import retry, stop_after_attempt, wait_exponential
from common import utility as helper
from common import gsheethelper as gsheetHelper
from common import basedbhelper as dbHelper
from common import market_parameter as marketParameter

def extractStock(config, dbHelper):
    sql_str = config['BREAK_OUT_MONITOR']['FILTER_SQL_01']
    sql_replaced = sql_str.replace('${last_trading_date}','20260904')
    df = dbHelper.readDataFrame(sql_replaced)
    
    # Concatenate first 3 columns and last 3 columns
    df_subset = pd.concat([df.iloc[:, :3], df.iloc[:, -3:]], axis=1)
    logging.info("資料預覽（前 5 行）：")
    logging.info(f"\n{df_subset.head()}")  

if __name__ == "__main__":
    config, dbHelper, avienUri = marketParameter.parse_argument()
    extractStock(config, dbHelper)