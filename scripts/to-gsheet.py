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

# Create a wrapped function with retry mechanism
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    reraise=True,
)

def _publish_with_retry(df, file, tabName):
    gsheetHelper.publish_gsheet(df, file, tabName)

def fetch_and_populate(conn, sql):        
    df = gsheetHelper.fetch_and_populate(conn, sql)
    return df

def populate(config, dbHelper, id):
    # step1 query data from sqlite file
    sql = config[id]['SQL']
    df = dbHelper.callbackWithConn(fetch_and_populate, sql)

    # publish to google-sheet
    targetFile = config[id]['FILE']
    tabName = config[id]['TAB_NAME']
    _publish_with_retry(df, targetFile, tabName) 

if __name__ == "__main__":
    config, dbHelper, avienUri  = marketParameter.parse_argument()
    populate(config, dbHelper, 'GOOGLE-SPREADSHEET-01')
    populate(config, dbHelper, 'GOOGLE-SPREADSHEET-02')