import os
from datetime import date, timedelta
import logging
import re
from basehk import config, sqliteDbHelper
from common import utility as helper
from common.quoteparser import QuoteParser

def downloadLastDays(hkexConfig, targetDays):
  # Get today's date
  today = date.today()

  # Generate the last 5 dates (including today)
  last_5_dates = [today - timedelta(days=i) for i in range(targetDays)]

  for dt in last_5_dates:
    if(helper.isWeekDay(dt)):
        dateStr = dt.strftime("%y%m%d")
        helper.downloadHtm(hkexConfig, "d" + dateStr + "e.htm")

def downloadStockList(hkexConfig):
  # download hkex stock list
  targetUrl = hkexConfig['URL']
  downloadPath = hkexConfig['DOWNLOAD_PATH']
  # downloadFileName = hkexConfig['LIST_OF_SECURITIES']
  helper.downloadByChrome(targetUrl, downloadPath)     

def downloadHistoricalQuoteFile(hkexConfig):
  # download stock price file
  helper.removeHistorialFiles(hkexConfig)
  downloadLastDays(hkexConfig, 7)             

def traverse_dir(sqlitehelper, config):
    """Traverses configured directory for HKEX files and processes them."""
    hkex_path = os.path.join(os.getcwd(), config["HKEX"]["DOWNLOAD_PATH"])
    file_regex = re.compile(r"^d(\d{6})e\.htm$")

    if not os.path.exists(hkex_path):
        logging.error(f"Directory does not exist: {hkex_path}")
    else:
        logging.info(f"Directory exists: {hkex_path}")
        files = helper.traverse_directory(hkex_path, file_regex)
        logging.info(f"Found {len(files)} files to process.")

        for file_item in files:
            file_name = (
                file_item["file"]
                if isinstance(file_item, dict)
                else file_item.file
            )
            file_full_path = (
                file_item["path"]
                if isinstance(file_item, dict)
                else file_item.path
            )

            if file_regex.match(file_name):
                quoteParser.parse_hkex_file(sqlitehelper, file_full_path)
#
# Main program
#
if __name__ == "__main__": 
  quoteParser = QuoteParser()
  hkexConfig = config['HKEX']
  logging.info(f"hkexConfig : {hkexConfig}") 
  downloadStockList(hkexConfig)
  downloadHistoricalQuoteFile(hkexConfig)
  traverse_dir(sqliteDbHelper, config)


