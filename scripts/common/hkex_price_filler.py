import os
from datetime import date, timedelta
import logging
import re
from . import utility as helper
from common.quoteparser import QuoteParser
from common.daily_price_filler import PriceFiller

class HkexPriceFiller(PriceFiller):
    def __init__(self, config, dbHelper):
        self.dbHelper = dbHelper
        self.config = config

    def downloadLastDays(self, hkexConfig, targetDays):
        # Get today's date
        today = date.today()

        # Generate the last 5 dates (including today)
        last_5_dates = [today - timedelta(days=i) for i in range(targetDays)]

        for dt in last_5_dates:
            if(helper.isWeekDay(dt)):
                dateStr = dt.strftime("%y%m%d")
                helper.downloadHtm(hkexConfig, "d" + dateStr + "e.htm")

    def downloadHistoricalQuoteFile(self, hkexConfig):
        # download stock price file
        helper.removeHistorialFiles(hkexConfig)
        self.downloadLastDays(hkexConfig, 7)             

    def traverse_dir(self):
        """Traverses configured directory for HKEX files and processes them."""
        hkex_path = os.path.join(os.getcwd(), self.config["TICKERS"]["DOWNLOAD_PATH"])
        file_regex = re.compile(r"^d(\d{6})e\.htm$")
        rowUpdate = 0
        
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
                    quoteParser = QuoteParser()
                    rowUpdate += quoteParser.parse_hkex_file(self.dbHelper, file_full_path)

        return rowUpdate

    def fill(self):
        hkexConfig = self.config["TICKERS"]

        # download historical quote html file
        self.downloadHistoricalQuoteFile(hkexConfig)

        # traverse each files from directory and store in db
        return self.traverse_dir()