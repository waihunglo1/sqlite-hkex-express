import configparser
import logging
import os
import sys
from dotenv import load_dotenv
from .sqlitehelper import SqliteDbHelper
from .quoteparser import QuoteParser
from .utility import (
    removeHistorialFiles,
    downloadHtm,
    downloadByChrome,
    dumpErrorRecord,
    splitStringToArray,
    reformat_symbol_for_hk,
    traverse_directory,
    is_empty,
    today_string,
    today_year_month,
    unzip_file,
    create_directory_if_not_exists
)
from .translator import (
    financial_term
)
from .gsheethelper import (
    publish_gsheet
)

# Execute initialization immediately on package import
load_dotenv('.env')
load_dotenv('.env.local', override=True)

# 1. 設定日誌格式：包含 [時間] [層級] 檔案名稱:行數 - 訊息
logging.basicConfig(
    level=logging.INFO,  # 設定最低捕捉層級
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)04d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # 精簡時間格式
)

analyst_ini_path = os.getenv("ANALYST_DATA_INI")
config = configparser.ConfigParser()

if analyst_ini_path and os.path.exists(analyst_ini_path):
    config.read(analyst_ini_path, encoding="utf-8")
    logging.info("Initialization complete: Loaded INI config.")

    # sqlite file
    sqliteFile = config['SQLITE']['FILE']
    logging.info(f"SQLITE : {sqliteFile}") 
    sqliteDbHelper = SqliteDbHelper(sqliteFile)   

    # quote parser
    quoteParser = QuoteParser() 
else:
    logging.warning("ANALYST_DATA_INI path is missing or invalid.")
    sys.exit(1)