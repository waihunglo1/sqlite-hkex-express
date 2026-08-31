import configparser
import logging
import os
import sys
from .duckdbhelper import DuckDbHelper
from dotenv import load_dotenv

# Execute initialization immediately on package import
load_dotenv('.env')
load_dotenv('.env.us')

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

    # duck file
    duckFile = config['DUCKDB']['FILE']
    logging.info(f"DUCKDB : {duckFile}")  
    duckDbHelper = DuckDbHelper(duckFile)   

else:
    logging.warning("ANALYST_DATA_INI path is missing or invalid.")
    sys.exit(1)