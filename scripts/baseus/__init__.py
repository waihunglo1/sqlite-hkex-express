import configparser
import logging
import os
import sys
from .duckdbhelper import DuckDbHelper
from .sqlite_helper import SqliteDbHelper
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

def avienConnectionString():
    # Retrieve environment variables
    AVIEN_DB_USER = os.getenv("AVIEN_DB_USER")
    AVIEN_DB_PASSWORD = os.getenv("AVIEN_DB_PASSWORD")
    AVIEN_DB_HOST = os.getenv("AVIEN_DB_HOST")
    AVIEN_DB_PORT = os.getenv("AVIEN_DB_PORT", "5432")
    AVIEN_DB_DATABASE = os.getenv("AVIEN_DB_DATABASE")

    # Construct connection string (Aiven requires sslmode=require)
    AVIEN_URI = f"postgresql://{AVIEN_DB_USER}:{AVIEN_DB_PASSWORD}@{AVIEN_DB_HOST}:{AVIEN_DB_PORT}/{AVIEN_DB_DATABASE}?sslmode=require"
    return AVIEN_URI
analyst_ini_path = os.getenv("ANALYST_DATA_INI")
config = configparser.ConfigParser()


#
# INIT
#

# Force UTF-8 output streams for standard terminal logging
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# read init
if analyst_ini_path and os.path.exists(analyst_ini_path):
    config.read(analyst_ini_path, encoding="utf-8")
    logging.info("Initialization complete: Loaded INI config.")

    # duck file
    duckFile = config['DUCKDB']['FILE']
    logging.info(f"DUCKDB : {duckFile}")  
    # duckDbHelper = DuckDbHelper(duckFile)   

    sqliteFile = config['SQLITE']['FILE']
    logging.info(f"SQLITE : {sqliteFile}")
    duckDbHelper = SqliteDbHelper(sqliteFile)

    # avienUri
    avienUri = avienConnectionString()  
else:
    logging.warning("ANALYST_DATA_INI path is missing or invalid.")
    sys.exit(1)