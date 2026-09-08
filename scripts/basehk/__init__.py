import configparser
import logging
import os
import sys
from dotenv import load_dotenv
from .sqlitehelper import SqliteDbHelper

# Execute initialization immediately on package import
load_dotenv('.env')
load_dotenv('.env.hk')

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

if analyst_ini_path and os.path.exists(analyst_ini_path):
    config.read(analyst_ini_path, encoding="utf-8")
    logging.info("Initialization complete: Loaded INI config.")

    # sqlite file
    sqliteFile = config['SQLITE']['FILE']
    logging.info(f"SQLITE : {sqliteFile}") 
    sqliteDbHelper = SqliteDbHelper(sqliteFile) 
    avienUri = avienConnectionString()  

else:
    logging.warning("ANALYST_DATA_INI path is missing or invalid.")
    sys.exit(1)