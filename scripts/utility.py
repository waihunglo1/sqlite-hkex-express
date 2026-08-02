from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from datetime import date, timedelta
import os
import datetime
import time
from pathlib import Path
import posixpath
from urllib.parse import urlsplit
import logging
import pandas as pd

# Define the number of days for the cutoff
days_cutoff = 6

# Calculate the cutoff timestamp (15 days ago)
# time.time() gives current time in seconds since the epoch
# 24 * 3600 is the number of seconds in a day
cutoff_timestamp = time.time() - (days_cutoff * 24 * 3600)

# Iterate over all items in the directory
def removeHistorialFiles(hkexConfig):
    downloadPath = hkexConfig['DOWNLOAD_PATH']
    current_dir = Path.cwd()
    expected_filepath = os.path.join(str(current_dir), downloadPath)

    for filename in os.listdir(expected_filepath):
        file_path = os.path.join(expected_filepath, filename)

        # Check if the item is a file and not a directory
        if os.path.isfile(file_path):
            # Get the file's last modification time
            file_mtime = os.path.getmtime(file_path)
            logging.info(f"Examine File: {filename} (Modified date: {datetime.datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d')})")

            # Compare the file's modification time with the cutoff timestamp
            if file_mtime < cutoff_timestamp:
                try:
                    # Delete the file
                    os.remove(file_path)
                    logging.info(f"Deleted: {filename} (Modified date: {datetime.datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d')})")
                except OSError as e:
                    logging.info(f"Error deleting file {filename}: {e}")

        # Optional: If you also want to remove empty directories or older directories recursively, 
        # you might consider using os.walk and shutil.rmtree for a more robust solution, 
        # but the above code safely handles single-level files.

def isWeekDay(date_obj):
  # Weekdays are 0 (Monday) to 4 (Friday)
  return date_obj.weekday() < 5

def downloadHtm(hkexConfig, fileName):
    # Define the file path and name
    downloadPath = hkexConfig['DOWNLOAD_PATH']
    current_dir = Path.cwd()
    targetPath = os.path.join(str(current_dir), downloadPath)
    save_path = os.path.join(targetPath, fileName)

    if os.path.isfile(save_path):
        logging.info("file existed:" + save_path + " [download abort]")
        return

    # Automatically manages the browser driver (e.g., ChromeDriver, FirefoxDriver)
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(options=chrome_options) 

    try:
        # Navigate to a website
        url = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/" + fileName
        logging.info("assessing : " + url)
        driver.get(url)
        time.sleep(3)

        # Save the content to a local file with utf-8 encoding
        with open(save_path, "w", encoding='utf-8') as f:
            content = driver.page_source
            f.write(content)
            
        # Assert that the page title contains "Python"
        assert "Hong Kong Exchanges and Clearing Limited" in driver.title, "Can't download valid file : " + fileName
        logging.info(f"Successfully saved HTML to: {save_path}")

    except AssertionError as e:
        logging.error(e)
        os.remove(save_path)

    finally:
        # Give some time to see the result (optional)
        time.sleep(3) 
        # Close the browser session
        driver.quit()

def chromeOptions(expected_filepath):
    # 2. 設定 Chrome 瀏覽器參數
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')  # 💡 隱景運行（不開啟瀏覽器視窗）
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    # 3. 關鍵設定：強制 Chrome 將檔案下載到目前指定的專案目錄，且不跳出確認視窗
    prefs = {
        "download.default_directory": expected_filepath,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    return chrome_options        

def downloadByChrome(targetUrl, downloadPath):
    path = urlsplit(targetUrl).path
    filename = posixpath.basename(path)
    logging.info(f"filename : {filename}")

    # 1. 取得目前專案執行的絕對路徑
    current_dir = Path.cwd()
    logging.info(f"CURRENT DIR: {current_dir}")
    expected_filepath = os.path.join(str(current_dir), downloadPath)
    download_filepath = os.path.join(expected_filepath, filename)

    logging.info(f"DOWNLOAD URL  : {targetUrl}")
    logging.info(f"DOWNLOAD PATH : {download_filepath}")
    
    # 4. 啟動瀏覽器
    logging.info("正在啟動 Chrome 瀏覽器...")
    chrome_options = chromeOptions(expected_filepath)
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # 如果舊檔案存在則先刪除，以便等一下確認是否有成功下載新檔
        if os.path.exists(download_filepath):
            os.remove(download_filepath)
            
        logging.info("正在導向港交所下載網址...")
        driver.get(targetUrl)
        
        # 5. 等待下載完成 (Selenium 不會等檔案下載完才結束程式，需要用迴圈監聽檔案是否存在)
        logging.info("正在下載檔案，請稍候...")
        timeout = 30  # 最多等 30 秒
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # 如果目錄下出現該 Excel 檔案，且不是 Chrome 的下載暫存檔 (.crdownload)
            if os.path.exists(download_filepath) and not os.path.exists(download_filepath + ".crdownload"):
                logging.info(f"✅ 下載成功！檔案已儲存至: {download_filepath}")
                return True
            time.sleep(1)
            
        logging.info("❌ 下載超時，未能成功獲取檔案。")
        return False
        
    except Exception as e:
        logging.info(f"❌ 執行過程中發生錯誤: {e}")
        return False
        
    finally:
        driver.quit()

def dumpErrorRecord(title, records):
    if len(records) > 0:
        df = pd.DataFrame(records)
        logging.info(f"Error Record : {title}")
        logging.info("\n" + df.to_markdown(index=False).lstrip())          

if __name__ == "__main__":
    logging.info("This is a different version of the module.py file.")    