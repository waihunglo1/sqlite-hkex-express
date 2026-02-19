from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from datetime import date, timedelta
import os
import datetime
import time


# Define the directory path where files need to be deleted
# Replace '.' with the specific path, e.g., r'C:\Users\YourUser\Downloads\Test'
hkex_quote_dir_path = "/Users/user/Downloads/hkex-quote"

# Define the number of days for the cutoff
days_cutoff = 15

# Calculate the cutoff timestamp (15 days ago)
# time.time() gives current time in seconds since the epoch
# 24 * 3600 is the number of seconds in a day
cutoff_timestamp = time.time() - (days_cutoff * 24 * 3600)

# Iterate over all items in the directory
def removeHistorialFiles():
    for filename in os.listdir(hkex_quote_dir_path):
        file_path = os.path.join(hkex_quote_dir_path, filename)

        # Check if the item is a file and not a directory
        if os.path.isfile(file_path):
            # Get the file's last modification time
            file_mtime = os.path.getmtime(file_path)
            print(f"Examine File: {filename} (Modified date: {datetime.datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d')})")

            # Compare the file's modification time with the cutoff timestamp
            if file_mtime < cutoff_timestamp:
                try:
                    # Delete the file
                    os.remove(file_path)
                    print(f"Deleted: {filename} (Modified date: {datetime.datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d')})")
                except OSError as e:
                    print(f"Error deleting file {filename}: {e}")

        # Optional: If you also want to remove empty directories or older directories recursively, 
        # you might consider using os.walk and shutil.rmtree for a more robust solution, 
        # but the above code safely handles single-level files.

def isWeekDay(date_obj):
  # Weekdays are 0 (Monday) to 4 (Friday)
  return date_obj.weekday() < 5

def downloadHtm(fileName):
  # Define the file path and name
  targetPath = "/Users/user/Downloads/hkex-quote"
  save_path = os.path.join(targetPath, fileName)

  if os.path.isfile(save_path):
     print("file existed:" + save_path + " [download abort]")
     return

  # Automatically manages the browser driver (e.g., ChromeDriver, FirefoxDriver)
  chrome_options = Options()
  chrome_options.add_argument('--headless')
  driver = webdriver.Chrome(options=chrome_options) 

  try:
    # Navigate to a website
    url = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/" + fileName
    print("assessing : " + url)
    driver.get(url)
    time.sleep(3)

    # Save the content to a local file with utf-8 encoding
    with open(save_path, "w", encoding='utf-8') as f:
        content = driver.page_source
        f.write(content)
        
    # Assert that the page title contains "Python"
    assert "Hong Kong Exchanges and Clearing Limited" in driver.title
    print(f"Successfully saved HTML to: {save_path}")    

  finally:
    # Give some time to see the result (optional)
    time.sleep(3) 
    # Close the browser session
    driver.quit()


if __name__ == "__main__":
    print("This is a different version of the module.py file.")    