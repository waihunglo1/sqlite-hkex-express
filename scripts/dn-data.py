from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import date, timedelta
import time
import os

def isWeekDay(date_obj):
  # Weekdays are 0 (Monday) to 4 (Friday)
  return date_obj.weekday() < 5

def last7days():
  # Get today's date
  today = date.today()

  # Generate the last 5 dates (including today)
  last_5_dates = [today - timedelta(days=i) for i in range(7)]

  for dt in last_5_dates:
    if(isWeekDay(dt)):
        dateStr = dt.strftime("%y%m%d")
        downloadHtm("d" + dateStr + "e.htm")

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

#Main program
last7days()  
