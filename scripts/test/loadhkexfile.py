import configparser
from datetime import datetime
import io
import logging
import os
import re
import sys
from bs4 import BeautifulSoup
import pandas as pd
from core import config, sqliteDbHelper, quoteParser
from core import utility as helper

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


if __name__ == "__main__": 
    traverse_dir(sqliteDbHelper, config)