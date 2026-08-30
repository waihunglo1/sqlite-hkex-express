import configparser
from datetime import datetime
import io
import logging
import os
import re
import sys
from bs4 import BeautifulSoup
import pandas as pd
from . import utility as helper

# Fixed width field definitions
LINE01_SPECS = [
    {"name": "code", "start": 1, "width": 5},
    {"name": "previousClose", "start": 28, "width": 9},
    {"name": "ask", "start": 37, "width": 9},
    {"name": "high", "start": 46, "width": 9},
    {"name": "sharesTraded", "start": 55, "width": 20},
]

LINE02_SPECS = [
    {"name": "close", "start": 28, "width": 9},
    {"name": "bid", "start": 37, "width": 9},
    {"name": "low", "start": 46, "width": 9},
    {"name": "turnover", "start": 55, "width": 20},
]

SALES_RECORD_SPECS = [
    {"name": "code", "start": 0, "width": 5},
    {"name": "salesRecord", "start": 23, "width": 60},
]

class QuoteParser:

    def parse_fixed_width(self, line: str, specs: list[dict]) -> dict:
        """Helper to parse a line using 0-based fixed-width character field specs."""
        parsed = {}
        for spec in specs:
            start = spec["start"]
            end = start + spec["width"]
            val = line[start:end] if len(line) >= start else ""
            parsed[spec["name"]] = val.strip()
        return parsed

    def convert_value(self, obj: dict) -> dict:
        """Cleans up values in a parsed dictionary object."""
        cleaned = {}
        for key, val in obj.items():
            s = val.strip()
            if s in ("-", "N/A", ""):
                s = "0"
            cleaned[key] = s
        return cleaned


    def search_quote_date(self, line: str) -> str | None:
        """Extracts date from a string matching format DD MMM YYYY."""
        date_regex = re.compile(r"(\d{1,2})\s*([A-Za-z]{3})\s*(\d{4})")
        match = date_regex.search(line)
        if match:
            day, month, year = match.groups()
            date_str = f"{day}/{month}/{year}"
            try:
                dt = datetime.strptime(date_str, "%d/%b/%Y")
                return dt.strftime("%Y%m%d")
            except ValueError:
                logging.error(f"Invalid date format: {date_str}")
                return None
        return None


    def process_sales_records(self, symbol: str, sales_records: list[str], prices: list[dict]):
        """Extracts opening price from sales record tags and updates prices list."""
        text = "".join(sales_records).replace(",", "")
        regexes = [re.compile(r"<(.+?)>"), re.compile(r"\[(.+?)\]")]

        auction_session = []
        normal_session = []

        for index, re_pattern in enumerate(regexes):
            extracted_data = re_pattern.findall(text)

            if index == 0:
                for data in extracted_data:
                    session01 = data.strip().split(" ")
                    if session01:
                        auction_session.append(session01[-1])
            else:
                for data in extracted_data:
                    session01 = data.strip().split(" ")
                    if session01:
                        normal_session.append(session01[0])

        open_price = None

        if auction_session and "-" in auction_session[0]:
            open_price = auction_session[0].split("-")[1]

        if (
            helper.is_empty(open_price)
            and normal_session
            and "-" in normal_session[0]
        ):
            open_price = normal_session[0].split("-")[1]

        if (
            helper.is_empty(open_price)
            and len(normal_session) > 1
            and "-" in normal_session[1]
        ):
            open_price = normal_session[1].split("-")[1]

        if (
            helper.is_empty(open_price)
            and len(auction_session) > 1
            and "-" in auction_session[1]
        ):
            open_price = auction_session[1].split("-")[1]

        formatted_symbol = helper.reformat_symbol_for_hk(f"{symbol}.HK")
        for price in prices:
            if price["symbol"] == formatted_symbol:
                price["open"] = open_price


    def process_2_lines(
        self,
        prices: list[dict],
        previous_line: str,
        current_line: str,
        quote_date: str | None,
    ):
        """Processes two lines of stock prices and appends record to prices list."""
        if helper.is_empty(previous_line):
            return

        line01_data = self.convert_value(self.parse_fixed_width(previous_line, LINE01_SPECS))
        line02_data = self.convert_value(self.parse_fixed_width(current_line, LINE02_SPECS))

        high = (
            line02_data["close"]
            if line01_data["high"] == "0"
            else line01_data["high"]
        )
        low = (
            line02_data["close"]
            if line02_data["low"] == "0"
            else line02_data["low"]
        )

        price = {
            "symbol": helper.reformat_symbol_for_hk(f"{line01_data['code']}.HK"),
            "period": "D",
            "dt": quote_date,
            "tm": "000000",
            "open": 0,
            "high": high,
            "low": low,
            "close": line02_data["close"],
            "volume": line01_data["sharesTraded"],
            "adj_close": 0,
            "open_int": 0,
        }

        prices.append(price)


    def process_line_by_line(self, string_stream: io.StringIO):
        """Processes HKEX text stream line by line."""
        quotation = " CODE  NAME OF STOCK    CUR PRV.CLO./    ASK/    HIGH/      SHARES TRADED/"
        section = (
            "-------------------------------------------------------------------------------"
        )
        trade_suspended = "TRADING SUSPENDED"
        sales_record = "                            SALES RECORDS FOR ALL STOCKS"

        quotation_line_count = 0
        sales_record_line_count = 0
        line_count = 0
        state = 0

        previous_line = None
        sales_records = []
        symbol = None
        prices = []
        quote_date = None

        for line in string_stream:
            line_count += 1
            line = line.replace(",", "")

            if line_count == 7:
                quote_date = self.search_quote_date(line)

            if quotation in line:
                state = 1

            if sales_record in line:
                state = 2

            if state == 1:
                quotation_line_count += 1

                if section in line:
                    state = 0

                if quotation_line_count >= 4:
                    line01_data = self.parse_fixed_width(line, LINE01_SPECS)
                    code = line01_data.get("code")

                    if not helper.is_empty(code) and trade_suspended not in line:
                        previous_line = line
                        continue

                    if helper.is_empty(code) and not helper.is_empty(
                        previous_line
                    ):
                        self.process_2_lines(prices, previous_line, line, quote_date)
                        previous_line = None
                        continue

            if state == 2:
                sales_record_line_count += 1

                if section in line and sales_record_line_count >= 14:
                    state = 0

                if sales_record_line_count >= 14 and len(line) > 5:
                    line01_data = self.parse_fixed_width(line, SALES_RECORD_SPECS)
                    code = line01_data.get("code")

                    if not helper.is_empty(code):
                        if not helper.is_empty(symbol) and symbol != code:
                            try:
                                self.process_sales_records(
                                    symbol, sales_records, prices
                                )
                            except Exception as e:
                                logging.error(
                                    f"Error processing sales records for symbol [{symbol}]: {e}"
                                )
                                sys.exit(1)
                        symbol = code
                        sales_records = [line01_data["salesRecord"]]
                    else:
                        sales_records.append(line01_data["salesRecord"])

        return quote_date, prices


    def parse_hkex_file(self, sqlitehelper, file_path: str):
        """Scrapes HTML file using BeautifulSoup and inserts stock prices into database."""
        with open(file_path, "rb") as f:
            buffer = f.read()

        soup = BeautifulSoup(buffer, "html.parser")
        fonts = [font.get_text() for font in soup.find_all("font")]

        # Stream text via StringIO
        string_stream = io.StringIO("".join(fonts))

        quote_date, prices = self.process_line_by_line(string_stream)
        updated = sqlitehelper.insertOrReplacePriceRecords(prices)

        logging.info(f"[{file_path}] {quote_date} : {len(prices)} / Updated : {updated}")
        df = pd.DataFrame(prices)
        logging.info("\n" + df.head(5).to_string())