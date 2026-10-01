import pandas as pd
import requests
import logging
from common import market_parameter as marketParameter
from common import utility as helper

def fetch(url:str):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    logging.info(f"Fetching SCTR {url}")
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()

    logging.info(f"Downloaded JSON Payload: {helper.format_bytes(len(response.content))}")
    return response.json()

def store(dbHelper, table_name:str, response):
    # Parse JSON payload structure
    if isinstance(response, dict):
        items = response.get("data") or response.get("items") or response.get("rows", [])
    else:
        items = response

    if not items or len(items) < 2:
        logging.error("⚠️ Payload contains insufficient items. Ingestion aborted.")
        logging.error(response) 
        return

    # 1. Extract raw date from the VERY FIRST item
    first_item = items[0]
    raw_date = (
        first_item.get("dt")
        or first_item.get("date")
        or first_item.get("tradeDate")
        or first_item.get("asOfDate")
    )

    # Convert date to standard YYYYMMDD string format
    formatted_date = pd.to_datetime(raw_date).strftime("%Y%m%d")
    logging.info(f"📅 Extracted Trade Date: {formatted_date} (raw: {raw_date})")

    # Build DataFrame from raw items
    df = pd.DataFrame(items)

    # Normalize column names to lower case
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # 2. Assign the YYYYMMDD date to 'dt' column
    df["dt"] = formatted_date
    df["date"] = formatted_date #display purpose

    # 3. Drop the first row to remove header/summary meta-data
    df_clean = df.iloc[1:].copy()

    # Filter out any invalid rows where symbol is missing or empty
    df_clean = df_clean[
        df_clean["symbol"].notna()
        & (df_clean["symbol"].astype(str).str.strip() != "")
    ]

    logging.info("\n" + df_clean.head(5).to_string())

    # Restrict DataFrame columns to ONLY match defined SQLite columns
    valid_db_columns = [
        "dt",
        "symbol",
        "name",
        "sctr",
        "close",
        "chg_pct",
        "vol",
        "marketcap",
        "delta",
        "industry",
        "sector"
    ]
    insert_cols = [col for col in valid_db_columns if col in df_clean.columns]
    df_clean = df_clean[insert_cols]

    # Convert to dictionary records for binding
    records = df_clean.to_dict(orient="records")
    dbHelper.insertOrReplaceSctrTable(records, table_name, insert_cols)

    logging.info(f"✅ Successfully upserted {len(df_clean)} records into '{table_name}'.")


if __name__ == "__main__":
    config, dbHelper, avienUri =  marketParameter.parse_argument()

    target_url = config['STOCK-CHART']['URL']
    views = helper.splitStringToArray(config['STOCK-CHART']['views'])

    for view in views:
        url = target_url.format(view=view)
        response = fetch(url)

        if view == "E":
            store(dbHelper, "STOCKCHARTS_SCTR_DAILY_ETF", response)
        else:
            store(dbHelper, "STOCKCHARTS_SCTR_DAILY_EQT", response)