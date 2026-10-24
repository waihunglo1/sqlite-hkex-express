import argparse
import importlib

def parse_argument():
    # 1. Parse command-line flags
    parser = argparse.ArgumentParser(description="Publish stock stats to Google Sheets")
    parser.add_argument(
        "--market",
        choices=["hk", "us"],
        default="hk",
        help="Target market (hk or us)",
    )

    parser.add_argument(
        "--fillstockinfo",
        choices=["y", "n"],
        default="y",
        help="fill stock info (y or n)",
    )    

    parser.add_argument(
        "--tradedate",
        default="",
        help="trade date to process",
    )    


    args = parser.parse_args()

    # 2. Dynamically import config and dbHelper based on market choice
    if args.market == "us":
        from baseus import config, duckDbHelper as dbHelper, avienUri 
    else:
        from basehk import config, sqliteDbHelper as dbHelper, avienUri

    # Ensure the 'PARAMETER' section exists in the ConfigParser object
    if not config.has_section("PARAMETER"):
        config.add_section("PARAMETER")

    # Safely set the option
    config.set("PARAMETER", "MARKET", str(args.market))
    config.set("PARAMETER", "FILL_STOCK_INFO", str(args.fillstockinfo))    
    config.set("PARAMETER", "TRADE_DATE", str(args.tradedate))        

    return config, dbHelper, avienUri