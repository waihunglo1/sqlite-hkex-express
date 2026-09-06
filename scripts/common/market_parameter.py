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
    args = parser.parse_args()

    # 2. Dynamically import config and dbHelper based on market choice
    if args.market == "us":
        from baseus import config, duckDbHelper as dbHelper  # or duckDbHelper
    else:
        from basehk import config, sqliteDbHelper as dbHelper

    return config, dbHelper