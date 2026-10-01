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
        from baseus import config, duckDbHelper as dbHelper, avienUri 
    else:
        from basehk import config, sqliteDbHelper as dbHelper, avienUri

    # Ensure the 'PARAMETER' section exists in the ConfigParser object
    if not config.has_section("PARAMETER"):
        config.add_section("PARAMETER")

    # Safely set the option
    config.set("PARAMETER", "MARKET", str(args.market))

    return config, dbHelper, avienUri