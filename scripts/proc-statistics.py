import logging
import pandas as pd
from core import config, sqliteDbHelper, quoteParser
from core import utility as helper

def newSectorStats() -> dict:
    return {
        "dt": "",
        "XLB_U4SM": 0,
        "XLB_D4SM": 0,
        "XLB_SM": 0,
        "XLC_U4SM": 0,
        "XLC_D4SM": 0,
        "XLC_SM": 0,
        "XLY_U4SM": 0,
        "XLY_D4SM": 0,
        "XLY_SM": 0,
        "XLP_U4SM": 0,
        "XLP_D4SM": 0,
        "XLP_SM": 0,
        "XLE_U4SM": 0,
        "XLE_D4SM": 0,
        "XLE_SM": 0,
        "XLF_U4SM": 0,
        "XLF_D4SM": 0,
        "XLF_SM": 0,
        "XLV_U4SM": 0,
        "XLV_D4SM": 0,
        "XLV_SM": 0,
        "XLI_U4SM": 0,
        "XLI_D4SM": 0,
        "XLI_SM": 0,
        "XLRE_U4SM": 0,
        "XLRE_D4SM": 0,
        "XLRE_SM": 0,
        "XLK_U4SM": 0,
        "XLK_D4SM": 0,
        "XLK_SM": 0,
        "XLU_U4SM": 0,
        "XLU_D4SM": 0,
        "XLU_SM": 0,
        "XLX_U4SM": 0,
        "XLX_D4SM": 0,
        "XLX_SM": 0,
    }


def populateSectorStatistics(sqliteDbHelper, config):
    sql = config['SECTOR-STATS-01']['SQL']
    sector_stats = sqliteDbHelper.fetchAllRows(sql)

    stats = newSectorStats()
    last_dt = ""
    count = 0

    sectorStats = []

    for sector_stat in sector_stats:
        current_dt = sector_stat["dt"]
        current_sector = sector_stat["sector"]

        if last_dt != "" and last_dt != current_dt:
            sectorStats.append(stats)
            stats = newSectorStats()
            count += 1


        stats["dt"] = current_dt

        # Dynamically set sector fields
        valid_sectors = [
            "XLB",
            "XLC",
            "XLY",
            "XLP",
            "XLE",
            "XLF",
            "XLV",
            "XLI",
            "XLRE",
            "XLK",
            "XLU",
            "XLX",
        ]

        if current_sector in valid_sectors:
            stats[f"{current_sector}_U4SM"] = sector_stat["u4sm"]
            stats[f"{current_sector}_D4SM"] = sector_stat["d4sm"]
            stats[f"{current_sector}_SM"] = sector_stat["sm"]
        else:
            logging.info(
                f"[ERROR] Unknown sector: {current_sector} on {current_dt}"
            )

        last_dt = current_dt

    # new_sector_status_to_db(last_stats)
    df = pd.DataFrame(sectorStats)
    logging.info("\n" + df.iloc[:5, :10].to_string())
    updatedRow = sqliteDbHelper.insertOrReplaceSectorRecords(sectorStats)
    logging.info(f"Sectors stats updated. Total records: {count} / Row updated : {updatedRow}")


def populateMarketStatistics(sqliteDbHelper, config):
    sql = config['MARKET-STATS-01']['SQL']
    market_stats = sqliteDbHelper.fetchAllRows(sql)

    params = [
        (
            ms["dt"],
            ms["up4pct1d"],
            ms["dn4pct1d"],
            ms["up25pctin100d"],
            ms["dn25pctin100d"],
            ms["up25pctin20d"],
            ms["dn25pctin20d"],
            ms["up50pctin20d"],
            ms["dn50pctin20d"],
            ms["noofstocks"],
            ms["above200smapct"],
            ms["above150smapct"],
            ms["above50smapct"],
            ms["above20smapct"],
            ms["hsi"],
            ms["hsce"],
        )
        for ms in market_stats
    ]

    df = pd.DataFrame(params)
    logging.info("\n" + df.head(5).to_string())
    updatedRow = sqliteDbHelper.insertOrReplaceMarketRecords(params)
    logging.info(f"Market stats updated. Total records: {len(market_stats)} / Row updated : {updatedRow}")


#
# Main program
#
if __name__ == "__main__": 
    populateSectorStatistics(sqliteDbHelper, config)
    populateMarketStatistics(sqliteDbHelper, config)