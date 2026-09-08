import logging
import os
import sqlite3
import pandas as pd
import psycopg
from dotenv import load_dotenv
from common import market_parameter as marketParameter

# Execute initialization immediately on package import
load_dotenv('.env')
load_dotenv('.env.hk')



def push_df_to_aiven(table_name: str, df: pd.DataFrame, arienUri):
    """Clears target Aiven table and uploads DataFrame records in batch."""
    records = df.to_dict(orient="records")
    if not records:
        return

    columns = list(records[0].keys())
    sql_insert = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join([f'%({c})s' for c in columns])});"

    with psycopg.connect(arienUri) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table_name};")
            cur.executemany(sql_insert, records)
        conn.commit()
    logging.info(f"Successfully pushed {len(records)} rows to Aiven [{table_name}]")


def sync_daily_stock_stats(dbHelper):

        sql_main = """
            SELECT DAILY_STOCK_STATS.*, STOCK.sector, STOCK.industry, SUBSTR(STOCK.name, 1, 100) AS short_name 
            FROM DAILY_STOCK_STATS 
            JOIN STOCK ON DAILY_STOCK_STATS.symbol = STOCK.symbol
            WHERE dt = (SELECT MAX(dt) FROM DAILY_STOCK_STATS)
        """
        df_main = dbHelper.readDataFrame(sql_main)

        # Query 20-day history for all stocks & pivot into sctr1..20, normalise_rs1..20
        df_hist = dbHelper.readDataFrame(
            """
            SELECT symbol, sctr, normalise_rs FROM DAILY_STOCK_STATS 
            """
        )

        # Create row numbers per stock for the top 20 records
        df_hist["rn"] = (
            df_hist.groupby("symbol").cumcount() + 1
        )
        df_hist = df_hist[df_hist["rn"] <= 20]

        # logging.info("\n" + df_hist.to_string())

        # Pivot metrics
        sctr_piv = df_hist.pivot(
            index="symbol", columns="rn", values="sctr"
        ).add_prefix("sctr")
        rs_piv = df_hist.pivot(
            index="symbol", columns="rn", values="normalise_rs"
        ).add_prefix("normalise_rs")

        # Merge pivoted metrics back into df_main
        df_final = (
            df_main.merge(sctr_piv, on="symbol", how="left")
            .merge(rs_piv, on="symbol", how="left")
            .fillna(0)
        )

        # 4. DROP columns that do NOT exist in the PostgreSQL target schema
        cols_to_drop = [
            col
            for col in ["sctr", "normalise_rs","rs"]
            if col in df_final.columns
        ]
        df_final.drop(columns=cols_to_drop, inplace=True)

        return df_final




if __name__ == "__main__":
    config, dbHelper, avienUri = marketParameter.parse_argument()        
    df = sync_daily_stock_stats(dbHelper)

    # Upload to Aiven
    push_df_to_aiven("daily_stock_stats", df, avienUri)

