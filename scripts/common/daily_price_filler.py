import os
from datetime import date, timedelta
import logging
import re
from . import utility as helper
from abc import ABC, abstractmethod

class PriceFiller(ABC):
    def __init__(self, config, dbHelper):
        self.dbHelper = dbHelper
        self.config = config

    @abstractmethod
    def fill(self) -> int:
        pass

    def dumpStatistics(self) -> int:
        sql_query = """
            select 
                dt, 
                sum(case when close > 0 then 1 else 0 end) as close_not_zero,
                count(1) as row_count,
                ROUND(
                    sum(case when close > 0 then 1 else 0 end) * 100.0 / count(1),
                    2
                ) as pct
            from daily_stock_price
            where dt in
            (
                select dt from daily_stock_price
                group by dt
                order by dt desc
                limit 5
            )
            group by dt
            order by dt desc            
        """

        df_raw = self.dbHelper.readDataFrame(sql_query)
        logging.info("Price Data Statistics:\n" + df_raw.to_string())