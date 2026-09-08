from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseDbHelper(ABC):

    @abstractmethod
    def fetchAllRows(
        self, query: str, params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all query results formatted as a list of dictionaries."""
        pass

    @abstractmethod
    def insertOrReplacePriceStats(
        self, price_stats_list: List[Dict[str, Any]]
    ) -> int:
        """Upsert daily price statistics into the database."""
        pass

    @abstractmethod
    def insertOrReplaceSectorRecords(self, records) -> int:
        pass

    @abstractmethod
    def insertOrReplaceMarketRecords(self, records) -> int:
        pass

    @abstractmethod
    def insertDailyStockPrice(self, prices: list[dict]) -> int:
        pass

    @abstractmethod
    def readDataFrame(self, sql_main:str):
        pass    
