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