from dataclasses import dataclass, field
import logging
import math
import time
import sys
from typing import Any, Dict, List, Optional, Tuple
from scipy import stats
from baseus import config, duckDbHelper
from common import translator as translaterHelper
from common.daily_price_processor import DailyPriceProcessor

# Run Configuration
QUERY_DATE = ""  # e.g., '20260730'
QUERY_SYMBOL = ""  # e.g., '2697.HK'

if __name__ == "__main__": 
    # Pass helper to your processor
    processor = DailyPriceProcessor(dbHelper=duckDbHelper, whereClause="AND quote_type = 'EQUITY'")
    processor.process_data_local(QUERY_DATE, QUERY_SYMBOL)