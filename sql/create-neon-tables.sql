CREATE TABLE IF NOT EXISTS DAILY_MARKET_STATS
(
  dt               VARCHAR(10),
  up4pct1d int,
  dn4pct1d int,
  up25pctin100d int,
  dn25pctin100d int,
  up25pctin20d int,
  dn25pctin20d int,
  up50pctin20d int,
  dn50pctin20d int,  
  noofstocks int,
  above200smapct real,
  above150smapct real,
  above20smapct real
)
;

create unique index if not exists idx_DAILY_MARKET_STATS ON DAILY_MARKET_STATS (dt);