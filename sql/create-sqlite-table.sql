drop table if exists STOCK;

CREATE TABLE IF NOT EXISTS STOCK
(
  symbol           VARCHAR(10) PRIMARY KEY,
  name             VARCHAR(100),
  sector           VARCHAR(50),
  industry         VARCHAR(50),
  country          VARCHAR(50),
  currency         VARCHAR(10),
  exchange         VARCHAR(10),
  market_cap       REAL,
  last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);  


drop table if exists DAILY_STOCK_PRICE;  
CREATE TABLE IF NOT EXISTS DAILY_STOCK_PRICE
(
  symbol           VARCHAR(10),
  period           VARCHAR(1),
  dt               VARCHAR(10),
  tm               VARCHAR(6),
  open             REAL,
  high             REAL,
  low              REAL,
  close            REAL,
  volume           REAL,
  adj_close        REAL,
  open_int         INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_stock_price ON DAILY_STOCK_PRICE (symbol, dt);

drop table if exists DAILY_STOCK_STATS; 

CREATE TABLE IF NOT EXISTS DAILY_STOCK_STATS
(
  symbol           VARCHAR(10),
  dt               VARCHAR(10),
  start_dt         VARCHAR(10),
  open             REAL,
  high             REAL,
  low              REAL,
  close            REAL,
  volume           REAL,
  prev_open        REAL,
  prev_high       REAL,
  prev_low        REAL,
  prev_close      REAL,
  prev_volume      REAL,
  roc020           REAL,  
  roc125           REAL,
  rsi014           REAL,
  sma200           REAL,
  sma150           REAL,
  sma100           REAL,
  sma050           REAL,
  sma020           REAL,
  sma010           REAL,
  sma005           REAL,
  sma003           REAL,
  ema050           REAL,
  ema200           REAL,
  ema200pref       REAL,  
  sma200pref       REAL,
  ema500pref       REAL,
  sma50pref        REAL,
  rsi14sctr       REAL,
  ppo01sctr       REAL,
  roc125sctr     REAL,
  sctr           REAL,
  histDay        REAL, 
  chg_pct_1d    REAL, 
  chg_pct_5d    REAL, 
  chg_pct_10d   REAL,
  chg_pct_20d   REAL,
  chg_pct_50d   REAL,
  chg_pct_100d  REAL,
  sma10turnover REAL,
  sma20turnover REAL,
  sma50turnover REAL,
  above_200d_sma REAL,
  above_150d_sma REAL,
  above_100d_sma REAL,
  above_50d_sma  REAL,
  above_20d_sma  REAL,
  above_10d_sma  REAL,
  above_5d_sma   REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_stock_stats ON DAILY_STOCK_STATS (symbol, dt);


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
  above20smapct real,
  hsi real,
  hsce real
)
;

create unique index if not exists idx_DAILY_MARKET_STATS ON DAILY_MARKET_STATS (dt);

CREATE TABLE IF NOT EXISTS DAILY_SECTORS_STATS 
(
  dt               VARCHAR(10),
  XLB_U4SM         REAL,
  XLB_D4SM         REAL,  
  XLB_SM           REAL, 
  XLC_U4SM         REAL,
  XLC_D4SM         REAL,  
  XLC_SM           REAL, 
  XLY_U4SM         REAL,
  XLY_D4SM         REAL,  
  XLY_SM           REAL, 
  XLP_U4SM         REAL,
  XLP_D4SM         REAL,  
  XLP_SM           REAL, 
  XLE_U4SM         REAL,
  XLE_D4SM         REAL,  
  XLE_SM           REAL, 
  XLF_U4SM         REAL,
  XLF_D4SM         REAL,  
  XLF_SM           REAL,   
  XLV_U4SM         REAL,
  XLV_D4SM         REAL,  
  XLV_SM           REAL, 
  XLI_U4SM         REAL,
  XLI_D4SM         REAL,  
  XLI_SM           REAL, 
  XLRE_U4SM         REAL,
  XLRE_D4SM         REAL,  
  XLRE_SM           REAL, 
  XLK_U4SM         REAL,
  XLK_D4SM         REAL,  
  XLK_SM           REAL, 
  XLU_U4SM         REAL,
  XLU_D4SM         REAL,  
  XLU_SM           REAL, 
  XLX_U4SM         REAL,
  XLX_D4SM         REAL,  
  XLX_SM           REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_DAILY_SECTORS_STATS ON DAILY_SECTORS_STATS (dt);


ALTER TABLE DAILY_STOCK_STATS ADD COLUMN vp_high real;
ALTER TABLE DAILY_STOCK_STATS ADD COLUMN vp_low real;
ALTER TABLE DAILY_STOCK_STATS ADD COLUMN vp_bullish real;
ALTER TABLE DAILY_STOCK_STATS ADD COLUMN vp_bearish real;


ALTER TABLE DAILY_MARKET_STATS ADD COLUMN above50smapct real;

ALTER TABLE DAILY_STOCK_STATS ADD COLUMN rs real;
ALTER TABLE DAILY_STOCK_STATS ADD COLUMN normalise_rs real;
ALTER TABLE DAILY_STOCK_STATS ADD COLUMN rs_priceOverSMA20 real;
ALTER TABLE DAILY_STOCK_STATS ADD COLUMN rs_slopeSMA20 real;
ALTER TABLE DAILY_STOCK_STATS ADD COLUMN rs_slopeSMA50 real;
ALTER TABLE DAILY_STOCK_STATS ADD COLUMN rs_slopeSMA150 real;

select * from "DAILY_STOCK_STATS"
where dt = '20260710'

select dt from daily_stock_stats where dt >= '20260101'

delete from daily_stock_stats where dt >= '20260101'

select * from "STOCK"
order by last_updated desc 
where symbol = '2715.HK'

select * from "STOCK"
order by last_updated desc

            select 
                stock.sector, DAILY_STOCK_STATS.dt,
                sum(case when chg_pct_1d >= 4 then 1 else 0 end)  up4pct1d  , 
                sum(case when chg_pct_1d<= -4 then 1 else 0 end)  dn4pct1d ,
                sum(case when chg_pct_1d > 0  then 1 else 0 end)  up0pct1d ,
                sum(case when chg_pct_1d < 0 then 1 else 0 end)  dn0pct1d ,
                sum(case when chg_pct_1d < 0 then 1 else 0 end)  dn0pct1d ,
                count(1) tot
            from stock, DAILY_STOCK_STATS
            where stock.symbol = DAILY_STOCK_STATS.symbol
            and sector != 'UNKNOWN'
            group by stock.sector, DAILY_STOCK_STATS.dt
            order by sector

            select 
                stock.sector, DAILY_STOCK_STATS.dt,
                avg(normalise_rs), min(normalise_rs), max(normalise_rs), count(1) tot
            from stock, DAILY_STOCK_STATS
            where stock.symbol = DAILY_STOCK_STATS.symbol
            and sector != 'UNKNOWN'
            and normalise_rs > 50
            group by stock.sector, DAILY_STOCK_STATS.dt
            order by DAILY_STOCK_STATS.dt desc,    stock.sector    

            select *
            from stock, DAILY_STOCK_STATS
            where stock.symbol = DAILY_STOCK_STATS.symbol
            and sector != 'UNKNOWN'
            and rs > 50
            and sector = 'Financial Services'
            and dt = '20260717'
                 