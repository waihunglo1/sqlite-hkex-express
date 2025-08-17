select dt, count(1) from daily_stock_stats
group by dt
order by dt desc

select count(1) from stock where market_cap is not null

select * from stock where sector = 'UNKNOWN'  


delete from daily_stock_stats where dt >= '20250707'


select symbol from daily_stock_stats
where dt = '20250704'
except
select symbol from daily_stock_stats
where dt = '20250707'

select * from daily_stock_price where symbol = '1.HK'
order by dt desc

delete from daily_stock_price where dt >= '20250703'



select 
        dt,
        case sector
        when 'Basic Materials' then 'XLB'
        when 'Communication Services' then 'XLC'
        when 'Consumer Cyclical' then 'XLY'
        when 'Consumer Cyclical' then 'XLY'
        when 'Consumer Defensive' then 'XLP'
        when 'Energy' then 'XLE'
        when 'Financial Services' then 'XLF'
        when 'Healthcare' then 'XLV'
        when 'Industrials' then 'XLI'
        when 'Real Estate' then 'XLF'
        when 'Technology' then 'XLK'
        when 'Utilities' then 'XLU'   
        else 'XLX'
        end sector,
        round(cast(up4pct1d as float) / tot * 100.0, 2) u4sm, 
        round(cast(dn4pct1d as float) / tot * 100.0, 2) d4sm,
        round(cast(up0pct1d as float) / tot * 100.0, 2) sm
        FROM
        (
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
        ) order by dt desc, sector

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


select * from DAILY_SECTORS_STATS
order by dt desc