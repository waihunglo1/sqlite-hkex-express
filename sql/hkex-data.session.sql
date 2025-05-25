SELECT * FROM DAILY_STOCK_PRICE where symbol = '0001.HK' and dt <= '20250516' order by dt desc limit 200





select dt, count(1)  from DAILY_STOCK_STATS
where chg_pct_1d >= 4
and symbol not like '%-%'
group by dt
order by dt desc

select close, prev_close, chg_pct_1d, (close - prev_close) / prev_close, *  

select count(1) from DAILY_STOCK_STATS
where chg_pct_1d <= -4
union
select count(1) from DAILY_STOCK_STATS
where chg_pct_1d >= 4

select 
  dt,
  sum(case when chg_pct_1d >= 4 then 1 else 0 end)  up4pct1d,
  sum(case when chg_pct_1d<= -4 then 1 else 0 end)  dn4pct1d,
  sum(case when chg_pct_100d >= 25 then 1 else 0 end)  up25pctin100d,
  sum(case when chg_pct_100d<= -25 then 1 else 0 end)  dn25pctin100d,
  sum(case when chg_pct_20d >= 25 then 1 else 0 end)  up25pctin20d,
  sum(case when chg_pct_20d<= -25 then 1 else 0 end)  dn25pctin20d,  
  sum(case when chg_pct_20d >= 50 then 1 else 0 end)  up25pctin20d,
  sum(case when chg_pct_20d<= -50 then 1 else 0 end)  dn25pctin20d,
  count(1) noofstocks
  
from DAILY_STOCK_STATS
group by dt
order by dt desc


SELECT dt FROM DAILY_STOCK_PRICE group by dt order by dt desc limit 200
 
select * from DAILY_STOCK_STATS
where dt = '20250516'
order by sctr



