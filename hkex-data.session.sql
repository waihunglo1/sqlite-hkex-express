select dt, count(1) from daily_stock_stats
group by dt
order by dt desc

select * from stock


delete from daily_stock_stats where dt >= '20250707'


select symbol from daily_stock_stats
where dt = '20250704'
except
select symbol from daily_stock_stats
where dt = '20250707'

select * from daily_stock_price where symbol = '1.HK'
order by dt desc

delete from daily_stock_price where dt >= '20250703'