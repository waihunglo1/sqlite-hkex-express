require('dotenv').config();
const { ANALYST_DATA_INI } = process.env;
const logger = require('./logger')
const fs = require('node:fs');
const ini = require('ini');
const { db } = require('@vercel/postgres');
const moment = require('moment');
const analystConfig = ini.parse(fs.readFileSync(ANALYST_DATA_INI, 'utf-8'));
const sqliteDb = require('better-sqlite3')(analystConfig.SQLITE.FILE, {});
sqliteDb.pragma('journal_mode = WAL');

/**
 * 
 */
async function dumpSqliteVerion () {
    const row01 = sqliteDb.prepare('SELECT sqlite_version() AS version').get();
    logger.info(`SQLite Version: ${row01.version}`);

    const sqlStmt = sqliteDb.prepare('SELECT name FROM sqlite_master');
    const schemas = sqlStmt.all();
        
    for (const schema of schemas) {
        logger.info(`Object : ${schema.name}`);
    }
}

/**
 * 
 */
const queryDailyStockPriceStatistics = async () => {
    const hcSql =
        `SELECT 
           max(dt) max_dt, 
           min(dt) min_dt, 
           COUNT(1) as historialPriceCount, 
           count(distinct symbol) as noOfProduct 
         FROM DAILY_STOCK_PRICE 
         WHERE symbol not like '^%'`;
    const row02 = sqliteDb.prepare(hcSql).get();
    logger.info("[INFO] sqlite db statistics");
    logger.info(row02);
}


/**
 * Insert stock data into the STOCK table.
 */
async function insertStockData(stocks) {
    const insert = sqliteDb.prepare('REPLACE INTO STOCK (symbol,name,industry,sector,market_cap) VALUES (@code,@name,@industry,@sector,@marketCap)');
    const insertMany = sqliteDb.transaction((stocks) => {
        for (const stock of stocks) {
            try {
                if(!unknownStockLogger(stock)) {
                    insert.run(stock);
                }
            } catch (error) {
                console.error(`Error inserting stock ${stock.symbol}:`, error);
            }   
        }
    });

    insertMany(stocks);
}

function unknownStockLogger(stock) {
    if(stock.industry !== "UNKNOWN" && stock.sector !== "UNKNOWN") {  
        return false;  
    }
    
    // UNknown sector stock, check if it is already in the DB
    const dbStock = queryStockByCode(stock);
    if(dbStock && dbStock.length > 0) { 
        return true;
    }

    const logTime = moment().format('YYYY-MM-DD HH:mm:ss');
    logger.info(`[${logTime}] [WARN] Unknown stock: ${stock.code} - ${stock.name}`);    

    return false;
}

/**
 * Process all dates in the database
 */
function queryStockByCode(stock) {
    // query db
    const sqlStr = `SELECT sector FROM STOCK where symbol = ?`;

    const sqlStmt = sqliteDb.prepare(sqlStr);
    const dbStock = sqlStmt.all(stock.code);

    return dbStock;
}


/**
 * insert daily stock price
 */
async function insertDailyStockPrice(prices) {
    const insert = sqliteDb.prepare(
        'REPLACE INTO DAILY_STOCK_PRICE (symbol,period,dt,tm,open,high,low,close,volume,adj_close,open_int) ' +
        'VALUES (@symbol,@period,@dt,@tm,@open,@high,@low,@close,@volume,@adj_close,@open_int)');

    const insertMany = sqliteDb.transaction((stockPrices) => {
        for (const stockPrice of stockPrices) insert.run(stockPrice);
    });

    insertMany(prices);
}

/**
 * Insert price statistics into the database
 * @param {*} priceStats 
 */
function insertPriceStats(priceStats) {
    const INSERT_SQL = `
      REPLACE INTO DAILY_STOCK_STATS 
      (symbol, dt, start_dt, open, high, low, close, volume, 
      prev_open, prev_high, prev_low, prev_close, prev_volume, 
      roc020, roc125, rsi014, sma200, sma150, sma100, sma050, sma020, sma010, sma005, sma003, 
      ema050, ema200, ema200pref, sma200pref, ema500pref, sma50pref, rsi14sctr, ppo01sctr, roc125sctr, sctr, 
      histDay, chg_pct_1d, chg_pct_5d, chg_pct_10d, chg_pct_20d, chg_pct_50d, chg_pct_100d, sma10turnover, 
      sma20turnover, sma50turnover, above_200d_sma ,above_150d_sma ,above_100d_sma ,above_50d_sma, 
      above_20d_sma ,above_10d_sma ,above_5d_sma,
      vp_high, vp_low, vp_bullish, vp_bearish,
      rs, normalise_rs, rs_priceOverSMA20, rs_slopeSMA20, rs_slopeSMA50, rs_slopeSMA150
      )   
      VALUES 
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?
      )`;

    const stmt = sqliteDb.prepare(INSERT_SQL);
    const info = stmt.run(priceStats.symbol, priceStats.dt, priceStats.start_dt,
        priceStats.open, priceStats.high, priceStats.low, priceStats.close, priceStats.volume,
        priceStats.prev_open, priceStats.prev_high, priceStats.prev_low, priceStats.prev_close,
        priceStats.prev_volume, priceStats.roc20, priceStats.roc125, priceStats.rsi14,
        priceStats.sma200, priceStats.sma150, priceStats.sma100, priceStats.sma50,
        priceStats.sma20, priceStats.sma10, priceStats.sma05, priceStats.sma03,
        priceStats.ema50, priceStats.ema200, priceStats.ema200pref, priceStats.sma200pref,
        priceStats.ema200pref, priceStats.sma50pref, priceStats.rsi14sctr, priceStats.ppo01sctr,
        priceStats.roc125sctr, priceStats.sctr, priceStats.histDay, priceStats.chg_pct_1d, priceStats.chg_pct_5d,
        priceStats.chg_pct_10d, priceStats.chg_pct_20d, priceStats.chg_pct_50d, priceStats.chg_pct_100d,
        priceStats.sma10turnover, priceStats.sma20turnover, priceStats.sma50turnover, 
        priceStats.above_200d_sma, priceStats.above_150d_sma, priceStats.above_100d_sma,
        priceStats.above_50d_sma, priceStats.above_20d_sma, priceStats.above_10d_sma, priceStats.above_5d_sma,
        priceStats.vp_high, priceStats.vp_low, priceStats.vp_bullish, priceStats.vp_bearish,
        priceStats.rs, priceStats.normalise_rs, priceStats.rs_priceOverSMA20, priceStats.rs_slopeSMA20, 
        priceStats.rs_slopeSMA50, priceStats.rs_slopeSMA150
    );

    if (info.changes <= 0) {
        logger.info("[ERROR] Inserted " + priceStats.symbol + " " + priceStats.dt);
    }

}

module.exports = {
    insertDailyStockPrice,
    insertStockData,
    queryDailyStockPriceStatistics,
    dumpSqliteVerion,
    insertPriceStats
};