const { db } = require('@vercel/postgres');
const config = require('config');
const sqliteDb = require('better-sqlite3')(config.db.sqlite.file, {});
const moment = require('moment');
sqliteDb.pragma('journal_mode = WAL');

/**
 * 
 */
const dumpSqliteVerion = async () => {
    const row01 = sqliteDb.prepare('SELECT sqlite_version()').get();
    console.log(row01);
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
    console.log("[INFO] sqlite db statistics");
    console.log(row02);
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
    console.log(`[${logTime}] [WARN] Unknown stock: ${stock.code} - ${stock.name}`);    

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

module.exports = {
    insertDailyStockPrice,
    insertStockData,
    queryDailyStockPriceStatistics,
    dumpSqliteVerion
};