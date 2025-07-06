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
        `SELECT max(dt), min(dt), COUNT(1) as historialPriceCount, count(distinct symbol) as noOfProduct 
    FROM DAILY_STOCK_PRICE`;
    const row02 = sqliteDb.prepare(hcSql).get();
    console.log("[INFO] sqlite db statistics");
    console.log(row02);
}


/**
 * Insert stock data into the STOCK table.
 */
async function insertStockData(stocks) {
    const insert = sqliteDb.prepare('REPLACE INTO STOCK (symbol,name,industry,sector) VALUES (@code,@name,@industry,@sector)');
    const insertMany = sqliteDb.transaction((stocks) => {
        for (const stock of stocks) insert.run(stock);
    });

    insertMany(stocks);
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