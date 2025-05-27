require('dotenv').config();
const postgres = require('postgres');
const { AVIEN_DB_USER, AVIEN_DB_PASSWORD, AVIEN_DB_HOST, AVIEN_DB_PORT, AVIEN_DB_DATABASE } = process.env;
const sql = postgres(`postgres://${AVIEN_DB_USER}:${AVIEN_DB_PASSWORD}@${AVIEN_DB_HOST}:${AVIEN_DB_PORT}/${AVIEN_DB_DATABASE}?sslmode=require`, {
  idle_timeout: 20,
  max_lifetime: 60 * 30
});

async function getAivenPgVersion() {
  const result = await sql`SELECT version()`;
  var version = result[0];
  return version;
}

async function updateMarketStats(marketStats) {
    await sql`delete from daily_market_stats;`; // Clear existing data
    console.log("Cleared existing market stats data.");

    for (const marketStat of marketStats) {
        var result = await insertMarketStats(marketStat);
        // console.log("Inserted market stats: ", result);
    }
}

async function updateDailyStockStats(dailyStockStats) {
  await sql`delete from daily_stock_stats;`; // Clear existing data
  console.log("Cleared existing daily stock stats data.");

  for (const dailyStockStat of dailyStockStats) {
    var result = await insertDailyStockStats(dailyStockStat);
    // console.log("Inserted daily stock stats: ", result);
  }
}

async function insertDailyStockStats(dailyStockStat) {
  const result = await sql`
    INSERT INTO daily_stock_stats (symbol, dt, start_dt, open, high, low, close, volume, 
    prev_open, prev_high, prev_low, prev_close, prev_volume, 
    roc020, roc125, rsi014, sma200, sma150, sma100, sma050, sma020, sma010, sma005, sma003, 
    ema050, ema200, ema200pref, sma200pref, ema500pref, sma50pref, rsi14sctr, ppo01sctr, roc125sctr, 
    sctr, histDay, chg_pct_1d, chg_pct_5d, chg_pct_10d, chg_pct_20d, chg_pct_50d, chg_pct_100d, 
    sma10turnover, sma20turnover, sma50turnover, 
    above_200d_sma ,above_150d_sma ,above_100d_sma ,above_50d_sma  ,above_20d_sma  ,above_10d_sma  ,above_5d_sma,
    industry, sector, short_name)
    VALUES (${dailyStockStat.symbol}, ${dailyStockStat.dt}, ${dailyStockStat.start_dt}, ${dailyStockStat.open}, 
    ${dailyStockStat.high}, ${dailyStockStat.low}, ${dailyStockStat.close}, ${dailyStockStat.volume}, ${dailyStockStat.prev_open}, 
    ${dailyStockStat.prev_high}, ${dailyStockStat.prev_low}, ${dailyStockStat.prev_close}, ${dailyStockStat.prev_volume}, 
    ${dailyStockStat.roc020}, ${dailyStockStat.roc125}, ${dailyStockStat.rsi014}, ${dailyStockStat.sma200}, ${dailyStockStat.sma150}, 
    ${dailyStockStat.sma100}, ${dailyStockStat.sma050}, ${dailyStockStat.sma020}, ${dailyStockStat.sma010}, ${dailyStockStat.sma005}, 
    ${dailyStockStat.sma003}, ${dailyStockStat.ema050}, ${dailyStockStat.ema200}, ${dailyStockStat.ema200pref}, ${dailyStockStat.sma200pref}, 
    ${dailyStockStat.ema500pref}, ${dailyStockStat.sma50pref}, ${dailyStockStat.rsi14sctr}, ${dailyStockStat.ppo01sctr}, 
    ${dailyStockStat.roc125sctr}, ${dailyStockStat.sctr}, ${dailyStockStat.histDay}, ${dailyStockStat.chg_pct_1d}, 
    ${dailyStockStat.chg_pct_5d}, ${dailyStockStat.chg_pct_10d}, ${dailyStockStat.chg_pct_20d}, ${dailyStockStat.chg_pct_50d}, 
    ${dailyStockStat.chg_pct_100d}, ${dailyStockStat.sma10turnover}, ${dailyStockStat.sma20turnover}, ${dailyStockStat.sma50turnover}, 
    ${dailyStockStat.above_200d_sma}, ${dailyStockStat.above_150d_sma}, ${dailyStockStat.above_100d_sma}, ${dailyStockStat.above_50d_sma}, 
    ${dailyStockStat.above_20d_sma}, ${dailyStockStat.above_10d_sma}, ${dailyStockStat.above_5d_sma}, ${dailyStockStat.industry}, 
    ${dailyStockStat.sector}, ${dailyStockStat.short_name})
    RETURNING *;
  `;

  return result[0];
}

async function insertMarketStats(marketStat) {
  const result = await sql`
    INSERT INTO daily_market_stats (dt, up4pct1d, dn4pct1d, up25pctin100d, dn25pctin100d, up25pctin20d, dn25pctin20d, up50pctin20d, dn50pctin20d, noofstocks, above200smapct, above150smapct, above20smapct)
    VALUES (${marketStat.dt}, ${marketStat.up4pct1d}, ${marketStat.dn4pct1d}, ${marketStat.up25pctin100d}, ${marketStat.dn25pctin100d}, ${marketStat.up25pctin20d}, ${marketStat.dn25pctin20d}, ${marketStat.up50pctin20d}, ${marketStat.dn50pctin20d}, ${marketStat.noofstocks}, ${marketStat.above200smapct}, ${marketStat.above150smapct}, ${marketStat.above20smapct})
    RETURNING *;
  `;

  return result[0];
}

module.exports = {
    getAivenPgVersion,
    updateMarketStats,
    updateDailyStockStats
};
