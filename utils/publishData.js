const helper = require("./helper");
const config = require('config');
const avienDbHelper = require('./pgDbConnHelper.js');
const sqliteDb = require('better-sqlite3')(config.db.sqlite.file, {});
sqliteDb.pragma('journal_mode = WAL');

populateStatisticsToAvien();

/**
 * Populate statistics into Aiven database
 */
async function populateStatisticsToAvien() {
    // populate statistics Aiven database
    console.log("Start updating Aiven database.");
    await aivenDbUpdate().then(() => {
        aivenDbUpdateForDailyStockStats().then(() => {
            sqliteDb.close();
        }).catch((error) => {
            console.error("Error updating Aiven daily stock stats:", error);
        });
    }).catch((error) => {
        console.error("Error updating Aiven database:", error);
    });
}

async function aivenDbUpdate() {
    var version = await avienDbHelper.getAivenPgVersion();
    console.log("Aiven Version: ", version);

    const sqlMarketStats =
        `select dt, up4pct1d, dn4pct1d, up25pctin100d, dn25pctin100d, up25pctin20d, dn25pctin20d, up50pctin20d, dn50pctin20d, 
         noofstocks, above200smapct, above150smapct, above20smapct, hsi, hsce 
         from daily_market_stats order by dt desc`;
    const marketStats = sqliteDb.prepare(sqlMarketStats).all();
    await avienDbHelper.updateMarketStats(marketStats);

    console.log("Aiven market stats updated. Total records: " + marketStats.length);
}

async function aivenDbUpdateForDailyStockStats() {
    const sqlDailyStockStats = `
        select DAILY_STOCK_STATS.*, STOCK.sector, STOCK.industry, STOCK.name as short_name from DAILY_STOCK_STATS, STOCK
        where DAILY_STOCK_STATS.symbol = STOCK.symbol
        and dt in (
            select dt from DAILY_STOCK_STATS
            group by dt
            order by dt DESC
            limit 1
        )
        order by dt DESC`;

    const dailyStockStats = sqliteDb.prepare(sqlDailyStockStats).all();
    console.log("Aiven daily stock stats: ", dailyStockStats.length);

    for(const dailyStat of dailyStockStats) {
        fillHistoricalSCTR(dailyStat);
    }

    await avienDbHelper.updateDailyStockStats(dailyStockStats);
    console.log("Aiven daily stock stats updated. Total records: " + dailyStockStats.length);
}

function fillHistoricalSCTR(dailyStat) {
    // long term indicator weighting
    const sqlSCTR = `select sctr from DAILY_STOCK_STATS where symbol = ? order by dt desc limit 20`;
    const sctr = sqliteDb.prepare(sqlSCTR).all(dailyStat.symbol);

    if (sctr.length < 20) {
        console.log("Not enough SCTR data for " + dailyStat.symbol + ". Only " + sctr.length + " records found.");
        // Fill with zeros if not enough data
        for (let i = sctr.length; i < 20; i++) {
            sctr.push({ sctr: 0 });
        }
    }

    dailyStat.sctr1 = sctr.shift().sctr || 0;
    dailyStat.sctr2 = sctr.shift().sctr || 0;
    dailyStat.sctr3 = sctr.shift().sctr || 0;
    dailyStat.sctr4 = sctr.shift().sctr || 0;
    dailyStat.sctr5 = sctr.shift().sctr || 0;
    dailyStat.sctr6 = sctr.shift().sctr || 0;
    dailyStat.sctr7 = sctr.shift().sctr || 0;
    dailyStat.sctr8 = sctr.shift().sctr || 0;
    dailyStat.sctr9 = sctr.shift().sctr || 0;
    dailyStat.sctr10 = sctr.shift().sctr || 0;
    dailyStat.sctr11 = sctr.shift().sctr || 0;
    dailyStat.sctr12 = sctr.shift().sctr || 0;
    dailyStat.sctr13 = sctr.shift().sctr || 0;
    dailyStat.sctr14 = sctr.shift().sctr || 0;
    dailyStat.sctr15 = sctr.shift().sctr || 0;
    dailyStat.sctr16 = sctr.shift().sctr || 0;
    dailyStat.sctr17 = sctr.shift().sctr || 0;
    dailyStat.sctr18 = sctr.shift().sctr || 0;
    dailyStat.sctr19 = sctr.shift().sctr || 0;
    dailyStat.sctr20 = sctr.shift().sctr || 0;
}

