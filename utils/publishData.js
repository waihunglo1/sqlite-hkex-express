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

    // market stats
    const sqlMarketStats =
        `select dt, up4pct1d, dn4pct1d, up25pctin100d, dn25pctin100d, up25pctin20d, dn25pctin20d, up50pctin20d, dn50pctin20d, 
         noofstocks, above200smapct, above150smapct, above50smapct, above20smapct, hsi, hsce 
         from daily_market_stats order by dt desc`;
    const marketStats = sqliteDb.prepare(sqlMarketStats).all();
    await avienDbHelper.updateMarketStats(marketStats);

    // sector status
    const sqlSectorStats =
        `select *
        from DAILY_SECTORS_STATS`;
    const sectorStats = sqliteDb.prepare(sqlSectorStats).all();
    await avienDbHelper.updateSectorStats(sectorStats);

    console.log("Aiven market stats updated. Total records: " + marketStats.length + ", sector status: " + sectorStats.length);
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
        fillHistoricalNormalizeRS(dailyStat);
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

function fillHistoricalNormalizeRS(dailyStat) {
    // long term indicator weighting
    const sqlRS = `select normalise_rs from DAILY_STOCK_STATS where symbol = ? order by dt desc limit 20`;
    const rs = sqliteDb.prepare(sqlRS).all(dailyStat.symbol);

    if (rs.length < 20) {
        console.log("Not enough rs data for " + dailyStat.symbol + ". Only " + rs.length + " records found.");
        // Fill with zeros if not enough data
        for (let i = rs.length; i < 20; i++) {
            rs.push({ rs: 0 });
        }
    }

    dailyStat.normalise_rs1 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs2 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs3 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs4 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs5 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs6 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs7 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs8 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs9 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs10 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs11 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs12 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs13 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs14 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs15 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs16 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs17 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs18 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs19 = rs.shift().normalise_rs || 0;
    dailyStat.normalise_rs20 = rs.shift().normalise_rs || 0;
}

