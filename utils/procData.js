const taIndicator = require('@debut/indicators');
const { createTrend } = require('trendline');
const helper = require("./helper");
const config = require('config');
const VolumeProfile = require('technicalindicators').VolumeProfile;

const sqliteDb = require('better-sqlite3')(config.db.sqlite.file, {});
sqliteDb.pragma('journal_mode = WAL');

const queryDate = ''; //20250516
const querySymbol = '';// = '9992.HK';

/**
 * Main entry point for processing data
 * This function initializes the process by checking if a specific date or symbol is provided.
 */
processDataLocal();

/**
 * Main function to process data
 */
function processDataLocal() {
    // process data by dates
    console.log("Start processing data. file path: " + config.db.sqlite.file);
    helper.isEmpty(queryDate) ? sqliteProcessMultipleDates() : sqliteProcessSingleDate(queryDate, querySymbol);
    sqliteLocalUpdateMarketStats();
    sqliteLocalUpdateSectorsStats();
    console.log("Completed processing data. file path: " + config.db.sqlite.file);
}


/**
 * Process all dates in the database
 */
function sqliteProcessMultipleDates() {
    // query db
    const sqlDateStr = `
        select dt from ( 
            SELECT dt FROM DAILY_STOCK_PRICE 
            group by dt 
            order by dt desc 
            limit 200 
        ) 
        except 
        select dt from daily_stock_stats group by dt`;

    const dateStmt = sqliteDb.prepare(sqlDateStr);
    const dates = dateStmt.all();
    if (dates.length > 0) { 
        dates.forEach((date) => {
            var count = sqliteProcessSingleDate(date.dt);
            console.log("[INFO] " + date.dt + " processed. count: " + count);
        });
    }
}

/**
 * Process a single date
 * @param {*} queryDate 
 * @param {*} querySymbol 
 */
function sqliteProcessSingleDate(queryDate, querySymbol) {
    // format sql
    var count = 0;
    var sqlSymbolByDateStr = `
      SELECT DAILY_STOCK_PRICE.symbol FROM DAILY_STOCK_PRICE, STOCK 
      WHERE DAILY_STOCK_PRICE.dt = ? 
      AND DAILY_STOCK_PRICE.symbol = stock.symbol`;
      
    if (!helper.isEmpty(querySymbol)) {
        sqlSymbolByDateStr = sqlSymbolByDateStr + ' and DAILY_STOCK_PRICE.symbol = ?';
    }

    // query db
    const stmt = sqliteDb.prepare(sqlSymbolByDateStr);
    const symbols = helper.isEmpty(querySymbol) ? stmt.all(queryDate) : stmt.all(queryDate, querySymbol);

    for (const symbol of symbols) {
        var priceStats = calculateStatistics(symbol, queryDate);
        insertPriceStats(priceStats);
        count++;
    }

    return count;
}

/**
 * update market stats
 */
function sqliteLocalUpdateMarketStats() {
    const sqlMarketStats = 
    `select DAILY_STOCK_STATS.dt, 
        sum(case when chg_pct_1d >= 4 then 1 else 0 end)  up4pct1d, 
        sum(case when chg_pct_1d<= -4 then 1 else 0 end)  dn4pct1d, 
        sum(case when chg_pct_100d >= 25 then 1 else 0 end)  up25pctin100d, 
        sum(case when chg_pct_100d<= -25 then 1 else 0 end)  dn25pctin100d, 
        sum(case when chg_pct_20d >= 25 then 1 else 0 end)  up25pctin20d, 
        sum(case when chg_pct_20d<= -25 then 1 else 0 end)  dn25pctin20d, 
        sum(case when chg_pct_20d >= 50 then 1 else 0 end)  up50pctin20d, 
        sum(case when chg_pct_20d<= -50 then 1 else 0 end)  dn50pctin20d, 
        count(1) noofstocks, 
        round(sum(above_200d_sma) / count(1) * 100,2) above200smapct, 
        round(sum(above_150d_sma) / count(1) * 100,2) above150smapct, 
        round(sum(above_50d_sma) / count(1) * 100,2) above50smapct, 
        round(sum(above_20d_sma) / count(1) * 100,2) above20smapct,
        hsi, hsce 
        from DAILY_STOCK_STATS 
        left outer join 
        (
            select dt, close as hsi from daily_stock_price
            where symbol = '^HSI'
        ) HSI on DAILY_STOCK_STATS.dt = HSI.dt
        left outer join 
        (
            select dt, close as hsce from daily_stock_price
            where symbol = '^HSCE'
        ) HSCE on DAILY_STOCK_STATS.dt = HSCE.dt        
        group by DAILY_STOCK_STATS.dt 
        order by DAILY_STOCK_STATS.dt desc`;

    const stmt = sqliteDb.prepare(sqlMarketStats);
    const marketStats = stmt.all();
    
    const INSERT_SQL = 
      `REPLACE INTO DAILY_MARKET_STATS 
      (dt, up4pct1d, dn4pct1d, up25pctin100d, dn25pctin100d, up25pctin20d, dn25pctin20d, 
      up50pctin20d, dn50pctin20d, noofstocks, above200smapct, above150smapct, above50smapct, above20smapct, hsi, hsce) 
      VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    const stmtInsert = sqliteDb.prepare(INSERT_SQL);
    marketStats.forEach((marketStat) => {
        const info = stmtInsert.run(marketStat.dt, marketStat.up4pct1d, marketStat.dn4pct1d, marketStat.up25pctin100d, marketStat.dn25pctin100d, 
            marketStat.up25pctin20d, marketStat.dn25pctin20d, marketStat.up50pctin20d, marketStat.dn50pctin20d, marketStat.noofstocks, 
            marketStat.above200smapct, marketStat.above150smapct, marketStat.above50smapct, marketStat.above20smapct, marketStat.hsi, marketStat.hsce);
        if (info.changes <= 0) {
            console.log("[ERROR] Inserted " + marketStat.dt);
        }
    });

    console.log("Market stats updated. Total records: " + marketStats.length);
}

/**
 * update market stats
 */
function sqliteLocalUpdateSectorsStats() {
    const sqlSectorStats =
        `select 
        dt,
        case sector
        when 'Basic Materials' then 'XLB'
        when 'Communication Services' then 'XLC'
        when 'Consumer Cyclical' then 'XLY'
        when 'Consumer Defensive' then 'XLP'
        when 'Energy' then 'XLE'
        when 'Financial Services' then 'XLF'
        when 'Healthcare' then 'XLV'
        when 'Industrials' then 'XLI'
        when 'Real Estate' then 'XLRE'
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
        ) order by dt, sector
        `;

    const stmt = sqliteDb.prepare(sqlSectorStats);
    const sectorStats = stmt.all();
    
    var stats = newSectorStatus();
    var lastStats = newSectorStatus();
    var lastdt = '';
    var count = 0;

    sectorStats.forEach((sectorStat) => {
        if(lastdt != '' && lastdt != sectorStat.dt) {
            newSectorStatusToDb(lastStats);
            stats = newSectorStatus();
            count++;
        }

        stats.dt = sectorStat.dt;

        switch(sectorStat.sector) {
            case 'XLB':
                stats.XLB_U4SM = sectorStat.u4sm;
                stats.XLB_D4SM = sectorStat.d4sm;
                stats.XLB_SM = sectorStat.sm;
                break;
            case 'XLC':
                stats.XLC_U4SM = sectorStat.u4sm;
                stats.XLC_D4SM = sectorStat.d4sm;
                stats.XLC_SM = sectorStat.sm;
                break;
            case 'XLY':
                stats.XLY_U4SM = sectorStat.u4sm;
                stats.XLY_D4SM = sectorStat.d4sm;
                stats.XLY_SM = sectorStat.sm;
                break;
            case 'XLP':
                stats.XLP_U4SM = sectorStat.u4sm;
                stats.XLP_D4SM = sectorStat.d4sm;
                stats.XLP_SM = sectorStat.sm;
                break;
            case 'XLE':
                stats.XLE_U4SM = sectorStat.u4sm;
                stats.XLE_D4SM = sectorStat.d4sm;
                stats.XLE_SM = sectorStat.sm;
                break;
            case 'XLF':
                stats.XLF_U4SM = sectorStat.u4sm;
                stats.XLF_D4SM = sectorStat.d4sm;
                stats.XLF_SM = sectorStat.sm;
                break;
            case 'XLV':
                stats.XLV_U4SM = sectorStat.u4sm;
                stats.XLV_D4SM = sectorStat.d4sm;
                stats.XLV_SM = sectorStat.sm;
                break;
            case 'XLI':
                stats.XLI_U4SM = sectorStat.u4sm;
                stats.XLI_D4SM = sectorStat.d4sm;
                stats.XLI_SM = sectorStat.sm;
                break;
            case 'XLRE':
                stats.XLRE_U4SM = sectorStat.u4sm;
                stats.XLRE_D4SM = sectorStat.d4sm;
                stats.XLRE_SM = sectorStat.sm;
                break;
            case 'XLK':
                stats.XLK_U4SM = sectorStat.u4sm;
                stats.XLK_D4SM = sectorStat.d4sm;
                stats.XLK_SM = sectorStat.sm;
                break;
            case 'XLU':
                stats.XLU_U4SM = sectorStat.u4sm;
                stats.XLU_D4SM = sectorStat.d4sm;
                stats.XLU_SM = sectorStat.sm;
                break;
            case 'XLX':
                stats.XLX_U4SM = sectorStat.u4sm;
                stats.XLX_D4SM = sectorStat.d4sm;
                stats.XLX_SM = sectorStat.sm;
                break;
            default:
                console.log("[ERROR] Unknown sector: " + sectorStat.sector + " on " + sectorStat.dt);
                break;
        }

        lastdt = sectorStat.dt;
        lastStats = stats;  
    });

    newSectorStatusToDb(lastStats);
    console.log("Market stats updated. Total records: " + count);
}

function newSectorStatusToDb(stats) {
    const INSERT_SQL = 
      `REPLACE INTO DAILY_SECTORS_STATS 
      (dt, 
      XLB_U4SM,  XLB_D4SM,  XLB_SM, 
      XLC_U4SM,  XLC_D4SM,  XLC_SM, 
      XLY_U4SM,  XLY_D4SM,  XLY_SM, 
      XLP_U4SM,  XLP_D4SM,  XLP_SM, 
      XLE_U4SM,  XLE_D4SM,  XLE_SM, 
      XLF_U4SM,  XLF_D4SM,  XLF_SM, 
      XLV_U4SM,  XLV_D4SM,  XLV_SM, 
      XLI_U4SM,  XLI_D4SM,  XLI_SM, 
      XLRE_U4SM, XLRE_D4SM, XLRE_SM, 
      XLK_U4SM,  XLK_D4SM,  XLK_SM, 
      XLU_U4SM,  XLU_D4SM,  XLU_SM, 
      XLX_U4SM,  XLX_D4SM,  XLX_SM       
      ) 
      VALUES (?,
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?                              
      )`

    const stmtInsert = sqliteDb.prepare(INSERT_SQL);
    const info = stmtInsert.run(stats.dt,
        stats.XLB_U4SM, stats.XLB_D4SM, stats.XLB_SM,
        stats.XLC_U4SM, stats.XLC_D4SM, stats.XLC_SM,
        stats.XLY_U4SM, stats.XLY_D4SM, stats.XLY_SM,
        stats.XLP_U4SM, stats.XLP_D4SM, stats.XLP_SM,
        stats.XLE_U4SM, stats.XLE_D4SM, stats.XLE_SM,
        stats.XLF_U4SM, stats.XLF_D4SM, stats.XLF_SM,
        stats.XLV_U4SM, stats.XLV_D4SM, stats.XLV_SM,
        stats.XLI_U4SM, stats.XLI_D4SM, stats.XLI_SM,
        stats.XLRE_U4SM, stats.XLRE_D4SM, stats.XLRE_SM,
        stats.XLK_U4SM, stats.XLK_D4SM, stats.XLK_SM,
        stats.XLU_U4SM, stats.XLU_D4SM, stats.XLU_SM,
        stats.XLX_U4SM, stats.XLX_D4SM, stats.XLX_SM
    );
    if (info.changes <= 0) {
        console.log("[ERROR] Inserted " + marketStat.dt);
    }   
}

function newSectorStatus() {
    var sectorStat = {
        dt: '',
        XLB_U4SM: 0, XLB_D4SM: 0, XLB_SM: 0,
        XLC_U4SM: 0, XLC_D4SM: 0, XLC_SM: 0,
        XLY_U4SM: 0, XLY_D4SM: 0, XLY_SM: 0,
        XLP_U4SM: 0, XLP_D4SM: 0, XLP_SM: 0,
        XLE_U4SM: 0, XLE_D4SM: 0, XLE_SM: 0,
        XLF_U4SM: 0, XLF_D4SM: 0, XLF_SM: 0,
        XLV_U4SM: 0, XLV_D4SM: 0, XLV_SM: 0,
        XLI_U4SM: 0, XLI_D4SM: 0, XLI_SM: 0,
        XLRE_U4SM: 0, XLRE_D4SM: 0, XLRE_SM: 0,
        XLK_U4SM: 0, XLK_D4SM: 0, XLK_SM: 0,
        XLU_U4SM: 0, XLU_D4SM: 0, XLU_SM: 0,
        XLX_U4SM: 0, XLX_D4SM: 0, XLX_SM: 0
    };

    return sectorStat;
}

/**
 * Calculate statistics for a single stock price on a specific date
 * @param {*} stockPrice 
 * @param {*} queryDate 
 * @returns 
 */
function calculateStatistics(stockPrice, queryDate) {
    const stmt = sqliteDb.prepare('SELECT * FROM DAILY_STOCK_PRICE where symbol = ? and dt <= ? order by dt desc limit 200');
    const priceHistory = stmt.all(stockPrice.symbol, queryDate);

    // initialize technical indicator calculators
    var calculators = {
        roc020Ind: new taIndicator.ROC(20),
        roc125Ind: new taIndicator.ROC(125),
        rsi014Ind: new taIndicator.RSI(14),
        sma200Ind: new taIndicator.SMA(200),
        sma150Ind: new taIndicator.SMA(150),
        sma100Ind: new taIndicator.SMA(100),
        sma050Ind: new taIndicator.SMA(50),
        sma020Ind: new taIndicator.SMA(20),
        sma010Ind: new taIndicator.SMA(10),
        sma005Ind: new taIndicator.SMA(5),
        sma003Ind: new taIndicator.SMA(3),
        ema050Ind: new taIndicator.EMA(50),
        ema200Ind: new taIndicator.EMA(200),
        macd01Ind: new taIndicator.MACD(12, 26, 9),
        sma010TurnoverInd: new taIndicator.SMA(10),
        sma020TurnoverInd: new taIndicator.SMA(20),
        sma050TurnoverInd: new taIndicator.SMA(50)
    };

    var priceStats = 
    {
        symbol: stockPrice.symbol,
        dt: queryDate,
        start_dt: priceHistory[priceHistory.length - 1].dt,        
        open: 0,
        high: 0,
        low: 0,
        close: 0,
        volume: 0,
        prev_open: 0,
        prev_high: 0,
        prev_low: 0,
        prev_close: 0,
        prev_volume: 0,
        roc20 : 0,
        roc125 : 0,
        rsi14 : 0,
        sma200 : 0,
        sma150: 0,
        sma100: 0,
        sma50: 0,
        sma20: 0,
        sma10: 0,
        sma05: 0,
        sma03: 0,
        ema200: 0,
        ema50: 0,
        macd01: 0,
        macd02: 0,
        macd03: 0,
        ema200pref: 0,
        sma200pref: 0,
        ema50pref: 0,
        sma50pref: 0,
        rsi14sctr: 0,
        ppo01sctr: 0,
        roc125sctr: 0,
        roc20sctr: 0,   
        sctr: 0,
        histDay: priceHistory.length,
        chg_pct_1d: 0,
        chg_pct_5d: 0,
        chg_pct_10d: 0,
        chg_pct_20d: 0,
        chg_pct_50d: 0,
        chg_pct_100d: 0,
        sma10turnover: 0,
        sma20turnover: 0,
        sma50turnover: 0,
        above_200d_sma: 0,
        above_150d_sma: 0,
        above_100d_sma: 0,
        above_50d_sma: 0,
        above_20d_sma: 0,
        above_10d_sma: 0,
        above_5d_sma: 0,
        vp_high: 0,
        vp_low: 0,
        vp_bullish: 0,
        vp_bearish: 0,
    }

    // calculate technical indicators
    calculateTechnicalIndicator(priceHistory, priceStats, calculators);
    calculateSctr(priceStats);
    calculateVolumeProfile(priceHistory, priceStats);

    // if(priceStats.sctr >= 75) {
    //    console.log(stockPrice.symbol + " price history length: " + priceHistory.length + " sctr: " + priceStats.sctr + " dt: " + priceStats.dt);
    // }
    return priceStats;
}

/**
 * calculate Volume Profile
 */
function calculateVolumeProfile(priceHistory, priceStats) {
    var input = { high: [], low: [], open: [], close: [], volume: [] , noOfBars : 100};
    var vpCount = 0;
    priceHistory.map(priceHist => {
        if (vpCount++ < 150) {
            input.high.push(priceHist.high);
            input.low.push(priceHist.low);
            input.close.push(priceHist.close);
            input.open.push(priceHist.open);
            input.volume.push(priceHist.volume);
        }
    });

    let volumeprofile = VolumeProfile.calculate(input);
    let volSortVolProfile = volumeprofile.sort((a, b) => b.totalVolume - a.totalVolume);

    if(volSortVolProfile.length > 0) {
        let vp = volSortVolProfile[0];
        priceStats.vp_high = vp.rangeEnd;
        priceStats.vp_low = vp.rangeStart;
        priceStats.vp_bullish = vp.bullishVolume;
        priceStats.vp_bearish = vp.bearishVolume;
    }
}   

/**
 * Calculate technical indicators for a stock price
 * @param {*} priceHistory 
 * @param {*} priceStats 
 * @param {*} calculators 
 */
function calculateTechnicalIndicator(priceHistory, priceStats, calculators) {
    var lastQuote = null;
    var results = [];

    // calculate technical indicators
    priceHistory.reverse().forEach((history, idx) => {
        if (priceHistory.length >= 20) {
            priceStats.roc20 = calculators.roc020Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 125) {
            priceStats.roc125 = calculators.roc125Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 14) {
            priceStats.rsi14 = calculators.rsi014Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 3) {
            priceStats.sma03 = calculators.sma003Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 5) {
            priceStats.sma05 = calculators.sma005Ind.nextValue(history.close);
            priceStats.close >= priceStats.sma05 ? priceStats.above_5d_sma = 1 : priceStats.above_5d_sma = 0;
        }

        if (priceHistory.length >= 10) {
            priceStats.sma10 = calculators.sma010Ind.nextValue(history.close);
            priceStats.sma10turnover = calculators.sma010TurnoverInd.nextValue(history.close * history.volume);
        }

        if (priceHistory.length >= 20) {
            priceStats.sma20 = calculators.sma020Ind.nextValue(history.close);
            priceStats.sma20turnover = calculators.sma020TurnoverInd.nextValue(history.close * history.volume);
        }

        if (priceHistory.length >= 50) {
            priceStats.sma50 = calculators.sma050Ind.nextValue(history.close);
            priceStats.ema50 = calculators.ema050Ind.nextValue(history.close);
            priceStats.sma50turnover = calculators.sma050TurnoverInd.nextValue(history.close * history.volume);
        }

        if (priceHistory.length >= 100) {
            priceStats.sma100 = calculators.sma100Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 150) {
            priceStats.sma150 = calculators.sma150Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 200) {
            priceStats.sma200 = calculators.sma200Ind.nextValue(history.close);
            priceStats.ema200 = calculators.ema200Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 26) {
            // save last 2 days macd
            priceStats.macd03 = priceStats.macd02;
            priceStats.macd02 = priceStats.macd01;
            priceStats.macd01 = calculators.macd01Ind.nextValue(history.close);
        }

        priceStats.open = history.open;
        priceStats.high = history.high;
        priceStats.low = history.high;
        priceStats.close = history.close;
        priceStats.volume = history.volume;

        if (!helper.isEmpty(lastQuote)) {
            priceStats.prev_open = lastQuote.open;
            priceStats.prev_high = lastQuote.high;
            priceStats.prev_low = lastQuote.low;
            priceStats.prev_close = lastQuote.close;
            priceStats.prev_volume = lastQuote.volume;
        }

        if(! helper.isEmpty(lastQuote)) {
            priceStats.chg_pct_1d = (priceStats.close - priceStats.prev_close) / priceStats.prev_close * 100;
        }

        lastQuote = history;
    });

    // above? sma
    priceStats.close >= priceStats.sma10 ? priceStats.above_10d_sma = 1 : priceStats.above_10d_sma = 0;
    priceStats.close >= priceStats.sma20 ? priceStats.above_20d_sma = 1 : priceStats.above_20d_sma = 0;
    priceStats.close >= priceStats.sma50 ? priceStats.above_50d_sma = 1 : priceStats.above_50d_sma = 0;
    priceStats.close >= priceStats.sma100 ? priceStats.above_100d_sma = 1 : priceStats.above_100d_sma = 0;
    priceStats.close >= priceStats.sma150 ? priceStats.above_150d_sma = 1 : priceStats.above_150d_sma = 0;
    priceStats.close >= priceStats.sma200 ? priceStats.above_200d_sma = 1 : priceStats.above_200d_sma = 0;

    // calculate chg_pct
    calculateChgPct(priceHistory, priceStats);
}

/**
 * Calculate the percentage change for a stock price over different time periods
 * @param {*} priceHistory 
 * @param {*} priceStats 
 */
function calculateChgPct(priceHistory, priceStats) {
    priceHistory.reverse();

    if(priceHistory.length >= 5) { 
        priceStats.chg_pct_5d = (priceStats.close - priceHistory[4].close) / priceHistory[4].close * 100;
    }

    if(priceHistory.length >= 10) {
        priceStats.chg_pct_10d = (priceStats.close - priceHistory[9].close) / priceHistory[9].close * 100;
    }   

    if(priceHistory.length >= 20) {
        priceStats.chg_pct_20d = (priceStats.close - priceHistory[19].close) / priceHistory[19].close * 100;
    }   

    if(priceHistory.length >= 50) {
        priceStats.chg_pct_50d = (priceStats.close - priceHistory[49].close) / priceHistory[49].close * 100;
    }

    if(priceHistory.length >= 100) {
        priceStats.chg_pct_100d = (priceStats.close - priceHistory[99].close) / priceHistory[99].close * 100;
    }
}

/**
 * Calculate the short-term trend for a stock price
 * @param {*} priceStats 
 */
function calculateSctr(priceStats) {
    // long term indicator weighting
    if(priceStats.sma200 != 0) {
        priceStats.sma200pref = (priceStats.close - priceStats.sma200) / priceStats.sma200 * 100;
        priceStats.ema200pref = (priceStats.close - priceStats.ema200) / priceStats.ema200 * 100;
    }

    if(!isNaN(priceStats.roc125)) {
        priceStats.roc125sctr = priceStats.roc125;
    } 

    // medium term indicator weighting
    if(priceStats.sma50 != 0) {
        priceStats.sma50pref = (priceStats.close - priceStats.sma50) / priceStats.sma50 * 100;
        priceStats.ema50pref = (priceStats.close - priceStats.ema50) / priceStats.ema50 * 100;
    }

    if(!isNaN(priceStats.roc20)) {
        priceStats.roc20sctr = priceStats.roc20;
    } 

    // short term indicator weighting
    if(! isNaN(priceStats.rsi14)) {
        priceStats.rsi14sctr = priceStats.rsi14;
    }
    priceStats.ppo01sctr = calculatePPO01(priceStats);

    // sum up values
    priceStats.sctr = (0.60 * (priceStats.ema200pref + priceStats.roc125sctr) + 0.30 * (priceStats.ema50pref + priceStats.roc20sctr) + 0.10 * (priceStats.ppo01sctr + priceStats.rsi14sctr));
}

/**
 * Calculate the Percentage Price Oscillator (PPO) for a stock price
 * @param {*} priceStats 
 * @returns 
 */
function calculatePPO01(priceStats) {
    if (helper.isEmpty(priceStats.macd01) || helper.isEmpty(priceStats.macd02) || helper.isEmpty(priceStats.macd03)) {
        console.log("[ERROR] macd01, macd02 or macd03 is empty for " + priceStats.symbol + " on " + priceStats.dt);
        return 0;   
    }

    if (helper.isEmpty(priceStats.macd01.histogram) || helper.isEmpty(priceStats.macd02.histogram) || helper.isEmpty(priceStats.macd03.histogram)) {
        console.log("[ERROR] macd01, macd02 or macd03 is empty for " + priceStats.symbol + " on " + priceStats.dt);
        return 0;
    }

    // trendline for macd histogram
    const data = [
      { x: 1, y: priceStats.macd03.histogram },
      { x: 2, y: priceStats.macd02.histogram },
      { x: 3, y: priceStats.macd01.histogram }
    ];

    const trend = createTrend(data, 'x', 'y');
    const radians = Math.atan2(trend.slope, 1); // Assuming run is 1
    const degrees = radians * (180 / Math.PI);
    var ppo01Score = 0;
    if(degrees >= 45) {
        ppo01Score = 5;
    } else if (degrees <= -45) {
        ppo01Score = 0;
    } else {
        ppo01Score = (trend.slope + 1) * 50
    }

    return ppo01Score;
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
      vp_high, vp_low, vp_bullish, vp_bearish
      )   
      VALUES 
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

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
        priceStats.vp_high, priceStats.vp_low, priceStats.vp_bullish, priceStats.vp_bearish 
    );

    if (info.changes <= 0) {
        console.log("[ERROR] Inserted " + priceStats.symbol + " " + priceStats.dt);
    }

}