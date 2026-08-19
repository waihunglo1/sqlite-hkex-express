require('dotenv').config();
const { ANALYST_DATA_INI } = process.env;

const taIndicator = require('@debut/indicators');
const { createTrend } = require('trendline');
const VolumeProfile = require('technicalindicators').VolumeProfile;
const simpleStatistics = require('simple-statistics');
const ini = require('ini');
const fs = require('fs');
const formularjs = require('@formulajs/formulajs');
const dfd = require("danfojs");
const minimist = require('minimist');
const { performance } = require('perf_hooks');

const analystConfig = ini.parse(fs.readFileSync(ANALYST_DATA_INI, 'utf-8'));
const sqliteDb = require('better-sqlite3')(analystConfig.SQLITE.FILE, {});
sqliteDb.pragma('journal_mode = WAL');
const helper = require("./helper.js");
const sqliteHelper = require('./sqliteHelper.js');
const logger = require('./logger')

const queryDate = ''; // = '20260730'
const querySymbol = '' // '2697.HK';
const queryStartDate = ''; // '20260730'
const queryEndDate = ''; // '20260730'

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
    logger.info("Start processing data. file path: " + analystConfig.SQLITE.FILE);

    if(! helper.isEmpty(queryDate) && !helper.isEmpty(querySymbol)) {
        sqliteProcessSingleDate(queryDate, querySymbol);
    } else if (!helper.isEmpty(queryDate)) {
        sqliteProcessSingleDate(queryDate);
    } else if (!helper.isEmpty(queryStartDate) && !helper.isEmpty(queryEndDate)) {
        sqliteProcessMultipleDatesByEndDate(queryStartDate, queryEndDate);
    } else {
        sqliteProcessMultipleDates();
    }

    logger.info("Completed processing data. file path: " + analystConfig.SQLITE.FILE);
}

/**
 * Process all dates in the database
 */
function sqliteProcessMultipleDatesByEndDate(queryStartDate, queryEndDate) {
    // query db
    const sqlDateStr = `
        select dt from ( 
            SELECT dt FROM DAILY_STOCK_PRICE 
            where dt between ? and ?
            group by dt 
            order by dt desc 
        ) 
        except 
        select dt from daily_stock_stats group by dt`;

    const dateStmt = sqliteDb.prepare(sqlDateStr);
    const dates = dateStmt.all(queryStartDate, queryEndDate);
    sqliteProcessDates(dates);
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
    sqliteProcessDates(dates);
}

function sqliteProcessDates(dates) {
    if (dates.length > 0) {
        dates.forEach((date) => {
            logger.info("process " + date.dt + " started.");
            var count = sqliteProcessSingleDate(date.dt);
            logger.info("process " + date.dt + " completed. count: " + count);
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

    sqlSymbolByDateStr = sqlSymbolByDateStr + ' order by DAILY_STOCK_PRICE.symbol asc';

    // query db
    const stmt = sqliteDb.prepare(sqlSymbolByDateStr);
    const symbols = helper.isEmpty(querySymbol) ? stmt.all(queryDate) : stmt.all(queryDate, querySymbol);

    const priceStatsList = [];
    const priceOverSMA20List = [];
    const slopeSMA20List = [];
    const slopeSMA50List = [];
    const slopeSMA150List = [];
    const warningList = [];

    for (const symbol of symbols) {
        var priceStats = calculateStatistics(symbol, queryDate, warningList);
        priceStatsList.push(priceStats);

        // append to list for normalization
        priceOverSMA20List.push(priceStats.rs_priceOverSMA20);
        slopeSMA20List.push(priceStats.rs_slopeSMA20);
        slopeSMA50List.push(priceStats.rs_slopeSMA50);
        slopeSMA150List.push(priceStats.rs_slopeSMA150);
    }

    // warning list
    logger.info("warning list for " + queryDate + " : " + warningList.length);
    let df = new dfd.DataFrame(warningList)
    df.print(); 
    
    if(priceStatsList.length <= 0) {
        return 0;
    }
    
    normalizeRelativeStrength(priceStatsList, priceOverSMA20List, slopeSMA20List, slopeSMA50List, slopeSMA150List);

    const rsOver50 = [];
    priceStatsList.map((priceStats) => {
        if (priceStats.normalise_rs > 50) {
            rsOver50.push(priceStats);
        }
        sqliteHelper.insertPriceStats(priceStats);
    });

    return priceStatsList.length;
}



/**
 * Calculate statistics for a single stock price on a specific date
 * @param {*} stockPrice 
 * @param {*} queryDate 
 * @returns 
 */
function calculateStatistics(stockPrice, queryDate, warningList) {
    const dailyStockPriceStmt = sqliteDb.prepare('SELECT * FROM DAILY_STOCK_PRICE where symbol = ? and dt <= ? order by dt desc limit 200');
    const priceHistory = dailyStockPriceStmt.all(stockPrice.symbol, queryDate);

    const dailyStockStatsStmt = sqliteDb.prepare('SELECT * FROM DAILY_STOCK_STATS where symbol = ? and dt < ? order by dt desc limit 200');
    const priceStatsHistory = dailyStockStatsStmt.all(stockPrice.symbol, queryDate);

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
        rs : 0,
        normalise_rs: 0,
        rs_priceOverSMA20: 0,
        rs_slopeSMA20: 0,
        rs_slopeSMA50: 0,
        rs_slopeSMA150: 0,
    }

    // calculate technical indicators
    calculateTechnicalIndicator(priceHistory, priceStats, calculators);
    calculateSctr(priceStats);
    calculateVolumeProfile(priceHistory, priceStats);

    if(priceStatsHistory.length > 0) {
        calculateRelativeStrength(priceHistory, priceStats, priceStatsHistory, warningList);
    }

    // if(priceStats.sctr >= 75) {
    //    logger.info(stockPrice.symbol + " price history length: " + priceHistory.length + " sctr: " + priceStats.sctr + " dt: " + priceStats.dt);
    // }
    return priceStats;
}

function normalizeRelativeStrength(priceStatsList, priceOverSMA20List, slopeSMA20List, slopeSMA50List, slopeSMA150List) {
    // Load and describe the distribution
    let data =
    {
        'priceOverSMA20List': priceOverSMA20List,
        'slopeSMA20List': slopeSMA20List,
        'slopeSMA50List': slopeSMA50List,
        'slopeSMA150List': slopeSMA150List
    }

    logger.info("Data distribution before normalization:");
    let df = new dfd.DataFrame(data)
    df.describe().print(); 

    const startTime = performance.now();
    const relativeStrengthList = [];
    priceStatsList.map(ps => {
        try {
            ps.rs_priceOverSMA20 = formularjs.PERCENTRANKINC(priceOverSMA20List, ps.rs_priceOverSMA20, 2) * 100;
            ps.rs_slopeSMA20 = formularjs.PERCENTRANKINC(slopeSMA20List, ps.rs_slopeSMA20, 2) * 100;
            ps.rs_slopeSMA50 = formularjs.PERCENTRANKINC(slopeSMA50List, ps.rs_slopeSMA50, 2) * 100;
            ps.rs_slopeSMA150 = formularjs.PERCENTRANKINC(slopeSMA150List, ps.rs_slopeSMA150, 2) * 100;

            ps.normalise_rs = 0.05 * ps.rs_priceOverSMA20 + 
                              0.05 * ps.rs_slopeSMA20 + 
                              0.4 * ps.rs_slopeSMA50 + 
                              0.5 * ps.rs_slopeSMA150;

            relativeStrengthList.push(ps.normalise_rs);
        } catch (error) {
            logger.info("[ERROR STEP 1] " + ps.symbol + " error: " + error);
        }
    });

    priceStatsList.map(ps => {
        try {
            ps.normalise_rs_v2 = formularjs.PERCENTRANKINC(relativeStrengthList, ps.normalise_rs, 2) * 100;
        } catch (error) {
            logger.info("[ERROR STEP 2] " + ps.symbol + " error: " + error);
        }
    });    

    const endTime = performance.now();
    const duration = endTime - startTime;
    data =
    {
        'priceOverSMA20List': priceStatsList.map(item => item.rs_priceOverSMA20),
        'slopeSMA20List': priceStatsList.map(item => item.rs_slopeSMA20),
        'slopeSMA50List': priceStatsList.map(item => item.rs_slopeSMA50),
        'slopeSMA150List': priceStatsList.map(item => item.rs_slopeSMA150),
        'normalise_rs': priceStatsList.map(item => item.normalise_rs),
        'normalise_rs_v2': priceStatsList.map(item => item.normalise_rs_v2)
    }

    logger.info(`Normalize 處理總共花費了 ${duration.toFixed(2)} 毫秒。`);
    df = new dfd.DataFrame(data)
    df.describe().print(); 
}

function calculateRelativeStrength(priceHistory, priceStats, priceStatsHistory, warningList) {
    const slopeSMA20 = calculateSMASlope(priceHistory, priceStats, priceStatsHistory, 20, 'sma020', 'sma20');
    const slopeSMA50 = calculateSMASlope(priceHistory, priceStats, priceStatsHistory, 50, 'sma050', 'sma50');
    const slopeSMA150 = calculateSMASlope(priceHistory, priceStats, priceStatsHistory, 150, 'sma150', 'sma150');
    var priceOverSMA20 = 0;

    if(priceStats.sma20 > 0) {
        priceOverSMA20 = priceStats.close / priceStats.sma20 * 100;
    } else {        
        warningList.push({
            "symbol" : priceStats.symbol,
            "priceOverSMA20": priceOverSMA20.toFixed(2),
            "close": priceStats.close,
            "slopeSMA20": slopeSMA20.toFixed(2),
            "condition": "sma20 is zero"
        });
        priceOverSMA20 = 0;
    }

    if(priceOverSMA20 > 300) {
        warningList.push({
            "symbol" : priceStats.symbol,
            "priceOverSMA20": priceOverSMA20.toFixed(2),
            "close": priceStats.close,
            "slopeSMA20": slopeSMA20.toFixed(2),
            "condition": "priceOverSMA20 is greater than 300"
        });
        priceOverSMA20 = 0;   
    } 

    priceStats.rs_priceOverSMA20 = priceOverSMA20;
    priceStats.rs_slopeSMA20 = slopeSMA20;
    priceStats.rs_slopeSMA50 = slopeSMA50;
    priceStats.rs_slopeSMA150 = slopeSMA150;
}

function calculateSMASlope(priceHistory, priceStats, priceStatsHistory, smaPeriod = 20, targetKey1 = 'sma020', targetKey2 = 'sma20') {

    // first 19 priceStatsHistory
    const sma20Data = priceStatsHistory.slice(0,smaPeriod - 1).map((prcStats, index) => [prcStats.dt, prcStats[targetKey1]]);
    sma20Data.unshift([priceStats.dt, priceStats[targetKey2]]);
    const dataForSlope = sma20Data.reverse().map((data, index) => [index , data[1]]);

    // logger.info("smaData: " + JSON.stringify(sma20Data));
    // logger.info("smaData: " + dataForSlope);

    const slope = simpleStatistics.linearRegression(dataForSlope).m; 

    // logger.info("slope: " + slope);

    return slopeToDegrees(slope);
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
        // logger.info("[ERROR] macd01, macd02 or macd03 is empty for " + priceStats.symbol + " on " + priceStats.dt);
        return 0;   
    }

    if (helper.isEmpty(priceStats.macd01.histogram) || helper.isEmpty(priceStats.macd02.histogram) || helper.isEmpty(priceStats.macd03.histogram)) {
        // logger.info("[ERROR] macd01, macd02 or macd03 is empty for " + priceStats.symbol + " on " + priceStats.dt);
        return 0;
    }

    // trendline for macd histogram
    const data = [
      { x: 1, y: priceStats.macd03.histogram },
      { x: 2, y: priceStats.macd02.histogram },
      { x: 3, y: priceStats.macd01.histogram }
    ];

    const trend = createTrend(data, 'x', 'y');
    const degrees = slopeToDegrees(trend.slope);
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

function slopeToDegrees(slope) {
    const radians = Math.atan(slope, 1);
    return radians * (180 / Math.PI);
}

