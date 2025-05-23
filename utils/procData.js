const fs = require("fs");
const util = require("util");
const { parse } = require("csv-parse");
const taIndicator = require('@debut/indicators');
const { createTrend } = require('trendline');
const helper = require("./helper");
const db = require('better-sqlite3')('data/hkex-market-breadth.db', {});
db.pragma('journal_mode = WAL');

const queryDate = '20250513';
const querySymbol = '';// = '9992.HK';

// format sql
var sqlStr = 'SELECT symbol FROM DAILY_STOCK_PRICE where dt = ?';
if(! helper.isEmpty(querySymbol)) {
    sqlStr = sqlStr + ' and symbol = ?';
}

// query db
const stmt = db.prepare(sqlStr);
const symbols = stmt.all(queryDate);
for (const symbol of symbols) {
    var priceStats = calculateStatistics(symbol, queryDate);
    insertPriceStats(priceStats);
}

function calculateStatistics(stockPrice, queryDate) {
    const stmt = db.prepare('SELECT * FROM DAILY_STOCK_PRICE where symbol = ? and dt <= ? order by dt desc limit 200');
    const priceHistory = stmt.all(stockPrice.symbol, queryDate);

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
        sma50turnover: 0
    }

    // calculate technical indicators
    calculateTechnicalIndicator(priceHistory, priceStats, calculators);
    calculateSctr(priceStats);

    if(priceStats.sctr >= 75) {
        console.log(stockPrice.symbol + " price history length: " + priceHistory.length + " sctr: " + priceStats.sctr + " dt: " + priceStats.dt);
    }
    
    return priceStats;
}

function calculateTechnicalIndicator(priceHistory, priceStats, calculators) {
    var lastQuote = null;

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

    // calculate chg_pct
    calculateChgPct(priceHistory, priceStats);
}

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

function calculatePPO01(priceStats) {
    if (helper.isEmpty(priceStats.macd01.histogram) || helper.isEmpty(priceStats.macd02.histogram) || helper.isEmpty(priceStats.macd03.histogram)) {
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

function insertPriceStats(priceStats) {
    const INSERT_SQL = "REPLACE INTO DAILY_STOCK_STATS (symbol, dt, start_dt, open, high, low, close, volume, prev_open, prev_high, prev_low, prev_close, prev_volume, roc020, roc125, rsi014, sma200, sma150, sma100, sma050, sma020, sma010, sma005, sma003, ema050, ema200, ema200pref, sma200pref, ema500pref, sma50pref, rsi14sctr, ppo01sctr, roc125sctr, histDay, chg_pct_1d, chg_pct_5d, chg_pct_10d, chg_pct_20d, chg_pct_50d, chg_pct_100d, sma10turnover, sma20turnover, sma50turnover ) " + 
                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    const stmt = db.prepare(INSERT_SQL);
    const info = stmt.run(priceStats.symbol, priceStats.dt, priceStats.start_dt,
        priceStats.open, priceStats.high, priceStats.low, priceStats.close, priceStats.volume,
        priceStats.prev_open, priceStats.prev_high, priceStats.prev_low, priceStats.prev_close,
        priceStats.prev_volume, priceStats.roc20, priceStats.roc125, priceStats.rsi14,
        priceStats.sma200, priceStats.sma150, priceStats.sma100, priceStats.sma50,
        priceStats.sma20, priceStats.sma10, priceStats.sma05, priceStats.sma03,
        priceStats.ema50, priceStats.ema200, priceStats.ema200pref, priceStats.sma200pref,
        priceStats.ema200pref, priceStats.sma50pref, priceStats.rsi14sctr, priceStats.ppo01sctr,
        priceStats.roc125sctr, priceStats.histDay, priceStats.chg_pct_1d, priceStats.chg_pct_5d,
        priceStats.chg_pct_10d, priceStats.chg_pct_20d, priceStats.chg_pct_50d, priceStats.chg_pct_100d,
        priceStats.sma10turnover, priceStats.sma20turnover, priceStats.sma50turnover);


    if (info.changes > 0) {
        console.log("Inserted " + priceStats.symbol + " " + priceStats.dt + " successfully.");
    }

}