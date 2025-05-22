const fs = require("fs");
const util = require("util");
const { parse } = require("csv-parse");
const taIndicator = require('@debut/indicators');
const { createTrend } = require('trendline');
const helper = require("./helper");
const db = require('better-sqlite3')('data/hkex-market-breadth.db', {});
db.pragma('journal_mode = WAL');

const queryDate = '20250513';
const querySymbol = 'PLTR.US';// = '9992.HK';

// format sql
var sqlStr = 'SELECT symbol FROM DAILY_STOCK_PRICE where dt = ?';
if(! helper.isEmpty(querySymbol)) {
    sqlStr = sqlStr + ' and symbol = ?';
}

// query db
const stmt = db.prepare(sqlStr);
const symbols = stmt.all(queryDate, querySymbol);
for (const symbol of symbols) {
    calculateStatistics(symbol, queryDate);
}

async function calculateStatistics(stockPrice, queryDate) {
    const stmt = db.prepare('SELECT * FROM DAILY_STOCK_PRICE where symbol = ? and dt <= ? order by dt desc limit 200');
    const priceHistory = stmt.all(stockPrice.symbol, queryDate);

    var lastQuote = null;
    var roc020Ind = new taIndicator.ROC(20);
    var roc125Ind = new taIndicator.ROC(125);
    var rsi014Ind = new taIndicator.RSI(14);
    var sma200Ind = new taIndicator.SMA(200);
    var sma150Ind = new taIndicator.SMA(150);
    var sma100Ind = new taIndicator.SMA(100);
    var sma050Ind = new taIndicator.SMA(50);
    var sma020Ind = new taIndicator.SMA(20);
    var sma010Ind = new taIndicator.SMA(10);
    var sma005Ind = new taIndicator.SMA(5);
    var sma003Ind = new taIndicator.SMA(3);
    var ema050Ind = new taIndicator.EMA(50);
    var ema200Ind = new taIndicator.EMA(200);
    var macd01Ind = new taIndicator.MACD(12, 26, 9);

    var priceStats = 
    {
        symbol: stockPrice.symbol,
        dt: queryDate,
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
        sma200sctr: 0,
        roc125sctr: 0,
        sma50sctr: 0,
        roc20sctr: 0,
        rsi14sctr: 0,
        ppo01Sctr: 0,
        sctr: 0,
        start_dt: priceHistory[priceHistory.length - 1].dt,
        histDay: priceHistory.length
    }

    priceHistory.reverse().forEach((history, idx) => {
        if (priceHistory.length >= 20) {
            priceStats.roc20 = roc020Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 125) {
            priceStats.roc125 = roc125Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 14) {
            priceStats.rsi14 = rsi014Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 3) {
            priceStats.sma03 = sma003Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 5) {
            priceStats.sma05 = sma005Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 10) {
            priceStats.sma10 = sma010Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 20) {
            priceStats.sma20 = sma020Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 50) {
            priceStats.sma50 = sma050Ind.nextValue(history.close);
            priceStats.ema50 = ema050Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 100) {
            priceStats.sma100 = sma100Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 150) {
            priceStats.sma150 = sma150Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 200) {
            priceStats.sma200 = sma200Ind.nextValue(history.close);
            priceStats.ema200 = ema200Ind.nextValue(history.close);
        }

        if (priceHistory.length >= 26) {
            // save last 2 days macd
            priceStats.macd03 = priceStats.macd02;
            priceStats.macd02 = priceStats.macd01;
            priceStats.macd01 = macd01Ind.nextValue(history.close);
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

        lastQuote = history;
    });

    sctr(priceStats);

    if(priceStats.sctr >= 75) {
        console.log(stockPrice.symbol + " price history length: " + priceHistory.length + " sctr: " + priceStats.sctr + " dt: " + priceStats.dt);
    }
    
}

function sctr(priceStats) {
    // long term indicator weighting
    var sma200Score = 0;
    var ema200Score = 0;
    if(priceStats.sma200 != 0) {
        sma200Score = (priceStats.close - priceStats.sma200) / priceStats.sma200 * 100 * 0.3;
        ema200Score = (priceStats.close - priceStats.ema200) / priceStats.ema200 * 100;
    }
    var roc125Score = 0;
    if(!isNaN(priceStats.roc125)) {
        // roc125Score = priceStats.roc125 > 100 ? 30 : priceStats.roc125 * 0.3;
        roc125Score = priceStats.roc125 * 0.3;
    } 

    // medium term indicator weighting
    var sma050Score = 0;
    var ema050Score = 0;
    if(priceStats.sma50 != 0) {
        sma050Score = (priceStats.close - priceStats.sma50) / priceStats.sma50 * 100 * 0.15;
        ema050Score = (priceStats.close - priceStats.ema50) / priceStats.ema50 * 100;
    }
    var roc020Score = 0;
    if(!isNaN(priceStats.roc20)) {
        roc020Score = priceStats.roc20 * 0.15;
    } 

    // short term indicator weighting
    var rsi14Score = 0;
    if(! isNaN(priceStats.rsi14)) {
        rsi14Score = priceStats.rsi14 * 0.05;
    }
    priceStats.ppo01Sctr = calculatePPO01(priceStats);

    // sum up values
    priceStats.sma200sctr = sma200Score;
    priceStats.roc125sctr = roc125Score;
    priceStats.sma50sctr = sma050Score;
    priceStats.roc20sctr = roc020Score;
    priceStats.rsi14sctr = rsi14Score;
    priceStats.sctr = ema200Score * 0.3 + roc125Score + ema050Score * 0.15 + roc020Score + priceStats.ppo01Sctr * 0.05 + rsi14Score;

    var SCTR = (0.60 * ((ema200Score + priceStats.roc125) / 2) + 0.30 * ((ema050Score + priceStats.roc20) / 2) + 0.10 * ((priceStats.ppo01Sctr + priceStats.rsi14 - 50) / 2));
    var SCTR01 = 50 + 2.5 * SCTR;
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