const fs = require("fs");
const util = require("util");
const { parse } = require("csv-parse");
const helper = require("./helper");
const db = require('better-sqlite3')('data/hkex-market-breadth.db', {});
db.pragma('journal_mode = WAL');
// create table if not exist
db.exec(`
drop table if exists DAILY_STOCK_PRICE;  
CREATE TABLE IF NOT EXISTS DAILY_STOCK_PRICE
(
  symbol           VARCHAR(10),
  period           VARCHAR(1),
  dt               VARCHAR(10),
  tm               VARCHAR(6),
  open             REAL,
  high             REAL,
  low              REAL,
  close            REAL,
  volume           REAL,
  adj_close        REAL,
  open_int         INTEGER
);

CREATE INDEX IF NOT EXISTS idx_daily_stock_price ON DAILY_STOCK_PRICE (symbol, dt);

CREATE TABLE IF NOT EXISTS DAILY_STOCK_STATS
(
  symbol           VARCHAR(10),
  dt               VARCHAR(10),
  open             REAL,
  high             REAL,
  low              REAL,
  close            REAL,
  volume           REAL,
  adj_close        REAL,
  prev_open        REAL,
  prev_high       REAL,
  prev_low        REAL,
  prev_close      REAL,
  prev_adj_close   REAL,
  prev_volume      REAL,
  rsi14          REAL,  
  sma03          REAL,
  sma05          REAL,  
  sma10          REAL,
  sma20          REAL,      
  sma50          REAL,
  sma100         REAL,
  sma150         REAL,
  sma200         REAL,
  roc125        REAL,
  roc20        REAL,
  ema12         REAL,
  ema26         REAL,
  ema09         REAL,
  ppo01        REAL,
  sctr01        REAL
);

CREATE INDEX IF NOT EXISTS idx_daily_stock_stats ON DAILY_STOCK_STATS (symbol, dt);

`);

const usPath = "C:/Users/user/Downloads/d_us_txt_20250522/data/daily/us";
const hkPath = "C:/Users/user/Downloads/d_hk_txt_20250519/data/daily/hk/hkex stocks";
const files = helper.traverseDirectory(usPath);

const start = async function() {
  const fileCount = await traverseDir(files);
  console.log("Total files processed: " + fileCount);

  const row01 = db.prepare('SELECT sqlite_version()').get();
  console.log(row01);

  const row02 = db.prepare('SELECT COUNT(1) FROM DAILY_STOCK_PRICE').get();
  console.log(row02);

  db.close();
}

start();
// db.close();
// readDataFromCsv();



async function parseAndInsertCsvData(filePath) {
  // console.log("Parsing CSV file: " + filePath);
  const insert = db.prepare('INSERT INTO DAILY_STOCK_PRICE (symbol,period,dt,tm,open,high,low,close,volume,adj_close,open_int) VALUES (@symbol,@period,@dt,@tm,@open,@high,@low,@close,@volume,@adj_close,@open_int)');
  const update = db.prepare('UPDATE DAILY_STOCK_PRICE SET period=@period,dt=@dt,tm=@tm,open=@open,high=@high,low=@low,close=@close,volume=@volume,adj_close=@adj_close,open_int=@open_int WHERE symbol=@symbol');

  const insertMany = db.transaction((stockPrices) => {
    for (const stockPrice of stockPrices) insert.run(stockPrice);
  });

  var rows = [];

  const resolvedPromise = new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(parse({ delimiter: ",", from_line: 2 }))
      .on("data", function (row) {
        var price = {
          symbol: helper.reformatSymbolForHK(row[0]),
          period: row[1],
          dt: row[2],
          tm: row[3],
          open: row[4],
          high: row[5],
          low: row[6],
          close: row[7],
          volume: row[8],
          adj_close: 0,
          open_int: row[9]
        };
        rows.push(price);
      })
      .on("end", function () {
        insertMany(rows);
        resolve(rows.length);
      })
      .on("error", function (error) {
        console.log(error.message);
      });
  });

  await Promise.all([resolvedPromise])
    .then((values) => {
      // console.log("Processed :" + filePath + " / " + values[0] + " rows");
    });
}

async function traverseDir(files) {
  var fileCount = 0;
  for (const file of files) {
    if (file.type === "file") {
      await parseAndInsertCsvData(file.path);
      ++fileCount;
    } else {
      console.log("Directory: " + file.path);
      fileCount += await traverseDir(file.files);
    }
  }

  return fileCount;
}