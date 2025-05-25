const fs = require("fs");
const util = require("util");
const path = require('path');
const { parse } = require("csv-parse");
const config = require('config');
const helper = require("./helper");
const mmutils = require('./mm-utils.js');
const { resourceLimits } = require("worker_threads");
const yahooFinance = require('yahoo-finance2').default; // NOTE the .default
const db = require('better-sqlite3')(config.db.sqlite.file, {});
db.pragma('journal_mode = WAL');

const start = async function(extractionPath) {
  const files = helper.traverseDirectory(extractionPath + config.file.path.load);
  const fileCount = await traverseDir(files);
  console.log("Total files processed: " + fileCount);

  const row01 = db.prepare('SELECT sqlite_version()').get();
  console.log(row01);

  const row02 = db.prepare('SELECT COUNT(1) FROM DAILY_STOCK_PRICE').get();
  console.log(row02);

  db.close();
}

const hkexDownload = async () => {
  await mmutils.queryExcelView().then(async (data) => {

    for (const item of data) {
      const result = await yahooFinance.search(item.code, { region: 'HK' });
      if (result && result.quotes && result.quotes.length > 0) {
        item.name = result.quotes[0].shortName || item.name;

        item.industry = ! helper.isEmpty(result.quotes[0].industry) ? result.quotes[0].industry.toUpperCase() : "UNKNOWN";
        item.sector = ! helper.isEmpty(result.quotes[0].sector) ? result.quotes[0].sector.toUpperCase() : "UNKNOWN";
      } else {
        console.warn("No quote found for symbol: " + item.code);
        item.industry = "UNKNOWN";
        item.sector = "UNKNOWN";
      }
    }

    await insertStockData(data);
    console.log("HKEX data downloaded successfully. Total stocks inserted: " + data.length);
  }).catch((error) => {
    console.error("Error downloading HKEX data:", error);
  });
}

hkexDownload();
// start("C:/Users/user/Downloads/2025-05-25");

/**
 * Insert stock data into the STOCK table.
 */
async function insertStockData(stockData) {
  const insert = db.prepare('REPLACE INTO STOCK (symbol,name,industry,sector) VALUES (@code,@name,@industry,@sector)');
  const insertMany = db.transaction((stocks) => {
    for (const stock of stocks) insert.run(stock);
  });

  insertMany(stockData);
}

/**
 * Parse a CSV file and insert the data into the DAILY_STOCK_PRICE table.
 * @param {*} filePath 
 */
async function parseAndInsertCsvData(filePath) {
  // console.log("Parsing CSV file: " + filePath);
  const insert = db.prepare(
    'REPLACE INTO DAILY_STOCK_PRICE (symbol,period,dt,tm,open,high,low,close,volume,adj_close,open_int) ' +
    'VALUES (@symbol,@period,@dt,@tm,@open,@high,@low,@close,@volume,@adj_close,@open_int)');

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
