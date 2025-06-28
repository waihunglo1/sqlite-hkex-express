const fs = require("fs");
const util = require("util");
const path = require('path');
const { parse } = require("csv-parse");
const config = require('config');
const yahooFinance = require('yahoo-finance2').default; // NOTE the .default
const sqliteDb = require('better-sqlite3')(config.db.sqlite.file, {});
const moment = require('moment');
sqliteDb.pragma('journal_mode = WAL');

// Import helper functions and utilities
const helper = require("./helper");
const mmutils = require('./mm-utils.js');
const scraper = require('./scraper.js'); // Import the traverseDir function


/**
 * Load data from HKEX and process it.
 * This function checks if the extraction path exists, and if not, it unzips the files.
 */
const loadData = async function () {
  const zipFullPath = path.join(config.file.path.extract, helper.todayString());
  if (!fs.existsSync(zipFullPath)) {
    console.info("Extraction path does not exist: " + zipFullPath);
    unzipFiles(zipFullPath);
  } else {
    await scraper.traverseDir().then(() => {
      console.info("Extraction path exists: " + zipFullPath);
      // const stat = fs.statSync(zipFullPath);  
      traverseDirAndInsertData(zipFullPath);
    }).catch((error) => {
      console.error("Error during HKEX data extraction:", error);
    });


  }
}

/**
 * Traverse the directory and insert data into the database.
 * @param {*} zipFullPath 
 * @returns 
 */
const traverseDirAndInsertData = async (zipFullPath) => {
  const pathToLoads = [config.file.path.load.dir1, config.file.path.load.dir2, config.file.path.load.dir3];
  var paths = pathToLoads.map(dir => path.join(zipFullPath, dir));

  for (const fullPath of paths) {
    if (!fs.existsSync(fullPath)) {
      console.error("Directory does not exist: " + fullPath);
      return;
    } else {
      console.log("Directory exists: " + fullPath);
      const files = helper.traverseDirectory(fullPath);
      const fileCount = await traverseDir(files);
      console.log("Total files processed: " + fileCount + " in " + fullPath);
    }
  }

  const hcSql = 
    `SELECT max(dt), min(dt), COUNT(1) as historialPriceCount, count(distinct symbol) as noOfProduct 
    FROM DAILY_STOCK_PRICE`;
  const row02 = sqliteDb.prepare(hcSql).get();
  console.log(row02);
}

/**
 * Download HKEX data and fill it with Yahoo Finance data.
 * This function queries the HKEX data, fills it with additional information from Yahoo Finance,
 */
const hkexDownload = async () => {
  if( !config.hkex.enable) {
    console.log("HKEX data download is disabled in the configuration.");
    return;
  }

  await mmutils.queryExcelView().then(async (data) => {
    await fillDataByYahooFinance(data);
    await insertStockData(data);
    console.log("HKEX data downloaded successfully. Total stocks inserted: " + data.length);
  }).catch((error) => {
    console.error("Error downloading HKEX data:", error);
  });
}

/**
 * Fill missing data in the HKEX dataset using Yahoo Finance API.
 * @param {*} data 
 */
const fillDataByYahooFinance = async (data) => {
  var count = 0;
  for (const item of data) {
    const result = await yahooFinance.search(item.code, { region: 'HK', lang: 'zh_HK' }, { validateResult: false });
    if (result && result.quotes && result.quotes.length > 0) {
      item.name = result.quotes[0].shortName || item.name;
      item.industry = !helper.isEmpty(result.quotes[0].industry) ? result.quotes[0].industry : "UNKNOWN";
      item.sector = !helper.isEmpty(result.quotes[0].sector) ? result.quotes[0].sector : "UNKNOWN";
    }

    if (++count % 100 === 0) {
      console.log("Yahoo data file Processed " + count + " / " + data.length + " stocks");
    }
  }
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

/**
 * Parse a CSV file and insert the data into the DAILY_STOCK_PRICE table.
 * @param {*} filePath 
 */
async function parseAndInsertCsvData(filePath) {
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
        insertDailyStockPrice(rows);
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

/**
 * Traverse the directory and insert data into the database.
 * @param {*} files 
 * @returns 
 */
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

/**
 * Unzip HKEX files.
 * @param {*} fullPath 
 */
function unzipFiles(fullPath) {
  helper.unzipFile(config.file.path.hk, fullPath)
    .then(() => {
      console.log("HK files unzipped successfully. " + fullPath);
      traverseDirAndInsertData(fullPath);
    })
    .catch((error) => {
      console.error("Error unzipping HK files:", error);
    }); 
}

/**
 * Main function to load data from HKEX and process it.
 */
const row01 = sqliteDb.prepare('SELECT sqlite_version()').get();
console.log(row01);

const queryDataByYahooFinance = async (data) => {
  const indexes = ['^HSI', '^HSCE'];
  const queryOptions = { period1: '2024-01-01', /* ... */ };

  for (const index of indexes) {
    const result01 = await yahooFinance.chart(index, queryOptions);

    var rows = [];
    if (result01 && result01.quotes && result01.quotes.length > 0) {
      for (const quote of result01.quotes) {
        var price = {
          symbol: result01.meta.symbol,
          period: 'D',
          dt: moment(quote.date).format('YYYYMMDD'),
          tm: '000000',
          open: quote.open,
          high: quote.high,
          low: quote.low,
          close: quote.close,
          volume: quote.volume,
          adj_close: quote.adjclose,
          open_int: 0
        };
        rows.push(price);
      }

      console.log("Yahoo indexes Processed[" + index + "] : " + rows.length);
      insertDailyStockPrice(rows);
    }
  }
}

const runMain = async (data) => {
  hkexDownload().then(() => {
    console.log("HKEX data download and processing completed.");
    loadData().then(() => {
      queryDataByYahooFinance();
    }).catch((error) => {
      console.error("Error during CSV download and processing:", error);
    });
    
  }).catch((error) => {
    console.error("Error during HKEX data download and processing:", error);
  });
}

// parseAndInsertCsvData('c:/Users/user/Downloads/hkex/d250613e.txt');
runMain();