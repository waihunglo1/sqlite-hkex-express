const fs = require("fs");
const util = require("util");
const path = require('path');
const { parse } = require("csv-parse");
const config = require('config');
const helper = require("./helper");
const mmutils = require('./mm-utils.js');
const yahooFinance = require('yahoo-finance2').default; // NOTE the .default
const sqliteDb = require('better-sqlite3')(config.db.sqlite.file, {});
sqliteDb.pragma('journal_mode = WAL');

const loadData = async function () {
  const zipFullPath = path.join(config.file.path.extract, helper.todayString());
  if (!fs.existsSync(zipFullPath)) {
    console.info("Extraction path does not exist: " + zipFullPath);
    unzipFiles(zipFullPath);
  } else {  
    console.info("Extraction path exists: " + zipFullPath);
    const stat = fs.statSync(zipFullPath);  
    traverseDirAndInsertData(zipFullPath);
  }
}

const traverseDirAndInsertData = async (zipFullPath) => {
  const pathToLoads = [config.file.path.load.dir1, config.file.path.load.dir2];

  for (const pathToLoad of pathToLoads) {
    const fullPath = path.join(zipFullPath, pathToLoad);
    if (!fs.existsSync(fullPath)) {
      console.error("Directory does not exist: " + fullPath);
      return;
    } else {
      console.log("Directory exists: " + fullPath);
      const files = helper.traverseDirectory(fullPath);
      const fileCount = await traverseDir(files);
      console.log(fullPath + "Total files processed: " + fileCount);
    }
  }

  const row02 = sqliteDb.prepare('SELECT max(dt), min(dt), COUNT(1) FROM DAILY_STOCK_PRICE').get();
  console.log(row02);

  sqliteDb.close();
}


const hkexDownload = async () => {
  await mmutils.queryExcelView().then(async (data) => {
    await fillDataByYahooFinance(data);
    await insertStockData(data);
    console.log("HKEX data downloaded successfully. Total stocks inserted: " + data.length);
  }).catch((error) => {
    console.error("Error downloading HKEX data:", error);
  });
}

const fillDataByYahooFinance = async (data) => {
  var count = 0;
  for (const item of data) {
    const result = await yahooFinance.search(item.code, { region: 'HK' });
    if (result && result.quotes && result.quotes.length > 0) {
      item.name = result.quotes[0].shortName || item.name;
      item.industry = !helper.isEmpty(result.quotes[0].industry) ? result.quotes[0].industry.toUpperCase() : "UNKNOWN";
      item.sector = !helper.isEmpty(result.quotes[0].sector) ? result.quotes[0].sector.toUpperCase() : "UNKNOWN";

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
 * Parse a CSV file and insert the data into the DAILY_STOCK_PRICE table.
 * @param {*} filePath 
 */
async function parseAndInsertCsvData(filePath) {
  // console.log("Parsing CSV file: " + filePath);
  const insert = sqliteDb.prepare(
    'REPLACE INTO DAILY_STOCK_PRICE (symbol,period,dt,tm,open,high,low,close,volume,adj_close,open_int) ' +
    'VALUES (@symbol,@period,@dt,@tm,@open,@high,@low,@close,@volume,@adj_close,@open_int)');

  const insertMany = sqliteDb.transaction((stockPrices) => {
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

hkexDownload().then(() => {
  console.log("HKEX data download and processing completed.");
  loadData();
}).catch((error) => {
  console.error("Error during HKEX data download and processing:", error);
});
