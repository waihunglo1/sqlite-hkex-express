const fs = require("fs");
const util = require("util");
const path = require('path');
const { parse } = require("csv-parse");
const config = require('config');
const yahooFinance = require('yahoo-finance2').default; // NOTE the .default
const moment = require('moment');

// Import helper functions and utilities
const helper = require("./helper");
const mmutils = require('./mm-utils.js');
const scraper = require('./scraper.js'); // Import the traverseDir function
const sqliteHelper = require('./sqliteHelper.js');

/**
 * Load data from HKEX and process it.
 * This function checks if the extraction path exists, and if not, it unzips the files.
 */
const loadData = async function () {
  const zipFullPath = path.join(config.file.path.extract, helper.todayString());
  if (!fs.existsSync(zipFullPath)) {
    console.info("Extraction path does not exist: " + zipFullPath);
    unzipFiles(zipFullPath);
  }

  // process files
  await scraper.traverseDir().then(() => {
    console.info("Extraction path exists: " + zipFullPath);
    traverseDirAndInsertData(zipFullPath);
  }).catch((error) => {
    console.error("Error during HKEX data extraction:", error);
  });
}

/**
 * Traverse the directory and insert data into the database.
 * @param {*} zipFullPath 
 * @returns 
 */
const traverseDirAndInsertData = async (zipFullPath) => {
  const pathToLoads = [config.file.path.load.dir1, config.file.path.load.dir2];
  var paths = pathToLoads.map(dir => path.join(zipFullPath, dir));

  for (const fullPath of paths) {
    if (!fs.existsSync(fullPath)) {
      console.error("Directory does not exist: " + fullPath);
    } else {
      console.log("Directory exists: " + fullPath);
      const files = helper.traverseDirectory(fullPath);
      const fileCount = await traverseDir(files);
      console.log("Total files processed: " + fileCount + " in " + fullPath);
    }
  }

  await sqliteHelper.queryDailyStockPriceStatistics();
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
        sqliteHelper.insertDailyStockPrice(rows);
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
      console.log("Traverse Directory: " + file.path);
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
    })
    .catch((error) => {
      console.error("Error unzipping HK files:", error);
    }); 
}

/**
 * 
 * @param {*} data 
 */
const runMain = async (data) => {
  await sqliteHelper.dumpSqliteVerion();

  loadIndexDataByYahooFinance().then(() => {
    console.log("[INFO] Index data load completed.");
    loadData().then(() => {
      console.log("[INFO] CSV load file completed.");
    }).catch((error) => {
      console.error("[ERROR] during CSV download and processing:", error);
    });
  }).catch((error) => {
    console.error("[ERROR] during index data load from yahoo", error);
  });
}

// parseAndInsertCsvData('c:/Users/user/Downloads/hkex/d250613e.txt');

/**
 * Main function to load data from HKEX and process it.
 */
// process.env.NO_PROXY = "*"; // Or a specific list of hosts

runMain();