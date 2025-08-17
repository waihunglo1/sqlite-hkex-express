const fs = require('fs');
const path = require('path');
const util = require('util');
const extract = require('extract-zip')
const { mkdir } = require('node:fs/promises');
const sqliteHelper = require('./sqliteHelper.js');
const moment = require('moment');

/**
 * 
 * @param {*} symbol 
 * @returns 
 */
const reformatSymbolForHK = (symbol) => {
    const index = symbol.search(/.HK$/);
    if(index < 0) {
        return symbol;
    }

    // pad leading 0
    var code = symbol.replace(/.HK$/gi, '');

    // remove the last 4 characters
    if (code.length > 4 && code.charAt(0) == '0') {
        code = code.substring(1, 5);
    }

    if (code.length < 4) {
        code = code.padStart(4, "0");
    }

    return code + ".HK";
}

/**
 * 
 * @param {*} dir 
 * @param {*} result 
 * @returns 
 */
const traverseDirectory = (dir, regex, result = []) => {
    // list files in directory and loop through
    fs.readdirSync(dir).forEach((file) => {

        // builds full path of file
        const fPath = path.resolve(dir, file);

        // prepare stats obj
        const fileStats = { file, path: fPath };

        // is the file a directory ? 
        // if yes, traverse it also, if no just add it to the result
        if (fs.statSync(fPath).isDirectory()) {
            fileStats.type = 'dir';
            fileStats.files = [];
            // result.push(fileStats);
            return traverseDirectory(fPath, regex, fileStats.files)
        }

        // if file path matches regex, add it to the result
        if (regex && regex.exec(fileStats.file)) {
            fileStats.type = 'file';
            result.push(fileStats);
        } 
    });
    return result;
};

/**
 * 
 * @param {*} value 
 * @returns 
 */
function isEmpty(value) {
    return (
        value === null || value === undefined || value === '' ||
        (Array.isArray(value) && value.length === 0) ||
        (!(value instanceof Date) && typeof value === 'object' && Object.keys(value).length === 0)
    );
}

/**
 * 
 * @returns 
 */
function todayString() {
    const today = new Date();
    return today.toISOString().split('T')[0]; // YYYY-MM-DD format
}

/**
 * 
 * @param {*} path 
 * @param {*} outputDir 
 */
async function unzipFile(path, outputDir) {
    await extract(path, { dir: outputDir })
}

/**
 * 
 * @param {*} directoryPath 
 */
async function createDirectoryIfNotExists(directoryPath) {
  try {
    await mkdir(directoryPath, { recursive: true });
    console.log(`Directory created or already exists at: ${directoryPath}`);
  } catch (error) {
    console.error(`Error creating directory: ${error.message}`);
  }
}

/**
 *  Parse and insert CSV data into the database.
 */
const loadIndexDataByYahooFinance = async (yahooFinance) => {
  const indexes = ['^HSI', '^HSCE'];
  const queryOptions = { period1: '2024-01-01', /* ... */ };

  for (const index of indexes) {
    const result01 = await yahooFinance.chart(index, queryOptions, { validateResult: false });

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

      await sqliteHelper.insertDailyStockPrice(rows);
      console.log("Yahoo indexes Processed[" + index + "] : " + rows.length);
    }
  }
}

module.exports = {
    reformatSymbolForHK,
    traverseDirectory,
    isEmpty,
    createDirectoryIfNotExists,
    todayString,
    unzipFile,
    loadIndexDataByYahooFinance
};