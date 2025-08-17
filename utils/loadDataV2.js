// Import helper functions and utilities
const helper = require("./helper.js");
const mmutils = require('./mm-utils.js');
const config = require('config');
const sqliteHelper = require('./sqliteHelper.js');
const scraper = require('./scraper.js'); // Import the traverseDir function
const yahooFinance = require('yahoo-finance2').default; // NOTE the .default

/**
 * Download HKEX data and fill it with Yahoo Finance data.
 * This function queries the HKEX data, fills it with additional information from Yahoo Finance,
 */
const fillStockData = async (yahooFinance) => {
  if( !config.hkex.enable) {
    console.log("HKEX data download is disabled in the configuration.");
    return;
  }

  await mmutils.queryExcelView(yahooFinance).then(async (data) => {
    console.log("HKEX data downloaded successfully. Total stocks inserted: " + data.length);
    await sqliteHelper.insertStockData(data);
    console.log("Stock data inserted into the database successfully.");   
  }).catch((error) => {
    console.error("Error downloading HKEX data:", error);
  });
}

/*
 * main function to execute the data loading and filling process
 */
helper.loadIndexDataByYahooFinance(yahooFinance).then(() => {
  fillStockData(yahooFinance).then(async () => {
    scraper.traverseDir();
  }).catch((error) => {
    console.error("Error loading index data:", error);
  });
}).catch((error) => {
  console.error("Error in the data loading and filling process:", error);
});