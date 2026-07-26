// Import helper functions and utilities
const helper = require("./helper.js");
const mmutils = require('./mm-utils.js');
const config = require('config');
const sqliteHelper = require('./sqliteHelper.js');
const scraper = require('./scraper.js'); // Import the traverseDir function

// yahoo finance api
const YahooFinance = require('yahoo-finance2').default; // NOTE the .default
const yahooFinance = new YahooFinance({
 suppressNotices: ["yahooSurvey"] // optional
});

/**
 * Download HKEX data and fill it with Yahoo Finance data.
 * This function queries the HKEX data, fills it with additional information from Yahoo Finance
 * 
 * deprecated: use loadDataV2.js instead
 */
const fillStockData = async (yahooFinance) => {
  console.log("HKEX data enabled: " + config.hkex.enable);

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
    exit(1); // Exit the process with an error code
  });
}

/*
 * main function to execute the data loading and filling process
 */
sqliteHelper.dumpSqliteVerion().then(() => {
    helper.loadIndexDataByYahooFinance(yahooFinance).then(async () => {
      try {
        await scraper.traverseDir();
        console.log("Data loading and filling process completed successfully.");
      }
      catch(error) {
        console.error("Error in the data loading and filling process:", error);
      }
    }).catch((error) => {
        console.error("Error loading index data:", error);
    })
});