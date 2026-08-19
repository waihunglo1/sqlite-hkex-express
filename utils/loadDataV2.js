// Import helper functions and utilities
require('dotenv').config();
const helper = require("./helper.js");
const sqliteHelper = require('./sqliteHelper.js');
const scraper = require('./scraper.js'); // Import the traverseDir function

// yahoo finance api
const YahooFinance = require('yahoo-finance2').default; // NOTE the .default
const yahooFinance = new YahooFinance({
 suppressNotices: ["yahooSurvey"] // optional
});
const logger = require('./logger')
/*
 * main function to execute the data loading and filling process
 */
sqliteHelper.dumpSqliteVerion().then(() => {
    helper.loadIndexDataByYahooFinance(yahooFinance).then(async () => {
      try {
        const result = await scraper.traverseDir();
        logger.info("Data loading and filling process completed successfully. result = ", result);
      }
      catch(error) {
        logger.error("Error in the data loading and filling process:", error);
      }
    }).catch((error) => {
        logger.error("Error loading index data:", error);
    })
});