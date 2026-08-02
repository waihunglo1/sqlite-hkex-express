// Import helper functions and utilities
const helper = require("./helper.js");
const sqliteHelper = require('./sqliteHelper.js');
const scraper = require('./scraper.js'); // Import the traverseDir function

// yahoo finance api
const YahooFinance = require('yahoo-finance2').default; // NOTE the .default
const yahooFinance = new YahooFinance({
 suppressNotices: ["yahooSurvey"] // optional
});

/*
 * main function to execute the data loading and filling process
 */
sqliteHelper.dumpSqliteVerion().then(() => {
    helper.loadIndexDataByYahooFinance(yahooFinance).then(async () => {
      try {
        const result = await scraper.traverseDir();
        console.log("Data loading and filling process completed successfully. result = ", result);
      }
      catch(error) {
        console.error("Error in the data loading and filling process:", error);
      }
    }).catch((error) => {
        console.error("Error loading index data:", error);
    })
});