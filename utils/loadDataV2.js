// Import helper functions and utilities
require('dotenv').config();
const helper = require("./helper.js");
const sqliteHelper = require('./sqliteHelper.js');
const scraper = require('./scraper.js'); // Import the traverseDir function
const logger = require('./logger')
/*
 * main function to execute the data loading and filling process
 */
sqliteHelper.dumpSqliteVerion().then(async () => {
    try {
      await scraper.traverseDir();
      logger.info("Data loading and filling process completed successfully.");
    }
    catch(error) {
      logger.error("Error in the data loading and filling process:", error);
    }
});