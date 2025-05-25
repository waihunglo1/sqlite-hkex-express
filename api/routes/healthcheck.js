var express = require('express');
var router = express.Router();
const dbHelper = require('../../utils/dbConnHelper.js');
/**
 * main function
 */
router.get('/', async function (req, res, next) {
  var version = await dbHelper.getAivenPgVersion();
  console.log("Aiven Version: ", version);

  var hc = {
    "health": "yes",
    "aivenVersion": version
  }

  res.json(hc);
});

module.exports = router;