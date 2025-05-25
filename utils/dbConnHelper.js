require('dotenv').config();
const postgres = require('postgres');
const { AVIEN_DB_USER, AVIEN_DB_PASSWORD, AVIEN_DB_HOST, AVIEN_DB_PORT, AVIEN_DB_DATABASE } = process.env;
const sql = postgres(`postgres://${AVIEN_DB_USER}:${AVIEN_DB_PASSWORD}@${AVIEN_DB_HOST}:${AVIEN_DB_PORT}/${AVIEN_DB_DATABASE}?sslmode=require`);

async function getAivenPgVersion() {

  const resolvedPromise = new Promise(async (resolve, reject) => {
    const result = await sql`SELECT version()`;
    var version = result[0];
    resolve(version);
  });

  var version = null;
  await Promise.all([resolvedPromise])
    .then((values) => {
      version = values[0].version;
    });

  return version;
}

async function insertMarketStats(marketStats) {
  const result = await sql`
    INSERT INTO daily_market_stats (dt, up4pct1d, dn4pct1d, up25pctin100d, dn25pctin100d, up25pctin20d, dn25pctin20d, up50pctin20d, dn50pctin20d, noofstocks, above200smapct, above150smapct, above20smapct)
    VALUES (${marketStats.dt}, ${marketStats.up4pct1d}, ${marketStats.dn4pct1d}, ${marketStats.up25pctin100d}, ${marketStats.dn25pctin100d}, ${marketStats.up25pctin20d}, ${marketStats.dn25pctin20d}, ${marketStats.up50pctin20d}, ${marketStats.dn50pctin20d}, ${marketStats.noofstocks}, ${marketStats.above200smapct}, ${marketStats.above150smapct}, ${marketStats.above20smapct})
    RETURNING *;
  `;

  return result[0];
}

module.exports = {
    getAivenPgVersion,
    insertMarketStats
};
