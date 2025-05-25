require('dotenv').config();
const pgClient = require('postgrejs');
const postgres = require('postgres');
const { AVIEN_DB_USER, AVIEN_DB_PASSWORD, AVIEN_DB_HOST, AVIEN_DB_PORT, AVIEN_DB_DATABASE } = process.env;

const sql = postgres('postgres://{username}:{password}@{host}:{port}/{database}', {
  host                 : AVIEN_DB_HOST,            // Postgres ip address[s] or domain name[s]
  port                 : AVIEN_DB_PORT,          // Postgres server port[s]
  database             : AVIEN_DB_DATABASE,            // Name of database to connect to
  username             : AVIEN_DB_USER,            // Username of database user
  password             : AVIEN_DB_PASSWORD,            // Password of database user
});

const config = {
    user: AVIEN_DB_USER,
    password: AVIEN_DB_PASSWORD,
    host: AVIEN_DB_HOST,
    port: AVIEN_DB_PORT,
    database: AVIEN_DB_DATABASE,
    ssl: {
        rejectUnauthorized: true,
        ca: `-----BEGIN CERTIFICATE-----
MIIEQTCCAqmgAwIBAgIUP5AfiQ02hVgPWMyctBn+FVxoerQwDQYJKoZIhvcNAQEM
BQAwOjE4MDYGA1UEAwwvMjYyNWJhNmUtZGU4ZC00MGQwLTk5ZDQtNmIxMmNlZWVk
ZjQyIFByb2plY3QgQ0EwHhcNMjQwODA4MTMyMjEwWhcNMzQwODA2MTMyMjEwWjA6
MTgwNgYDVQQDDC8yNjI1YmE2ZS1kZThkLTQwZDAtOTlkNC02YjEyY2VlZWRmNDIg
UHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCCAYoCggGBAJ2bPF6N
Jzq+UDu3ps+VKjToYHD/Ph8hJA/iMzP2BaR3PB/sBK49Rt1PeeXgQubwdl7uvn4u
hcx+kIkcoOBvH7WXeVWbbbgc12i2M7EZxfSUSmDh7VhsLLKpox5L4Gco5/Oh+Zt6
dBHzps64EjC7CLP0Mz7LGzUmKMxqLU4G4ISSjhlbTiwS6p6Q2q/mYdMfX8UVKMvR
jgCdiKQ7iquy6qtH9W1VnHJSfXi2rHwfeM1IXeE0J4QxXusWd/ThsVBxqmAGcUjJ
yndVf+WfTE1Ch9qdWpQApdScUW5M9f0sHyWF950iLg2GEAsBOng4Zyt4esKnwqDf
vnEbkOJToUzf2b+fnTux7aV85JgVo4UEN15tIalNuckS968U6crY0QOqfvoaY4TJ
ZHDU3oBFrcg8mdFoOAOMjJ5P7bUmrp/DpIqJ2K0CIUnBTVBysDYYK3K/rM3+CrRb
kXCwvBOXnj54bxBYlR3xMPYGLaWoJhFvEhP9bHCZy0jsEUOUy/BK/wtdqQIDAQAB
oz8wPTAdBgNVHQ4EFgQUCfE+FmcJgCEEaUB0KI3kMpNi22kwDwYDVR0TBAgwBgEB
/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQADggGBAGScX4DmW0eoLecW
MLq5E5V/SE3AQZ5GvPihm6hEF0aBUyBQfitznxlp6hi+y5W8tHPYXdUtRJLacc+7
wpJGuzZZ2/wmqIx0J4VogQ1JBbyo+M9m9xKyH/s6cXQ4P5J/vIYGHFq88dopZTUJ
6TAYtRD3BpcWYjtePY7mqZAqbyPxOA6F7gqCRvCdlhY2X73gup+2QjWqeng+XT2m
cEaYf239ARoy0e4Sp15KbujfUPdXQv94km9eVK3Bl5GUIZwcKhnQqp4ix9UQY+px
fe8pFyTnD9BQXMcXEnE1qXpufNEjCAfQA6SX5ri2HiLKoshO1lLKVRvtpo/L1Azm
GmCDlgdwA08ijHKxMeolaaHkK56AT8YIMYiPlG/k3fuzHoIFjpMYf+Jcz+BQDF+J
TVYDM8XXvtNdBRYbLaG/1/ffwqDUqwmtTXPbg1t/eJewV51qZ3ngioDItOcoMk0n
sFSecPmZ5WUwaDqMHemEjLMCrFhLzwO3nMBqHTDl37IX2Bbuiw==
-----END CERTIFICATE-----`,
    },
};



const connection = new pgClient.Connection(config);

async function getAivenPgVersion() {

    // Connect to database server
    await connection.connect();

    // Execute query and fetch rows
    const result = await connection.query('SELECT version()');

    // Disconnect from server
    await connection.close();

    console.log("Aiven version: ", result.rows[0]);

    return result.rows[0];
}

async function insertMarketStats(marketStats) {

      // Connect to database server
    await connection.connect();

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
