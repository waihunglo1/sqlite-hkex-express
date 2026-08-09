// Run this with: node app.js --date=2026-08-09 --ticker=AAPL
// Or:           node app.js --ticker AAPL --date 2026-08-09

const minimist = require('minimist');

// Parse arguments and set fallback defaults if missing
const argv = minimist(process.argv.slice(2), {
  string: ['date', 'ticker'], // Treats inputs strictly as strings
  alias: { d: 'date', t: 'ticker' } // Allows short flags (-d and -t)
});

const dateParam = argv.date;
const tickerParam = argv.ticker;

if (!dateParam || !tickerParam) {
  console.error('Usage: node app.js --date <YYYY-MM-DD> --ticker <SYMBOL>');
  process.exit(1);
}

// Process data
const targetDate = new Date(dateParam);
const ticker = tickerParam.toUpperCase();

console.log(`Processing ticker ${ticker} for date: ${targetDate.toISOString().split('T')[0]}`);



const dfd = require("danfojs");
const priceOverSMA20List = [1.02, 0.98, 1.05, 1.01, 0.95];
const slopeSMA20List = [11.02, 2.98, 11.05, 12.01, 3.95];
let data =
        {
           'priceOverSMA20List': priceOverSMA20List,
           'slopeSMA20List': slopeSMA20List
        }

let df = new dfd.DataFrame(data)
df.describe().print(); 

const percentRank = require('percentile-rank');
const formularjs = require('@formulajs/formulajs');

const dataset = [1, 5, 20, 50, 55, 60, 70, 80, 90, 100];
const rank = percentRank(dataset, 44);

console.log(rank); // Output: 0.3155555555555556


let array =[71,13,23,32,45,99,103,71,43,11,91,21,45,45,89,66,41,29,66,63];
let compare_array =[11,19,17,31,32,43,71,63,35,13,73,74,81,100,13,41,31,29,31,33];

compare_array.forEach(function(n){

     let pr = formularjs.PERCENTRANKINC(array,n, 2);
     console.log(n + " / " + pr * 100); 
 
});

pr = percentRank(dataset, 44);
console.log(44 + " / " + pr * 100); 
