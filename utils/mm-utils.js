var XLSX = require("xlsx");
var axios = require('axios').default;
const config = require("config");
const helper = require("./helper");
const fincal = require("fincal");
const moment = require("moment");

axios.defaults.timeout = 1000;

/**
 * 
 * @returns 
 */
const queryExcelView = async (yahooFinance) => {
    const excelFileUrl = config.get("hkex.url");
    const file = await (await fetch(excelFileUrl)).arrayBuffer();
    console.log("fetched file :" + excelFileUrl + " bytelength:" + file.byteLength); 

    // read file
    // const pathToLook = config.hkex.download.path;
    // const fileToLoad = config.hkex.download.listOfSecurities;
    // const targetPath = path.join(pathToLook, fileToLoad);

    // console.log("fetched file :" + targetPath); 

    // parse file
    const workbook = XLSX.readFile(file);
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    worksheet['!ref'] = "A4:C99999" // change the sheet range to A2:C3
    // convert to json
    const jsonData = XLSX.utils.sheet_to_json(worksheet, {header:1, blankrows: false}); 
    console.log("Row count:" + jsonData.length);
    return reformatData(yahooFinance, jsonData);
}

/**
 * 
 * @param {*} data 
 * @returns 
 */
const reformatData = async (yahooFinance, data) => {
    const items = data
        .filter(function (item) {
            return item[2] && item[2].length > 0 && (item[2].indexOf("股本") > -1 || item[2].indexOf("交易所買賣產品") > -1);
        })
        .map(item => {
            return {
                code: reformatSymbolinHK(item[0]),
                name: item[1],
                type: item[2],
                industry: "UNKNOWN",
                sector: "UNKNOWN",
                marketCap: "0"
            };
        });

    var count = 0;
    var {startDate, endDate} = determineTargetDateString();
    for (const item of items) {
        await yahooFinanceFiller(yahooFinance, item, startDate);

        if (++count % 500 === 0) {
            console.log("[INFO] Yahoo data file Processed " + count + " / " + items.length + " stocks");
        }
    }

    return items;
}

/**
 * 
 * @param {*} symbol 
 * @returns 
 */
const reformatSymbolinHK = (symbol) => {
    // remove the last 4 characters
    if (symbol.charAt(0) == '0' && symbol.length > 4) {
        symbol = symbol.substring(1, 5);
    }

    return symbol + ".HK";
}


const yahooFinanceFiller = async (yahooFinance, item, startDate) => {
    try {
        const yfStock = await yahooFinance.search(item.code, { region: 'HK', lang: 'zh_HK' }, { validateResult: false });
        if (yfStock && yfStock.quotes && yfStock.quotes.length > 0) {
            item.name = yfStock.quotes[0].shortName || item.name;
            item.industry = !helper.isEmpty(yfStock.quotes[0].industry) ? yfStock.quotes[0].industry : "UNKNOWN";
            item.sector = !helper.isEmpty(yfStock.quotes[0].sector) ? yfStock.quotes[0].sector : "UNKNOWN"; 
        } 

        item.marketCap = "0";
        const yfQuoteSummary = await yahooFinance.quoteSummary(item.code, { modules: ['summaryDetail'] }, { validateResult: false });
        if (yfQuoteSummary && yfQuoteSummary.summaryDetail) {
            item.marketCap = yfQuoteSummary.summaryDetail.marketCap ? yfQuoteSummary.summaryDetail.marketCap.toString() : "0";
        }

        // fetch historical prices
        if(config.yahoofinance.historicalData.enable) {
            const queryOptions = { period1: startDate, /* ... */ };
            const yfChart = await yahooFinance.chart(item.code, queryOptions, { validateResult: false });
            if (yfChart && yfChart.quotes) {
                console.log("fetched " + item.code + " " + item.name + " " + item.industry + " " + item.sector + " " + item.marketCap + " quotes:" + yfChart.quotes.length);
            }
        }

    } catch (error) {
        if (error instanceof yahooFinance.errors.FailedYahooValidationError) {
        } else if (error instanceof yahooFinance.errors.HTTPError) {
        } else {
            // console.error(error);
        }
    }
}

const determineTargetDateString = () => {
    var days = 300;
  
    // Calendar class
    var hkCalendar = fincal.calendar("hong_kong");
    var previousTradingDate = hkCalendar.previousTradingDay(moment(), "YYYY-MM-DD");
    var targetEndDate = moment(previousTradingDate).subtract(days, "days");
    var targetTradingEndDate = hkCalendar.previousTradingDay(targetEndDate, "YYYY-MM-DD");
    var formattedTargetTradingEndDate = targetTradingEndDate.format("YYYY-MM-DD");

    // console.log(hkCalendar.name); // > "New York"
    // console.log(hkCalendar.locale); // > [Object]
    console.log("date range: " + formatDate(previousTradingDate,"YYYY-MM-DD") + " to " + formattedTargetTradingEndDate); 

    return {
        startDate: formattedTargetTradingEndDate,
        endDate:   formatDate(previousTradingDate,"YYYY-MM-DD")
    };
}

const formatDate = (sourceDate, formatStr) => {
    return moment(sourceDate).format(formatStr);
}

module.exports = {
    queryExcelView
};