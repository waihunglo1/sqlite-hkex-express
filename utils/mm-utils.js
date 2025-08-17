var XLSX = require("xlsx");
var axios = require('axios').default;
const config = require('config');
const helper = require("./helper");
const yahooFinance = require('yahoo-finance2').default; // NOTE the .default
const path = require('path');
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
    for (const item of items) {
        await yahooFinanceFiller(yahooFinance, item);

        if (++count % 100 === 0) {
            console.log("Yahoo data file Processed " + count + " / " + data.length + " stocks");
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


const yahooFinanceFiller = async (yahooFinance, item) => {
    try {
        const result = await yahooFinance.search(item.code, { region: 'HK', lang: 'zh_HK' }, { validateResult: false });
        if (result && result.quotes && result.quotes.length > 0) {
            item.name = result.quotes[0].shortName || item.name;
            item.industry = !helper.isEmpty(result.quotes[0].industry) ? result.quotes[0].industry : "UNKNOWN";
            item.sector = !helper.isEmpty(result.quotes[0].sector) ? result.quotes[0].sector : "UNKNOWN";
        }

        item.marketCap = "0";
        const queryOptions = { modules: ['summaryDetail'] }; // defaults
        const result01 = await yahooFinance.quoteSummary(item.code, queryOptions, { validateResult: false });
        if (result01 && result01.summaryDetail) {
        item.marketCap = result01.summaryDetail.marketCap ? result01.summaryDetail.marketCap.toString() : "0";
        }
    } catch (error) {
        if (error instanceof yahooFinance.errors.FailedYahooValidationError) {
        } else if (error instanceof yahooFinance.errors.HTTPError) {
        } else {
        }
    }
}

module.exports = {
    queryExcelView
};