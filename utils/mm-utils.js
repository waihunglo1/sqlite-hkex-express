var XLSX = require("xlsx");
var axios = require('axios').default;
const config = require('config');
axios.defaults.timeout = 1000;

const queryExcelView = async () => {
    const excelFileUrl = config.get("hkex.url");

    // read file
    const file = await (await fetch(excelFileUrl)).arrayBuffer();
    console.log("fetched file :" + excelFileUrl + " bytelength:" + file.byteLength); 

    // parse file
    const workbook = XLSX.read(file);
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    worksheet['!ref'] = "A4:C99999" // change the sheet range to A2:C3

    var range = XLSX.utils.decode_range(worksheet['!ref']);
    var numRows = range.e.r - range.s.r + 1
    var numCols = range.e.r - range.s.r + 1
    console.log("parsed file :" + excelFileUrl + " rows:" + numRows + " cols:" + numCols);

    // convert to json
    const jsonData = XLSX.utils.sheet_to_json(worksheet, {header:1, blankrows: false}); 
    return reformatData(jsonData);
}

const reformatData = (data) => {
    return data
        .filter(function (item) {
            return item[2] && item[2].length > 0 && item[2].indexOf("Equity") > -1;
        })
        .map(item => {
            
            return {
                code: reformatSymbolinHK(item[0]),
                name: item[1],
                type: item[2]
            };
        });
}

const reformatSymbolinHK = (symbol) => {
    // remove the last 4 characters
    if (symbol.charAt(0) == '0' && symbol.length > 4) {
        symbol = symbol.substring(1, 5);
    }

    return symbol + ".HK";
}

module.exports = {
    queryExcelView
};