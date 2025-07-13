const axios = require('axios');
const cheerio = require('cheerio');
const https = require('https');
const fs = require('fs');
const readline = require('readline');
const { FixedWidthParser } = require('fixed-width-parser');
const { Readable } = require('stream');
const moment = require('moment');
const path = require('path');
const sqliteHelper = require('./sqliteHelper.js');

// local modules
const config = require('config');
const helper = require("./helper");

const line01 = new FixedWidthParser([
    {
        name: 'code',
        start: 1,
        width: 5,
        type: 'string'
    },
    {
        name: 'previousClose',
        start: 28,
        width: 9,
        type: 'number', 
        precision: 2
    },
    {
        name: 'ask',
        start: 37,
        width: 9,
        type: 'number', 
        precision: 2        
    },
    {
        name: 'high',
        start: 46,
        width: 9,
        type: 'number', 
        precision: 2        
    },
    {
        name: 'sharesTraded',
        start: 55,
        width: 20,
        type: 'number', 
        precision: 2        
    }
]);

const line02 = new FixedWidthParser([
    {
        name: 'close',
        start: 28,
        width: 9,
        type: 'number', 
        precision: 2        
    },
    {
        name: 'bid',
        start: 37,
        width: 9,
        type: 'number', 
        precision: 2        
    },
    {
        name: 'low',
        start: 46,
        width: 9,
        type: 'number', 
        precision: 2        
    },
    {
        name: 'turnover',
        start: 55,
        width: 20,
        type: 'number', 
        precision: 2        
    }
]);

const salesRecordParser = new FixedWidthParser([
    {
        name: 'code',
        start: 0,
        width: 5,
    },
    {
        name: 'salesRecord',
        start: 23,
        width: 60,
    }
]);

/**
 * Scrapes data from a file and writes it to an output file.
 * @param {*} filePath 
 * @param {*} outputFilePath 
 */
async function scrapeData(filePath, outputFilePath) {
    const buffer = fs.readFileSync(filePath);
    const $ = cheerio.loadBuffer(buffer);

    // Example: Get the title of the page
    const pageTitle = $('title').text();
    // console.log(`Page Title: ${pageTitle}`);

    // Example: Get all fonts from the page
    const fonts = [];
    $('font').each((i, element) => {
        fonts.push($(element).text());
    });

    // format readable
    const stringStream = new Readable();
    stringStream.push(fonts.join());
    stringStream.push(null); // Indicate end of stream

    await writeFileAsync(outputFilePath, fonts.join());
    var {quoteDate, prices} = await processLineByLine(stringStream);
    await sqliteHelper.insertDailyStockPrice(prices);

    console.log(`${quoteDate} : ${prices.length}`);;
}

/**
 * Writes content to a file asynchronously.
 * @param {*} filePath 
 * @param {*} content 
 */
async function writeFileAsync(filePath, content) {
    try {
        await fs.promises.writeFile(filePath, content, 'utf-8');
        // console.log(`File "${filePath}" has been successfully written.`);
    } catch (error) {
        console.error(`Error writing to file "${filePath}": ${error.message}`);
    }
}

/**
 * Processes the lines of a string stream.
 * @param {*} stringStream 
 * @returns 
 */
async function processLineByLine(stringStream) {
    const quotation = ' CODE  NAME OF STOCK    CUR PRV.CLO./    ASK/    HIGH/      SHARES TRADED/';
    const section = '-------------------------------------------------------------------------------';
    const tradeSuspended = 'TRADING SUSPENDED';
    const salesRecord = '                            SALES RECORDS FOR ALL STOCKS';

    // const fileStream = fs.createReadStream(filePath);

    const rl = readline.createInterface({
        input: stringStream,
        crlfDelay: Infinity // To recognize all common newline characters (\n, \r, \r\n)
    });

    var quotationLineCount = 0;
    var salesRecordLineCount = 0;
    var lineCount = 0;
    var state = 0;
    var previousLine;
    var salesRecords = [];
    var symbol;
    var prices = [];
    var quoteDate = null;
    for await (var line of rl) {
        lineCount++;
        line = line.replace(/,/g, '');
        if (lineCount === 7) {
            quoteDate = searchQuoteDate(line);
        }

        if (line.includes(quotation)) {
            state = 1;
        }

        if (line.includes(salesRecord)) {
            state = 2;
        }

        if (state === 1) {
            quotationLineCount++;

            if (line.includes(section)) {
                state = 0;
            }

            if (quotationLineCount >= 4) {
                const line01Data = line01.parse(line);
                if (!helper.isEmpty(line01Data[0].code) && !line.includes(tradeSuspended)) {
                    previousLine = line;
                    continue;
                }

                // second line processing
                if (helper.isEmpty(line01Data[0].code) && !helper.isEmpty(previousLine)) {
                    process2Lines(prices, previousLine, line, quoteDate);
                    previousLine = null;
                    continue;
                }
            }
        }

        if (state === 2) {
            salesRecordLineCount++;
            if (line.includes(section) && salesRecordLineCount >= 14) {
                state = 0;
            }

            if (salesRecordLineCount >= 14 && line.length > 5) {
                const line01Data = salesRecordParser.parse(line);
                if (!helper.isEmpty(line01Data[0].code)) {
                    if (!helper.isEmpty(symbol) && symbol != line01Data[0].code) {
                        processSalesRecords(symbol, salesRecords, prices);
                    }
                    symbol = line01Data[0].code;
                    salesRecords = [];
                    salesRecords.push(line01Data[0].salesRecord);
                } else {
                    salesRecords.push(line01Data[0].salesRecord);
                }
            }
        }
    }

    return {quoteDate, prices};
}

/**
 * Extracts the quote date from a line of text.
 * @param {*} line 
 * @returns 
 */
function searchQuoteDate(line) {
    const dateRegex = /(\d{1,2})\s*([A-Zaz]{3})\s*(\d{4})/;
    const match = line.match(dateRegex);
    if (match) {
        const day = match[1];
        const month = match[2];
        const year = match[3];
        // console.log(`Quote date found: ${day} ${month} ${year}`);
        const momentObject = moment(`${day}/${month}/${year}`, 'DD/MMM/YYYY');
        if (momentObject.isValid()) {
            return momentObject.format('YYYYMMDD');
        } else {
            console.error(`Invalid date format: ${day}/${month}/${year}`);
            return null;
        }
    }
}

/**
 * Extracts the quote date from a line of text.
 * @param {*} symbol 
 * @param {*} salesRecords 
 * @param {*} prices 
 */
function processSalesRecords(symbol, salesRecords, prices) {
    const text = salesRecords.join().replace(/,/g, '');
    // console.log(`Text: ${text}`);
    const regex = [/\<(.+?)\>/g, /\[(.+?)\]/g];

    var index = 0;
    var auctionSession = [];
    var normalSession = [];
    for (const re of regex) {
        const extractedData = [];
        while ((match = re.exec(text)) !== null) {
            extractedData.push(match[1]);
        }

        if (index === 0) {
            for (const data of extractedData) {
                const session01 = data.trim().split(' ');
                if (session01.length > 0) {
                    auctionSession.push(session01[session01.length - 1]);
                }
            }
        } else {
            for (const data of extractedData) {
                const session01 = data.trim().split(' ');
                if (session01.length > 0) {
                    normalSession.push(session01[0]);
                }
            }
        }

        index++;
    }

    var open;
    if (auctionSession.length > 0) {
        open = auctionSession[0].split('-')[1];
    }

    if (helper.isEmpty(open) && normalSession.length > 0) {
        open = normalSession[0].split('-')[1];
    }

    if (helper.isEmpty(open) && normalSession.length > 0) {
        open = normalSession[1].split('-')[1];
    }

    if (helper.isEmpty(open) && auctionSession.length > 0) {
        open = auctionSession[1].split('-')[1];
    }

    // console.log(`${symbol} / ${open} : ${auctionSession} / ${normalSession}`);
    // if(auctionSession.length <= 0 && normalSession.length <= 0) {
    //    console.log(`${text}`);
    // }

    prices.filter(price => price.symbol === symbol.trim() + '.HK').forEach(price => {
        price.open = open;
    });
}

/**
 * Processes two lines of stock data.
 * @param {*} prices 
 * @param {*} previousLine 
 * @param {*} currentLine 
 * @returns 
 */
function process2Lines(prices, previousLine, currentLine, quoteDate) {
    // console.log(previousLine);
    // console.log(currentLine);
    // console.log('-------------------------');

    if (helper.isEmpty(previousLine)) {
        return;
    }

    const line01Data = convertValue(line01.parse(previousLine)[0]);
    const line02Data = convertValue(line02.parse(currentLine)[0]);

    var price = {
        symbol: helper.reformatSymbolForHK(line01Data.code + '.HK'),
        period: 'D',
        dt: quoteDate,
        tm: '000000',
        open: 0,
        high: line01Data.high === '0' ? line02Data.close : line01Data.high,
        low: line02Data.low === '0' ? line02Data.close : line02Data.low,
        close: line02Data.close,
        volume: line01Data.sharesTraded,
        adj_close: 0,
        open_int: 0
    };

    prices.push(price);
}

function convertValue(obj) {
    for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
            var str = obj[key].trim();
            if(str === '-' || str === 'N/A') {
                str = '0';
            }
            obj[key] = str;
        }
    } 
    
    return obj;
}

const traverseDir = async () => {
    const pathToLook = config.hkex.download.path;
    const folderTarget = config.file.path.load.dir3;
    const targetPath = path.join(path.join(config.file.path.extract, helper.todayString()), folderTarget);
    const regex = /^d(\d{6})e\.htm$/; // Example regex for matching files like d250627e.htm

    if (!fs.existsSync(pathToLook)) {
        console.error("Directory does not exist: " + pathToLook);
        return;
    } else {
        console.log("Directory exists: " + pathToLook);
        helper.createDirectoryIfNotExists(targetPath).then(() => {
            const files = helper.traverseDirectory(pathToLook);
            files.forEach(file => {
                const match = regex.exec(file.file);
                if (match) {
                    const destFile = `d${match[1]}e.txt`;
                    scrapeData(file.path, path.join(targetPath, destFile));
                }
            });
        });
    }
}

module.exports = {
    traverseDir
}

/**
 * Main function to start the scraping process.
 */
// traverseDir();
