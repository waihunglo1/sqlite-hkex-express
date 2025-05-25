const fs = require('fs');
const path = require('path');
const util = require('util');

const reformatSymbolForHK = (symbol) => {
    const index = symbol.search(/.HK$/);
    if(index < 0) {
        return symbol;
    }

    // pad leading 0
    var code = symbol.replace(/.HK$/gi, '');

    // remove the last 4 characters
    if (code.length > 4 && code.charAt(0) == '0') {
        code = code.substring(1, 5);
    }

    if (code.length < 4) {
        code = code.padStart(4, "0");
    }

    return code + ".HK";
}

const traverseDirectory = (dir, result = []) => {
    // list files in directory and loop through
    fs.readdirSync(dir).forEach((file) => {

        // builds full path of file
        const fPath = path.resolve(dir, file);

        // prepare stats obj
        const fileStats = { file, path: fPath };

        // is the file a directory ? 
        // if yes, traverse it also, if no just add it to the result
        if (fs.statSync(fPath).isDirectory()) {
            fileStats.type = 'dir';
            fileStats.files = [];
            result.push(fileStats);
            return traverseDirectory(fPath, fileStats.files)
        }

        fileStats.type = 'file';
        result.push(fileStats);
    });
    return result;
};

function isEmpty(value) {
    return (
        value === null || value === undefined || value === '' ||
        (Array.isArray(value) && value.length === 0) ||
        (!(value instanceof Date) && typeof value === 'object' && Object.keys(value).length === 0)
    );
}

function todayString() {
    const today = new Date();
    return today.toISOString().split('T')[0]; // YYYY-MM-DD format
}

module.exports = {
    reformatSymbolForHK,
    traverseDirectory,
    isEmpty,
    todayString
};