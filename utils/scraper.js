const axios = require('axios');
const cheerio = require('cheerio');
const https = require('https');
const fs = require('fs');

async function scrapeData(url) {
    const html = await axios.get(url, {
        headers: {
            'Cache-Control': 'no-cache, no-store, must-revalidate', // Prevent caching and require revalidation
            'Pragma': 'no-cache', // For backward compatibility with HTTP/1.0
            'Expires': '0' // Indicates the content is already expired
        }
    });
    const $ = cheerio.load(html.data);

    // Example: Get the title of the page
    const pageTitle = $('title').text();
    console.log(`Page Title: ${pageTitle}`);

    // Example: Get all links from the page
    const links = [];
    $('font').each((i, element) => {
        links.push($(element));
    });
    console.log('Links:', links);
}


const url = 'https://www.hkex.com.hk/eng/stat/smstat/dayquot/d250613e.htm';
scrapeData(url);
