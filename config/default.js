const config = {
    db: {
        sqlite: {
            file: 'data/hkex-market-breadth.db'
        }
    },
    file: {
        path: {
            us: 'C:/Users/user/Downloads/d_us_txt.zip',
            hk: 'C:/Users/user/Downloads/d_hk_txt.zip',
            extract: 'c:/Users/user/Downloads',
            load: '/data/daily/hk/hkex stocks'
        } 
    },
    hkex: {
        url: 'https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx'
    }
};

// export default config;

module.exports = config;