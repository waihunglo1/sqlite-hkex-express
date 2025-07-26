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
            load: {
                dir1 : '/data/daily/hk/hkex stocks',
                dir2 : '/data/daily/hk/hkex etfs',
                dir3 : '/data/daily/hk/hkex-quot'
            }
        } 
    },
    hkex: {
        enable: true,
        url: 'https://www.hkex.com.hk/chi/services/trading/securities/securitieslists/ListOfSecurities_c.xlsx',
        download: {
            path: 'C:/Users/user/Downloads',
            filePattern: 'd(\\d{6})e.htm'
        }
    }
};

// export default config;

module.exports = config;