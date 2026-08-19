require('dotenv').config();
const logger = require('./logger')
const { ANALYST_DATA_INI } = process.env;
const ini = require('ini');
const fs = require('fs');
const analystConfig = ini.parse(fs.readFileSync(ANALYST_DATA_INI, 'utf-8'));
const sqliteDb = require('better-sqlite3')(analystConfig.SQLITE.FILE, {});
sqliteDb.pragma('journal_mode = WAL');
const sqliteHelper = require('./sqliteHelper.js');

function sqliteLocalUpdateSectorsStats() {
    const sqlSectorStats =
        `select 
        dt,
        case sector
        when 'Basic Materials' then 'XLB'
        when 'Communication Services' then 'XLC'
        when 'Consumer Cyclical' then 'XLY'
        when 'Consumer Defensive' then 'XLP'
        when 'Energy' then 'XLE'
        when 'Financial Services' then 'XLF'
        when 'Healthcare' then 'XLV'
        when 'Industrials' then 'XLI'
        when 'Real Estate' then 'XLRE'
        when 'Technology' then 'XLK'
        when 'Utilities' then 'XLU'   
        when '基礎材料' then 'XLB'
        when '通訊服務' then 'XLC'
        when '非必需消費品' then 'XLY'
        when '必需消費品' then 'XLP'
        when '能源' then 'XLE'
        when '金融服務' then 'XLF'
        when '醫療保健' then 'XLV'
        when '工業' then 'XLI'
        when '房地產' then 'XLRE'
        when '科技' then 'XLK'
        when '公用事業' then 'XLU'           
        else 'XLX'
        end sector,
        round(cast(up4pct1d as float) / tot * 100.0, 2) u4sm, 
        round(cast(dn4pct1d as float) / tot * 100.0, 2) d4sm,
        round(cast(up0pct1d as float) / tot * 100.0, 2) sm
        FROM
        (
            select 
                stock.sector, DAILY_STOCK_STATS.dt,
                sum(case when chg_pct_1d >= 4 then 1 else 0 end)  up4pct1d  , 
                sum(case when chg_pct_1d<= -4 then 1 else 0 end)  dn4pct1d ,
                sum(case when chg_pct_1d > 0  then 1 else 0 end)  up0pct1d ,
                sum(case when chg_pct_1d < 0 then 1 else 0 end)  dn0pct1d ,
                sum(case when chg_pct_1d < 0 then 1 else 0 end)  dn0pct1d ,
                count(1) tot
            from stock, DAILY_STOCK_STATS
            where stock.symbol = DAILY_STOCK_STATS.symbol
            and sector != 'UNKNOWN'
            group by stock.sector, DAILY_STOCK_STATS.dt
            order by sector
        ) order by dt, sector
        `;

    const stmt = sqliteDb.prepare(sqlSectorStats);
    const sectorStats = stmt.all();
    
    var stats = newSectorStatus();
    var lastStats = newSectorStatus();
    var lastdt = '';
    var count = 0;

    sectorStats.forEach((sectorStat) => {
        if(lastdt != '' && lastdt != sectorStat.dt) {
            newSectorStatusToDb(lastStats);
            stats = newSectorStatus();
            count++;
        }

        stats.dt = sectorStat.dt;

        switch(sectorStat.sector) {
            case 'XLB':
                stats.XLB_U4SM = sectorStat.u4sm;
                stats.XLB_D4SM = sectorStat.d4sm;
                stats.XLB_SM = sectorStat.sm;
                break;
            case 'XLC':
                stats.XLC_U4SM = sectorStat.u4sm;
                stats.XLC_D4SM = sectorStat.d4sm;
                stats.XLC_SM = sectorStat.sm;
                break;
            case 'XLY':
                stats.XLY_U4SM = sectorStat.u4sm;
                stats.XLY_D4SM = sectorStat.d4sm;
                stats.XLY_SM = sectorStat.sm;
                break;
            case 'XLP':
                stats.XLP_U4SM = sectorStat.u4sm;
                stats.XLP_D4SM = sectorStat.d4sm;
                stats.XLP_SM = sectorStat.sm;
                break;
            case 'XLE':
                stats.XLE_U4SM = sectorStat.u4sm;
                stats.XLE_D4SM = sectorStat.d4sm;
                stats.XLE_SM = sectorStat.sm;
                break;
            case 'XLF':
                stats.XLF_U4SM = sectorStat.u4sm;
                stats.XLF_D4SM = sectorStat.d4sm;
                stats.XLF_SM = sectorStat.sm;
                break;
            case 'XLV':
                stats.XLV_U4SM = sectorStat.u4sm;
                stats.XLV_D4SM = sectorStat.d4sm;
                stats.XLV_SM = sectorStat.sm;
                break;
            case 'XLI':
                stats.XLI_U4SM = sectorStat.u4sm;
                stats.XLI_D4SM = sectorStat.d4sm;
                stats.XLI_SM = sectorStat.sm;
                break;
            case 'XLRE':
                stats.XLRE_U4SM = sectorStat.u4sm;
                stats.XLRE_D4SM = sectorStat.d4sm;
                stats.XLRE_SM = sectorStat.sm;
                break;
            case 'XLK':
                stats.XLK_U4SM = sectorStat.u4sm;
                stats.XLK_D4SM = sectorStat.d4sm;
                stats.XLK_SM = sectorStat.sm;
                break;
            case 'XLU':
                stats.XLU_U4SM = sectorStat.u4sm;
                stats.XLU_D4SM = sectorStat.d4sm;
                stats.XLU_SM = sectorStat.sm;
                break;
            case 'XLX':
                stats.XLX_U4SM = sectorStat.u4sm;
                stats.XLX_D4SM = sectorStat.d4sm;
                stats.XLX_SM = sectorStat.sm;
                break;
            default:
                logger.info("[ERROR] Unknown sector: " + sectorStat.sector + " on " + sectorStat.dt);
                break;
        }

        lastdt = sectorStat.dt;
        lastStats = stats;  
    });

    newSectorStatusToDb(lastStats);
    logger.info("Sectors stats updated. Total records: " + count);
}

function newSectorStatusToDb(stats) {
    const INSERT_SQL = 
      `REPLACE INTO DAILY_SECTORS_STATS 
      (dt, 
      XLB_U4SM,  XLB_D4SM,  XLB_SM, 
      XLC_U4SM,  XLC_D4SM,  XLC_SM, 
      XLY_U4SM,  XLY_D4SM,  XLY_SM, 
      XLP_U4SM,  XLP_D4SM,  XLP_SM, 
      XLE_U4SM,  XLE_D4SM,  XLE_SM, 
      XLF_U4SM,  XLF_D4SM,  XLF_SM, 
      XLV_U4SM,  XLV_D4SM,  XLV_SM, 
      XLI_U4SM,  XLI_D4SM,  XLI_SM, 
      XLRE_U4SM, XLRE_D4SM, XLRE_SM, 
      XLK_U4SM,  XLK_D4SM,  XLK_SM, 
      XLU_U4SM,  XLU_D4SM,  XLU_SM, 
      XLX_U4SM,  XLX_D4SM,  XLX_SM       
      ) 
      VALUES (?,
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?, 
      ?, ?, ?                              
      )`

    const stmtInsert = sqliteDb.prepare(INSERT_SQL);
    const info = stmtInsert.run(stats.dt,
        stats.XLB_U4SM, stats.XLB_D4SM, stats.XLB_SM,
        stats.XLC_U4SM, stats.XLC_D4SM, stats.XLC_SM,
        stats.XLY_U4SM, stats.XLY_D4SM, stats.XLY_SM,
        stats.XLP_U4SM, stats.XLP_D4SM, stats.XLP_SM,
        stats.XLE_U4SM, stats.XLE_D4SM, stats.XLE_SM,
        stats.XLF_U4SM, stats.XLF_D4SM, stats.XLF_SM,
        stats.XLV_U4SM, stats.XLV_D4SM, stats.XLV_SM,
        stats.XLI_U4SM, stats.XLI_D4SM, stats.XLI_SM,
        stats.XLRE_U4SM, stats.XLRE_D4SM, stats.XLRE_SM,
        stats.XLK_U4SM, stats.XLK_D4SM, stats.XLK_SM,
        stats.XLU_U4SM, stats.XLU_D4SM, stats.XLU_SM,
        stats.XLX_U4SM, stats.XLX_D4SM, stats.XLX_SM
    );
    if (info.changes <= 0) {
        logger.info("[ERROR] Inserted " + marketStat.dt);
    }   
}

function newSectorStatus() {
    var sectorStat = {
        dt: '',
        XLB_U4SM: 0, XLB_D4SM: 0, XLB_SM: 0,
        XLC_U4SM: 0, XLC_D4SM: 0, XLC_SM: 0,
        XLY_U4SM: 0, XLY_D4SM: 0, XLY_SM: 0,
        XLP_U4SM: 0, XLP_D4SM: 0, XLP_SM: 0,
        XLE_U4SM: 0, XLE_D4SM: 0, XLE_SM: 0,
        XLF_U4SM: 0, XLF_D4SM: 0, XLF_SM: 0,
        XLV_U4SM: 0, XLV_D4SM: 0, XLV_SM: 0,
        XLI_U4SM: 0, XLI_D4SM: 0, XLI_SM: 0,
        XLRE_U4SM: 0, XLRE_D4SM: 0, XLRE_SM: 0,
        XLK_U4SM: 0, XLK_D4SM: 0, XLK_SM: 0,
        XLU_U4SM: 0, XLU_D4SM: 0, XLU_SM: 0,
        XLX_U4SM: 0, XLX_D4SM: 0, XLX_SM: 0
    };

    return sectorStat;
}

/**
 * update market stats
 */
function sqliteLocalUpdateMarketStats() {
    const sqlMarketStats = 
    `select DAILY_STOCK_STATS.dt, 
        sum(case when chg_pct_1d >= 4 then 1 else 0 end)  up4pct1d, 
        sum(case when chg_pct_1d<= -4 then 1 else 0 end)  dn4pct1d, 
        sum(case when chg_pct_100d >= 25 then 1 else 0 end)  up25pctin100d, 
        sum(case when chg_pct_100d<= -25 then 1 else 0 end)  dn25pctin100d, 
        sum(case when chg_pct_20d >= 25 then 1 else 0 end)  up25pctin20d, 
        sum(case when chg_pct_20d<= -25 then 1 else 0 end)  dn25pctin20d, 
        sum(case when chg_pct_20d >= 50 then 1 else 0 end)  up50pctin20d, 
        sum(case when chg_pct_20d<= -50 then 1 else 0 end)  dn50pctin20d, 
        count(1) noofstocks, 
        round(sum(above_200d_sma) / count(1) * 100,2) above200smapct, 
        round(sum(above_150d_sma) / count(1) * 100,2) above150smapct, 
        round(sum(above_50d_sma) / count(1) * 100,2) above50smapct, 
        round(sum(above_20d_sma) / count(1) * 100,2) above20smapct,
        hsi, hsce 
        from DAILY_STOCK_STATS 
        left outer join 
        (
            select dt, close as hsi from daily_stock_price
            where symbol = '^HSI'
        ) HSI on DAILY_STOCK_STATS.dt = HSI.dt
        left outer join 
        (
            select dt, close as hsce from daily_stock_price
            where symbol = '^HSCE'
        ) HSCE on DAILY_STOCK_STATS.dt = HSCE.dt        
        group by DAILY_STOCK_STATS.dt 
        order by DAILY_STOCK_STATS.dt desc`;

    const stmt = sqliteDb.prepare(sqlMarketStats);
    const marketStats = stmt.all();
    
    const INSERT_SQL = 
      `REPLACE INTO DAILY_MARKET_STATS 
      (dt, up4pct1d, dn4pct1d, up25pctin100d, dn25pctin100d, up25pctin20d, dn25pctin20d, 
      up50pctin20d, dn50pctin20d, noofstocks, above200smapct, above150smapct, above50smapct, above20smapct, hsi, hsce) 
      VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    const stmtInsert = sqliteDb.prepare(INSERT_SQL);
    marketStats.forEach((marketStat) => {
        const info = stmtInsert.run(marketStat.dt, marketStat.up4pct1d, marketStat.dn4pct1d, marketStat.up25pctin100d, marketStat.dn25pctin100d, 
            marketStat.up25pctin20d, marketStat.dn25pctin20d, marketStat.up50pctin20d, marketStat.dn50pctin20d, marketStat.noofstocks, 
            marketStat.above200smapct, marketStat.above150smapct, marketStat.above50smapct, marketStat.above20smapct, marketStat.hsi, marketStat.hsce);
        if (info.changes <= 0) {
            logger.info("[ERROR] Inserted " + marketStat.dt);
        }
    });

    logger.info("Market stats updated. Total records: " + marketStats.length);
}

module.exports = {
    sqliteLocalUpdateMarketStats,
    sqliteLocalUpdateSectorsStats
}