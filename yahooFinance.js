/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Yahoo Finance API Client using yahoo-finance2 library v3
 * More reliable than direct API calls
 */

const YahooFinance = require('yahoo-finance2').default;
const yahooFinance = new YahooFinance();

class YahooFinanceClient {
    constructor() {
        // Extended Italian stocks mapping (symbol -> Yahoo symbol)
        this.italianStocksMap = {
            'ENEL': 'ENEL.MI',
            'ENI': 'ENI.MI',
            'INTESA': 'ISP.MI',
            'ISP': 'ISP.MI',
            'UNICREDIT': 'UCG.MI',
            'UCG': 'UCG.MI',
            'GENERALI': 'G.MI',
            'FERRARI': 'RACE.MI',
            'RACE': 'RACE.MI',
            'STELLANTIS': 'STLAM.MI',
            'STLA': 'STLAM.MI',
            'STLAM': 'STLAM.MI',
            'LEONARDO': 'LDO.MI',
            'LDO': 'LDO.MI',
            'PRYSMIAN': 'PRY.MI',
            'PRY': 'PRY.MI',
            'TELECOM': 'TIT.MI',
            'TIT': 'TIT.MI',
            'TIM': 'TIT.MI',
            'TELECOM ITALIA': 'TIT.MI',
            'STM': 'STMMI.MI',
            'STMICROELECTRONICS': 'STMMI.MI',
            'TENARIS': 'TEN.MI',
            'TEN': 'TEN.MI',
            'MONCLER': 'MONC.MI',
            'MONC': 'MONC.MI',
            'CAMPARI': 'CPR.MI',
            'CPR': 'CPR.MI',
            'PIRELLI': 'PIRC.MI',
            'PIRC': 'PIRC.MI',
            'AMPLIFON': 'AMP.MI',
            'AMP': 'AMP.MI',
            'RECORDATI': 'REC.MI',
            'REC': 'REC.MI',
            'DIASORIN': 'DIA.MI',
            'DIA': 'DIA.MI',
            'NEXI': 'NEXI.MI',
            'INWIT': 'INW.MI',
            'INW': 'INW.MI',
            'SNAM': 'SRG.MI',
            'SRG': 'SRG.MI',
            'TERNA': 'TRN.MI',
            'TRN': 'TRN.MI',
            'ITALGAS': 'IG.MI',
            'IG': 'IG.MI',
            'MEDIOBANCA': 'MB.MI',
            'MB': 'MB.MI',
            'BPER': 'BPE.MI',
            'BPE': 'BPE.MI',
            'BANCO BPM': 'BAMI.MI',
            'BAMI': 'BAMI.MI',
            'POSTE': 'PST.MI',
            'PST': 'PST.MI',
            'POSTE ITALIANE': 'PST.MI',
            'A2A': 'A2A.MI',
            'HERA': 'HER.MI',
            'HER': 'HER.MI',
            'SAIPEM': 'SPM.MI',
            'SPM': 'SPM.MI',
            'ATLANTIA': 'ATL.MI',
            'ATL': 'ATL.MI',
            'FINECOBANK': 'FBK.MI',
            'FBK': 'FBK.MI',
            'FINECO': 'FBK.MI',
            'ASSICURAZIONI GENERALI': 'G.MI',
            'LUXOTTICA': 'LUX.MI',
            'BUZZI': 'BZU.MI',
            'BZU': 'BZU.MI',
            'BRUNELLO CUCINELLI': 'BC.MI',
            'BC': 'BC.MI',
            'INTERPUMP': 'IP.MI',
            'IP': 'IP.MI',
            'IVECO': 'IVG.MI',
            'IVG': 'IVG.MI'
        };

        // Exchange to currency mapping
        this.exchangeCurrencyMap = {
            'MIL': 'EUR', 'Milan': 'EUR', 'MI': 'EUR',
            'NYSE': 'USD', 'NMS': 'USD', 'NYQ': 'USD', 'NASDAQ': 'USD', 'NGM': 'USD', 'NCM': 'USD', 'PCX': 'USD',
            'LSE': 'GBP', 'LON': 'GBP', 'L': 'GBP', 'IOB': 'GBP',
            'FRA': 'EUR', 'XETRA': 'EUR', 'GER': 'EUR', 'ETR': 'EUR', 'BER': 'EUR', 'STU': 'EUR', 'MUN': 'EUR', 'HAM': 'EUR', 'DUS': 'EUR',
            'PAR': 'EUR', 'EPA': 'EUR', 'ENX': 'EUR',
            'AMS': 'EUR', 'AS': 'EUR',
            'BRU': 'EUR', 'EBR': 'EUR',
            'BME': 'EUR', 'MCE': 'EUR', 'MC': 'EUR',
            'LIS': 'EUR', 'ELI': 'EUR',
            'SWX': 'CHF', 'VTX': 'CHF', 'SW': 'CHF',
            'STO': 'SEK', 'OMX': 'SEK', 'HEL': 'EUR', 'CPH': 'DKK', 'OSL': 'NOK', 'ICE': 'ISK',
            'VIE': 'EUR', 'WSE': 'PLN', 'PRA': 'CZK', 'BUD': 'HUF', 'ATH': 'EUR', 'IST': 'TRY',
            'TYO': 'JPY', 'HKG': 'HKD', 'SHG': 'CNY', 'SHE': 'CNY', 'KRX': 'KRW', 'KSC': 'KRW',
            'TWO': 'TWD', 'TPE': 'TWD', 'NSE': 'INR', 'BSE': 'INR', 'BOM': 'INR',
            'SES': 'SGD', 'SGX': 'SGD', 'ASX': 'AUD', 'NZX': 'NZD',
            'TSX': 'CAD', 'TOR': 'CAD', 'MEX': 'MXN', 'SAO': 'BRL', 'BVMF': 'BRL'
        };
    }

    /**
     * Normalize symbol - convert Italian names to Yahoo format
     */
    normalizeSymbol(query) {
        const upperQuery = query.toUpperCase().trim();
        
        if (this.italianStocksMap[upperQuery]) {
            return this.italianStocksMap[upperQuery];
        }
        
        if (upperQuery.endsWith('.MI')) {
            return upperQuery;
        }
        
        return query;
    }

    /**
     * Get currency from exchange code
     */
    getCurrencyFromExchange(exchange) {
        if (!exchange) return 'USD';
        return this.exchangeCurrencyMap[exchange] || this.exchangeCurrencyMap[exchange.toUpperCase()] || 'USD';
    }

    /**
     * Search for stocks
     */
    async search(query) {
        try {
            const searchQuery = this.normalizeSymbol(query);
            console.log(`[Yahoo] Searching for: "${query}" -> "${searchQuery}"`);
            
            const results = await yahooFinance.search(searchQuery, {
                quotesCount: 15,
                newsCount: 0
            });

            if (!results.quotes || results.quotes.length === 0) {
                return { success: false, results: [] };
            }

            const mappedResults = results.quotes
                .filter(q => q.symbol && (q.quoteType === 'EQUITY' || q.quoteType === 'ETF'))
                .map(quote => ({
                    symbol: quote.symbol,
                    name: quote.shortname || quote.longname,
                    description: quote.longname || quote.shortname,
                    type: quote.quoteType === 'ETF' ? 'ETF' : 'Stock',
                    exchange: quote.exchange,
                    currency: this.getCurrencyFromExchange(quote.exchange) || 'USD',
                    isin: null,
                    country: this.getCountryFromExchange(quote.exchange),
                    price: null,
                    change: null,
                    changePercent: null
                }));

            return { success: true, results: mappedResults, source: 'yahoo' };

        } catch (error) {
            console.error('[Yahoo] Search error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    /**
     * Get quote for a symbol
     */
    async getQuote(symbol) {
        try {
            const normalizedSymbol = this.normalizeSymbol(symbol);
            console.log(`[Yahoo] Getting quote for: "${symbol}" -> "${normalizedSymbol}"`);
            
            const quote = await yahooFinance.quote(normalizedSymbol);

            if (!quote || !quote.regularMarketPrice) {
                console.warn(`[Yahoo] No quote found for ${normalizedSymbol}`);
                return { success: false };
            }

            const currency = quote.currency || this.getCurrencyFromExchange(quote.exchange) || 'USD';

            return {
                success: true,
                data: {
                    symbol: quote.symbol,
                    name: quote.shortName || quote.longName,
                    price: quote.regularMarketPrice,
                    open: quote.regularMarketOpen,
                    high: quote.regularMarketDayHigh,
                    low: quote.regularMarketDayLow,
                    volume: quote.regularMarketVolume,
                    change: quote.regularMarketChange,
                    changePercent: quote.regularMarketChangePercent,
                    currency: currency,
                    exchange: quote.exchange,
                    marketCap: quote.marketCap,
                    timestamp: new Date().toISOString()
                },
                source: 'yahoo'
            };

        } catch (error) {
            console.error('[Yahoo] Quote error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Get historical data
     */
    async getHistoricalData(symbol, period = '1M') {
        try {
            const normalizedSymbol = this.normalizeSymbol(symbol);
            console.log(`[Yahoo] Getting historical data for: "${symbol}" -> "${normalizedSymbol}", period: ${period}`);
            
            // Map period to date range
            const periodConfig = this.getPeriodConfig(period);
            
            const result = await yahooFinance.historical(normalizedSymbol, {
                period1: periodConfig.startDate,
                period2: new Date(),
                interval: periodConfig.interval
            });

            if (!result || result.length === 0) {
                console.warn(`[Yahoo] No historical data for ${normalizedSymbol}`);
                return { success: false };
            }

            const historicalData = result.map(item => ({
                date: item.date.toISOString().split('T')[0],
                open: item.open,
                high: item.high,
                low: item.low,
                close: item.close,
                volume: item.volume
            })).filter(item => item.close !== null);

            return {
                success: true,
                symbol: normalizedSymbol,
                data: historicalData,
                source: 'yahoo'
            };

        } catch (error) {
            console.error('[Yahoo] Historical error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Get period configuration for historical data
     */
    getPeriodConfig(period) {
        const now = new Date();
        const configs = {
            '1D': { days: 1, interval: '1d' },
            '1W': { days: 7, interval: '1d' },
            '1M': { days: 30, interval: '1d' },
            '3M': { days: 90, interval: '1d' },
            '6M': { days: 180, interval: '1d' },
            'YTD': { startOfYear: true, interval: '1d' },
            '1Y': { days: 365, interval: '1d' },
            '3Y': { days: 365 * 3, interval: '1wk' },
            '5Y': { days: 365 * 5, interval: '1wk' },
            'MAX': { days: 365 * 20, interval: '1mo' }
        };

        const config = configs[period] || configs['1M'];
        let startDate;

        if (config.startOfYear) {
            startDate = new Date(now.getFullYear(), 0, 1);
        } else {
            startDate = new Date(now.getTime() - (config.days * 24 * 60 * 60 * 1000));
        }

        return {
            startDate,
            interval: config.interval
        };
    }

    /**
     * Get country from exchange code
     */
    getCountryFromExchange(exchange) {
        const exchangeCountryMap = {
            'MIL': 'IT', 'Milan': 'IT', 'MI': 'IT',
            'NYSE': 'US', 'NMS': 'US', 'NYQ': 'US', 'NASDAQ': 'US',
            'LSE': 'GB', 'LON': 'GB', 'L': 'GB',
            'FRA': 'DE', 'XETRA': 'DE', 'GER': 'DE', 'ETR': 'DE',
            'PAR': 'FR', 'EPA': 'FR', 'ENX': 'FR',
            'AMS': 'NL', 'AS': 'NL',
            'BME': 'ES', 'MCE': 'ES',
            'SWX': 'CH', 'VTX': 'CH'
        };
        return exchangeCountryMap[exchange] || 'US';
    }

    async searchByISIN(isin) {
        return this.search(isin);
    }
}

module.exports = YahooFinanceClient;
