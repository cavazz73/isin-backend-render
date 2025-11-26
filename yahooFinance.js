/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Yahoo Finance API Client (Primary Source - Unlimited, Free)
 * Enhanced support for European/Italian stocks
 */

const axios = require('axios');

class YahooFinanceClient {
    constructor() {
        this.baseUrl = 'https://query1.finance.yahoo.com';
        this.baseUrlV7 = 'https://query1.finance.yahoo.com/v7';
        this.baseUrlV8 = 'https://query2.finance.yahoo.com/v8';
        
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

        // Exchange to currency mapping (comprehensive)
        this.exchangeCurrencyMap = {
            // Italian
            'MIL': 'EUR', 'Milan': 'EUR', 'MI': 'EUR',
            // US
            'NYSE': 'USD', 'NMS': 'USD', 'NYQ': 'USD', 'NASDAQ': 'USD', 'NGM': 'USD', 'NCM': 'USD', 'PCX': 'USD',
            // UK
            'LSE': 'GBP', 'LON': 'GBP', 'L': 'GBP', 'IOB': 'GBP',
            // Germany
            'FRA': 'EUR', 'XETRA': 'EUR', 'GER': 'EUR', 'ETR': 'EUR', 'BER': 'EUR', 'STU': 'EUR', 'MUN': 'EUR', 'HAM': 'EUR', 'DUS': 'EUR',
            // France
            'PAR': 'EUR', 'EPA': 'EUR', 'ENX': 'EUR',
            // Netherlands
            'AMS': 'EUR', 'AS': 'EUR',
            // Belgium
            'BRU': 'EUR', 'EBR': 'EUR',
            // Spain
            'BME': 'EUR', 'MCE': 'EUR', 'MC': 'EUR',
            // Portugal
            'LIS': 'EUR', 'ELI': 'EUR',
            // Switzerland
            'SWX': 'CHF', 'VTX': 'CHF', 'SW': 'CHF',
            // Nordic
            'STO': 'SEK', 'OMX': 'SEK', 'HEL': 'EUR', 'CPH': 'DKK', 'OSL': 'NOK', 'ICE': 'ISK',
            // Other European
            'VIE': 'EUR', 'WSE': 'PLN', 'PRA': 'CZK', 'BUD': 'HUF', 'ATH': 'EUR', 'IST': 'TRY',
            // Asia Pacific
            'TYO': 'JPY', 'HKG': 'HKD', 'SHG': 'CNY', 'SHE': 'CNY', 'KRX': 'KRW', 'KSC': 'KRW',
            'TWO': 'TWD', 'TPE': 'TWD', 'NSE': 'INR', 'BSE': 'INR', 'BOM': 'INR',
            'SES': 'SGD', 'SGX': 'SGD', 'ASX': 'AUD', 'NZX': 'NZD',
            // Americas
            'TSX': 'CAD', 'TOR': 'CAD', 'MEX': 'MXN', 'SAO': 'BRL', 'BVMF': 'BRL'
        };
    }

    /**
     * Normalize symbol - convert Italian names to Yahoo format
     */
    normalizeSymbol(query) {
        const upperQuery = query.toUpperCase().trim();
        
        // Check if it's a known Italian stock
        if (this.italianStocksMap[upperQuery]) {
            return this.italianStocksMap[upperQuery];
        }
        
        // If already has .MI suffix, return as-is
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
     * Get country from exchange code
     */
    getCountryFromExchange(exchange) {
        const exchangeCountryMap = {
            'MIL': 'IT', 'Milan': 'IT', 'MI': 'IT',
            'NYSE': 'US', 'NMS': 'US', 'NYQ': 'US', 'NASDAQ': 'US', 'NGM': 'US', 'NCM': 'US', 'PCX': 'US',
            'LSE': 'GB', 'LON': 'GB', 'L': 'GB', 'IOB': 'GB',
            'FRA': 'DE', 'XETRA': 'DE', 'GER': 'DE', 'ETR': 'DE',
            'PAR': 'FR', 'EPA': 'FR', 'ENX': 'FR',
            'AMS': 'NL', 'AS': 'NL',
            'BRU': 'BE', 'EBR': 'BE',
            'BME': 'ES', 'MCE': 'ES', 'MC': 'ES',
            'SWX': 'CH', 'VTX': 'CH',
            'STO': 'SE', 'OMX': 'SE',
            'HEL': 'FI',
            'CPH': 'DK',
            'OSL': 'NO',
            'TYO': 'JP',
            'HKG': 'HK',
            'SHG': 'CN', 'SHE': 'CN',
            'TSX': 'CA', 'TOR': 'CA',
            'ASX': 'AU',
            'NSE': 'IN', 'BSE': 'IN', 'BOM': 'IN'
        };
        return exchangeCountryMap[exchange] || 'US';
    }

    /**
     * Check if symbol is European
     */
    isEuropeanSymbol(symbol) {
        const upperSymbol = symbol.toUpperCase();
        return upperSymbol.endsWith('.MI') ||
               upperSymbol.endsWith('.PA') ||
               upperSymbol.endsWith('.DE') ||
               upperSymbol.endsWith('.AS') ||
               upperSymbol.endsWith('.L') ||
               upperSymbol.endsWith('.MC') ||
               upperSymbol.endsWith('.SW') ||
               this.italianStocksMap[upperSymbol] !== undefined;
    }

    async search(query) {
        try {
            // Normalize query for Italian/European stocks
            const searchQuery = this.normalizeSymbol(query);
            console.log(`[Yahoo] Searching for: "${query}" -> "${searchQuery}"`);
            
            const url = `${this.baseUrlV7}/finance/search`;
            const response = await axios.get(url, {
                params: {
                    q: searchQuery,
                    lang: 'en-US',
                    region: 'US',
                    quotesCount: 15,
                    newsCount: 0,
                    enableFuzzyQuery: true
                },
                headers: { 
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                timeout: 10000
            });

            if (!response.data?.quotes) {
                return { success: false, results: [] };
            }

            const results = response.data.quotes
                .filter(q => q.symbol && (q.quoteType === 'EQUITY' || q.quoteType === 'ETF'))
                .map(quote => ({
                    symbol: quote.symbol,
                    name: quote.shortname || quote.longname,
                    description: quote.longname || quote.shortname,
                    type: quote.quoteType === 'ETF' ? 'ETF' : 'Stock',
                    exchange: quote.exchange,
                    currency: this.getCurrencyFromExchange(quote.exchange) || quote.currency || 'USD',
                    isin: quote.isin || null,
                    country: this.getCountryFromExchange(quote.exchange),
                    price: null,
                    change: null,
                    changePercent: null
                }));

            return { success: true, results, source: 'yahoo' };

        } catch (error) {
            console.error('[Yahoo] Search error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    async getQuote(symbol) {
        try {
            // Normalize symbol for European stocks
            const normalizedSymbol = this.normalizeSymbol(symbol);
            console.log(`[Yahoo] Getting quote for: "${symbol}" -> "${normalizedSymbol}"`);
            
            const url = `${this.baseUrlV7}/finance/quote`;
            const response = await axios.get(url, {
                params: {
                    symbols: normalizedSymbol,
                    fields: 'symbol,regularMarketPrice,regularMarketChange,regularMarketChangePercent,currency,shortName,longName,exchange,marketCap,regularMarketOpen,regularMarketDayHigh,regularMarketDayLow,regularMarketVolume'
                },
                headers: { 
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                timeout: 10000
            });

            const quote = response.data?.quoteResponse?.result?.[0];
            if (!quote) {
                console.warn(`[Yahoo] No quote found for ${normalizedSymbol}`);
                return { success: false };
            }

            // Determine currency from exchange if not provided
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

    async getHistoricalData(symbol, period = '1M') {
        try {
            const normalizedSymbol = this.normalizeSymbol(symbol);
            console.log(`[Yahoo] Getting historical data for: "${symbol}" -> "${normalizedSymbol}", period: ${period}`);
            
            const periodMap = {
                '1D': { range: '1d', interval: '5m' },
                '1W': { range: '5d', interval: '1h' },
                '1M': { range: '1mo', interval: '1d' },
                '3M': { range: '3mo', interval: '1d' },
                '6M': { range: '6mo', interval: '1d' },
                'YTD': { range: 'ytd', interval: '1d' },
                '1Y': { range: '1y', interval: '1d' },
                '3Y': { range: '3y', interval: '1wk' },
                '5Y': { range: '5y', interval: '1wk' },
                'MAX': { range: 'max', interval: '1mo' }
            };

            const params = periodMap[period] || periodMap['1M'];
            const url = `${this.baseUrlV8}/finance/chart/${normalizedSymbol}`;
            
            const response = await axios.get(url, {
                params: { range: params.range, interval: params.interval },
                headers: { 
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                timeout: 15000
            });

            const result = response.data?.chart?.result?.[0];
            const timestamps = result?.timestamp;
            const quotes = result?.indicators?.quote?.[0];

            if (!timestamps || !quotes) {
                console.warn(`[Yahoo] No historical data for ${normalizedSymbol}`);
                return { success: false };
            }

            const historicalData = timestamps
                .map((timestamp, index) => ({
                    date: new Date(timestamp * 1000).toISOString().split('T')[0],
                    open: quotes.open[index],
                    high: quotes.high[index],
                    low: quotes.low[index],
                    close: quotes.close[index],
                    volume: quotes.volume[index]
                }))
                .filter(item => item.close !== null);

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

    async searchByISIN(isin) {
        return this.search(isin);
    }
}

module.exports = YahooFinanceClient;
