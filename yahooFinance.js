/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * Yahoo Finance API Client (Primary Source - Unlimited, Free)
 * V4.1 - WITH FUNDAMENTAL DATA
 */

const axios = require('axios');

class YahooFinanceClient {
    constructor() {
        this.baseUrl = 'https://query1.finance.yahoo.com';
        this.baseUrlV7 = 'https://query1.finance.yahoo.com/v7';
        this.baseUrlV8 = 'https://query2.finance.yahoo.com/v8';
    }

    async search(query) {
        try {
            // Yahoo supporta azioni italiane con suffisso .MI (Milano)
            // ENEL → ENEL.MI, ENI → ENI.MI
            const searchQuery = this.normalizeItalianSymbol(query);
            
            const url = `${this.baseUrlV7}/finance/search`;
            const response = await axios.get(url, {
                params: {
                    q: searchQuery,
                    lang: 'en-US',
                    region: 'US',
                    quotesCount: 10,
                    newsCount: 0,
                    enableFuzzyQuery: false
                },
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Referer': 'https://finance.yahoo.com',
                    'Origin': 'https://finance.yahoo.com'
                },
                timeout: 15000
            });

            if (!response.data?.quotes) {
                return { success: false, results: [] };
            }

            const results = response.data.quotes
                .filter(q => q.symbol && q.quoteType === 'EQUITY')
                .map(quote => ({
                    symbol: quote.symbol,
                    name: quote.shortname || quote.longname,
                    description: quote.longname || quote.shortname,
                    type: 'Stock',
                    exchange: quote.exchange,
                    currency: quote.currency || 'USD',
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
            const url = `${this.baseUrlV7}/finance/quote`;
            const response = await axios.get(url, {
                params: {
                    symbols: symbol,
                    // ✅ SIMPLIFIED: Only request essential fields to avoid rate limiting
                    fields: 'symbol,regularMarketPrice,regularMarketChange,regularMarketChangePercent,currency,shortName,longName,exchange,marketCap'
                },
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Referer': 'https://finance.yahoo.com',
                    'Origin': 'https://finance.yahoo.com'
                },
                timeout: 15000
            });

            const quote = response.data?.quoteResponse?.result?.[0];
            if (!quote) return { success: false };

            return {
                success: true,
                data: {
                    symbol: quote.symbol,
                    name: quote.shortName || quote.longName,
                    description: quote.longName || quote.shortName,  // ✅ DESCRIPTION RESTORED
                    price: quote.regularMarketPrice,
                    change: quote.regularMarketChange,
                    changePercent: quote.regularMarketChangePercent,
                    currency: quote.currency || 'USD',
                    exchange: quote.exchange,
                    // ✅ ONLY BASIC FIELDS (fundamentals from TwelveData)
                    marketCap: quote.marketCap || null,
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
            const url = `${this.baseUrlV8}/finance/chart/${symbol}`;
            
            const response = await axios.get(url, {
                params: { range: params.range, interval: params.interval },
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://finance.yahoo.com',
                    'Origin': 'https://finance.yahoo.com'
                },
                timeout: 15000
            });

            const result = response.data?.chart?.result?.[0];
            const timestamps = result?.timestamp;
            const quotes = result?.indicators?.quote?.[0];

            if (!timestamps || !quotes) {
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
                symbol: symbol,
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

    /**
     * Normalizza simboli italiani aggiungendo .MI se necessario
     */
    normalizeItalianSymbol(query) {
        const italianStocks = {
            'ENEL': 'ENEL.MI',
            'ENI': 'ENI.MI',
            'INTESA': 'ISP.MI',
            'UNICREDIT': 'UCG.MI',
            'GENERALI': 'G.MI',
            'FERRARI': 'RACE.MI',
            'STELLANTIS': 'STLA.MI',
            'LEONARDO': 'LDO.MI',
            'PRYSMIAN': 'PRY.MI',
            'TELECOM': 'TIT.MI'
        };

        const upperQuery = query.toUpperCase();
        return italianStocks[upperQuery] || query;
    }

    /**
     * Determina il paese dall'exchange
     */
    getCountryFromExchange(exchange) {
        const exchangeCountryMap = {
            'MIL': 'IT', 'Milan': 'IT',
            'NYSE': 'US', 'NMS': 'US', 'NYQ': 'US',
            'LSE': 'GB', 'LON': 'GB',
            'FRA': 'DE', 'XETRA': 'DE',
            'PAR': 'FR', 'EPA': 'FR',
            'AMS': 'NL',
            'SWX': 'CH',
            'BME': 'ES'
        };
        return exchangeCountryMap[exchange] || 'US';
    }
}

module.exports = YahooFinanceClient;
