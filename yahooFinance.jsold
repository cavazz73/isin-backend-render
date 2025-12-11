/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * Yahoo Finance Client - PRIMARY SOURCE (Unlimited, Free)
 * FIXED: Headers completi + Normalizzazione simboli italiani
 */

const axios = require('axios');

class YahooFinanceClient {
    constructor() {
        this.baseUrl = 'https://query1.finance.yahoo.com';
        this.baseUrlV7 = 'https://query1.finance.yahoo.com/v7';
        this.baseUrlV8 = 'https://query2.finance.yahoo.com/v8';
        
        // FIXED: Headers completi per evitare 401
        this.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://finance.yahoo.com'
        };
    }

    /**
     * Normalizza simboli italiani aggiungendo .MI
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
            'TELECOM': 'TIT.MI',
            'AZIMUT': 'AZM.MI',
            'POSTE': 'PST.MI'
        };

        const upperQuery = query.toUpperCase();
        return italianStocks[upperQuery] || query;
    }

    async search(query) {
        try {
            const searchQuery = this.normalizeItalianSymbol(query);
            console.log(`[Yahoo] Search: "${query}" -> "${searchQuery}"`);
            
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
                headers: this.headers,
                timeout: 10000
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

            console.log(`[Yahoo] Found ${results.length} results`);
            return { success: true, results, source: 'yahoo' };

        } catch (error) {
            console.error('[Yahoo] Search error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    async getQuote(symbol) {
        try {
            const normalizedSymbol = this.normalizeItalianSymbol(symbol);
            console.log(`[Yahoo] Quote: "${symbol}" -> "${normalizedSymbol}"`);
            
            const url = `${this.baseUrlV7}/finance/quote`;
            const response = await axios.get(url, {
                params: {
                    symbols: normalizedSymbol,
                    fields: 'symbol,regularMarketPrice,regularMarketChange,regularMarketChangePercent,currency,shortName,longName,exchange'
                },
                headers: this.headers,
                timeout: 10000
            });

            const quote = response.data?.quoteResponse?.result?.[0];
            if (!quote || !quote.regularMarketPrice) {
                return { success: false };
            }

            console.log(`[Yahoo] Quote OK: ${quote.regularMarketPrice} ${quote.currency}`);
            
            return {
                success: true,
                data: {
                    symbol: quote.symbol,
                    name: quote.shortName || quote.longName,
                    price: quote.regularMarketPrice,
                    change: quote.regularMarketChange,
                    changePercent: quote.regularMarketChangePercent,
                    currency: quote.currency || 'USD',
                    exchange: quote.exchange,
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
            const normalizedSymbol = this.normalizeItalianSymbol(symbol);
            
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
                headers: this.headers,
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

    getCountryFromExchange(exchange) {
        const exchangeCountryMap = {
            'MIL': 'IT', 'Milan': 'IT', 'MTA': 'IT',
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
