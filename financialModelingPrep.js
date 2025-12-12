/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Financial Modeling Prep API Client
 * COMPLETE FUNDAMENTALS: Market Cap, P/E, Dividend, 52W Range, Description
 */

const axios = require('axios');

class FinancialModelingPrepClient {
    constructor(apiKey) {
        this.apiKey = apiKey;
        this.baseUrl = 'https://financialmodelingprep.com/stable';  // ✅ CORRECT URL
        this.requestCount = 0;
    }

    /**
     * Search for stocks
     */
    async search(query) {
        try {
            const url = `${this.baseUrl}/search-name`;  // ✅ CORRECT ENDPOINT
            const response = await axios.get(url, {
                params: {
                    query: query,
                    limit: 10,
                    apikey: this.apiKey
                },
                timeout: 10000
            });

            this.requestCount++;

            if (!response.data || response.data.length === 0) {
                return { success: false, results: [] };
            }

            const results = response.data.map(item => ({
                symbol: item.symbol,
                name: item.name,
                description: item.name,
                type: 'Stock',
                exchange: item.exchangeShortName || item.stockExchange,
                currency: item.currency || 'USD',
                isin: null,
                country: this.getCountryFromExchange(item.exchangeShortName),
                price: null,
                change: null,
                changePercent: null
            }));

            console.log(`[FinancialModelingPrep] Found ${results.length} results for: ${query}`);
            return { success: true, results, source: 'fmp' };

        } catch (error) {
            console.error('[FinancialModelingPrep] Search error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    /**
     * Get quote with COMPLETE FUNDAMENTALS
     */
    async getQuote(symbol) {
        try {
            // Get real-time quote
            const quoteUrl = `${this.baseUrl}/quote`;  // ✅ CORRECT ENDPOINT
            const quoteResponse = await axios.get(quoteUrl, {
                params: { 
                    symbol: symbol,  // ✅ QUERY PARAMETER
                    apikey: this.apiKey 
                },
                timeout: 10000
            });

            this.requestCount++;

            const quote = quoteResponse.data?.[0];
            if (!quote) {
                console.log('[FinancialModelingPrep] No quote data for:', symbol);
                return { success: false };
            }

            // Get company profile for DETAILED DESCRIPTION
            let description = quote.name;
            let sector = null;
            let industry = null;

            try {
                const profileUrl = `${this.baseUrl}/profile`;  // ✅ CORRECT ENDPOINT
                const profileResponse = await axios.get(profileUrl, {
                    params: { 
                        symbol: symbol,  // ✅ QUERY PARAMETER
                        apikey: this.apiKey 
                    },
                    timeout: 10000
                });

                this.requestCount++;

                const profile = profileResponse.data?.[0];
                if (profile) {
                    description = profile.description || profile.companyName || quote.name;
                    sector = profile.sector;
                    industry = profile.industry;
                }
            } catch (profileError) {
                console.log('[FinancialModelingPrep] Profile not available for:', symbol);
            }

            const result = {
                success: true,
                data: {
                    symbol: quote.symbol,
                    name: quote.name,
                    description: description,  // ✅ DETAILED DESCRIPTION
                    price: quote.price,
                    change: quote.change,
                    changePercent: quote.changesPercentage,
                    currency: 'USD',
                    exchange: quote.exchange,
                    // ✅ COMPLETE FUNDAMENTALS
                    marketCap: quote.marketCap || null,
                    peRatio: quote.pe || null,
                    dividendYield: quote.yield ? quote.yield : null,
                    week52High: quote.yearHigh || null,
                    week52Low: quote.yearLow || null,
                    // Additional data
                    sector: sector,
                    industry: industry,
                    volume: quote.volume,
                    avgVolume: quote.avgVolume,
                    timestamp: new Date().toISOString()
                },
                source: 'fmp'
            };

            console.log(`[FinancialModelingPrep] Quote for ${symbol}: ${quote.price} USD (with complete fundamentals)`);
            return result;

        } catch (error) {
            console.error('[FinancialModelingPrep] Quote error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Get historical data
     */
    async getHistoricalData(symbol, period = '1M') {
        try {
            const periodMap = {
                '1D': { from: this.getDateDaysAgo(1) },
                '1W': { from: this.getDateDaysAgo(7) },
                '1M': { from: this.getDateDaysAgo(30) },
                '3M': { from: this.getDateDaysAgo(90) },
                '6M': { from: this.getDateDaysAgo(180) },
                'YTD': { from: this.getYearStart() },
                '1Y': { from: this.getDateDaysAgo(365) },
                '3Y': { from: this.getDateDaysAgo(1095) },
                '5Y': { from: this.getDateDaysAgo(1825) }
            };

            const params = periodMap[period] || periodMap['1M'];
            const url = `${this.baseUrl}/historical-price-eod/full`;  // ✅ CORRECT ENDPOINT

            const response = await axios.get(url, {
                params: {
                    symbol: symbol,  // ✅ QUERY PARAMETER
                    from: params.from,
                    apikey: this.apiKey
                },
                timeout: 15000
            });

            this.requestCount++;

            const historical = response.data?.historical;
            if (!historical || historical.length === 0) {
                return { success: false };
            }

            const data = historical.reverse().map(item => ({
                date: item.date,
                open: item.open,
                high: item.high,
                low: item.low,
                close: item.close,
                volume: item.volume
            }));

            console.log(`[FinancialModelingPrep] Historical data for ${symbol}: ${data.length} points`);
            return { success: true, data, source: 'fmp' };

        } catch (error) {
            console.error('[FinancialModelingPrep] Historical error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Helper: Get date N days ago in YYYY-MM-DD format
     */
    getDateDaysAgo(days) {
        const date = new Date();
        date.setDate(date.getDate() - days);
        return date.toISOString().split('T')[0];
    }

    /**
     * Helper: Get year start date
     */
    getYearStart() {
        const date = new Date();
        date.setMonth(0, 1);
        return date.toISOString().split('T')[0];
    }

    /**
     * Helper: Get country from exchange
     */
    getCountryFromExchange(exchange) {
        const exchangeMap = {
            'NASDAQ': 'US',
            'NYSE': 'US',
            'AMEX': 'US',
            'LSE': 'GB',
            'MIL': 'IT',
            'EPA': 'FR',
            'FRA': 'DE',
            'AMS': 'NL',
            'BME': 'ES',
            'SIX': 'CH',
            'TSX': 'CA',
            'JPX': 'JP'
        };
        return exchangeMap[exchange] || 'US';
    }

    /**
     * Get request count
     */
    getRequestCount() {
        return this.requestCount;
    }
}

module.exports = FinancialModelingPrepClient;
