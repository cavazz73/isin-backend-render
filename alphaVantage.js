/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * Alpha Vantage API Client (Backup Source - 25 req/day limit)
 */

const axios = require('axios');

class AlphaVantageClient {
    constructor(apiKey) {
        this.apiKey = apiKey;
        this.baseUrl = 'https://www.alphavantage.co/query';
        this.requestCount = 0;
        this.dailyLimit = 25;
    }

    async search(query) {
        if (this.requestCount >= this.dailyLimit) {
            console.warn('[AlphaVantage] Daily limit reached');
            return { success: false, results: [], error: 'Daily limit reached' };
        }

        try {
            const response = await axios.get(this.baseUrl, {
                params: {
                    function: 'SYMBOL_SEARCH',
                    keywords: query,
                    apikey: this.apiKey
                },
                timeout: 10000
            });

            this.requestCount++;

            if (!response.data?.bestMatches) {
                return { success: false, results: [] };
            }

            const results = response.data.bestMatches
                .slice(0, 10)
                .map(match => ({
                    symbol: match['1. symbol'],
                    name: match['2. name'],
                    description: match['2. name'],
                    type: match['3. type'],
                    exchange: match['4. region'],
                    currency: match['8. currency'],
                    isin: null,
                    price: null,
                    change: null,
                    changePercent: null
                }));

            return { success: true, results, source: 'alphavantage' };

        } catch (error) {
            console.error('[AlphaVantage] Search error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    async getCompanyOverview(symbol) {
        if (this.requestCount >= this.dailyLimit) {
            return { success: false, error: 'Daily limit reached' };
        }

        try {
            const response = await axios.get(this.baseUrl, {
                params: {
                    function: 'OVERVIEW',
                    symbol: symbol,
                    apikey: this.apiKey
                },
                timeout: 15000
            });

            this.requestCount++;

            const data = response.data;
            
            // Check if we got valid data
            if (!data || !data.Symbol) {
                return { success: false, data: null };
            }

            return {
                success: true,
                data: data,
                source: 'alphavantage'
            };

        } catch (error) {
            console.error('[AlphaVantage] Company overview error:', error.message);
            return { success: false, data: null, error: error.message };
        }
    }

    async getQuote(symbol) {
        if (this.requestCount >= this.dailyLimit) {
            return { success: false, error: 'Daily limit reached' };
        }

        try {
            const response = await axios.get(this.baseUrl, {
                params: {
                    function: 'GLOBAL_QUOTE',
                    symbol: symbol,
                    apikey: this.apiKey
                },
                timeout: 10000
            });

            this.requestCount++;

            const quote = response.data?.['Global Quote'];
            if (!quote || !quote['05. price']) {
                return { success: false };
            }

            return {
                success: true,
                data: {
                    symbol: quote['01. symbol'],
                    name: quote['01. symbol'],
                    price: parseFloat(quote['05. price']),
                    change: parseFloat(quote['09. change']),
                    changePercent: parseFloat(quote['10. change percent'].replace('%', '')),
                    currency: 'USD',
                    exchange: 'UNKNOWN',
                    timestamp: new Date().toISOString()
                },
                source: 'alphavantage'
            };

        } catch (error) {
            console.error('[AlphaVantage] Quote error:', error.message);
            return { success: false, error: error.message };
        }
    }

    async getHistoricalData(symbol, period = '1M') {
        if (this.requestCount >= this.dailyLimit) {
            return { success: false, error: 'Daily limit reached' };
        }

        try {
            const func = period === '1D' ? 'TIME_SERIES_INTRADAY' : 'TIME_SERIES_DAILY';
            const params = {
                function: func,
                symbol: symbol,
                apikey: this.apiKey
            };

            if (func === 'TIME_SERIES_INTRADAY') {
                params.interval = '5min';
            } else {
                params.outputsize = 'full';
            }

            const response = await axios.get(this.baseUrl, {
                params,
                timeout: 15000
            });

            this.requestCount++;

            const timeSeries = response.data?.[`Time Series (Daily)`] || 
                               response.data?.[`Time Series (5min)`];
            
            if (!timeSeries) {
                return { success: false };
            }

            const historicalData = Object.entries(timeSeries)
                .slice(0, this.getDataPoints(period))
                .map(([date, values]) => ({
                    date: date.split(' ')[0],
                    open: parseFloat(values['1. open']),
                    high: parseFloat(values['2. high']),
                    low: parseFloat(values['3. low']),
                    close: parseFloat(values['4. close']),
                    volume: parseFloat(values['5. volume'])
                }))
                .reverse();

            return {
                success: true,
                symbol: symbol,
                data: historicalData,
                source: 'alphavantage'
            };

        } catch (error) {
            console.error('[AlphaVantage] Historical error:', error.message);
            return { success: false, error: error.message };
        }
    }

    async searchByISIN(isin) {
        return this.search(isin);
    }

    getDataPoints(period) {
        const map = {
            '1D': 100, '1W': 7, '1M': 30, '3M': 90,
            '6M': 180, 'YTD': 365, '1Y': 365,
            '3Y': 1095, '5Y': 1825, 'MAX': 5000
        };
        return map[period] || 30;
    }
}

module.exports = AlphaVantageClient;
