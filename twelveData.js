/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Twelve Data API Client (PRIMARY Source for European Markets)
 * Plan: Free tier - 800 requests/day, 8 requests/minute
 * Excellent for: Italian, French, German, UK stocks with correct EUR/GBP pricing
 */

const axios = require('axios');

class TwelveDataClient {
    constructor(apiKey) {
        this.apiKey = apiKey || process.env.TWELVE_DATA_API_KEY;
        this.baseUrl = 'https://api.twelvedata.com';
        this.requestCount = 0;
        this.dailyLimit = 800;
        this.minuteLimit = 8;
        
        console.log('[TwelveData] Initialized with API key:', this.apiKey ? 'YES' : 'NO');
    }

    /**
     * Search for stocks/securities
     */
    async search(query) {
        try {
            if (!this.apiKey) {
                throw new Error('TwelveData API key not configured');
            }

            const url = `${this.baseUrl}/symbol_search`;
            const response = await axios.get(url, {
                params: {
                    symbol: query,
                    apikey: this.apiKey
                },
                timeout: 10000
            });

            this.requestCount++;

            if (!response.data?.data || response.data.data.length === 0) {
                console.log('[TwelveData] No results for:', query);
                return { success: false, results: [] };
            }

            const results = response.data.data
                .filter(item => item.instrument_type === 'Common Stock' || item.instrument_type === 'ETF')
                .slice(0, 10)
                .map(item => ({
                    symbol: item.symbol,
                    name: item.instrument_name,
                    description: item.instrument_name,
                    type: item.instrument_type,
                    exchange: item.exchange,
                    mic_code: item.mic_code,
                    currency: item.currency,
                    country: item.country,
                    isin: null,
                    price: null,
                    change: null,
                    changePercent: null
                }));

            console.log(`[TwelveData] Found ${results.length} results for: ${query}`);
            return { success: true, results, source: 'twelvedata' };

        } catch (error) {
            console.error('[TwelveData] Search error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    /**
     * Get real-time quote for a symbol
     */
    async getQuote(symbol) {
        try {
            if (!this.apiKey) {
                throw new Error('TwelveData API key not configured');
            }

            const url = `${this.baseUrl}/quote`;
            const response = await axios.get(url, {
                params: {
                    symbol: symbol,
                    apikey: this.apiKey
                },
                timeout: 10000
            });

            this.requestCount++;

            const data = response.data;
            
            if (!data || data.status === 'error' || !data.close) {
                console.log('[TwelveData] No quote data for:', symbol);
                return { success: false };
            }

            const price = parseFloat(data.close);
            const prevClose = parseFloat(data.previous_close);
            const change = price - prevClose;
            const changePercent = (change / prevClose) * 100;

            const result = {
                success: true,
                data: {
                    symbol: data.symbol,
                    name: data.name,
                    price: price,
                    change: change,
                    changePercent: changePercent,
                    currency: data.currency || 'USD',
                    exchange: data.exchange,
                    timestamp: data.datetime || new Date().toISOString(),
                    open: parseFloat(data.open),
                    high: parseFloat(data.high),
                    low: parseFloat(data.low),
                    volume: parseInt(data.volume),
                    previousClose: prevClose
                },
                source: 'twelvedata'
            };

            console.log(`[TwelveData] Quote for ${symbol}: ${price} ${result.data.currency}`);
            return result;

        } catch (error) {
            console.error('[TwelveData] Quote error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Get historical data (OHLCV)
     */
    async getHistoricalData(symbol, period = '1M') {
        try {
            if (!this.apiKey) {
                throw new Error('TwelveData API key not configured');
            }

            const periodMap = {
                '1D': { interval: '5min', outputsize: 78 },
                '1W': { interval: '30min', outputsize: 224 },
                '1M': { interval: '1day', outputsize: 30 },
                '3M': { interval: '1day', outputsize: 90 },
                '6M': { interval: '1day', outputsize: 180 },
                'YTD': { interval: '1day', outputsize: 365 },
                '1Y': { interval: '1day', outputsize: 365 },
                '3Y': { interval: '1week', outputsize: 156 },
                '5Y': { interval: '1week', outputsize: 260 },
                'MAX': { interval: '1month', outputsize: 500 }
            };

            const params = periodMap[period] || periodMap['1M'];
            
            const url = `${this.baseUrl}/time_series`;
            const response = await axios.get(url, {
                params: {
                    symbol: symbol,
                    interval: params.interval,
                    outputsize: params.outputsize,
                    apikey: this.apiKey
                },
                timeout: 15000
            });

            this.requestCount++;

            if (!response.data?.values || response.data.values.length === 0) {
                console.log('[TwelveData] No historical data for:', symbol);
                return { success: false };
            }

            const historicalData = response.data.values
                .map(item => ({
                    date: item.datetime.split(' ')[0],
                    open: parseFloat(item.open),
                    high: parseFloat(item.high),
                    low: parseFloat(item.low),
                    close: parseFloat(item.close),
                    volume: parseInt(item.volume)
                }))
                .reverse();

            console.log(`[TwelveData] Historical data for ${symbol}: ${historicalData.length} points`);

            return {
                success: true,
                symbol: symbol,
                data: historicalData,
                source: 'twelvedata',
                period: period
            };

        } catch (error) {
            console.error('[TwelveData] Historical data error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Search by ISIN (limited support)
     */
    async searchByISIN(isin) {
        console.log('[TwelveData] ISIN search not directly supported, trying symbol search');
        return this.search(isin);
    }

    /**
     * Get current usage statistics
     */
    getUsageStats() {
        return {
            requestsMade: this.requestCount,
            dailyLimit: this.dailyLimit,
            minuteLimit: this.minuteLimit,
            remainingDaily: this.dailyLimit - this.requestCount
        };
    }
}

module.exports = TwelveDataClient;
