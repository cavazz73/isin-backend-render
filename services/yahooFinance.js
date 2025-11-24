/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S.
 * Yahoo Finance Service - Optimized for Render
 */

const axios = require('axios');

class YahooFinanceService {
    constructor() {
        this.baseUrl = 'https://query2.finance.yahoo.com';
        this.cache = new Map();
        this.cacheDuration = 60000; // 1 min
    }

    async search(query) {
        try {
            const url = `${this.baseUrl}/v1/finance/search`;
            const response = await axios.get(url, {
                params: { 
                    q: query, 
                    quotesCount: 10,
                    newsCount: 0,
                    enableFuzzyQuery: true  // Abilita ricerca fuzzy
                },
                headers: this.getHeaders(),
                timeout: 10000
            });

            const quotes = response.data?.quotes || [];
            
            // Filtra solo equity, ETF, e mutual funds (non crypto, index, etc)
            const filtered = quotes.filter(q => {
                const type = q.quoteType?.toUpperCase();
                return type === 'EQUITY' || type === 'ETF' || type === 'MUTUALFUND';
            });
            
            return filtered.map(q => this.normalizeQuote(q));
        } catch (error) {
            throw new Error(`Yahoo search failed: ${error.message}`);
        }
    }

    async getQuote(symbol) {
        try {
            const url = `${this.baseUrl}/v7/finance/quote`;
            const response = await axios.get(url, {
                params: { symbols: symbol },
                headers: this.getHeaders(),
                timeout: 10000
            });

            const quote = response.data?.quoteResponse?.result?.[0];
            if (!quote) throw new Error('Quote not found');
            
            return this.normalizeQuote(quote);
        } catch (error) {
            throw new Error(`Yahoo quote failed: ${error.message}`);
        }
    }

    async getHistoricalData(symbol, period = '1M') {
        try {
            const range = this.convertPeriod(period);
            const url = `${this.baseUrl}/v8/finance/chart/${symbol}`;
            
            const response = await axios.get(url, {
                params: { 
                    range: range,
                    interval: this.getInterval(period)
                },
                headers: this.getHeaders(),
                timeout: 15000
            });

            const result = response.data?.chart?.result?.[0];
            if (!result) throw new Error('Historical data not found');
            
            return this.normalizeHistorical(result);
        } catch (error) {
            throw new Error(`Yahoo historical failed: ${error.message}`);
        }
    }

    normalizeQuote(quote) {
        return {
            symbol: quote.symbol || 'N/A',
            name: quote.longName || quote.shortName || 'N/A',
            type: quote.quoteType || 'Unknown',
            exchange: quote.fullExchangeName || quote.exchange || 'N/A',
            currency: quote.currency || 'USD',
            price: quote.regularMarketPrice || null,
            change: quote.regularMarketChange || null,
            changePercent: quote.regularMarketChangePercent || null,
            volume: quote.regularMarketVolume || null,
            marketCap: quote.marketCap || null,
            dayHigh: quote.regularMarketDayHigh || null,
            dayLow: quote.regularMarketDayLow || null,
            source: 'yahoo'
        };
    }

    normalizeHistorical(result) {
        const timestamps = result.timestamp || [];
        const indicators = result.indicators?.quote?.[0] || {};
        
        return {
            symbol: result.meta?.symbol || 'N/A',
            currency: result.meta?.currency || 'USD',
            data: timestamps.map((ts, i) => ({
                date: new Date(ts * 1000).toISOString(),
                open: indicators.open?.[i] || null,
                high: indicators.high?.[i] || null,
                low: indicators.low?.[i] || null,
                close: indicators.close?.[i] || null,
                volume: indicators.volume?.[i] || null
            })),
            source: 'yahoo'
        };
    }

    getHeaders() {
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        };
    }

    convertPeriod(period) {
        const map = {
            '1D': '1d', '1W': '5d', '1M': '1mo', '3M': '3mo',
            '6M': '6mo', 'YTD': 'ytd', '1Y': '1y', '3Y': '3y',
            '5Y': '5y', 'MAX': 'max'
        };
        return map[period] || '1mo';
    }

    getInterval(period) {
        if (['1D'].includes(period)) return '5m';
        if (['1W', '1M'].includes(period)) return '1d';
        return '1d';
    }
}

module.exports = new YahooFinanceService();
