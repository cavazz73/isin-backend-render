/**
 * Yahoo Finance Service - Simplified for Render
 * Copyright (c) 2024-2025 Mutna S.R.L.S.
 */

const axios = require('axios');

class YahooFinanceService {
    constructor() {
        this.baseUrl = 'https://query2.finance.yahoo.com';
        this.cache = new Map();
    }

    async search(query) {
        try {
            const type = this.detectType(query);
            
            if (type === 'SYMBOL') {
                return await this.getQuote(query);
            } else {
                return await this.searchByName(query);
            }
        } catch (error) {
            console.error('[Yahoo] Error:', error.message);
            throw error;
        }
    }

    detectType(query) {
        const clean = query.trim().toUpperCase();
        if (/^[A-Z]{2}[A-Z0-9]{10}$/.test(clean)) return 'ISIN';
        if (/^[A-Z]{1,5}$/.test(clean)) return 'SYMBOL';
        return 'NAME';
    }

    async searchByName(query) {
        const url = `${this.baseUrl}/v1/finance/search`;
        
        const response = await axios.get(url, {
            params: { q: query, quotesCount: 10 },
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            },
            timeout: 10000
        });

        const quotes = response.data?.quotes || [];
        return quotes.map(q => ({
            symbol: q.symbol || 'N/A',
            name: q.longname || q.shortname || 'N/A',
            type: q.quoteType || 'Stock',
            exchange: q.exchange || 'N/A',
            currency: q.currency || 'USD',
            price: q.regularMarketPrice || null,
            change: q.regularMarketChange || null,
            changePercent: q.regularMarketChangePercent || null,
            source: 'yahoo'
        }));
    }

    async getQuote(symbol) {
        const url = `${this.baseUrl}/v7/finance/quote`;
        
        const response = await axios.get(url, {
            params: { symbols: symbol },
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            },
            timeout: 10000
        });

        const quote = response.data?.quoteResponse?.result?.[0];
        if (!quote) throw new Error('Quote not found');

        return [{
            symbol: quote.symbol || 'N/A',
            name: quote.longName || quote.shortName || 'N/A',
            type: quote.quoteType || 'Stock',
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
        }];
    }

    async getHistoricalData(symbol, period = '1M') {
        const range = this.convertPeriod(period);
        const url = `${this.baseUrl}/v8/finance/chart/${symbol}`;
        
        const response = await axios.get(url, {
            params: { range, interval: this.getInterval(period) },
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            timeout: 15000
        });

        const result = response.data?.chart?.result?.[0];
        if (!result) throw new Error('No historical data');

        const timestamps = result.timestamp || [];
        const prices = result.indicators?.quote?.[0] || {};

        return {
            symbol,
            currency: result.meta?.currency || 'USD',
            data: timestamps.map((ts, i) => ({
                date: new Date(ts * 1000).toISOString(),
                open: prices.open?.[i] || null,
                high: prices.high?.[i] || null,
                low: prices.low?.[i] || null,
                close: prices.close?.[i] || null,
                volume: prices.volume?.[i] || null
            })),
            source: 'yahoo'
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
        if (['1W'].includes(period)) return '15m';
        if (['1M', '3M', '6M', 'YTD', '1Y'].includes(period)) return '1d';
        return '1wk';
    }
}

module.exports = new YahooFinanceService();
