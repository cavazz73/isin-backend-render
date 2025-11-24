/**
 * Finnhub Service - Simplified for Render
 * Copyright (c) 2024-2025 Mutna S.R.L.S.
 */

const axios = require('axios');

class FinnhubService {
    constructor() {
        this.baseUrl = 'https://finnhub.io/api/v1';
        this.apiKey = process.env.FINNHUB_API_KEY || 'demo';
    }

    async search(query) {
        try {
            const url = `${this.baseUrl}/search`;
            const response = await axios.get(url, {
                params: { q: query, token: this.apiKey },
                timeout: 10000
            });

            const results = response.data?.result || [];
            return results.slice(0, 10).map(item => ({
                symbol: item.symbol || 'N/A',
                name: item.description || 'N/A',
                type: item.type || 'Stock',
                exchange: 'N/A',
                currency: 'USD',
                price: null,
                change: null,
                changePercent: null,
                source: 'finnhub'
            }));
        } catch (error) {
            console.error('[Finnhub] Search error:', error.message);
            throw error;
        }
    }

    async getQuote(symbol) {
        try {
            const [quote, profile] = await Promise.all([
                this.fetchQuote(symbol),
                this.fetchProfile(symbol)
            ]);

            return [{
                symbol: symbol,
                name: profile.name || symbol,
                type: 'Stock',
                exchange: profile.exchange || 'N/A',
                currency: profile.currency || 'USD',
                price: quote.c || null,
                change: quote.d || null,
                changePercent: quote.dp || null,
                dayHigh: quote.h || null,
                dayLow: quote.l || null,
                previousClose: quote.pc || null,
                marketCap: profile.marketCapitalization ? profile.marketCapitalization * 1000000 : null,
                source: 'finnhub'
            }];
        } catch (error) {
            console.error('[Finnhub] Quote error:', error.message);
            throw error;
        }
    }

    async fetchQuote(symbol) {
        const url = `${this.baseUrl}/quote`;
        const response = await axios.get(url, {
            params: { symbol, token: this.apiKey },
            timeout: 10000
        });
        return response.data || {};
    }

    async fetchProfile(symbol) {
        const url = `${this.baseUrl}/stock/profile2`;
        const response = await axios.get(url, {
            params: { symbol, token: this.apiKey },
            timeout: 10000
        });
        return response.data || {};
    }

    async getHistoricalData(symbol, period = '1M') {
        const { from, to, resolution } = this.getPeriodParams(period);
        const url = `${this.baseUrl}/stock/candle`;
        
        const response = await axios.get(url, {
            params: { symbol, resolution, from, to, token: this.apiKey },
            timeout: 15000
        });

        const data = response.data;
        if (data.s === 'no_data') throw new Error('No data');

        return {
            symbol,
            currency: 'USD',
            data: (data.t || []).map((ts, i) => ({
                date: new Date(ts * 1000).toISOString(),
                open: data.o?.[i] || null,
                high: data.h?.[i] || null,
                low: data.l?.[i] || null,
                close: data.c?.[i] || null,
                volume: data.v?.[i] || null
            })),
            source: 'finnhub'
        };
    }

    getPeriodParams(period) {
        const now = Math.floor(Date.now() / 1000);
        let from, resolution;

        switch (period) {
            case '1D': from = now - (24 * 3600); resolution = '5'; break;
            case '1W': from = now - (7 * 24 * 3600); resolution = '15'; break;
            case '1M': from = now - (30 * 24 * 3600); resolution = 'D'; break;
            case '3M': from = now - (90 * 24 * 3600); resolution = 'D'; break;
            case '6M': from = now - (180 * 24 * 3600); resolution = 'D'; break;
            case '1Y': from = now - (365 * 24 * 3600); resolution = 'D'; break;
            case '3Y': from = now - (3 * 365 * 24 * 3600); resolution = 'W'; break;
            case '5Y': from = now - (5 * 365 * 24 * 3600); resolution = 'W'; break;
            default: from = now - (30 * 24 * 3600); resolution = 'D';
        }

        return { from, to: now, resolution };
    }
}

module.exports = new FinnhubService();
