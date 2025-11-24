/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S.
 * Finnhub Service
 */

const axios = require('axios');

class FinnhubService {
    constructor() {
        this.baseUrl = 'https://finnhub.io/api/v1';
        this.apiKey = process.env.FINNHUB_API_KEY || '';
    }

    async search(query) {
        if (!this.apiKey) throw new Error('Finnhub API key not configured');
        
        try {
            const response = await axios.get(`${this.baseUrl}/search`, {
                params: { q: query, token: this.apiKey },
                timeout: 10000
            });

            const results = response.data?.result || [];
            return results.slice(0, 10).map(r => ({
                symbol: r.symbol,
                name: r.description,
                type: r.type || 'Unknown',
                exchange: r.displaySymbol || 'N/A',
                source: 'finnhub'
            }));
        } catch (error) {
            throw new Error(`Finnhub search failed: ${error.message}`);
        }
    }

    async getQuote(symbol) {
        if (!this.apiKey) throw new Error('Finnhub API key not configured');
        
        try {
            const [quote, profile] = await Promise.all([
                axios.get(`${this.baseUrl}/quote`, {
                    params: { symbol, token: this.apiKey },
                    timeout: 10000
                }),
                axios.get(`${this.baseUrl}/stock/profile2`, {
                    params: { symbol, token: this.apiKey },
                    timeout: 10000
                })
            ]);

            const q = quote.data;
            const p = profile.data;

            return {
                symbol: symbol,
                name: p.name || symbol,
                type: 'Stock',
                exchange: p.exchange || 'N/A',
                currency: p.currency || 'USD',
                price: q.c || null,
                change: q.d || null,
                changePercent: q.dp || null,
                dayHigh: q.h || null,
                dayLow: q.l || null,
                previousClose: q.pc || null,
                marketCap: p.marketCapitalization ? p.marketCapitalization * 1000000 : null,
                source: 'finnhub'
            };
        } catch (error) {
            throw new Error(`Finnhub quote failed: ${error.message}`);
        }
    }
}

module.exports = new FinnhubService();
