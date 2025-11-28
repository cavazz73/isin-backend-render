/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * Finnhub API Client (Backup Source - 60 req/min)
 */

const axios = require('axios');

class FinnhubClient {
    constructor(apiKey) {
        this.apiKey = apiKey;
        this.baseUrl = 'https://finnhub.io/api/v1';
        
        // Headers per evitare 403
        this.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        };
    }

    async search(query) {
        try {
            const url = `${this.baseUrl}/search`;
            const response = await axios.get(url, {
                params: { q: query, token: this.apiKey },
                headers: this.headers,
                timeout: 10000
            });

            if (!response.data?.result) {
                return { success: false, results: [] };
            }

            const results = response.data.result
                .slice(0, 10)
                .map(item => ({
                    symbol: item.symbol,
                    name: item.description,
                    description: item.description,
                    type: item.type || 'Stock',
                    exchange: item.displaySymbol?.split(':')[0] || 'UNKNOWN',
                    currency: 'USD',
                    isin: null,
                    price: null,
                    change: null,
                    changePercent: null
                }));

            return { success: true, results, source: 'finnhub' };

        } catch (error) {
            console.error('[Finnhub] Search error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    async getQuote(symbol) {
        try {
            const url = `${this.baseUrl}/quote`;
            const response = await axios.get(url, {
                params: { symbol: symbol, token: this.apiKey },
                headers: this.headers,
                timeout: 10000
            });

            const data = response.data;
            if (!data || data.c === 0) {
                return { success: false };
            }

            return {
                success: true,
                data: {
                    symbol: symbol,
                    name: symbol,
                    price: data.c,
                    change: data.d,
                    changePercent: data.dp,
                    currency: 'USD',
                    exchange: 'UNKNOWN',
                    timestamp: new Date(data.t * 1000).toISOString()
                },
                source: 'finnhub'
            };

        } catch (error) {
            console.error('[Finnhub] Quote error:', error.message);
            return { success: false, error: error.message };
        }
    }

    async searchByISIN(isin) {
        return this.search(isin);
    }
}

module.exports = FinnhubClient;
