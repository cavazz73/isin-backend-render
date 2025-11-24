/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S.
 * Alpha Vantage Service
 */

const axios = require('axios');

class AlphaVantageService {
    constructor() {
        this.baseUrl = 'https://www.alphavantage.co/query';
        this.apiKey = process.env.ALPHA_VANTAGE_API_KEY || 'demo';
    }

    async search(query) {
        try {
            const response = await axios.get(this.baseUrl, {
                params: {
                    function: 'SYMBOL_SEARCH',
                    keywords: query,
                    apikey: this.apiKey
                },
                timeout: 10000
            });

            const matches = response.data?.bestMatches || [];
            return matches.slice(0, 10).map(m => ({
                symbol: m['1. symbol'],
                name: m['2. name'],
                type: m['3. type'] || 'Unknown',
                exchange: m['4. region'] || 'N/A',
                currency: m['8. currency'] || 'USD',
                source: 'alphavantage'
            }));
        } catch (error) {
            throw new Error(`Alpha Vantage search failed: ${error.message}`);
        }
    }

    async getQuote(symbol) {
        try {
            const response = await axios.get(this.baseUrl, {
                params: {
                    function: 'GLOBAL_QUOTE',
                    symbol: symbol,
                    apikey: this.apiKey
                },
                timeout: 10000
            });

            const quote = response.data?.['Global Quote'];
            if (!quote || Object.keys(quote).length === 0) {
                throw new Error('Quote not found');
            }

            return {
                symbol: quote['01. symbol'],
                name: symbol,
                type: 'Stock',
                exchange: 'N/A',
                currency: 'USD',
                price: parseFloat(quote['05. price']) || null,
                change: parseFloat(quote['09. change']) || null,
                changePercent: parseFloat(quote['10. change percent']?.replace('%', '')) || null,
                volume: parseInt(quote['06. volume']) || null,
                dayHigh: parseFloat(quote['03. high']) || null,
                dayLow: parseFloat(quote['04. low']) || null,
                previousClose: parseFloat(quote['08. previous close']) || null,
                source: 'alphavantage'
            };
        } catch (error) {
            throw new Error(`Alpha Vantage quote failed: ${error.message}`);
        }
    }
}

module.exports = new AlphaVantageService();
