/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S.
 * Data Aggregator - Multi-Source with Fallback
 */

const yahooFinance = require('./yahooFinance');
const finnhub = require('./finnhub');
const alphaVantage = require('./alphaVantage');

class DataAggregator {
    constructor() {
        this.stats = {
            yahoo: { success: 0, errors: 0 },
            finnhub: { success: 0, errors: 0 },
            alphavantage: { success: 0, errors: 0 }
        };
    }

    async search(query) {
        const sources = [
            { name: 'finnhub', service: finnhub },      // Finnhub FIRST (more reliable)
            { name: 'yahoo', service: yahooFinance },
            { name: 'alphavantage', service: alphaVantage }
        ];

        const errors = [];

        for (const { name, service } of sources) {
            try {
                console.log(`[DataAggregator] Trying ${name} for search: ${query}`);
                const results = await service.search(query);
                
                if (results && results.length > 0) {
                    this.stats[name].success++;
                    console.log(`[DataAggregator] ✓ ${name} succeeded with ${results.length} results`);
                    
                    return {
                        success: true,
                        results: results,
                        source: name,
                        stats: this.getStats()
                    };
                } else {
                    console.log(`[DataAggregator] ✗ ${name} returned empty results`);
                    this.stats[name].errors++;
                }
            } catch (error) {
                this.stats[name].errors++;
                errors.push({ source: name, error: error.message });
                console.log(`[DataAggregator] ✗ ${name} failed: ${error.message}`);
                continue; // Try next source
            }
        }

        throw new Error(JSON.stringify({
            message: 'All data sources failed',
            errors: errors,
            stats: this.getStats()
        }));
    }

    async getQuote(symbol) {
        const sources = [
            { name: 'finnhub', service: finnhub },      // Finnhub FIRST (more reliable)
            { name: 'yahoo', service: yahooFinance },
            { name: 'alphavantage', service: alphaVantage }
        ];

        const errors = [];

        for (const { name, service } of sources) {
            try {
                console.log(`[DataAggregator] Trying ${name} for quote: ${symbol}`);
                const quote = await service.getQuote(symbol);
                
                if (quote && quote.price !== null && quote.price !== undefined) {
                    this.stats[name].success++;
                    console.log(`[DataAggregator] ✓ ${name} succeeded with price: ${quote.price}`);
                    
                    return {
                        success: true,
                        data: quote,
                        source: name,
                        stats: this.getStats()
                    };
                } else {
                    console.log(`[DataAggregator] ✗ ${name} returned quote with null price`);
                    this.stats[name].errors++;
                }
            } catch (error) {
                this.stats[name].errors++;
                errors.push({ source: name, error: error.message });
                console.log(`[DataAggregator] ✗ ${name} failed: ${error.message}`);
                continue; // Try next source
            }
        }

        throw new Error(JSON.stringify({
            message: 'All data sources failed',
            errors: errors,
            stats: this.getStats()
        }));
    }

    async getHistoricalData(symbol, period = '1M') {
        try {
            console.log(`[DataAggregator] Getting historical data for ${symbol}, period: ${period}`);
            const data = await yahooFinance.getHistoricalData(symbol, period);
            this.stats.yahoo.success++;
            
            return {
                success: true,
                data: data,
                source: 'yahoo',
                stats: this.getStats()
            };
        } catch (error) {
            this.stats.yahoo.errors++;
            throw new Error(JSON.stringify({
                message: 'Historical data failed',
                error: error.message,
                stats: this.getStats()
            }));
        }
    }

    async testAllSources() {
        const results = {};

        // Test Yahoo
        try {
            await yahooFinance.search('AAPL');
            results.yahoo = { status: 'OK', message: 'Working' };
        } catch (error) {
            results.yahoo = { status: 'FAILED', error: error.message };
        }

        // Test Finnhub
        try {
            await finnhub.search('AAPL');
            results.finnhub = { status: 'OK', message: 'Working' };
        } catch (error) {
            results.finnhub = { status: 'FAILED', error: error.message };
        }

        // Test Alpha Vantage
        try {
            await alphaVantage.search('AAPL');
            results.alphavantage = { status: 'OK', message: 'Working' };
        } catch (error) {
            results.alphavantage = { status: 'FAILED', error: error.message };
        }

        return results;
    }

    getStats() {
        return {
            yahoo: {
                ...this.stats.yahoo,
                successRate: this.calculateRate(this.stats.yahoo)
            },
            finnhub: {
                ...this.stats.finnhub,
                successRate: this.calculateRate(this.stats.finnhub)
            },
            alphavantage: {
                ...this.stats.alphavantage,
                successRate: this.calculateRate(this.stats.alphavantage)
            }
        };
    }

    calculateRate(stats) {
        const total = stats.success + stats.errors;
        return total > 0 ? ((stats.success / total) * 100).toFixed(1) + '%' : 'N/A';
    }

    resetStats() {
        this.stats = {
            yahoo: { success: 0, errors: 0 },
            finnhub: { success: 0, errors: 0 },
            alphavantage: { success: 0, errors: 0 }
        };
    }
}

module.exports = new DataAggregator();
