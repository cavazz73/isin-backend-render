/**
 * Data Aggregator - Multi-Source Orchestrator
 * Copyright (c) 2024-2025 Mutna S.R.L.S.
 */

const yahooFinance = require('./yahooFinance');
const finnhub = require('./finnhub');

class DataAggregator {
    constructor() {
        this.stats = {
            yahoo: { success: 0, errors: 0 },
            finnhub: { success: 0, errors: 0 }
        };
    }

    async search(query) {
        const sources = [
            { name: 'yahoo', fn: () => yahooFinance.search(query) },
            { name: 'finnhub', fn: () => finnhub.search(query) }
        ];

        return await this.executeWithFallback(sources, 'search', query);
    }

    async getQuote(symbol) {
        const sources = [
            { name: 'yahoo', fn: () => yahooFinance.getQuote(symbol) },
            { name: 'finnhub', fn: () => finnhub.getQuote(symbol) }
        ];

        return await this.executeWithFallback(sources, 'quote', symbol);
    }

    async getHistoricalData(symbol, period) {
        const sources = [
            { name: 'yahoo', fn: () => yahooFinance.getHistoricalData(symbol, period) },
            { name: 'finnhub', fn: () => finnhub.getHistoricalData(symbol, period) }
        ];

        return await this.executeWithFallback(sources, 'historical', symbol);
    }

    async executeWithFallback(sources, operation, query) {
        const errors = [];

        for (const source of sources) {
            try {
                console.log(`[Aggregator] Trying ${source.name} for ${operation}: ${query}`);
                
                const result = await source.fn();
                
                this.stats[source.name].success++;
                
                console.log(`[Aggregator] ✓ ${source.name} succeeded`);
                
                return {
                    data: result,
                    source: source.name,
                    query,
                    timestamp: new Date().toISOString()
                };

            } catch (error) {
                this.stats[source.name].errors++;
                errors.push({
                    source: source.name,
                    error: error.message
                });
                console.error(`[Aggregator] ✗ ${source.name} failed:`, error.message);
            }
        }

        // Tutte le fonti fallite
        throw new Error(JSON.stringify({
            message: 'All data sources failed',
            errors,
            stats: this.stats
        }));
    }

    async testAllSources() {
        const results = {};

        // Test Yahoo
        try {
            await yahooFinance.search('AAPL');
            results.yahoo = { status: 'OK', responseTime: '0ms' };
        } catch (error) {
            results.yahoo = { status: 'FAILED', error: error.message };
        }

        // Test Finnhub
        try {
            await finnhub.search('AAPL');
            results.finnhub = { status: 'OK', responseTime: '0ms' };
        } catch (error) {
            results.finnhub = { status: 'FAILED', error: error.message };
        }

        return results;
    }

    getStats() {
        const total = {};
        
        for (const [source, stats] of Object.entries(this.stats)) {
            const totalCalls = stats.success + stats.errors;
            total[source] = {
                success: stats.success,
                errors: stats.errors,
                total: totalCalls,
                successRate: totalCalls > 0 ? ((stats.success / totalCalls) * 100).toFixed(2) + '%' : '0%'
            };
        }

        return total;
    }

    resetStats() {
        this.stats = {
            yahoo: { success: 0, errors: 0 },
            finnhub: { success: 0, errors: 0 }
        };
    }

    clearAllCaches() {
        // Yahoo Finance cache
        if (yahooFinance.cache) {
            yahooFinance.cache.clear();
        }
        console.log('[Aggregator] All caches cleared');
    }
}

module.exports = new DataAggregator();
