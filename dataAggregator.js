/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Multi-Source Data Aggregator
 * Aggregates financial data from Yahoo Finance, Finnhub, and Alpha Vantage
 */

const YahooFinanceClient = require('./yahooFinance');
const FinnhubClient = require('./finnhub');
const AlphaVantageClient = require('./alphaVantage');

class DataAggregator {
    constructor(config = {}) {
        this.yahoo = new YahooFinanceClient();
        this.finnhub = new FinnhubClient(config.finnhubKey);
        this.alphavantage = new AlphaVantageClient(config.alphavantageKey);
        
        // FIXED: Yahoo PRIMARY (unlimited + free)
        this.sources = ['yahoo', 'finnhub', 'alphavantage'];
    }

    /**
     * Search across all sources and merge results
     */
    async search(query) {
        console.log(`[DataAggregator] Searching for: "${query}"`);
        
        // Try all sources in parallel for maximum speed
        const searchPromises = [
            this.yahoo.search(query).catch(e => ({ success: false, results: [], error: e.message })),
            this.finnhub.search(query).catch(e => ({ success: false, results: [], error: e.message })),
            this.alphavantage.search(query).catch(e => ({ success: false, results: [], error: e.message }))
        ];

        const results = await Promise.all(searchPromises);
        
        // Log results from each source
        results.forEach((result, index) => {
            const source = this.sources[index];
            if (result.success && result.results.length > 0) {
                console.log(`[${source}] Found ${result.results.length} results`);
            } else {
                console.log(`[${source}] No results or error: ${result.error || 'N/A'}`);
            }
        });

        // Merge and deduplicate results
        const mergedResults = this.mergeSearchResults(results);
        
        if (mergedResults.length === 0) {
            return {
                success: false,
                results: [],
                metadata: {
                    sources: this.sources,
                    errors: results.map((r, i) => ({ source: this.sources[i], error: r.error }))
                }
            };
        }

        // Enrich results with quotes from best available source
        const enrichedResults = await this.enrichWithQuotes(mergedResults);

        return {
            success: true,
            results: enrichedResults,
            metadata: {
                totalResults: enrichedResults.length,
                sources: this.sources.filter((_, i) => results[i].success),
                timestamp: new Date().toISOString()
            }
        };
    }

    /**
     * Merge search results from multiple sources, removing duplicates
     */
    mergeSearchResults(results) {
        const symbolMap = new Map();

        results.forEach((result, sourceIndex) => {
            if (!result.success || !result.results) return;

            const source = this.sources[sourceIndex];
            
            result.results.forEach(item => {
                const symbol = item.symbol;
                
                if (!symbolMap.has(symbol)) {
                    // First time seeing this symbol
                    symbolMap.set(symbol, {
                        ...item,
                        sources: [source]
                    });
                } else {
                    // Symbol already exists, merge data
                    const existing = symbolMap.get(symbol);
                    
                    // Prefer Yahoo Finance data, then Finnhub, then AlphaVantage
                    if (source === 'yahoo' || (source === 'finnhub' && existing.sources[0] === 'alphavantage')) {
                        symbolMap.set(symbol, {
                            ...existing,
                            ...item,
                            sources: [...existing.sources, source]
                        });
                    } else {
                        // Just add the source
                        existing.sources.push(source);
                    }
                }
            });
        });

        return Array.from(symbolMap.values());
    }

    /**
     * Enrich search results with real-time quotes
     */
    async enrichWithQuotes(results) {
        const enrichPromises = results.map(async (item) => {
            // If we already have price data, skip
            if (item.price != null && typeof item.price === 'number') {
                return item;
            }

            // Try to get quote from best source
            const quote = await this.getQuote(item.symbol);
            
            if (quote.success && quote.data) {
                return {
                    ...item,
                    price: quote.data.price,
                    change: quote.data.change,
                    changePercent: quote.data.changePercent,
                    currency: quote.data.currency || item.currency,
                    quoteSources: [quote.source]
                };
            }

            return item;
        });

        return await Promise.all(enrichPromises);
    }

    /**
     * Get quote with automatic fallback
     */
    async getQuote(symbol) {
        console.log(`[DataAggregator] Getting quote for: ${symbol}`);

        // Try Yahoo first (fastest and most reliable)
        try {
            const yahooQuote = await this.yahoo.getQuote(symbol);
            if (yahooQuote.success && yahooQuote.data) {
                console.log(`[yahoo] Quote found for ${symbol}`);
                return yahooQuote;
            }
        } catch (error) {
            console.error(`[yahoo] Error getting quote: ${error.message}`);
        }

        // Fallback to Finnhub
        try {
            const finnhubQuote = await this.finnhub.getQuote(symbol);
            if (finnhubQuote.success && finnhubQuote.data) {
                console.log(`[finnhub] Quote found for ${symbol}`);
                return finnhubQuote;
            }
        } catch (error) {
            console.error(`[finnhub] Error getting quote: ${error.message}`);
        }

        // Fallback to Alpha Vantage
        try {
            const avQuote = await this.alphavantage.getQuote(symbol);
            if (avQuote.success && avQuote.data) {
                console.log(`[alphavantage] Quote found for ${symbol}`);
                return avQuote;
            }
        } catch (error) {
            console.error(`[alphavantage] Error getting quote: ${error.message}`);
        }

        return {
            success: false,
            error: 'No quote data available from any source'
        };
    }

    /**
     * Get historical data with automatic fallback
     */
    async getHistoricalData(symbol, period = '1M') {
        console.log(`[DataAggregator] Getting historical data for: ${symbol}, period: ${period}`);

        // Try Yahoo first (best historical data)
        try {
            const yahooHistorical = await this.yahoo.getHistoricalData(symbol, period);
            if (yahooHistorical.success && yahooHistorical.data && yahooHistorical.data.length > 0) {
                console.log(`[yahoo] Historical data found: ${yahooHistorical.data.length} points`);
                return yahooHistorical;
            }
        } catch (error) {
            console.error(`[yahoo] Error getting historical data: ${error.message}`);
        }

        // Fallback to Alpha Vantage
        try {
            const avHistorical = await this.alphavantage.getHistoricalData(symbol, period);
            if (avHistorical.success && avHistorical.data && avHistorical.data.length > 0) {
                console.log(`[alphavantage] Historical data found: ${avHistorical.data.length} points`);
                return avHistorical;
            }
        } catch (error) {
            console.error(`[alphavantage] Error getting historical data: ${error.message}`);
        }

        // Finnhub doesn't provide good historical data for free tier
        
        return {
            success: false,
            error: 'No historical data available from any source'
        };
    }

    /**
     * Search by ISIN with fallback
     */
    async searchByISIN(isin) {
        console.log(`[DataAggregator] Searching by ISIN: ${isin}`);

        // Try Yahoo first
        try {
            const yahooResult = await this.yahoo.searchByISIN(isin);
            if (yahooResult.success && yahooResult.results.length > 0) {
                return yahooResult;
            }
        } catch (error) {
            console.error(`[yahoo] ISIN search error: ${error.message}`);
        }

        // Fallback to Finnhub
        try {
            const finnhubResult = await this.finnhub.searchByISIN(isin);
            if (finnhubResult.success && finnhubResult.results.length > 0) {
                return finnhubResult;
            }
        } catch (error) {
            console.error(`[finnhub] ISIN search error: ${error.message}`);
        }

        return {
            success: false,
            results: [],
            error: 'No results for ISIN from any source'
        };
    }

    /**
     * Health check for all sources
     */
    async healthCheck() {
        const checks = await Promise.all([
            this.yahoo.search('AAPL').then(() => ({ yahoo: 'OK' })).catch(() => ({ yahoo: 'FAIL' })),
            this.finnhub.search('AAPL').then(() => ({ finnhub: 'OK' })).catch(() => ({ finnhub: 'FAIL' })),
            this.alphavantage.search('IBM').then(() => ({ alphavantage: 'OK' })).catch(() => ({ alphavantage: 'FAIL' }))
        ]);

        return {
            status: 'operational',
            sources: Object.assign({}, ...checks),
            timestamp: new Date().toISOString()
        };
    }
}

module.exports = DataAggregator;
