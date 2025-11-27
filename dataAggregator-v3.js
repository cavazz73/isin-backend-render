/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Multi-Source Data Aggregator V3.0
 * PRIMARY: TwelveData (excellent for EU markets with correct EUR pricing)
 * FALLBACK: Yahoo Finance, Finnhub, Alpha Vantage
 */

const TwelveDataClient = require('./twelveData');
const YahooFinanceClient = require('./yahooFinance');
const FinnhubClient = require('./finnhub');
const AlphaVantageClient = require('./alphaVantage');

class DataAggregatorV3 {
    constructor(config = {}) {
        // Initialize all data sources
        this.twelvedata = new TwelveDataClient(config.twelveDataKey || process.env.TWELVE_DATA_API_KEY);
        this.yahoo = new YahooFinanceClient();
        this.finnhub = new FinnhubClient(config.finnhubKey || process.env.FINNHUB_API_KEY);
        this.alphavantage = new AlphaVantageClient(config.alphavantageKey || process.env.ALPHA_VANTAGE_API_KEY);
        
        // Priority order: TwelveData > Yahoo > Finnhub > AlphaVantage
        this.sources = ['twelvedata', 'yahoo', 'finnhub', 'alphavantage'];
        
        console.log('[DataAggregatorV3] Initialized with all sources');
    }

    /**
     * Determine if query is for European market
     */
    isEuropeanMarket(query) {
        const upperQuery = query.toUpperCase();
        
        // Italian stocks
        const italianStocks = ['ENEL', 'ENI', 'INTESA', 'UNICREDIT', 'GENERALI', 'FERRARI', 
                               'STELLANTIS', 'LEONARDO', 'PRYSMIAN', 'TELECOM'];
        
        // Check if query contains European exchange suffixes
        const europeanSuffixes = ['.MI', '.PA', '.DE', '.L', '.AS', '.SW', '.MC'];
        
        // Check if it's a known Italian stock or has EU suffix
        return italianStocks.includes(upperQuery) || 
               europeanSuffixes.some(suffix => upperQuery.includes(suffix));
    }

    /**
     * Search across all sources with intelligent routing
     */
    async search(query) {
        console.log(`[DataAggregatorV3] Searching for: "${query}"`);
        
        const isEU = this.isEuropeanMarket(query);
        console.log(`[DataAggregatorV3] European market: ${isEU ? 'YES' : 'NO'}`);

        // For European markets, prioritize TwelveData
        if (isEU) {
            try {
                const twelveResult = await this.twelvedata.search(query);
                if (twelveResult.success && twelveResult.results.length > 0) {
                    console.log(`[DataAggregatorV3] TwelveData found ${twelveResult.results.length} results (EU)`);
                    
                    // Enrich with quotes
                    const enriched = await this.enrichWithQuotes(twelveResult.results);
                    
                    return {
                        success: true,
                        results: enriched,
                        metadata: {
                            totalResults: enriched.length,
                            sources: ['twelvedata'],
                            primarySource: 'twelvedata',
                            region: 'EU',
                            timestamp: new Date().toISOString()
                        }
                    };
                }
            } catch (error) {
                console.error(`[DataAggregatorV3] TwelveData error: ${error.message}`);
            }
        }

        // Try all sources in parallel
        const searchPromises = [
            this.twelvedata.search(query).catch(e => ({ success: false, results: [], error: e.message })),
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
                    symbolMap.set(symbol, {
                        ...item,
                        sources: [source]
                    });
                } else {
                    const existing = symbolMap.get(symbol);
                    
                    // Prefer TwelveData, then Yahoo, then Finnhub, then AlphaVantage
                    if (source === 'twelvedata' || 
                        (source === 'yahoo' && existing.sources[0] !== 'twelvedata') ||
                        (source === 'finnhub' && !['twelvedata', 'yahoo'].includes(existing.sources[0]))) {
                        symbolMap.set(symbol, {
                            ...existing,
                            ...item,
                            sources: [...existing.sources, source]
                        });
                    } else {
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
            if (item.price != null && typeof item.price === 'number') {
                return item;
            }

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
     * Get quote with intelligent routing
     */
    async getQuote(symbol) {
        console.log(`[DataAggregatorV3] Getting quote for: ${symbol}`);

        // Try TwelveData first (best for EU markets)
        try {
            const twelveQuote = await this.twelvedata.getQuote(symbol);
            if (twelveQuote.success && twelveQuote.data) {
                console.log(`[twelvedata] Quote found: ${twelveQuote.data.price} ${twelveQuote.data.currency}`);
                return twelveQuote;
            }
        } catch (error) {
            console.error(`[twelvedata] Quote error: ${error.message}`);
        }

        // Fallback to Yahoo
        try {
            const yahooQuote = await this.yahoo.getQuote(symbol);
            if (yahooQuote.success && yahooQuote.data) {
                console.log(`[yahoo] Quote found: ${yahooQuote.data.price} ${yahooQuote.data.currency}`);
                return yahooQuote;
            }
        } catch (error) {
            console.error(`[yahoo] Quote error: ${error.message}`);
        }

        // Fallback to Finnhub
        try {
            const finnhubQuote = await this.finnhub.getQuote(symbol);
            if (finnhubQuote.success && finnhubQuote.data) {
                console.log(`[finnhub] Quote found: ${finnhubQuote.data.price}`);
                return finnhubQuote;
            }
        } catch (error) {
            console.error(`[finnhub] Quote error: ${error.message}`);
        }

        // Fallback to Alpha Vantage
        try {
            const avQuote = await this.alphavantage.getQuote(symbol);
            if (avQuote.success && avQuote.data) {
                console.log(`[alphavantage] Quote found: ${avQuote.data.price}`);
                return avQuote;
            }
        } catch (error) {
            console.error(`[alphavantage] Quote error: ${error.message}`);
        }

        return {
            success: false,
            error: 'No quote data available from any source'
        };
    }

    /**
     * Get historical data with intelligent routing
     */
    async getHistoricalData(symbol, period = '1M') {
        console.log(`[DataAggregatorV3] Getting historical data: ${symbol}, period: ${period}`);

        // Try TwelveData first
        try {
            const twelveHistorical = await this.twelvedata.getHistoricalData(symbol, period);
            if (twelveHistorical.success && twelveHistorical.data && twelveHistorical.data.length > 0) {
                console.log(`[twelvedata] Historical data: ${twelveHistorical.data.length} points`);
                return twelveHistorical;
            }
        } catch (error) {
            console.error(`[twelvedata] Historical error: ${error.message}`);
        }

        // Fallback to Yahoo
        try {
            const yahooHistorical = await this.yahoo.getHistoricalData(symbol, period);
            if (yahooHistorical.success && yahooHistorical.data && yahooHistorical.data.length > 0) {
                console.log(`[yahoo] Historical data: ${yahooHistorical.data.length} points`);
                return yahooHistorical;
            }
        } catch (error) {
            console.error(`[yahoo] Historical error: ${error.message}`);
        }

        // Fallback to Alpha Vantage
        try {
            const avHistorical = await this.alphavantage.getHistoricalData(symbol, period);
            if (avHistorical.success && avHistorical.data && avHistorical.data.length > 0) {
                console.log(`[alphavantage] Historical data: ${avHistorical.data.length} points`);
                return avHistorical;
            }
        } catch (error) {
            console.error(`[alphavantage] Historical error: ${error.message}`);
        }

        return {
            success: false,
            error: 'No historical data available from any source'
        };
    }

    /**
     * Search by ISIN with fallback
     */
    async searchByISIN(isin) {
        console.log(`[DataAggregatorV3] Searching by ISIN: ${isin}`);

        // Try TwelveData
        try {
            const twelveResult = await this.twelvedata.searchByISIN(isin);
            if (twelveResult.success && twelveResult.results.length > 0) {
                return twelveResult;
            }
        } catch (error) {
            console.error(`[twelvedata] ISIN search error: ${error.message}`);
        }

        // Try Yahoo
        try {
            const yahooResult = await this.yahoo.searchByISIN(isin);
            if (yahooResult.success && yahooResult.results.length > 0) {
                return yahooResult;
            }
        } catch (error) {
            console.error(`[yahoo] ISIN search error: ${error.message}`);
        }

        // Try Finnhub
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
        console.log('[DataAggregatorV3] Running health check...');
        
        const checks = await Promise.all([
            this.twelvedata.search('AAPL')
                .then(() => ({ twelvedata: 'OK' }))
                .catch(() => ({ twelvedata: 'FAIL' })),
            this.yahoo.search('AAPL')
                .then(() => ({ yahoo: 'OK' }))
                .catch(() => ({ yahoo: 'FAIL' })),
            this.finnhub.search('AAPL')
                .then(() => ({ finnhub: 'OK' }))
                .catch(() => ({ finnhub: 'FAIL' })),
            this.alphavantage.search('IBM')
                .then(() => ({ alphavantage: 'OK' }))
                .catch(() => ({ alphavantage: 'FAIL' }))
        ]);

        const sources = Object.assign({}, ...checks);
        const usageStats = this.twelvedata.getUsageStats();

        return {
            status: 'operational',
            version: '3.0.0',
            sources: sources,
            twelveDataUsage: usageStats,
            timestamp: new Date().toISOString()
        };
    }
}

module.exports = DataAggregatorV3;
