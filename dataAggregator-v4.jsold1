/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Multi-Source Data Aggregator V4.1 - WITH REDIS CACHE + FUNDAMENTAL DATA
 * PRIMARY: TwelveData (excellent for EU markets with correct EUR pricing)
 * FALLBACK: Yahoo Finance, Finnhub, Alpha Vantage
 * CACHE: Redis/Upstash for intelligent caching
 */

const TwelveDataClient = require('./twelveData');
const YahooFinanceClient = require('./yahooFinance');
const FinnhubClient = require('./finnhub');
const AlphaVantageClient = require('./alphaVantage');
const RedisCache = require('./redisCache');

class DataAggregatorV4 {
    constructor(config = {}) {
        // Initialize all data sources
        this.twelvedata = new TwelveDataClient(config.twelveDataKey || process.env.TWELVE_DATA_API_KEY);
        this.yahoo = new YahooFinanceClient();
        this.finnhub = new FinnhubClient(config.finnhubKey || process.env.FINNHUB_API_KEY);
        this.alphavantage = new AlphaVantageClient(config.alphavantageKey || process.env.ALPHA_VANTAGE_API_KEY);
        
        // Initialize Redis Cache
        this.cache = new RedisCache(config.redisUrl || process.env.REDIS_URL);
        
        // Priority order: TwelveData > Yahoo > Finnhub > AlphaVantage
        this.sources = ['twelvedata', 'yahoo', 'finnhub', 'alphavantage'];
        
        console.log('[DataAggregatorV4] Initialized with Redis caching');
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
     * Search across all sources with intelligent routing + CACHE
     */
    async search(query) {
        console.log(`[DataAggregatorV4] Searching for: "${query}"`);
        
        // 1. CHECK CACHE FIRST
        const cached = await this.cache.get('search', query);
        if (cached) {
            console.log(`[DataAggregatorV4] 🚀 CACHE HIT! Returning cached results`);
            return {
                ...cached,
                fromCache: true,
                cacheTimestamp: new Date().toISOString()
            };
        }
        
        const isEU = this.isEuropeanMarket(query);
        console.log(`[DataAggregatorV4] European market: ${isEU ? 'YES' : 'NO'}`);

        // For European markets, prioritize TwelveData
        if (isEU) {
            try {
                const twelveResult = await this.twelvedata.search(query);
                if (twelveResult.success && twelveResult.results.length > 0) {
                    console.log(`[DataAggregatorV4] TwelveData found ${twelveResult.results.length} results (EU)`);
                    
                    // Enrich with quotes
                    const enriched = await this.enrichWithQuotes(twelveResult.results);
                    
                    const response = {
                        success: true,
                        results: enriched,
                        metadata: {
                            totalResults: enriched.length,
                            sources: ['twelvedata'],
                            primarySource: 'twelvedata',
                            region: 'EU',
                            timestamp: new Date().toISOString(),
                            fromCache: false
                        }
                    };
                    
                    // SAVE TO CACHE
                    await this.cache.set('search', query, response);
                    
                    return response;
                }
            } catch (error) {
                console.error(`[DataAggregatorV4] TwelveData error: ${error.message}`);
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
                    errors: results.map((r, i) => ({ source: this.sources[i], error: r.error })),
                    fromCache: false
                }
            };
        }

        // Enrich results with quotes from best available source
        const enrichedResults = await this.enrichWithQuotes(mergedResults);

        const response = {
            success: true,
            results: enrichedResults,
            metadata: {
                totalResults: enrichedResults.length,
                sources: this.sources.filter((_, i) => results[i].success),
                timestamp: new Date().toISOString(),
                fromCache: false
            }
        };
        
        // SAVE TO CACHE
        await this.cache.set('search', query, response);

        return response;
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
     * Enrich search results with real-time quotes + FUNDAMENTAL DATA
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
                    // ✅ FUNDAMENTAL DATA MAPPING
                    marketCap: quote.data.marketCap || null,
                    peRatio: quote.data.peRatio || null,
                    dividendYield: quote.data.dividendYield || null,
                    week52High: quote.data.week52High || null,
                    week52Low: quote.data.week52Low || null,
                    quoteSources: [quote.source]
                };
            }

            return item;
        });

        return await Promise.all(enrichPromises);
    }

    /**
     * Get quote with intelligent routing + CACHE
     */
    async getQuote(symbol) {
        console.log(`[DataAggregatorV4] Getting quote for: ${symbol}`);

        // 1. CHECK CACHE FIRST
        const cached = await this.cache.get('quote', symbol);
        if (cached) {
            console.log(`[DataAggregatorV4] 🚀 QUOTE CACHE HIT: ${symbol}`);
            return {
                ...cached,
                fromCache: true
            };
        }

        // Try TwelveData first
        try {
            const twelveQuote = await this.twelvedata.getQuote(symbol);
            if (twelveQuote.success && twelveQuote.data) {
                console.log(`[twelvedata] Quote found: ${twelveQuote.data.price} ${twelveQuote.data.currency}`);
                
                // SAVE TO CACHE
                await this.cache.set('quote', symbol, twelveQuote);
                
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
                
                // SAVE TO CACHE
                await this.cache.set('quote', symbol, yahooQuote);
                
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
                
                // SAVE TO CACHE
                await this.cache.set('quote', symbol, finnhubQuote);
                
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
                
                // SAVE TO CACHE
                await this.cache.set('quote', symbol, avQuote);
                
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
        console.log(`[DataAggregatorV4] Getting historical data: ${symbol}, period: ${period}`);

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
     * Search by ISIN with fallback + CACHE
     */
    async searchByISIN(isin) {
        console.log(`[DataAggregatorV4] Searching by ISIN: ${isin}`);

        // 1. CHECK CACHE FIRST
        const cached = await this.cache.get('isin', isin);
        if (cached) {
            console.log(`[DataAggregatorV4] 🚀 ISIN CACHE HIT!`);
            return {
                ...cached,
                fromCache: true
            };
        }

        // Try TwelveData
        try {
            const twelveResult = await this.twelvedata.searchByISIN(isin);
            if (twelveResult.success && twelveResult.results.length > 0) {
                // SAVE TO CACHE (ISIN never changes, 30 days)
                await this.cache.set('isin', isin, twelveResult);
                return twelveResult;
            }
        } catch (error) {
            console.error(`[twelvedata] ISIN search error: ${error.message}`);
        }

        // Try Yahoo
        try {
            const yahooResult = await this.yahoo.searchByISIN(isin);
            if (yahooResult.success && yahooResult.results.length > 0) {
                // SAVE TO CACHE
                await this.cache.set('isin', isin, yahooResult);
                return yahooResult;
            }
        } catch (error) {
            console.error(`[yahoo] ISIN search error: ${error.message}`);
        }

        // Try Finnhub
        try {
            const finnhubResult = await this.finnhub.searchByISIN(isin);
            if (finnhubResult.success && finnhubResult.results.length > 0) {
                // SAVE TO CACHE
                await this.cache.set('isin', isin, finnhubResult);
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
     * Health check for all sources + Redis
     */
    async healthCheck() {
        console.log('[DataAggregatorV4] Running health check...');
        
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
        
        // Redis health check
        const redisHealth = await this.cache.healthCheck();
        const cacheStats = await this.cache.getStats();

        return {
            status: 'operational',
            version: '4.1.0',
            sources: sources,
            twelveDataUsage: usageStats,
            redis: {
                health: redisHealth,
                stats: cacheStats
            },
            timestamp: new Date().toISOString()
        };
    }

    /**
     * Get cache statistics
     */
    async getCacheStats() {
        return await this.cache.getStats();
    }

    /**
     * Clear all cache
     */
    async clearCache() {
        return await this.cache.clearAll();
    }
}

module.exports = DataAggregatorV4;
