/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Multi-Source Data Aggregator V4.2 FINAL
 * SEARCH: TwelveData primary for EU markets
 * QUOTE: Yahoo primary (complete data), TwelveData fallback + separate fundamentals call
 * CACHE: Redis/Upstash for intelligent caching
 * PERFORMANCE: Limit 3 results, sequential with delay
 */

const TwelveDataClient = require('./twelveData');
const YahooFinanceClient = require('./yahooFinance');
const FinnhubClient = require('./finnhub');
const AlphaVantageClient = require('./alphaVantage');
const FinancialModelingPrepClient = require('./financialModelingPrep');
const OpenFigiClient = require('./openFigi');
const RedisCache = require('./redisCache');

class DataAggregatorV4 {
    constructor(config = {}) {
        // Initialize all data sources
        this.fmp = new FinancialModelingPrepClient(config.fmpKey || process.env.FMP_API_KEY);  // âœ… PRIMARY SOURCE
        this.twelvedata = new TwelveDataClient(config.twelveDataKey || process.env.TWELVE_DATA_API_KEY);
        this.yahoo = new YahooFinanceClient();
        this.finnhub = new FinnhubClient(config.finnhubKey || process.env.FINNHUB_API_KEY);
        this.alphavantage = new AlphaVantageClient(config.alphavantageKey || process.env.ALPHA_VANTAGE_API_KEY);
        
        // Initialize Redis Cache
        this.cache = new RedisCache(config.redisUrl || process.env.REDIS_URL);
        
        // Initialize OpenFIGI (Bloomberg ISIN resolver)
        this.openfigi = new OpenFigiClient(config.openFigiKey || process.env.OPENFIGI_API_KEY);
        
        // Priority for SEARCH: TwelveData > FMP > Yahoo > Finnhub > AlphaVantage
        this.sources = ['twelvedata', 'fmp', 'yahoo', 'finnhub', 'alphavantage'];
        
        console.log('[DataAggregatorV4] Initialized with FMP (complete fundamentals) + OpenFIGI (ISIN resolver) + Redis caching');
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
            console.log(`[DataAggregatorV4] ðŸš€ CACHE HIT! Returning cached results`);
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
                    
                    // Enrich with quotes (LIMITED + SEQUENTIAL)
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

        // Enrich results with quotes from best available source (LIMITED + SEQUENTIAL)
        const enrichedResults = await this.enrichWithQuotes(mergedResults);

        const response = {
            success: true,
            results: enrichedResults,
            metadata: {
                totalResults: enrichedResults.length,
                sources: this.sources.filter((_, i) => results[i] && results[i].success),  // âœ… NULL-SAFE
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
     * FIXED: Limit to 3 results + sequential with delay to avoid rate limiting
     */
    async enrichWithQuotes(results) {
        // LIMIT TO 3 RESULTS
        const limitedResults = results.slice(0, 3);
        const enrichedResults = [];

        console.log(`[DataAggregatorV4] Enriching ${limitedResults.length} results (limited from ${results.length})`);

        // SEQUENTIAL (not parallel) to avoid rate limiting
        for (const item of limitedResults) {
            // Check if we already have BOTH price AND fundamentals
            const hasFundamentals = item.marketCap || item.peRatio || item.dividendYield || item.week52High;
            
            if (item.price != null && typeof item.price === 'number' && hasFundamentals) {
                // Already complete, skip
                enrichedResults.push(item);
                continue;
            }

            const quote = await this.getQuote(item.symbol);
            
            if (quote.success && quote.data) {
                enrichedResults.push({
                    ...item,
                    description: quote.data.description || item.description,
                    price: item.price || quote.data.price,
                    change: item.change ?? quote.data.change,
                    changePercent: item.changePercent ?? quote.data.changePercent,
                    currency: quote.data.currency || item.currency,
                    // FUNDAMENTAL DATA - use quote data if item doesn't have it
                    marketCap: item.marketCap || quote.data.marketCap || null,
                    peRatio: item.peRatio || quote.data.peRatio || null,
                    dividendYield: item.dividendYield || quote.data.dividendYield || null,
                    week52High: item.week52High || quote.data.week52High || null,
                    week52Low: item.week52Low || quote.data.week52Low || null,
                    quoteSources: [quote.source]
                });
            } else {
                enrichedResults.push(item);
            }

            // SMALL DELAY to avoid rate limiting
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        return enrichedResults;
    }

    /**
     * Get quote with intelligent routing + CACHE
     * STRATEGY: FMP first (complete fundamentals + description), then Yahoo, then TwelveData
     */
    async getQuote(symbol) {
        console.log(`[DataAggregatorV4] Getting quote for: ${symbol}`);

        // 1. CHECK CACHE FIRST
        const cached = await this.cache.get('quote', symbol);
        if (cached) {
            console.log(`[DataAggregatorV4] ðŸš€ QUOTE CACHE HIT: ${symbol}`);
            return {
                ...cached,
                fromCache: true
            };
        }

        // âœ… TRY FINANCIAL MODELING PREP FIRST (complete fundamentals + description!)
        try {
            const fmpQuote = await this.fmp.getQuote(symbol);
            if (fmpQuote.success && fmpQuote.data) {
                console.log(`[fmp] Quote found: ${fmpQuote.data.price} USD (complete fundamentals + description)`);
                
                // SAVE TO CACHE
                await this.cache.set('quote', symbol, fmpQuote);
                
                return fmpQuote;
            }
        } catch (error) {
            console.error(`[fmp] Quote error: ${error.message}`);
        }

        // FALLBACK: Yahoo (if available)
        try {
            const yahooQuote = await this.yahoo.getQuote(symbol);
            if (yahooQuote.success && yahooQuote.data) {
                console.log(`[yahoo] Quote found: ${yahooQuote.data.price} ${yahooQuote.data.currency}`);
                
                // GET FUNDAMENTALS from Yahoo (getQuote only returns price from chart endpoint)
                console.log(`[DataAggregatorV4] Fetching fundamentals from Yahoo for ${symbol}...`);
                const fundamentals = await this.yahoo.getFundamentals(symbol);
                
                // Combine price data with fundamentals
                const combinedQuote = {
                    ...yahooQuote,
                    data: {
                        ...yahooQuote.data,
                        description: (fundamentals.success && fundamentals.data.description) || yahooQuote.data.description,
                        marketCap: fundamentals.success ? fundamentals.data.marketCap : null,
                        peRatio: fundamentals.success ? fundamentals.data.peRatio : null,
                        dividendYield: fundamentals.success ? fundamentals.data.dividendYield : null,
                        week52High: fundamentals.success ? fundamentals.data.week52High : null,
                        week52Low: fundamentals.success ? fundamentals.data.week52Low : null,
                        sector: fundamentals.success ? fundamentals.data.sector : null,
                        industry: fundamentals.success ? fundamentals.data.industry : null
                    }
                };
                
                if (fundamentals.success) {
                    console.log(`[yahoo] Added fundamentals for ${symbol}`);
                }
                
                // SAVE TO CACHE
                await this.cache.set('quote', symbol, combinedQuote);
                
                return combinedQuote;
            }
        } catch (error) {
            console.error(`[yahoo] Quote error: ${error.message}`);
        }

        // âœ… FALLBACK: TwelveData for price + separate fundamentals call
        try {
            const twelveQuote = await this.twelvedata.getQuote(symbol);
            if (twelveQuote.success && twelveQuote.data) {
                console.log(`[twelvedata] Quote found: ${twelveQuote.data.price} ${twelveQuote.data.currency}`);
                
                // ðŸ†• GET FUNDAMENTALS from TwelveData
                console.log(`[DataAggregatorV4] Fetching fundamentals from TwelveData for ${symbol}...`);
                const fundamentals = await this.twelvedata.getFundamentals(symbol);
                
                // Combine price data with fundamentals
                const combinedQuote = {
                    ...twelveQuote,
                    data: {
                        ...twelveQuote.data,
                        marketCap: fundamentals.success ? fundamentals.data.marketCap : null,
                        peRatio: fundamentals.success ? fundamentals.data.peRatio : null,
                        dividendYield: fundamentals.success ? fundamentals.data.dividendYield : null,
                        week52High: fundamentals.success ? fundamentals.data.week52High : null,
                        week52Low: fundamentals.success ? fundamentals.data.week52Low : null
                    }
                };
                
                if (fundamentals.success) {
                    console.log(`[twelvedata] Added fundamentals for ${symbol}`);
                }
                
                // SAVE TO CACHE
                await this.cache.set('quote', symbol, combinedQuote);
                
                return combinedQuote;
            }
        } catch (error) {
            console.error(`[twelvedata] Quote error: ${error.message}`);
        }

        // Fallback to Finnhub (no fundamentals)
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

        // Fallback to Alpha Vantage (no fundamentals)
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
     * STRATEGY: Cache → OpenFIGI (ISIN→ticker) → TwelveData → Yahoo → Finnhub
     * Then enrich with quotes for price data
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

        // 2. Try OpenFIGI FIRST (Bloomberg ISIN resolver - best for funds/ETF/bonds)
        try {
            const figiResult = await this.openfigi.mapISIN(isin);
            if (figiResult.success && figiResult.results.length > 0) {
                console.log(`[OpenFIGI] Resolved ISIN ${isin} → ${figiResult.results.length} instrument(s)`);
                
                // Get the best Yahoo-compatible symbol
                const bestSymbol = OpenFigiClient.getBestYahooSymbol(figiResult.results);
                const bestResult = figiResult.results[0];
                
                // Build search results from OpenFIGI data
                const results = figiResult.results.slice(0, 3).map(r => {
                    const suffix = OpenFigiClient.exchangeToYahooSuffix(r.exchange);
                    return {
                        symbol: r.symbol + suffix,
                        name: r.name,
                        type: r.type,
                        exchange: r.exchange,
                        currency: r.currency || '',
                        isin: isin.toUpperCase(),
                        source: 'openfigi',
                        figi: r.figi
                    };
                }).filter(r => r.symbol); // Remove entries without ticker

                if (results.length > 0) {
                    // Enrich with real-time quotes (price, P/E, etc.)
                    const enriched = await this.enrichWithQuotes(results);
                    
                    const response = {
                        success: true,
                        results: enriched,
                        metadata: {
                            totalResults: enriched.length,
                            sources: ['openfigi'],
                            primarySource: 'openfigi',
                            isin: isin.toUpperCase(),
                            timestamp: new Date().toISOString(),
                            fromCache: false
                        }
                    };
                    
                    // SAVE TO CACHE (ISIN mapping is stable, long TTL)
                    await this.cache.set('isin', isin, response, 86400); // 24h
                    return response;
                }
            }
        } catch (error) {
            console.error(`[OpenFIGI] ISIN search error: ${error.message}`);
        }

        // 3. Fallback: Try TwelveData
        try {
            const twelveResult = await this.twelvedata.searchByISIN(isin);
            if (twelveResult.success && twelveResult.results.length > 0) {
                // Enrich with quotes
                const enriched = await this.enrichWithQuotes(twelveResult.results);
                const response = {
                    success: true,
                    results: enriched,
                    metadata: {
                        totalResults: enriched.length,
                        sources: ['twelvedata'],
                        isin: isin.toUpperCase(),
                        timestamp: new Date().toISOString(),
                        fromCache: false
                    }
                };
                await this.cache.set('isin', isin, response, 86400);
                return response;
            }
        } catch (error) {
            console.error(`[twelvedata] ISIN search error: ${error.message}`);
        }

        // 4. Fallback: Try Yahoo
        try {
            const yahooResult = await this.yahoo.searchByISIN(isin);
            if (yahooResult.success && yahooResult.results.length > 0) {
                const enriched = await this.enrichWithQuotes(yahooResult.results);
                const response = {
                    success: true,
                    results: enriched,
                    metadata: {
                        totalResults: enriched.length,
                        sources: ['yahoo'],
                        isin: isin.toUpperCase(),
                        timestamp: new Date().toISOString(),
                        fromCache: false
                    }
                };
                await this.cache.set('isin', isin, response, 86400);
                return response;
            }
        } catch (error) {
            console.error(`[yahoo] ISIN search error: ${error.message}`);
        }

        // 5. Fallback: Try Finnhub
        try {
            const finnhubResult = await this.finnhub.searchByISIN(isin);
            if (finnhubResult.success && finnhubResult.results.length > 0) {
                const enriched = await this.enrichWithQuotes(finnhubResult.results);
                const response = {
                    success: true,
                    results: enriched,
                    metadata: {
                        totalResults: enriched.length,
                        sources: ['finnhub'],
                        isin: isin.toUpperCase(),
                        timestamp: new Date().toISOString(),
                        fromCache: false
                    }
                };
                await this.cache.set('isin', isin, response, 86400);
                return response;
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
     * Get complete instrument details with fundamentals (description, marketCap, PE, etc)
     * PRIMARY: Financial Modeling Prep (complete data)
     * FALLBACK: Quote data
     */
    async getInstrumentDetails(symbol) {
        console.log(`[DataAggregatorV4] Getting instrument details for: ${symbol}`);
        
        // 1. CHECK CACHE FIRST
        const cached = await this.cache.get('details', symbol);
        if (cached) {
            console.log(`[DataAggregatorV4] ðŸš€ CACHE HIT! Returning cached details`);
            return {
                ...cached,
                fromCache: true
            };
        }
        
        // 2. Try Financial Modeling Prep (complete fundamentals)
        try {
            const fmpResult = await this.fmp.getQuote(symbol);
            if (fmpResult.success) {
                console.log(`[FMP] Complete details for ${symbol}: ${fmpResult.data.price} ${fmpResult.data.currency}`);
                
                const response = {
                    success: true,
                    data: fmpResult.data,
                    source: 'fmp',
                    fromCache: false
                };
                
                // SAVE TO CACHE (longer TTL for details: 1 hour)
                await this.cache.set('details', symbol, response, 3600);
                
                return response;
            }
        } catch (error) {
            console.error(`[FMP] Details error: ${error.message}`);
        }
        
        // 3. Fallback: Try to get quote data (less complete but better than nothing)
        try {
            const quoteResult = await this.getQuote(symbol);
            if (quoteResult.success) {
                console.log(`[DataAggregatorV4] Using quote data as fallback for details`);
                
                const response = {
                    success: true,
                    data: {
                        ...quoteResult.data,
                        description: quoteResult.data.name || `${symbol} stock`,
                        // Add placeholder fundamentals if missing
                        marketCap: quoteResult.data.marketCap || null,
                        peRatio: quoteResult.data.peRatio || null,
                        dividendYield: quoteResult.data.dividendYield || null,
                        week52High: quoteResult.data.week52High || null,
                        week52Low: quoteResult.data.week52Low || null
                    },
                    source: quoteResult.source,
                    fromCache: false
                };
                
                // SAVE TO CACHE
                await this.cache.set('details', symbol, response, 3600);
                
                return response;
            }
        } catch (error) {
            console.error(`[DataAggregatorV4] Fallback details error: ${error.message}`);
        }
        
        return {
            success: false,
            error: 'Could not fetch instrument details from any source'
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
                .catch(() => ({ alphavantage: 'FAIL' })),
            this.openfigi.mapISIN('US0378331005')
                .then(r => ({ openfigi: r.success ? 'OK' : 'FAIL' }))
                .catch(() => ({ openfigi: 'FAIL' }))
        ]);

        const sources = Object.assign({}, ...checks);
        const usageStats = this.twelvedata.getUsageStats();
        
        // Redis health check
        const redisHealth = await this.cache.healthCheck();
        const cacheStats = await this.cache.getStats();

        return {
            status: 'operational',
            version: '4.2.0-FINAL',
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
