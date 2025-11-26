/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Multi-Source Data Aggregator v2.3
 * Yahoo Finance Primary with Retry, Caching, and Rate Limiting
 * Optimized for free tier usage
 */

const YahooFinanceClient = require('./yahooFinance');
const FinnhubClient = require('./finnhub');
const AlphaVantageClient = require('./alphaVantage');

class DataAggregator {
    constructor(config = {}) {
        this.yahoo = new YahooFinanceClient();
        this.finnhub = new FinnhubClient(config.finnhubKey);
        this.alphavantage = new AlphaVantageClient(config.alphavantageKey);
        
        // v2.3: Yahoo as primary, others as emergency fallback only
        this.sources = ['yahoo', 'finnhub', 'alphavantage'];
        
        // In-memory cache with TTL
        this.cache = new Map();
        this.cacheTTL = 5 * 60 * 1000; // 5 minutes
        
        // Rate limiting
        this.lastRequestTime = 0;
        this.minRequestInterval = 200; // 200ms between requests
        
        // Retry configuration
        this.maxRetries = 3;
        this.baseDelay = 1000; // 1 second
        
        // Limit enrichment to reduce API calls
        this.maxEnrichResults = 3;
        
        // European stock indicators
        this.europeanSuffixes = ['.MI', '.PA', '.DE', '.AS', '.L', '.MC', '.SW', '.BR', '.LS', '.HE', '.ST', '.CO', '.OL'];
        this.italianStockNames = [
            'ENEL', 'ENI', 'INTESA', 'ISP', 'UNICREDIT', 'UCG', 'GENERALI', 
            'FERRARI', 'RACE', 'STELLANTIS', 'STLA', 'LEONARDO', 'LDO',
            'PRYSMIAN', 'PRY', 'TELECOM', 'TIT', 'TIM', 'MONCLER', 'MONC',
            'CAMPARI', 'CPR', 'PIRELLI', 'PIRC', 'NEXI', 'SNAM', 'SRG',
            'TERNA', 'TRN', 'MEDIOBANCA', 'MB', 'POSTE', 'PST', 'A2A',
            'HERA', 'HER', 'SAIPEM', 'SPM', 'AMPLIFON', 'AMP', 'DIASORIN', 'DIA'
        ];
        
        console.log('[DataAggregator v2.3] Initialized with Yahoo Finance primary, cache enabled');
    }

    // ========================================
    // CACHING UTILITIES
    // ========================================
    
    getCached(key) {
        const cached = this.cache.get(key);
        if (cached && Date.now() - cached.timestamp < this.cacheTTL) {
            console.log(`[Cache] HIT for: ${key}`);
            return cached.data;
        }
        if (cached) {
            this.cache.delete(key); // Expired
        }
        return null;
    }
    
    setCache(key, data) {
        this.cache.set(key, {
            data: data,
            timestamp: Date.now()
        });
        console.log(`[Cache] SET for: ${key}`);
    }
    
    clearExpiredCache() {
        const now = Date.now();
        for (const [key, value] of this.cache.entries()) {
            if (now - value.timestamp > this.cacheTTL) {
                this.cache.delete(key);
            }
        }
    }

    // ========================================
    // RATE LIMITING & RETRY
    // ========================================
    
    async waitForRateLimit() {
        const now = Date.now();
        const timeSinceLastRequest = now - this.lastRequestTime;
        if (timeSinceLastRequest < this.minRequestInterval) {
            const waitTime = this.minRequestInterval - timeSinceLastRequest;
            await this.sleep(waitTime);
        }
        this.lastRequestTime = Date.now();
    }
    
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    async retryWithBackoff(fn, context = 'operation') {
        let lastError;
        
        for (let attempt = 0; attempt < this.maxRetries; attempt++) {
            try {
                await this.waitForRateLimit();
                const result = await fn();
                return result;
            } catch (error) {
                lastError = error;
                const delay = this.baseDelay * Math.pow(2, attempt);
                console.warn(`[Retry] ${context} attempt ${attempt + 1}/${this.maxRetries} failed: ${error.message}. Waiting ${delay}ms`);
                
                // Check if it's a rate limit error (401, 403, 429)
                if (error.response?.status === 401 || 
                    error.response?.status === 403 || 
                    error.response?.status === 429) {
                    await this.sleep(delay);
                } else {
                    // For other errors, don't retry
                    throw error;
                }
            }
        }
        
        throw lastError;
    }

    // ========================================
    // SEARCH
    // ========================================

    isEuropeanQuery(query) {
        const upperQuery = query.toUpperCase().trim();
        
        for (const suffix of this.europeanSuffixes) {
            if (upperQuery.endsWith(suffix)) return true;
        }
        
        if (this.italianStockNames.includes(upperQuery)) return true;
        
        return false;
    }

    async search(query) {
        console.log(`[DataAggregator v2.3] Searching for: "${query}"`);
        const isEuropean = this.isEuropeanQuery(query);
        console.log(`[DataAggregator] European market: ${isEuropean}`);
        
        // Check cache first
        const cacheKey = `search:${query.toLowerCase()}`;
        const cached = this.getCached(cacheKey);
        if (cached) {
            return cached;
        }
        
        // v2.3: Try Yahoo first with retry
        let yahooResult = null;
        try {
            yahooResult = await this.retryWithBackoff(
                () => this.yahoo.search(query),
                `Yahoo search "${query}"`
            );
        } catch (error) {
            console.error(`[Yahoo] Search failed after retries: ${error.message}`);
            yahooResult = { success: false, results: [], error: error.message };
        }
        
        // If Yahoo succeeded with results, use them
        if (yahooResult.success && yahooResult.results?.length > 0) {
            console.log(`[Yahoo] Found ${yahooResult.results.length} results`);
            
            // Sort results for European queries
            let sortedResults = yahooResult.results;
            if (isEuropean) {
                sortedResults = this.sortEuropeanResults(yahooResult.results);
            }
            
            // v2.3: Enrich only top N results to save API calls
            const enrichedResults = await this.enrichWithQuotes(
                sortedResults.slice(0, this.maxEnrichResults), 
                isEuropean
            );
            
            // Add non-enriched results
            const finalResults = [
                ...enrichedResults,
                ...sortedResults.slice(this.maxEnrichResults)
            ];
            
            const response = {
                success: true,
                results: finalResults,
                metadata: {
                    totalResults: finalResults.length,
                    sources: ['yahoo'],
                    isEuropean: isEuropean,
                    enrichedCount: enrichedResults.length,
                    cached: false,
                    timestamp: new Date().toISOString()
                }
            };
            
            this.setCache(cacheKey, response);
            return response;
        }
        
        // v2.3: Fallback to Finnhub only if Yahoo completely failed
        console.log('[DataAggregator] Yahoo failed, trying Finnhub fallback...');
        try {
            const finnhubResult = await this.finnhub.search(query);
            if (finnhubResult.success && finnhubResult.results?.length > 0) {
                console.log(`[Finnhub] Found ${finnhubResult.results.length} results`);
                
                const response = {
                    success: true,
                    results: finnhubResult.results.slice(0, 5),
                    metadata: {
                        totalResults: finnhubResult.results.length,
                        sources: ['finnhub'],
                        isEuropean: isEuropean,
                        cached: false,
                        timestamp: new Date().toISOString()
                    }
                };
                
                this.setCache(cacheKey, response);
                return response;
            }
        } catch (error) {
            console.error(`[Finnhub] Fallback failed: ${error.message}`);
        }
        
        // No results from any source
        return {
            success: false,
            results: [],
            metadata: {
                sources: this.sources,
                isEuropean: isEuropean,
                error: 'No results from any source'
            }
        };
    }

    sortEuropeanResults(results) {
        return results.sort((a, b) => {
            // Italian stocks first (.MI)
            const aIsItalian = a.symbol?.endsWith('.MI');
            const bIsItalian = b.symbol?.endsWith('.MI');
            if (aIsItalian && !bIsItalian) return -1;
            if (!aIsItalian && bIsItalian) return 1;
            
            // Then other European
            const aIsEU = this.europeanSuffixes.some(s => a.symbol?.endsWith(s));
            const bIsEU = this.europeanSuffixes.some(s => b.symbol?.endsWith(s));
            if (aIsEU && !bIsEU) return -1;
            if (!aIsEU && bIsEU) return 1;
            
            return 0;
        });
    }

    // ========================================
    // ENRICH WITH QUOTES (OPTIMIZED)
    // ========================================

    async enrichWithQuotes(results, isEuropean = false) {
        console.log(`[DataAggregator] Enriching ${results.length} results with quotes`);
        
        // v2.3: Process sequentially with delay to avoid rate limits
        const enrichedResults = [];
        
        for (const item of results) {
            // Skip if already has valid price
            if (item.price != null && typeof item.price === 'number' && item.price > 0) {
                enrichedResults.push(item);
                continue;
            }
            
            // Check cache for quote
            const quoteCacheKey = `quote:${item.symbol}`;
            const cachedQuote = this.getCached(quoteCacheKey);
            
            if (cachedQuote) {
                enrichedResults.push({
                    ...item,
                    price: cachedQuote.price,
                    change: cachedQuote.change,
                    changePercent: cachedQuote.changePercent,
                    currency: cachedQuote.currency || item.currency,
                    quoteSource: 'cache'
                });
                continue;
            }
            
            // Get quote with retry
            try {
                const quote = await this.getQuote(item.symbol, isEuropean);
                
                if (quote.success && quote.data) {
                    enrichedResults.push({
                        ...item,
                        price: quote.data.price,
                        change: quote.data.change,
                        changePercent: quote.data.changePercent,
                        currency: quote.data.currency || item.currency,
                        quoteSource: quote.source
                    });
                } else {
                    enrichedResults.push(item);
                }
            } catch (error) {
                console.warn(`[Enrich] Failed to get quote for ${item.symbol}: ${error.message}`);
                enrichedResults.push(item);
            }
            
            // Small delay between requests
            await this.sleep(100);
        }
        
        return enrichedResults;
    }

    // ========================================
    // GET QUOTE (YAHOO PRIMARY)
    // ========================================

    async getQuote(symbol, isEuropean = null) {
        if (isEuropean === null) {
            isEuropean = this.isEuropeanQuery(symbol);
        }
        
        console.log(`[DataAggregator] Getting quote for: ${symbol}`);
        
        // Check cache first
        const cacheKey = `quote:${symbol}`;
        const cached = this.getCached(cacheKey);
        if (cached) {
            return { success: true, data: cached, source: 'cache' };
        }
        
        // v2.3: Yahoo with retry
        try {
            const result = await this.retryWithBackoff(
                () => this.yahoo.getQuote(symbol),
                `Yahoo quote "${symbol}"`
            );
            
            if (result.success && result.data && result.data.price != null) {
                console.log(`[Yahoo] Quote found for ${symbol}: ${result.data.price} ${result.data.currency}`);
                this.setCache(cacheKey, result.data);
                return result;
            }
        } catch (error) {
            console.error(`[Yahoo] Quote failed for ${symbol}: ${error.message}`);
        }
        
        // Fallback to Finnhub (only for non-European stocks)
        if (!isEuropean) {
            try {
                const finnhubResult = await this.finnhub.getQuote(symbol);
                if (finnhubResult.success && finnhubResult.data && finnhubResult.data.price != null) {
                    console.log(`[Finnhub] Quote found for ${symbol}`);
                    this.setCache(cacheKey, finnhubResult.data);
                    return finnhubResult;
                }
            } catch (error) {
                console.error(`[Finnhub] Quote failed for ${symbol}: ${error.message}`);
            }
        }
        
        return {
            success: false,
            error: 'No quote data available'
        };
    }

    // ========================================
    // HISTORICAL DATA (YAHOO ONLY)
    // ========================================

    async getHistoricalData(symbol, period = '1M') {
        console.log(`[DataAggregator] Getting historical data for: ${symbol}, period: ${period}`);
        
        // Check cache
        const cacheKey = `historical:${symbol}:${period}`;
        const cached = this.getCached(cacheKey);
        if (cached) {
            return { success: true, symbol, data: cached, source: 'cache' };
        }
        
        // v2.3: Yahoo with retry (historical data is more reliable)
        try {
            const result = await this.retryWithBackoff(
                () => this.yahoo.getHistoricalData(symbol, period),
                `Yahoo historical "${symbol}"`
            );
            
            if (result.success && result.data && result.data.length > 0) {
                console.log(`[Yahoo] Historical data found: ${result.data.length} points`);
                this.setCache(cacheKey, result.data);
                return result;
            }
        } catch (error) {
            console.error(`[Yahoo] Historical failed for ${symbol}: ${error.message}`);
        }
        
        // Fallback to Alpha Vantage (limited daily calls)
        try {
            const avResult = await this.alphavantage.getHistoricalData(symbol, period);
            if (avResult.success && avResult.data && avResult.data.length > 0) {
                console.log(`[AlphaVantage] Historical data found: ${avResult.data.length} points`);
                this.setCache(cacheKey, avResult.data);
                return avResult;
            }
        } catch (error) {
            console.error(`[AlphaVantage] Historical failed: ${error.message}`);
        }
        
        return {
            success: false,
            error: 'No historical data available from any source'
        };
    }

    // ========================================
    // ISIN SEARCH
    // ========================================

    async searchByISIN(isin) {
        console.log(`[DataAggregator] Searching by ISIN: ${isin}`);
        
        // Check cache
        const cacheKey = `isin:${isin}`;
        const cached = this.getCached(cacheKey);
        if (cached) {
            return cached;
        }
        
        try {
            const result = await this.retryWithBackoff(
                () => this.yahoo.searchByISIN(isin),
                `Yahoo ISIN "${isin}"`
            );
            
            if (result.success && result.results.length > 0) {
                this.setCache(cacheKey, result);
                return result;
            }
        } catch (error) {
            console.error(`[Yahoo] ISIN search failed: ${error.message}`);
        }
        
        return {
            success: false,
            results: [],
            error: 'No results for ISIN'
        };
    }

    // ========================================
    // HEALTH CHECK
    // ========================================

    async healthCheck() {
        const checks = {};
        
        // Test Yahoo
        try {
            await this.yahoo.search('AAPL');
            checks.yahoo = 'OK';
        } catch (error) {
            checks.yahoo = `FAIL: ${error.message}`;
        }
        
        // Test Finnhub
        try {
            await this.finnhub.search('AAPL');
            checks.finnhub = 'OK';
        } catch (error) {
            checks.finnhub = `FAIL: ${error.message}`;
        }
        
        // Test Alpha Vantage
        try {
            await this.alphavantage.search('IBM');
            checks.alphavantage = 'OK';
        } catch (error) {
            checks.alphavantage = `FAIL: ${error.message}`;
        }
        
        return {
            status: checks.yahoo === 'OK' ? 'operational' : 'degraded',
            sources: checks,
            cache: {
                size: this.cache.size,
                ttl: this.cacheTTL / 1000 + 's'
            },
            version: '2.3.0',
            timestamp: new Date().toISOString()
        };
    }

    // ========================================
    // STATS
    // ========================================

    getUsageStats() {
        return {
            cache: {
                entries: this.cache.size,
                ttl: this.cacheTTL / 1000 + 's'
            },
            alphavantage: {
                remaining: this.alphavantage.dailyLimit - this.alphavantage.requestCount,
                limit: 25,
                period: 'daily'
            },
            finnhub: {
                limit: 60,
                period: 'per minute'
            },
            yahoo: {
                limit: 'unlimited (with rate limiting)',
                period: 'N/A'
            }
        };
    }
}

module.exports = DataAggregator;
