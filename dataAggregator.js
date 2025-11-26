/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Multi-Source Data Aggregator v2.3
 * OPTIMIZED: Yahoo Finance only with rate limiting and caching
 * Fixes the "too many parallel requests" problem
 */

const YahooFinanceClient = require('./yahooFinance');

class DataAggregator {
    constructor(config = {}) {
        this.yahoo = new YahooFinanceClient();
        
        // Simple in-memory cache (TTL: 5 minutes)
        this.cache = new Map();
        this.cacheTTL = 5 * 60 * 1000; // 5 minutes
        
        // Rate limiting
        this.lastRequestTime = 0;
        this.minRequestInterval = 200; // 200ms between requests (5 req/sec max)
        
        // Italian/European stock indicators
        this.europeanSuffixes = ['.MI', '.PA', '.DE', '.AS', '.L', '.MC', '.SW', '.BR', '.LS', '.HE', '.ST', '.CO', '.OL'];
        this.italianStockNames = [
            'ENEL', 'ENI', 'INTESA', 'ISP', 'UNICREDIT', 'UCG', 'GENERALI', 
            'FERRARI', 'RACE', 'STELLANTIS', 'STLA', 'LEONARDO', 'LDO',
            'PRYSMIAN', 'PRY', 'TELECOM', 'TIT', 'TIM', 'MONCLER', 'MONC',
            'CAMPARI', 'CPR', 'PIRELLI', 'PIRC', 'NEXI', 'SNAM', 'SRG',
            'TERNA', 'TRN', 'MEDIOBANCA', 'MB', 'POSTE', 'PST', 'A2A',
            'HERA', 'HER', 'SAIPEM', 'SPM', 'AMPLIFON', 'AMP', 'DIASORIN', 'DIA'
        ];
    }

    /**
     * Rate-limited delay
     */
    async rateLimitDelay() {
        const now = Date.now();
        const timeSinceLastRequest = now - this.lastRequestTime;
        
        if (timeSinceLastRequest < this.minRequestInterval) {
            const waitTime = this.minRequestInterval - timeSinceLastRequest;
            await new Promise(resolve => setTimeout(resolve, waitTime));
        }
        
        this.lastRequestTime = Date.now();
    }

    /**
     * Get from cache if valid
     */
    getFromCache(key) {
        const cached = this.cache.get(key);
        if (cached && (Date.now() - cached.timestamp) < this.cacheTTL) {
            console.log(`[Cache] HIT for ${key}`);
            return cached.data;
        }
        return null;
    }

    /**
     * Set cache
     */
    setCache(key, data) {
        this.cache.set(key, {
            data: data,
            timestamp: Date.now()
        });
    }

    /**
     * Check if query is for European/Italian stock
     */
    isEuropeanQuery(query) {
        const upperQuery = query.toUpperCase().trim();
        
        for (const suffix of this.europeanSuffixes) {
            if (upperQuery.endsWith(suffix)) return true;
        }
        
        if (this.italianStockNames.includes(upperQuery)) return true;
        
        return false;
    }

    /**
     * Retry wrapper with exponential backoff
     */
    async withRetry(fn, maxRetries = 3, baseDelay = 1000) {
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                await this.rateLimitDelay();
                return await fn();
            } catch (error) {
                const isRateLimit = error.message?.includes('401') || 
                                   error.message?.includes('429') ||
                                   error.message?.includes('Too Many');
                
                if (isRateLimit && attempt < maxRetries) {
                    const delay = baseDelay * Math.pow(2, attempt - 1); // 1s, 2s, 4s
                    console.log(`[Retry] Attempt ${attempt} failed, waiting ${delay}ms...`);
                    await new Promise(resolve => setTimeout(resolve, delay));
                } else {
                    throw error;
                }
            }
        }
    }

    /**
     * Search - Yahoo Finance only with smart caching
     */
    async search(query) {
        console.log(`[DataAggregator v2.3] Searching for: "${query}"`);
        const isEuropean = this.isEuropeanQuery(query);
        console.log(`[DataAggregator] European market: ${isEuropean}`);
        
        // Check cache first
        const cacheKey = `search:${query.toLowerCase()}`;
        const cached = this.getFromCache(cacheKey);
        if (cached) {
            return cached;
        }

        try {
            // Use Yahoo with retry
            const result = await this.withRetry(() => this.yahoo.search(query));
            
            if (!result.success || !result.results?.length) {
                return {
                    success: false,
                    results: [],
                    metadata: {
                        source: 'yahoo',
                        isEuropean: isEuropean,
                        error: result.error || 'No results found'
                    }
                };
            }

            // Sort results: prioritize Italian (.MI) for European queries
            let sortedResults = result.results;
            if (isEuropean) {
                sortedResults = result.results.sort((a, b) => {
                    const aIsIT = a.symbol?.endsWith('.MI');
                    const bIsIT = b.symbol?.endsWith('.MI');
                    if (aIsIT && !bIsIT) return -1;
                    if (!aIsIT && bIsIT) return 1;
                    
                    // Then prioritize other European exchanges
                    const aIsEU = this.europeanSuffixes.some(s => a.symbol?.endsWith(s));
                    const bIsEU = this.europeanSuffixes.some(s => b.symbol?.endsWith(s));
                    if (aIsEU && !bIsEU) return -1;
                    if (!aIsEU && bIsEU) return 1;
                    
                    return 0;
                });
            }

            // IMPORTANT: Only get quote for the FIRST result, not all!
            // This prevents rate limiting
            if (sortedResults.length > 0 && !sortedResults[0].price) {
                try {
                    const quote = await this.withRetry(() => 
                        this.yahoo.getQuote(sortedResults[0].symbol)
                    );
                    
                    if (quote.success && quote.data) {
                        sortedResults[0] = {
                            ...sortedResults[0],
                            price: quote.data.price,
                            change: quote.data.change,
                            changePercent: quote.data.changePercent,
                            currency: quote.data.currency || sortedResults[0].currency
                        };
                    }
                } catch (quoteError) {
                    console.warn(`[DataAggregator] Quote failed for first result: ${quoteError.message}`);
                    // Continue without quote - not critical
                }
            }

            const response = {
                success: true,
                results: sortedResults,
                metadata: {
                    totalResults: sortedResults.length,
                    sources: ['yahoo'],
                    isEuropean: isEuropean,
                    timestamp: new Date().toISOString()
                }
            };

            // Cache the result
            this.setCache(cacheKey, response);
            
            return response;

        } catch (error) {
            console.error('[DataAggregator] Search error:', error.message);
            return {
                success: false,
                results: [],
                metadata: {
                    source: 'yahoo',
                    isEuropean: isEuropean,
                    error: error.message
                }
            };
        }
    }

    /**
     * Get quote - Yahoo Finance with caching
     */
    async getQuote(symbol, isEuropean = null) {
        if (isEuropean === null) {
            isEuropean = this.isEuropeanQuery(symbol);
        }
        
        console.log(`[DataAggregator v2.3] Getting quote for: ${symbol}`);

        // Check cache
        const cacheKey = `quote:${symbol.toUpperCase()}`;
        const cached = this.getFromCache(cacheKey);
        if (cached) {
            return cached;
        }

        try {
            const result = await this.withRetry(() => this.yahoo.getQuote(symbol));
            
            if (result.success && result.data) {
                this.setCache(cacheKey, result);
            }
            
            return result;

        } catch (error) {
            console.error(`[DataAggregator] Quote error for ${symbol}:`, error.message);
            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Get historical data - Yahoo Finance with caching
     */
    async getHistoricalData(symbol, period = '1M') {
        console.log(`[DataAggregator v2.3] Getting historical data for: ${symbol}, period: ${period}`);

        // Check cache
        const cacheKey = `historical:${symbol.toUpperCase()}:${period}`;
        const cached = this.getFromCache(cacheKey);
        if (cached) {
            return cached;
        }

        try {
            const result = await this.withRetry(() => 
                this.yahoo.getHistoricalData(symbol, period)
            );
            
            if (result.success && result.data?.length > 0) {
                this.setCache(cacheKey, result);
                console.log(`[yahoo] Historical data found: ${result.data.length} points`);
            }
            
            return result;

        } catch (error) {
            console.error(`[DataAggregator] Historical error for ${symbol}:`, error.message);
            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Search by ISIN
     */
    async searchByISIN(isin) {
        console.log(`[DataAggregator v2.3] Searching by ISIN: ${isin}`);
        return this.search(isin);
    }

    /**
     * Health check
     */
    async healthCheck() {
        try {
            const result = await this.withRetry(() => this.yahoo.search('AAPL'));
            return {
                status: result.success ? 'operational' : 'degraded',
                sources: {
                    yahoo: result.success ? 'OK' : 'FAIL'
                },
                cache: {
                    size: this.cache.size,
                    ttl: this.cacheTTL / 1000 + 's'
                },
                timestamp: new Date().toISOString()
            };
        } catch (error) {
            return {
                status: 'error',
                sources: { yahoo: 'FAIL' },
                error: error.message,
                timestamp: new Date().toISOString()
            };
        }
    }

    /**
     * Get cache stats
     */
    getCacheStats() {
        return {
            size: this.cache.size,
            ttl: this.cacheTTL / 1000 + ' seconds',
            entries: Array.from(this.cache.keys())
        };
    }

    /**
     * Clear cache
     */
    clearCache() {
        this.cache.clear();
        console.log('[DataAggregator] Cache cleared');
    }
}

module.exports = DataAggregator;
