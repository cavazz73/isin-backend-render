/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Multi-Source Data Aggregator v2.1
 * Aggregates financial data from Yahoo Finance, Marketstack, Finnhub, and Alpha Vantage
 * Enhanced support for European/Italian stocks
 */

const YahooFinanceClient = require('./yahooFinance');
const MarketstackClient = require('./marketstack');
const FinnhubClient = require('./finnhub');
const AlphaVantageClient = require('./alphaVantage');

class DataAggregator {
    constructor(config = {}) {
        this.yahoo = new YahooFinanceClient();
        this.marketstack = new MarketstackClient(config.marketstackKey);
        this.finnhub = new FinnhubClient(config.finnhubKey);
        this.alphavantage = new AlphaVantageClient(config.alphavantageKey);
        
        this.sources = ['yahoo', 'marketstack', 'finnhub', 'alphavantage'];
        
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
     * Search across all sources with intelligent routing
     */
    async search(query) {
        console.log(`[DataAggregator] Searching for: "${query}"`);
        const isEuropean = this.isEuropeanQuery(query);
        console.log(`[DataAggregator] European market: ${isEuropean}`);
        
        const searchPromises = [
            this.yahoo.search(query).catch(e => ({ success: false, results: [], error: e.message })),
            this.marketstack.search(query).catch(e => ({ success: false, results: [], error: e.message })),
            this.finnhub.search(query).catch(e => ({ success: false, results: [], error: e.message })),
            this.alphavantage.search(query).catch(e => ({ success: false, results: [], error: e.message }))
        ];

        const results = await Promise.all(searchPromises);
        
        results.forEach((result, index) => {
            const source = this.sources[index];
            if (result.success && result.results?.length > 0) {
                console.log(`[${source}] Found ${result.results.length} results`);
            } else {
                console.log(`[${source}] No results or error: ${result.error || 'N/A'}`);
            }
        });

        const mergedResults = this.mergeSearchResults(results, isEuropean);
        
        if (mergedResults.length === 0) {
            return {
                success: false,
                results: [],
                metadata: {
                    sources: this.sources,
                    isEuropean: isEuropean,
                    errors: results.map((r, i) => ({ source: this.sources[i], error: r.error }))
                }
            };
        }

        const enrichedResults = await this.enrichWithQuotes(mergedResults, isEuropean);

        return {
            success: true,
            results: enrichedResults,
            metadata: {
                totalResults: enrichedResults.length,
                sources: this.sources.filter((_, i) => results[i].success),
                isEuropean: isEuropean,
                timestamp: new Date().toISOString()
            }
        };
    }

    /**
     * Merge search results with priority based on market type
     */
    mergeSearchResults(results, isEuropean = false) {
        const symbolMap = new Map();

        const sourcePriority = isEuropean 
            ? ['yahoo', 'marketstack', 'finnhub', 'alphavantage']
            : ['yahoo', 'finnhub', 'alphavantage', 'marketstack'];

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
                    const existingPriority = sourcePriority.indexOf(existing.sources[0]);
                    const newPriority = sourcePriority.indexOf(source);
                    
                    if (newPriority < existingPriority) {
                        symbolMap.set(symbol, {
                            ...existing,
                            ...item,
                            sources: [source, ...existing.sources]
                        });
                    } else {
                        existing.sources.push(source);
                    }
                }
            });
        });

        let sorted = Array.from(symbolMap.values());
        if (isEuropean) {
            sorted.sort((a, b) => {
                const aIsEU = this.europeanSuffixes.some(s => a.symbol?.endsWith(s));
                const bIsEU = this.europeanSuffixes.some(s => b.symbol?.endsWith(s));
                if (aIsEU && !bIsEU) return -1;
                if (!aIsEU && bIsEU) return 1;
                return 0;
            });
        }

        return sorted;
    }

    /**
     * Enrich results with real-time quotes
     */
    async enrichWithQuotes(results, isEuropean = false) {
        const enrichPromises = results.map(async (item) => {
            if (item.price != null && typeof item.price === 'number' && item.price > 0) {
                return item;
            }

            const quote = await this.getQuote(item.symbol, isEuropean);
            
            if (quote.success && quote.data) {
                return {
                    ...item,
                    price: quote.data.price,
                    change: quote.data.change,
                    changePercent: quote.data.changePercent,
                    currency: quote.data.currency || item.currency,
                    quoteSource: quote.source
                };
            }

            return item;
        });

        return await Promise.all(enrichPromises);
    }

    /**
     * Get quote with intelligent fallback based on market
     */
    async getQuote(symbol, isEuropean = null) {
        if (isEuropean === null) {
            isEuropean = this.isEuropeanQuery(symbol);
        }
        
        console.log(`[DataAggregator] Getting quote for: ${symbol} (European: ${isEuropean})`);

        const sources = isEuropean
            ? [
                { name: 'yahoo', client: this.yahoo },
                { name: 'marketstack', client: this.marketstack },
                { name: 'finnhub', client: this.finnhub },
                { name: 'alphavantage', client: this.alphavantage }
            ]
            : [
                { name: 'yahoo', client: this.yahoo },
                { name: 'finnhub', client: this.finnhub },
                { name: 'alphavantage', client: this.alphavantage },
                { name: 'marketstack', client: this.marketstack }
            ];

        for (const source of sources) {
            try {
                const result = await source.client.getQuote(symbol);
                if (result.success && result.data && result.data.price != null) {
                    console.log(`[${source.name}] Quote found for ${symbol}: ${result.data.price} ${result.data.currency}`);
                    return result;
                }
            } catch (error) {
                console.error(`[${source.name}] Quote error for ${symbol}: ${error.message}`);
            }
        }

        return {
            success: false,
            error: 'No quote data available from any source'
        };
    }

    /**
     * Get historical data with fallback
     */
    async getHistoricalData(symbol, period = '1M') {
        const isEuropean = this.isEuropeanQuery(symbol);
        console.log(`[DataAggregator] Getting historical data for: ${symbol}, period: ${period}, European: ${isEuropean}`);

        const sources = isEuropean
            ? [
                { name: 'yahoo', client: this.yahoo },
                { name: 'marketstack', client: this.marketstack },
                { name: 'alphavantage', client: this.alphavantage }
            ]
            : [
                { name: 'yahoo', client: this.yahoo },
                { name: 'alphavantage', client: this.alphavantage },
                { name: 'marketstack', client: this.marketstack }
            ];

        for (const source of sources) {
            try {
                const result = await source.client.getHistoricalData(symbol, period);
                if (result.success && result.data && result.data.length > 0) {
                    console.log(`[${source.name}] Historical data found: ${result.data.length} points`);
                    return result;
                }
            } catch (error) {
                console.error(`[${source.name}] Historical data error: ${error.message}`);
            }
        }

        return {
            success: false,
            error: 'No historical data available from any source'
        };
    }

    /**
     * Search by ISIN
     */
    async searchByISIN(isin) {
        console.log(`[DataAggregator] Searching by ISIN: ${isin}`);

        try {
            const yahooResult = await this.yahoo.searchByISIN(isin);
            if (yahooResult.success && yahooResult.results.length > 0) {
                return yahooResult;
            }
        } catch (error) {
            console.error(`[yahoo] ISIN search error: ${error.message}`);
        }

        try {
            const msResult = await this.marketstack.searchByISIN(isin);
            if (msResult.success && msResult.results.length > 0) {
                return msResult;
            }
        } catch (error) {
            console.error(`[marketstack] ISIN search error: ${error.message}`);
        }

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
            this.marketstack.search('AAPL').then(() => ({ marketstack: 'OK' })).catch(() => ({ marketstack: 'FAIL' })),
            this.finnhub.search('AAPL').then(() => ({ finnhub: 'OK' })).catch(() => ({ finnhub: 'FAIL' })),
            this.alphavantage.search('IBM').then(() => ({ alphavantage: 'OK' })).catch(() => ({ alphavantage: 'FAIL' }))
        ]);

        return {
            status: 'operational',
            sources: Object.assign({}, ...checks),
            timestamp: new Date().toISOString()
        };
    }

    /**
     * Get API usage stats
     */
    getUsageStats() {
        return {
            marketstack: {
                remaining: this.marketstack.getRemainingCalls ? this.marketstack.getRemainingCalls() : 'N/A',
                limit: 100,
                period: 'monthly'
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
                limit: 'unlimited',
                period: 'N/A'
            }
        };
    }
}

module.exports = DataAggregator;
