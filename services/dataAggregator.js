/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Multi-Source Data Aggregator v2.1
 * 
 * Data source priority:
 * - European stocks (IT, FR, DE, etc.): Twelve Data -> Yahoo Finance
 * - US stocks: Yahoo Finance -> Finnhub -> Alpha Vantage
 * - Historical data: Yahoo Finance -> Twelve Data -> Alpha Vantage
 */

const YahooFinanceClient = require('./yahooFinance');
const FinnhubClient = require('./finnhub');
const AlphaVantageClient = require('./alphaVantage');
const TwelveDataClient = require('./twelveData');

class DataAggregator {
    constructor(config = {}) {
        this.yahoo = new YahooFinanceClient();
        this.finnhub = new FinnhubClient(config.finnhubKey);
        this.alphavantage = new AlphaVantageClient(config.alphavantageKey);
        this.twelvedata = new TwelveDataClient(config.twelveDataKey);
        
        // All available sources
        this.sources = ['twelvedata', 'yahoo', 'finnhub', 'alphavantage'];
        
        // European exchange suffixes
        this.europeanExchanges = ['.MI', '.PA', '.AS', '.BR', '.L', '.F', '.DE', '.MC', '.SW', '.VI', 
                                   ':MIL', ':EPA', ':AMS', ':EBR', ':LSE', ':FRA', ':XETRA', ':BME', ':SIX'];
        
        // Known Italian symbols (without exchange suffix)
        this.italianStocks = ['ENEL', 'ENI', 'INTESA', 'ISP', 'UNICREDIT', 'UCG', 'GENERALI', 
                              'FERRARI', 'RACE', 'STELLANTIS', 'STLA', 'LEONARDO', 'LDO',
                              'PRYSMIAN', 'PRY', 'TELECOM', 'TIT', 'A2A', 'HERA', 'SNAM',
                              'TERNA', 'AMPLIFON', 'RECORDATI', 'MONCLER', 'DIASORIN', 'NEXI',
                              'CAMPARI', 'PIRELLI', 'BUZZI', 'IVECO', 'POSTE', 'FINECO', 'FBK',
                              'MEDIOBANCA', 'SAIPEM', 'TENARIS', 'INTERPUMP', 'STM', 'BPER', 'BAMI'];
    }

    /**
     * Detect if symbol is European
     */
    isEuropeanStock(query) {
        const upperQuery = query.toUpperCase();
        
        // Check if has European exchange suffix
        for (const suffix of this.europeanExchanges) {
            if (upperQuery.includes(suffix.toUpperCase())) {
                return true;
            }
        }
        
        // Check if it's a known Italian stock
        if (this.italianStocks.includes(upperQuery)) {
            return true;
        }
        
        return false;
    }

    /**
     * Search across all sources with intelligent routing
     */
    async search(query) {
        console.log(`[DataAggregator] Searching for: "${query}"`);
        
        const isEuropean = this.isEuropeanStock(query);
        console.log(`[DataAggregator] Is European stock: ${isEuropean}`);
        
        let searchPromises;
        
        if (isEuropean) {
            // For European stocks: prioritize Twelve Data, then Yahoo
            searchPromises = [
                this.twelvedata.search(query).catch(e => ({ success: false, results: [], error: e.message })),
                this.yahoo.search(query).catch(e => ({ success: false, results: [], error: e.message }))
            ];
        } else {
            // For US/other stocks: all sources
            searchPromises = [
                this.yahoo.search(query).catch(e => ({ success: false, results: [], error: e.message })),
                this.twelvedata.search(query).catch(e => ({ success: false, results: [], error: e.message })),
                this.finnhub.search(query).catch(e => ({ success: false, results: [], error: e.message })),
                this.alphavantage.search(query).catch(e => ({ success: false, results: [], error: e.message }))
            ];
        }

        const results = await Promise.all(searchPromises);
        
        // Log results from each source
        const sourceOrder = isEuropean ? ['twelvedata', 'yahoo'] : ['yahoo', 'twelvedata', 'finnhub', 'alphavantage'];
        results.forEach((result, index) => {
            const source = sourceOrder[index];
            if (result.success && result.results.length > 0) {
                console.log(`[${source}] Found ${result.results.length} results`);
            } else {
                console.log(`[${source}] No results or error: ${result.error || 'N/A'}`);
            }
        });

        // Merge and deduplicate results
        const mergedResults = this.mergeSearchResults(results, sourceOrder);
        
        if (mergedResults.length === 0) {
            return {
                success: false,
                results: [],
                metadata: {
                    sources: sourceOrder,
                    errors: results.map((r, i) => ({ source: sourceOrder[i], error: r.error }))
                }
            };
        }

        // Enrich with quotes (prioritize based on market)
        const enrichedResults = await this.enrichWithQuotes(mergedResults, isEuropean);

        return {
            success: true,
            results: enrichedResults,
            metadata: {
                totalResults: enrichedResults.length,
                sources: sourceOrder.filter((_, i) => results[i].success),
                isEuropean: isEuropean,
                timestamp: new Date().toISOString()
            }
        };
    }

    /**
     * Merge search results from multiple sources
     */
    mergeSearchResults(results, sourceOrder) {
        const symbolMap = new Map();

        results.forEach((result, sourceIndex) => {
            if (!result.success || !result.results) return;

            const source = sourceOrder[sourceIndex];
            
            result.results.forEach(item => {
                // Normalize symbol for deduplication
                const normalizedSymbol = item.symbol.toUpperCase().replace(':MIL', '.MI');
                
                if (!symbolMap.has(normalizedSymbol)) {
                    symbolMap.set(normalizedSymbol, {
                        ...item,
                        sources: [source]
                    });
                } else {
                    const existing = symbolMap.get(normalizedSymbol);
                    
                    // Prefer Twelve Data for European, Yahoo for US
                    if (source === 'twelvedata' && existing.sources[0] !== 'twelvedata') {
                        symbolMap.set(normalizedSymbol, {
                            ...item,
                            sources: [source, ...existing.sources]
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
    async enrichWithQuotes(results, isEuropean) {
        const enrichPromises = results.map(async (item) => {
            // If we already have price data, skip
            if (item.price != null && typeof item.price === 'number' && item.price > 0) {
                return item;
            }

            // Get quote from best source based on market
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
     * Get quote with intelligent routing based on market
     */
    async getQuote(symbol, isEuropean = null) {
        // Auto-detect if not specified
        if (isEuropean === null) {
            isEuropean = this.isEuropeanStock(symbol);
        }
        
        console.log(`[DataAggregator] Getting quote for: ${symbol} (European: ${isEuropean})`);

        if (isEuropean) {
            // For European stocks: Twelve Data first, then Yahoo
            
            // Try Twelve Data first (best for European markets)
            try {
                const twelveResult = await this.twelvedata.getQuote(symbol);
                if (twelveResult.success && twelveResult.data && twelveResult.data.price > 0) {
                    console.log(`[twelvedata] Quote found for ${symbol}: ${twelveResult.data.price} ${twelveResult.data.currency}`);
                    return twelveResult;
                }
            } catch (error) {
                console.error(`[twelvedata] Error getting quote: ${error.message}`);
            }

            // Fallback to Yahoo
            try {
                const yahooQuote = await this.yahoo.getQuote(symbol);
                if (yahooQuote.success && yahooQuote.data && yahooQuote.data.price > 0) {
                    console.log(`[yahoo] Quote found for ${symbol}`);
                    return yahooQuote;
                }
            } catch (error) {
                console.error(`[yahoo] Error getting quote: ${error.message}`);
            }

        } else {
            // For US/other stocks: Yahoo first, then others
            
            // Try Yahoo first
            try {
                const yahooQuote = await this.yahoo.getQuote(symbol);
                if (yahooQuote.success && yahooQuote.data && yahooQuote.data.price > 0) {
                    console.log(`[yahoo] Quote found for ${symbol}`);
                    return yahooQuote;
                }
            } catch (error) {
                console.error(`[yahoo] Error getting quote: ${error.message}`);
            }

            // Try Twelve Data
            try {
                const twelveResult = await this.twelvedata.getQuote(symbol);
                if (twelveResult.success && twelveResult.data && twelveResult.data.price > 0) {
                    console.log(`[twelvedata] Quote found for ${symbol}`);
                    return twelveResult;
                }
            } catch (error) {
                console.error(`[twelvedata] Error getting quote: ${error.message}`);
            }

            // Fallback to Finnhub
            try {
                const finnhubQuote = await this.finnhub.getQuote(symbol);
                if (finnhubQuote.success && finnhubQuote.data && finnhubQuote.data.price > 0) {
                    console.log(`[finnhub] Quote found for ${symbol}`);
                    return finnhubQuote;
                }
            } catch (error) {
                console.error(`[finnhub] Error getting quote: ${error.message}`);
            }

            // Last resort: Alpha Vantage
            try {
                const avQuote = await this.alphavantage.getQuote(symbol);
                if (avQuote.success && avQuote.data && avQuote.data.price > 0) {
                    console.log(`[alphavantage] Quote found for ${symbol}`);
                    return avQuote;
                }
            } catch (error) {
                console.error(`[alphavantage] Error getting quote: ${error.message}`);
            }
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
        
        const isEuropean = this.isEuropeanStock(symbol);

        // Try Yahoo first (best historical data coverage)
        try {
            const yahooHistorical = await this.yahoo.getHistoricalData(symbol, period);
            if (yahooHistorical.success && yahooHistorical.data && yahooHistorical.data.length > 0) {
                console.log(`[yahoo] Historical data found: ${yahooHistorical.data.length} points`);
                return yahooHistorical;
            }
        } catch (error) {
            console.error(`[yahoo] Error getting historical data: ${error.message}`);
        }

        // For European stocks or if Yahoo failed, try Twelve Data
        if (isEuropean || true) {
            try {
                const twelveHistorical = await this.twelvedata.getHistoricalData(symbol, period);
                if (twelveHistorical.success && twelveHistorical.data && twelveHistorical.data.length > 0) {
                    console.log(`[twelvedata] Historical data found: ${twelveHistorical.data.length} points`);
                    return twelveHistorical;
                }
            } catch (error) {
                console.error(`[twelvedata] Error getting historical data: ${error.message}`);
            }
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

        // Determine if ISIN is European (IT, FR, DE, GB, etc.)
        const countryCode = isin.substring(0, 2).toUpperCase();
        const europeanCodes = ['IT', 'FR', 'DE', 'GB', 'NL', 'ES', 'BE', 'CH', 'AT', 'PT', 'IE'];
        const isEuropean = europeanCodes.includes(countryCode);

        if (isEuropean) {
            // Try Twelve Data first for European ISINs
            try {
                const twelveResult = await this.twelvedata.searchByISIN(isin);
                if (twelveResult.success && twelveResult.results.length > 0) {
                    return twelveResult;
                }
            } catch (error) {
                console.error(`[twelvedata] ISIN search error: ${error.message}`);
            }
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
            this.twelvedata.healthCheck().then(r => ({ twelvedata: r })).catch(() => ({ twelvedata: { status: 'FAIL' } })),
            this.yahoo.search('AAPL').then(() => ({ yahoo: 'OK' })).catch(() => ({ yahoo: 'FAIL' })),
            this.finnhub.search('AAPL').then(() => ({ finnhub: 'OK' })).catch(() => ({ finnhub: 'FAIL' })),
            this.alphavantage.search('IBM').then(() => ({ alphavantage: 'OK' })).catch(() => ({ alphavantage: 'FAIL' }))
        ]);

        return {
            status: 'operational',
            sources: Object.assign({}, ...checks),
            timestamp: new Date().toISOString(),
            twelveDataUsage: this.twelvedata.getUsageStats()
        };
    }

    /**
     * Get API usage statistics
     */
    getUsageStats() {
        return {
            twelvedata: this.twelvedata.getUsageStats()
        };
    }
}

module.exports = DataAggregator;
