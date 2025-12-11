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
const axios = require('axios');

class DataAggregator {
    constructor(config = {}) {
        this.yahoo = new YahooFinanceClient();
        this.finnhub = new FinnhubClient(config.finnhubKey);
        this.alphavantage = new AlphaVantageClient(config.alphavantageKey);
        
        // FIXED: Finnhub PRIMARY (60 req/min, funzionante)
        this.sources = ['finnhub', 'yahoo', 'alphavantage'];
    }

    /**
     * Search across all sources and merge results
     */
    async search(query) {
        console.log(`[DataAggregator] Searching for: "${query}"`);
        
        // Try all sources in parallel for maximum speed
        const searchPromises = [
            this.finnhub.search(query).catch(e => ({ success: false, results: [], error: e.message })),
            this.yahoo.search(query).catch(e => ({ success: false, results: [], error: e.message })),
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
     * FIXED: Limit to first 3 results + sequential to avoid rate limiting
     */
    async enrichWithQuotes(results) {
        // FIXED: Limita a primi 3 risultati per evitare rate limiting
        const limitedResults = results.slice(0, 3);
        const enrichedResults = [];

        // FIXED: Sequenziale invece di parallelo per evitare 403
        for (const item of limitedResults) {
            // If we already have price data, skip
            if (item.price != null && typeof item.price === 'number') {
                enrichedResults.push(item);
                continue;
            }

            // Try to get quote from best source
            const quote = await this.getQuote(item.symbol);
            
            if (quote.success && quote.data) {
                enrichedResults.push({
                    ...item,
                    price: quote.data.price,
                    change: quote.data.change,
                    changePercent: quote.data.changePercent,
                    currency: quote.data.currency || item.currency,
                    quoteSources: [quote.source]
                });
            } else {
                enrichedResults.push(item);
            }

            // Small delay to avoid rate limiting
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        return enrichedResults;
    }

    /**
     * Get quote with automatic fallback
     */
    async getQuote(symbol) {
        console.log(`[DataAggregator] Getting quote for: ${symbol}`);

        // Try Finnhub first (funzionante con headers)
        try {
            const finnhubQuote = await this.finnhub.getQuote(symbol);
            if (finnhubQuote.success && finnhubQuote.data) {
                console.log(`[finnhub] Quote found for ${symbol}`);
                return finnhubQuote;
            }
        } catch (error) {
            console.error(`[finnhub] Error getting quote: ${error.message}`);
        }

        // Fallback to Yahoo
        try {
            const yahooQuote = await this.yahoo.getQuote(symbol);
            if (yahooQuote.success && yahooQuote.data) {
                console.log(`[yahoo] Quote found for ${symbol}`);
                return yahooQuote;
            }
        } catch (error) {
            console.error(`[yahoo] Error getting quote: ${error.message}`);
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
     * Get full instrument details with rich metrics
     * NEW: Includes Market Cap, P/E, Dividend, Description, Logo
     */
    async getInstrumentDetails(symbol) {
        console.log(`[DataAggregator] Getting full details for: ${symbol}`);

        // Get quote data
        const quote = await this.getQuote(symbol);
        
        // Get company overview (metrics)
        const overview = await this.getCompanyOverview(symbol);
        
        // Get logo
        const logo = await this.getCompanyLogo(symbol, overview.data?.Name);

        return {
            success: true,
            data: {
                // Basic info
                symbol: symbol,
                name: overview.data?.Name || quote.data?.name || symbol,
                exchange: overview.data?.Exchange || quote.data?.exchange || 'UNKNOWN',
                currency: overview.data?.Currency || quote.data?.currency || 'USD',
                
                // Price data
                price: quote.data?.price || null,
                change: quote.data?.change || null,
                changePercent: quote.data?.changePercent || null,
                
                // Key Metrics
                marketCap: overview.data?.MarketCapitalization || null,
                peRatio: overview.data?.PERatio || null,
                pegRatio: overview.data?.PEGRatio || null,
                dividendYield: overview.data?.DividendYield || null,
                eps: overview.data?.EPS || null,
                beta: overview.data?.Beta || null,
                
                // 52-Week Range
                week52High: overview.data?.['52WeekHigh'] || null,
                week52Low: overview.data?.['52WeekLow'] || null,
                
                // Volume
                volume: quote.data?.volume || null,
                
                // Company Info
                sector: overview.data?.Sector || null,
                industry: overview.data?.Industry || null,
                description: overview.data?.Description || null,
                website: overview.data?.['Website'] || null,
                
                // Additional Metrics
                bookValue: overview.data?.BookValue || null,
                profitMargin: overview.data?.ProfitMargin || null,
                
                // Visual
                logo: logo,
                
                // Metadata
                lastUpdated: new Date().toISOString(),
                sources: {
                    quote: quote.source,
                    overview: overview.source,
                    logo: logo ? 'clearbit' : null
                }
            }
        };
    }

    /**
     * Get company overview with metrics from Alpha Vantage
     */
    async getCompanyOverview(symbol) {
        try {
            const avOverview = await this.alphavantage.getCompanyOverview(symbol);
            if (avOverview.success && avOverview.data) {
                console.log(`[alphavantage] Overview found for ${symbol}`);
                return avOverview;
            }
        } catch (error) {
            console.error(`[alphavantage] Overview error: ${error.message}`);
        }

        return { success: false, data: null, source: null };
    }

    /**
     * Get company logo from Clearbit (free)
     */
    async getCompanyLogo(symbol, companyName) {
        try {
            const domain = this.guessDomain(symbol, companyName);
            if (!domain) return null;
            
            const logoUrl = `https://logo.clearbit.com/${domain}`;
            const response = await axios.head(logoUrl, { timeout: 3000 });
            
            return response.status === 200 ? logoUrl : null;
        } catch (error) {
            return null;
        }
    }

    /**
     * Guess company domain from symbol/name for logo fetching
     */
    guessDomain(symbol, companyName) {
        // Known domains mapping
        const knownDomains = {
            'AAPL': 'apple.com',
            'MSFT': 'microsoft.com',
            'GOOGL': 'google.com',
            'GOOG': 'google.com',
            'AMZN': 'amazon.com',
            'META': 'meta.com',
            'TSLA': 'tesla.com',
            'NVDA': 'nvidia.com',
            'JPM': 'jpmorganchase.com',
            'V': 'visa.com',
            'WMT': 'walmart.com',
            'DIS': 'disney.com',
            'NFLX': 'netflix.com',
            'INTC': 'intel.com',
            'AMD': 'amd.com',
            'ORCL': 'oracle.com',
            'IBM': 'ibm.com',
            'ENEL.MI': 'enel.com',
            'ENI.MI': 'eni.com',
            'ISP.MI': 'intesasanpaolo.com',
            'UCG.MI': 'unicreditgroup.eu',
            'STLA.MI': 'stellantis.com'
        };

        if (knownDomains[symbol]) {
            return knownDomains[symbol];
        }

        // Try to extract from company name
        if (companyName) {
            const name = companyName.toLowerCase()
                .replace(/\s+(inc|corp|corporation|ltd|limited|plc|spa|nv|ag|gmbh|sa)\b.*$/i, '')
                .replace(/[^a-z0-9]/g, '');
            
            if (name) {
                return `${name}.com`;
            }
        }

        return null;
    }

    /**
     * Format large numbers for display
     */
    static formatNumber(num) {
        if (!num) return 'N/A';
        const n = parseFloat(num);
        if (isNaN(n)) return 'N/A';
        
        if (n >= 1e12) return `$${(n / 1e12).toFixed(2)}T`;
        if (n >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
        if (n >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
        return `$${n.toFixed(2)}`;
    }

    /**
     * Format percentage
     */
    static formatPercent(num) {
        if (!num) return 'N/A';
        const n = parseFloat(num);
        if (isNaN(n)) return 'N/A';
        return `${(n * 100).toFixed(2)}%`;
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
