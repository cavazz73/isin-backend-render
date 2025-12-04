/**
 * ============================================================================
 * ISIN Research & Compare - Enhanced Data Aggregator
 * Copyright (c) 2025 Mutna S.R.L.S. - All Rights Reserved
 * ============================================================================
 * 
 * Enhanced version with rich metrics (Market Cap, P/E, Dividend, etc)
 * Company: Mutna S.R.L.S. | P.IVA: 04219740364 | https://mutna.it
 * ============================================================================
 */

const YahooFinanceClient = require('./yahooFinance');
const FinnhubClient = require('./finnhub');
const AlphaVantageClient = require('./alphaVantage');
const axios = require('axios');

class EnhancedDataAggregator {
    constructor(config = {}) {
        this.yahoo = new YahooFinanceClient();
        this.finnhub = new FinnhubClient(config.finnhubKey);
        this.alphavantage = new AlphaVantageClient(config.alphavantageKey);
        this.sources = ['yahoo', 'finnhub', 'alphavantage'];
    }

    async search(query) {
        console.log(`[EnhancedAggregator] Searching: "${query}"`);
        
        const searchPromises = [
            this.yahoo.search(query).catch(e => ({ success: false, results: [], error: e.message })),
            this.finnhub.search(query).catch(e => ({ success: false, results: [], error: e.message })),
            this.alphavantage.search(query).catch(e => ({ success: false, results: [], error: e.message }))
        ];

        const results = await Promise.all(searchPromises);
        const mergedResults = this.mergeSearchResults(results);
        
        if (mergedResults.length === 0) {
            return { success: false, results: [], metadata: { sources: this.sources } };
        }

        const enrichedResults = await this.enrichWithFullData(mergedResults);

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

    async getInstrumentDetails(symbol) {
        console.log(`[EnhancedAggregator] Getting details: ${symbol}`);

        const quote = await this.getQuote(symbol);
        const overview = await this.getCompanyOverview(symbol);
        const logo = await this.getCompanyLogo(symbol, overview.data?.Name);

        return {
            success: true,
            data: {
                symbol,
                name: overview.data?.Name || quote.data?.name || symbol,
                exchange: overview.data?.Exchange || quote.data?.exchange || 'UNKNOWN',
                currency: overview.data?.Currency || quote.data?.currency || 'USD',
                
                price: quote.data?.price || null,
                change: quote.data?.change || null,
                changePercent: quote.data?.changePercent || null,
                
                marketCap: overview.data?.MarketCapitalization || null,
                peRatio: overview.data?.PERatio || null,
                dividendYield: overview.data?.DividendYield || null,
                eps: overview.data?.EPS || null,
                beta: overview.data?.Beta || null,
                week52High: overview.data?.['52WeekHigh'] || null,
                week52Low: overview.data?.['52WeekLow'] || null,
                
                sector: overview.data?.Sector || null,
                industry: overview.data?.Industry || null,
                description: overview.data?.Description || null,
                website: overview.data?.Website || null,
                
                logo,
                lastUpdated: new Date().toISOString(),
                sources: {
                    quote: quote.source,
                    overview: overview.source,
                    logo: logo ? 'clearbit' : null
                }
            }
        };
    }

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

    guessDomain(symbol, companyName) {
        const knownDomains = {
            'AAPL': 'apple.com', 'MSFT': 'microsoft.com', 'GOOGL': 'google.com',
            'AMZN': 'amazon.com', 'META': 'meta.com', 'TSLA': 'tesla.com',
            'NVDA': 'nvidia.com', 'JPM': 'jpmorganchase.com', 'V': 'visa.com',
            'ENEL.MI': 'enel.com', 'ENI.MI': 'eni.com', 'ISP.MI': 'intesasanpaolo.com'
        };

        if (knownDomains[symbol]) return knownDomains[symbol];

        if (companyName) {
            const name = companyName.toLowerCase()
                .replace(/\s+(inc|corp|ltd|plc|spa)\b.*$/i, '')
                .replace(/[^a-z0-9]/g, '');
            if (name) return `${name}.com`;
        }

        return null;
    }

    async getCompanyOverview(symbol) {
        try {
            const avOverview = await this.alphavantage.getCompanyOverview(symbol);
            if (avOverview.success && avOverview.data) {
                console.log(`[alphavantage] Overview found: ${symbol}`);
                return avOverview;
            }
        } catch (error) {
            console.error(`[alphavantage] Overview error: ${error.message}`);
        }

        return { success: false, data: null, source: null };
    }

    async enrichWithFullData(results) {
        const enrichPromises = results.slice(0, 5).map(async (item) => {
            try {
                const details = await this.getInstrumentDetails(item.symbol);
                if (details.success) {
                    return { ...item, ...details.data, enriched: true };
                }
            } catch (error) {
                console.error(`[Enrich] Error ${item.symbol}: ${error.message}`);
            }
            return item;
        });

        const enriched = await Promise.all(enrichPromises);
        if (results.length > 5) enriched.push(...results.slice(5));

        return enriched;
    }

    mergeSearchResults(results) {
        const symbolMap = new Map();

        results.forEach((result, sourceIndex) => {
            if (!result.success || !result.results) return;

            const source = this.sources[sourceIndex];
            
            result.results.forEach(item => {
                const symbol = item.symbol;
                
                if (!symbolMap.has(symbol)) {
                    symbolMap.set(symbol, { ...item, sources: [source] });
                } else {
                    const existing = symbolMap.get(symbol);
                    if (source === 'yahoo' || (source === 'finnhub' && existing.sources[0] === 'alphavantage')) {
                        symbolMap.set(symbol, { ...existing, ...item, sources: [...existing.sources, source] });
                    } else {
                        existing.sources.push(source);
                    }
                }
            });
        });

        return Array.from(symbolMap.values());
    }

    async getQuote(symbol) {
        const sources = [
            { name: 'yahoo', client: this.yahoo },
            { name: 'finnhub', client: this.finnhub },
            { name: 'alphavantage', client: this.alphavantage }
        ];

        for (const source of sources) {
            try {
                const quote = await source.client.getQuote(symbol);
                if (quote.success && quote.data) {
                    console.log(`[${source.name}] Quote found: ${symbol}`);
                    return quote;
                }
            } catch (error) {
                console.error(`[${source.name}] Quote error: ${error.message}`);
            }
        }

        return { success: false, error: 'No quote data available' };
    }

    async getHistoricalData(symbol, period = '1M') {
        try {
            const yahooHistorical = await this.yahoo.getHistoricalData(symbol, period);
            if (yahooHistorical.success && yahooHistorical.data?.length > 0) {
                return yahooHistorical;
            }
        } catch (error) {
            console.error(`[yahoo] Historical error: ${error.message}`);
        }

        try {
            const avHistorical = await this.alphavantage.getHistoricalData(symbol, period);
            if (avHistorical.success && avHistorical.data?.length > 0) {
                return avHistorical;
            }
        } catch (error) {
            console.error(`[alphavantage] Historical error: ${error.message}`);
        }
        
        return { success: false, error: 'No historical data available' };
    }

    static formatNumber(num) {
        if (!num) return 'N/A';
        const n = parseFloat(num);
        if (n >= 1e12) return `$${(n / 1e12).toFixed(2)}T`;
        if (n >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
        if (n >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
        return `$${n.toFixed(2)}`;
    }

    static formatPercent(num) {
        if (!num) return 'N/A';
        return `${(parseFloat(num) * 100).toFixed(2)}%`;
    }

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

module.exports = EnhancedDataAggregator;
