/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * Yahoo Finance API Client (Primary Source - Unlimited, Free)
 * V4.1 - WITH FUNDAMENTAL DATA
 */

const axios = require('axios');

class YahooFinanceClient {
    constructor() {
        this.baseUrl = 'https://query1.finance.yahoo.com';
        this.baseUrlV7 = 'https://query1.finance.yahoo.com/v7';
        this.baseUrlV8 = 'https://query2.finance.yahoo.com/v8';
        
        // Cookie/crumb authentication for v7/v10 endpoints
        this._crumb = null;
        this._cookie = null;
        this._crumbExpiry = 0;
    }

    /**
     * Get Yahoo authentication cookie + crumb (required for v7/v10 endpoints)
     * Crumb is cached for 30 minutes
     */
    async _ensureAuth() {
        if (this._crumb && this._cookie && Date.now() < this._crumbExpiry) {
            return true;
        }

        try {
            // Step 1: Get consent cookie by visiting Yahoo Finance
            const initResp = await axios.get('https://fc.yahoo.com', {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                timeout: 10000,
                maxRedirects: 5,
                validateStatus: () => true  // Accept any status
            });

            // Extract cookies from response
            const setCookies = initResp.headers['set-cookie'];
            if (setCookies) {
                this._cookie = setCookies.map(c => c.split(';')[0]).join('; ');
            }

            // Step 2: Get crumb using the cookie
            const crumbResp = await axios.get('https://query2.finance.yahoo.com/v1/test/getcrumb', {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Cookie': this._cookie || ''
                },
                timeout: 10000
            });

            if (crumbResp.data && typeof crumbResp.data === 'string') {
                this._crumb = crumbResp.data;
                this._crumbExpiry = Date.now() + 30 * 60 * 1000; // 30 min
                console.log('[Yahoo] Auth: crumb obtained successfully');
                return true;
            }
        } catch (error) {
            console.error('[Yahoo] Auth error:', error.message);
        }

        this._crumb = null;
        this._cookie = null;
        return false;
    }

    /**
     * Make authenticated request to Yahoo API
     */
    async _authGet(url, params = {}) {
        await this._ensureAuth();
        
        if (this._crumb) {
            params.crumb = this._crumb;
        }

        return axios.get(url, {
            params,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json',
                'Referer': 'https://finance.yahoo.com',
                'Origin': 'https://finance.yahoo.com',
                'Cookie': this._cookie || ''
            },
            timeout: 15000
        });
    }

    async search(query) {
        try {
            const searchQuery = this.normalizeItalianSymbol(query);
            
            // Use v1 search endpoint with auth
            const url = `https://query2.finance.yahoo.com/v1/finance/search`;
            const response = await this._authGet(url, {
                q: searchQuery,
                lang: 'en-US',
                region: 'US',
                quotesCount: 10,
                newsCount: 0,
                listsCount: 0,
                enableFuzzyQuery: false
            });

            if (!response.data?.quotes) {
                return { success: false, results: [] };
            }

            const results = response.data.quotes
                .filter(q => q.symbol)
                .map(quote => ({
                    symbol: quote.symbol,
                    name: quote.shortname || quote.longname || quote.symbol,
                    description: quote.longname || quote.shortname || '',
                    type: this._mapQuoteType(quote.quoteType),
                    exchange: quote.exchange,
                    currency: quote.currency || 'USD',
                    isin: quote.isin || null,
                    country: this.getCountryFromExchange(quote.exchange),
                    price: null,
                    change: null,
                    changePercent: null
                }));

            return { success: true, results, source: 'yahoo' };

        } catch (error) {
            console.error('[Yahoo] Search error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    /**
     * Map Yahoo quoteType to standard types
     */
    _mapQuoteType(quoteType) {
        const map = {
            'EQUITY': 'Stock',
            'ETF': 'ETF',
            'MUTUALFUND': 'Fund',
            'INDEX': 'Index',
            'CURRENCY': 'Currency',
            'CRYPTOCURRENCY': 'Crypto',
            'FUTURE': 'Future',
            'OPTION': 'Option',
        };
        return map[quoteType] || quoteType || 'Unknown';
    }

    async getQuote(symbol) {
        // PRIMARY: Use v8 chart endpoint (free, unlimited, global)
        try {
            const url = `${this.baseUrlV8}/finance/chart/${symbol}`;
            const response = await axios.get(url, {
                params: { range: '1d', interval: '1m' },
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json',
                    'Referer': 'https://finance.yahoo.com',
                    'Origin': 'https://finance.yahoo.com'
                },
                timeout: 15000
            });

            const result = response.data?.chart?.result?.[0];
            if (!result) return { success: false };

            const meta = result.meta;
            const price = meta.regularMarketPrice;
            const prevClose = meta.chartPreviousClose || meta.previousClose;

            if (!price) return { success: false };

            const change = prevClose ? +(price - prevClose).toFixed(4) : null;
            const changePercent = prevClose ? +((change / prevClose) * 100).toFixed(2) : null;

            return {
                success: true,
                data: {
                    symbol: meta.symbol || symbol,
                    name: meta.shortName || meta.longName || symbol,
                    description: meta.longName || meta.shortName || symbol,
                    price: price,
                    change: change,
                    changePercent: changePercent,
                    currency: meta.currency || 'USD',
                    exchange: meta.exchangeName || meta.exchange || '',
                    marketCap: null,
                    previousClose: prevClose,
                    dayHigh: meta.regularMarketDayHigh || null,
                    dayLow: meta.regularMarketDayLow || null,
                    volume: meta.regularMarketVolume || null,
                    timestamp: new Date().toISOString()
                },
                source: 'yahoo-chart'
            };

        } catch (error) {
            console.error('[Yahoo] Chart-quote error:', error.message);
        }

        // FALLBACK: Try v7 quote with auth
        try {
            const url = `${this.baseUrlV7}/finance/quote`;
            const response = await this._authGet(url, {
                symbols: symbol,
                fields: 'symbol,regularMarketPrice,regularMarketChange,regularMarketChangePercent,currency,shortName,longName,exchange,marketCap'
            });

            const quote = response.data?.quoteResponse?.result?.[0];
            if (!quote) return { success: false };

            return {
                success: true,
                data: {
                    symbol: quote.symbol,
                    name: quote.shortName || quote.longName,
                    description: quote.longName || quote.shortName,
                    price: quote.regularMarketPrice,
                    change: quote.regularMarketChange,
                    changePercent: quote.regularMarketChangePercent,
                    currency: quote.currency || 'USD',
                    exchange: quote.exchange,
                    marketCap: quote.marketCap || null,
                    timestamp: new Date().toISOString()
                },
                source: 'yahoo'
            };

        } catch (error) {
            console.error('[Yahoo] V7 quote error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Get fundamentals (marketCap, P/E, dividend, 52W, description) via Yahoo
     * Uses v8 chart for 52W range + quoteSummary for fundamentals
     */
    async getFundamentals(symbol) {
        const fundamentals = {
            marketCap: null,
            peRatio: null,
            dividendYield: null,
            week52High: null,
            week52Low: null,
            description: null,
            industry: null,
            sector: null
        };

        // 1. Get 52W High/Low from 1Y chart data
        try {
            const url = `${this.baseUrlV8}/finance/chart/${symbol}`;
            const response = await axios.get(url, {
                params: { range: '1y', interval: '1d' },
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Referer': 'https://finance.yahoo.com',
                    'Origin': 'https://finance.yahoo.com'
                },
                timeout: 15000
            });

            const result = response.data?.chart?.result?.[0];
            if (result) {
                const meta = result.meta;
                fundamentals.week52High = meta.fiftyTwoWeekHigh || null;
                fundamentals.week52Low = meta.fiftyTwoWeekLow || null;

                // Calculate from actual data if meta doesn't have it
                if (!fundamentals.week52High && result.indicators?.quote?.[0]) {
                    const highs = result.indicators.quote[0].high.filter(h => h != null);
                    const lows = result.indicators.quote[0].low.filter(l => l != null);
                    if (highs.length) fundamentals.week52High = Math.max(...highs);
                    if (lows.length) fundamentals.week52Low = Math.min(...lows);
                }
            }
        } catch (error) {
            console.error('[Yahoo] 52W chart error:', error.message);
        }

        // 2. Try quoteSummary for fundamentals + description (requires auth)
        try {
            const url = `https://query2.finance.yahoo.com/v10/finance/quoteSummary/${symbol}`;
            const response = await this._authGet(url, {
                modules: 'summaryProfile,defaultKeyStatistics,financialData,price'
            });

            const qr = response.data?.quoteSummary?.result?.[0];
            if (qr) {
                // Profile
                if (qr.summaryProfile) {
                    fundamentals.description = qr.summaryProfile.longBusinessSummary || null;
                    fundamentals.industry = qr.summaryProfile.industry || null;
                    fundamentals.sector = qr.summaryProfile.sector || null;
                }
                // Key Statistics
                if (qr.defaultKeyStatistics) {
                    const ks = qr.defaultKeyStatistics;
                    fundamentals.peRatio = ks.forwardPE?.raw || ks.trailingPE?.raw || null;
                }
                // Financial Data
                if (qr.financialData) {
                    const fd = qr.financialData;
                    fundamentals.marketCap = fd.marketCap?.raw || null;
                }
                // Price module (backup for marketCap, PE)
                if (qr.price) {
                    const p = qr.price;
                    if (!fundamentals.marketCap) fundamentals.marketCap = p.marketCap?.raw || null;
                    if (!fundamentals.peRatio) fundamentals.peRatio = p.trailingPE?.raw || null;
                    fundamentals.dividendYield = p.dividendYield?.raw 
                        ? +(p.dividendYield.raw * 100).toFixed(2) 
                        : null;
                }
            }
        } catch (error) {
            console.error('[Yahoo] QuoteSummary error:', error.message);
        }

        const hasData = Object.values(fundamentals).some(v => v != null);
        return {
            success: hasData,
            data: fundamentals,
            source: 'yahoo'
        };
    }

    async getHistoricalData(symbol, period = '1M') {
        try {
            const periodMap = {
                '1D': { range: '1d', interval: '5m' },
                '1W': { range: '5d', interval: '1h' },
                '1M': { range: '1mo', interval: '1d' },
                '3M': { range: '3mo', interval: '1d' },
                '6M': { range: '6mo', interval: '1d' },
                'YTD': { range: 'ytd', interval: '1d' },
                '1Y': { range: '1y', interval: '1d' },
                '3Y': { range: '3y', interval: '1wk' },
                '5Y': { range: '5y', interval: '1wk' },
                'MAX': { range: 'max', interval: '1mo' }
            };

            const params = periodMap[period] || periodMap['1M'];
            const url = `${this.baseUrlV8}/finance/chart/${symbol}`;
            
            const response = await axios.get(url, {
                params: { range: params.range, interval: params.interval },
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://finance.yahoo.com',
                    'Origin': 'https://finance.yahoo.com'
                },
                timeout: 15000
            });

            const result = response.data?.chart?.result?.[0];
            const timestamps = result?.timestamp;
            const quotes = result?.indicators?.quote?.[0];

            if (!timestamps || !quotes) {
                return { success: false };
            }

            const historicalData = timestamps
                .map((timestamp, index) => ({
                    date: new Date(timestamp * 1000).toISOString().split('T')[0],
                    open: quotes.open[index],
                    high: quotes.high[index],
                    low: quotes.low[index],
                    close: quotes.close[index],
                    volume: quotes.volume[index]
                }))
                .filter(item => item.close !== null);

            return {
                success: true,
                symbol: symbol,
                data: historicalData,
                source: 'yahoo'
            };

        } catch (error) {
            console.error('[Yahoo] Historical error:', error.message);
            return { success: false, error: error.message };
        }
    }

    async searchByISIN(isin) {
        return this.search(isin);
    }

    /**
     * Normalizza simboli italiani aggiungendo .MI se necessario
     */
    normalizeItalianSymbol(query) {
        const italianStocks = {
            'ENEL': 'ENEL.MI',
            'ENI': 'ENI.MI',
            'INTESA': 'ISP.MI',
            'UNICREDIT': 'UCG.MI',
            'GENERALI': 'G.MI',
            'FERRARI': 'RACE.MI',
            'STELLANTIS': 'STLA.MI',
            'LEONARDO': 'LDO.MI',
            'PRYSMIAN': 'PRY.MI',
            'TELECOM': 'TIT.MI'
        };

        const upperQuery = query.toUpperCase();
        return italianStocks[upperQuery] || query;
    }

    /**
     * Determina il paese dall'exchange
     */
    getCountryFromExchange(exchange) {
        const exchangeCountryMap = {
            'MIL': 'IT', 'Milan': 'IT',
            'NYSE': 'US', 'NMS': 'US', 'NYQ': 'US',
            'LSE': 'GB', 'LON': 'GB',
            'FRA': 'DE', 'XETRA': 'DE',
            'PAR': 'FR', 'EPA': 'FR',
            'AMS': 'NL',
            'SWX': 'CH',
            'BME': 'ES'
        };
        return exchangeCountryMap[exchange] || 'US';
    }
}

module.exports = YahooFinanceClient;
