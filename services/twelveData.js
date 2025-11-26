/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Twelve Data API Client
 * Primary source for European markets including Borsa Italiana (MIL)
 * 
 * Features:
 * - 800 requests/day free tier
 * - 8 requests/min rate limit
 * - Supports 50+ countries including Italy
 * - Real-time quotes for European stocks
 */

const axios = require('axios');

class TwelveDataClient {
    constructor(apiKey) {
        this.apiKey = apiKey;
        this.baseUrl = 'https://api.twelvedata.com';
        this.requestCount = 0;
        this.dailyLimit = 800;
        this.minuteLimit = 8;
        this.lastMinuteRequests = [];
        
        // Exchange mappings for symbol formatting
        this.exchangeMap = {
            'MI': 'MIL',      // Milan / Borsa Italiana
            'MIL': 'MIL',
            'PA': 'EPA',      // Paris / Euronext Paris
            'AS': 'AMS',      // Amsterdam
            'BR': 'EBR',      // Brussels
            'L': 'LSE',       // London
            'F': 'FRA',       // Frankfurt
            'DE': 'XETRA',    // XETRA
            'MC': 'BME',      // Madrid
            'SW': 'SIX',      // Swiss Exchange
            'VI': 'VIE',      // Vienna
        };

        // Italian stock symbols mapping (common names to Twelve Data format)
        this.italianStocks = {
            'ENEL': 'ENEL:MIL',
            'ENI': 'ENI:MIL',
            'INTESA': 'ISP:MIL',
            'ISP': 'ISP:MIL',
            'UNICREDIT': 'UCG:MIL',
            'UCG': 'UCG:MIL',
            'GENERALI': 'G:MIL',
            'FERRARI': 'RACE:MIL',
            'RACE': 'RACE:MIL',
            'STELLANTIS': 'STLAM:MIL',
            'STLA': 'STLAM:MIL',
            'LEONARDO': 'LDO:MIL',
            'LDO': 'LDO:MIL',
            'PRYSMIAN': 'PRY:MIL',
            'PRY': 'PRY:MIL',
            'TELECOM': 'TIT:MIL',
            'TIT': 'TIT:MIL',
            'A2A': 'A2A:MIL',
            'HERA': 'HER:MIL',
            'SNAM': 'SRG:MIL',
            'TERNA': 'TRN:MIL',
            'AMPLIFON': 'AMP:MIL',
            'RECORDATI': 'REC:MIL',
            'MONCLER': 'MONC:MIL',
            'DIASORIN': 'DIA:MIL',
            'NEXI': 'NEXI:MIL',
            'CAMPARI': 'CPR:MIL',
            'PIRELLI': 'PIRC:MIL',
            'BUZZI': 'BZU:MIL',
            'IVECO': 'IVG:MIL',
            'BANCA MEDIOLANUM': 'BMED:MIL',
            'BPER': 'BPE:MIL',
            'BANCO BPM': 'BAMI:MIL',
            'POSTE ITALIANE': 'PST:MIL',
            'FINECOBANK': 'FBK:MIL',
            'MEDIOBANCA': 'MB:MIL',
            'SAIPEM': 'SPM:MIL',
            'TENARIS': 'TEN:MIL',
            'INTERPUMP': 'IP:MIL',
            'STMICROELECTRONICS': 'STMMI:MIL',
            'STM': 'STMMI:MIL',
        };
    }

    /**
     * Check rate limits before making request
     */
    checkRateLimit() {
        const now = Date.now();
        
        // Clean old requests (older than 1 minute)
        this.lastMinuteRequests = this.lastMinuteRequests.filter(
            time => now - time < 60000
        );

        // Check minute limit
        if (this.lastMinuteRequests.length >= this.minuteLimit) {
            const waitTime = 60000 - (now - this.lastMinuteRequests[0]);
            console.warn(`[TwelveData] Rate limit: waiting ${Math.ceil(waitTime/1000)}s`);
            return { allowed: false, waitTime };
        }

        // Check daily limit
        if (this.requestCount >= this.dailyLimit) {
            console.warn('[TwelveData] Daily limit reached');
            return { allowed: false, waitTime: -1 };
        }

        return { allowed: true };
    }

    /**
     * Record a request for rate limiting
     */
    recordRequest() {
        this.lastMinuteRequests.push(Date.now());
        this.requestCount++;
    }

    /**
     * Normalize symbol to Twelve Data format
     * Converts: ENEL.MI -> ENEL:MIL, ENEL -> ENEL:MIL (if Italian)
     */
    normalizeSymbol(symbol) {
        const upperSymbol = symbol.toUpperCase().trim();

        // Check if it's a known Italian stock
        if (this.italianStocks[upperSymbol]) {
            return this.italianStocks[upperSymbol];
        }

        // Check if already in Twelve Data format (SYMBOL:EXCHANGE)
        if (upperSymbol.includes(':')) {
            return upperSymbol;
        }

        // Convert Yahoo format (ENEL.MI) to Twelve Data format (ENEL:MIL)
        if (upperSymbol.includes('.')) {
            const [ticker, exchange] = upperSymbol.split('.');
            const mappedExchange = this.exchangeMap[exchange] || exchange;
            return `${ticker}:${mappedExchange}`;
        }

        // Return as-is for US stocks or unknown
        return upperSymbol;
    }

    /**
     * Convert Twelve Data symbol back to standard format
     */
    toStandardSymbol(twelveDataSymbol) {
        if (!twelveDataSymbol.includes(':')) {
            return twelveDataSymbol;
        }
        const [ticker, exchange] = twelveDataSymbol.split(':');
        
        // Convert MIL back to .MI
        const exchangeMap = {
            'MIL': 'MI',
            'EPA': 'PA',
            'AMS': 'AS',
            'LSE': 'L',
            'FRA': 'F',
            'XETRA': 'DE',
            'BME': 'MC',
            'SIX': 'SW',
        };
        
        const stdExchange = exchangeMap[exchange] || exchange;
        return `${ticker}.${stdExchange}`;
    }

    /**
     * Search for symbols
     */
    async search(query) {
        const rateCheck = this.checkRateLimit();
        if (!rateCheck.allowed) {
            return { success: false, results: [], error: 'Rate limit exceeded' };
        }

        try {
            console.log(`[TwelveData] Searching for: "${query}"`);
            
            const response = await axios.get(`${this.baseUrl}/symbol_search`, {
                params: {
                    symbol: query,
                    apikey: this.apiKey,
                    outputsize: 15
                },
                timeout: 10000
            });

            this.recordRequest();

            if (!response.data?.data || response.data.data.length === 0) {
                console.log('[TwelveData] No results found');
                return { success: false, results: [] };
            }

            const results = response.data.data
                .filter(item => item.instrument_type === 'Common Stock' || 
                               item.instrument_type === 'ETF' ||
                               item.instrument_type === 'Equity')
                .slice(0, 10)
                .map(item => ({
                    symbol: this.toStandardSymbol(item.symbol),
                    twelveDataSymbol: item.symbol,
                    name: item.instrument_name,
                    description: item.instrument_name,
                    type: item.instrument_type || 'Stock',
                    exchange: item.exchange,
                    country: item.country,
                    currency: item.currency,
                    isin: null,
                    price: null,
                    change: null,
                    changePercent: null
                }));

            console.log(`[TwelveData] Found ${results.length} results`);
            return { success: true, results, source: 'twelvedata' };

        } catch (error) {
            console.error('[TwelveData] Search error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    /**
     * Get real-time quote
     */
    async getQuote(symbol) {
        const rateCheck = this.checkRateLimit();
        if (!rateCheck.allowed) {
            return { success: false, error: 'Rate limit exceeded' };
        }

        try {
            const normalizedSymbol = this.normalizeSymbol(symbol);
            console.log(`[TwelveData] Getting quote for: ${symbol} -> ${normalizedSymbol}`);

            const response = await axios.get(`${this.baseUrl}/quote`, {
                params: {
                    symbol: normalizedSymbol,
                    apikey: this.apiKey
                },
                timeout: 10000
            });

            this.recordRequest();

            const data = response.data;
            
            // Check for API errors
            if (data.status === 'error' || data.code) {
                console.error(`[TwelveData] API error: ${data.message || data.code}`);
                return { success: false, error: data.message };
            }

            // Check if we have valid price data
            if (!data.close && !data.price) {
                console.warn(`[TwelveData] No price data for ${normalizedSymbol}`);
                return { success: false, error: 'No price data available' };
            }

            const price = parseFloat(data.close || data.price);
            const prevClose = parseFloat(data.previous_close) || price;
            const change = price - prevClose;
            const changePercent = prevClose > 0 ? (change / prevClose) * 100 : 0;

            return {
                success: true,
                data: {
                    symbol: this.toStandardSymbol(normalizedSymbol),
                    twelveDataSymbol: normalizedSymbol,
                    name: data.name || symbol,
                    price: price,
                    open: parseFloat(data.open) || null,
                    high: parseFloat(data.high) || null,
                    low: parseFloat(data.low) || null,
                    previousClose: prevClose,
                    change: parseFloat(change.toFixed(4)),
                    changePercent: parseFloat(changePercent.toFixed(2)),
                    volume: parseInt(data.volume) || null,
                    currency: data.currency || this.detectCurrency(normalizedSymbol),
                    exchange: data.exchange,
                    timestamp: data.datetime || new Date().toISOString()
                },
                source: 'twelvedata'
            };

        } catch (error) {
            console.error('[TwelveData] Quote error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Get historical data (time series)
     */
    async getHistoricalData(symbol, period = '1M') {
        const rateCheck = this.checkRateLimit();
        if (!rateCheck.allowed) {
            return { success: false, error: 'Rate limit exceeded' };
        }

        try {
            const normalizedSymbol = this.normalizeSymbol(symbol);
            console.log(`[TwelveData] Getting historical data for: ${normalizedSymbol}, period: ${period}`);

            // Map periods to Twelve Data parameters
            const periodMap = {
                '1D': { interval: '5min', outputsize: 78 },      // ~6.5 hours of 5min bars
                '1W': { interval: '1h', outputsize: 40 },        // ~1 week hourly
                '1M': { interval: '1day', outputsize: 22 },      // ~1 month daily
                '3M': { interval: '1day', outputsize: 66 },      // ~3 months daily
                '6M': { interval: '1day', outputsize: 132 },     // ~6 months daily
                'YTD': { interval: '1day', outputsize: 252 },    // YTD daily
                '1Y': { interval: '1day', outputsize: 252 },     // 1 year daily
                '3Y': { interval: '1week', outputsize: 156 },    // 3 years weekly
                '5Y': { interval: '1week', outputsize: 260 },    // 5 years weekly
                'MAX': { interval: '1month', outputsize: 240 }   // 20 years monthly
            };

            const params = periodMap[period] || periodMap['1M'];

            const response = await axios.get(`${this.baseUrl}/time_series`, {
                params: {
                    symbol: normalizedSymbol,
                    interval: params.interval,
                    outputsize: params.outputsize,
                    apikey: this.apiKey
                },
                timeout: 15000
            });

            this.recordRequest();

            const data = response.data;

            // Check for API errors
            if (data.status === 'error' || data.code) {
                console.error(`[TwelveData] API error: ${data.message || data.code}`);
                return { success: false, error: data.message };
            }

            if (!data.values || data.values.length === 0) {
                console.warn(`[TwelveData] No historical data for ${normalizedSymbol}`);
                return { success: false, error: 'No historical data available' };
            }

            // Convert to standard format and reverse (oldest first)
            const historicalData = data.values
                .map(item => ({
                    date: item.datetime.split(' ')[0], // Extract date part
                    open: parseFloat(item.open),
                    high: parseFloat(item.high),
                    low: parseFloat(item.low),
                    close: parseFloat(item.close),
                    volume: parseInt(item.volume) || 0
                }))
                .reverse(); // Oldest first

            console.log(`[TwelveData] Historical data: ${historicalData.length} points`);

            return {
                success: true,
                symbol: this.toStandardSymbol(normalizedSymbol),
                data: historicalData,
                source: 'twelvedata',
                period: period,
                currency: data.meta?.currency || this.detectCurrency(normalizedSymbol)
            };

        } catch (error) {
            console.error('[TwelveData] Historical data error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Search by ISIN (converts to symbol search)
     */
    async searchByISIN(isin) {
        // Twelve Data doesn't directly support ISIN, so we search by ISIN string
        return this.search(isin);
    }

    /**
     * Detect currency from symbol/exchange
     */
    detectCurrency(symbol) {
        if (symbol.includes(':MIL') || symbol.includes('.MI')) return 'EUR';
        if (symbol.includes(':EPA') || symbol.includes('.PA')) return 'EUR';
        if (symbol.includes(':AMS') || symbol.includes('.AS')) return 'EUR';
        if (symbol.includes(':FRA') || symbol.includes('.F')) return 'EUR';
        if (symbol.includes(':XETRA') || symbol.includes('.DE')) return 'EUR';
        if (symbol.includes(':BME') || symbol.includes('.MC')) return 'EUR';
        if (symbol.includes(':LSE') || symbol.includes('.L')) return 'GBP';
        if (symbol.includes(':SIX') || symbol.includes('.SW')) return 'CHF';
        return 'USD'; // Default to USD
    }

    /**
     * Get API usage stats
     */
    getUsageStats() {
        return {
            dailyRequestsUsed: this.requestCount,
            dailyLimit: this.dailyLimit,
            dailyRemaining: this.dailyLimit - this.requestCount,
            minuteRequestsUsed: this.lastMinuteRequests.length,
            minuteLimit: this.minuteLimit,
            minuteRemaining: this.minuteLimit - this.lastMinuteRequests.length
        };
    }

    /**
     * Health check
     */
    async healthCheck() {
        try {
            const result = await this.getQuote('AAPL');
            return {
                status: result.success ? 'OK' : 'DEGRADED',
                message: result.success ? 'API responding' : result.error,
                usage: this.getUsageStats()
            };
        } catch (error) {
            return {
                status: 'ERROR',
                message: error.message,
                usage: this.getUsageStats()
            };
        }
    }
}

module.exports = TwelveDataClient;
