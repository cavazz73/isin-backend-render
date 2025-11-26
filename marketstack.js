/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Marketstack API Client
 * Excellent support for European/Italian stocks (Borsa Italiana)
 * Free tier: 100 requests/month, EOD data, 72+ exchanges
 * 
 * Exchange codes for Italian stocks:
 * - XMIL = Borsa Italiana (Milan Stock Exchange)
 */

const axios = require('axios');

class MarketstackClient {
    constructor(apiKey) {
        this.apiKey = apiKey;
        this.baseUrl = 'http://api.marketstack.com/v1'; // Note: HTTPS only on paid plans
        this.requestCount = 0;
        this.monthlyLimit = 100;
        
        // Italian stock symbol mappings to Marketstack format
        this.italianSymbolMap = {
            'ENEL': 'ENEL.XMIL',
            'ENEL.MI': 'ENEL.XMIL',
            'ENI': 'ENI.XMIL',
            'ENI.MI': 'ENI.XMIL',
            'ISP': 'ISP.XMIL',
            'ISP.MI': 'ISP.XMIL',
            'INTESA': 'ISP.XMIL',
            'UCG': 'UCG.XMIL',
            'UCG.MI': 'UCG.XMIL',
            'UNICREDIT': 'UCG.XMIL',
            'G': 'G.XMIL',
            'G.MI': 'G.XMIL',
            'GENERALI': 'G.XMIL',
            'RACE': 'RACE.XMIL',
            'RACE.MI': 'RACE.XMIL',
            'FERRARI': 'RACE.XMIL',
            'STLA': 'STLAM.XMIL',
            'STLA.MI': 'STLAM.XMIL',
            'STELLANTIS': 'STLAM.XMIL',
            'LDO': 'LDO.XMIL',
            'LDO.MI': 'LDO.XMIL',
            'LEONARDO': 'LDO.XMIL',
            'PRY': 'PRY.XMIL',
            'PRY.MI': 'PRY.XMIL',
            'PRYSMIAN': 'PRY.XMIL',
            'TIT': 'TIT.XMIL',
            'TIT.MI': 'TIT.XMIL',
            'TELECOM': 'TIT.XMIL',
            'TIM': 'TIT.XMIL',
            'STM': 'STM.XMIL',
            'STM.MI': 'STM.XMIL',
            'STMICROELECTRONICS': 'STM.XMIL',
            'TENARIS': 'TEN.XMIL',
            'TEN': 'TEN.XMIL',
            'TEN.MI': 'TEN.XMIL',
            'MONCLER': 'MONC.XMIL',
            'MONC': 'MONC.XMIL',
            'MONC.MI': 'MONC.XMIL',
            'CAMPARI': 'CPR.XMIL',
            'CPR': 'CPR.XMIL',
            'CPR.MI': 'CPR.XMIL',
            'PIRELLI': 'PIRC.XMIL',
            'PIRC': 'PIRC.XMIL',
            'PIRC.MI': 'PIRC.XMIL',
            'AMPLIFON': 'AMP.XMIL',
            'AMP': 'AMP.XMIL',
            'AMP.MI': 'AMP.XMIL',
            'ATLANTIA': 'ATL.XMIL',
            'ATL': 'ATL.XMIL',
            'ATL.MI': 'ATL.XMIL',
            'RECORDATI': 'REC.XMIL',
            'REC': 'REC.XMIL',
            'REC.MI': 'REC.XMIL',
            'DIASORIN': 'DIA.XMIL',
            'DIA': 'DIA.XMIL',
            'DIA.MI': 'DIA.XMIL',
            'NEXI': 'NEXI.XMIL',
            'NEXI.MI': 'NEXI.XMIL',
            'INWIT': 'INW.XMIL',
            'INW': 'INW.XMIL',
            'INW.MI': 'INW.XMIL',
            'SNAM': 'SRG.XMIL',
            'SRG': 'SRG.XMIL',
            'SRG.MI': 'SRG.XMIL',
            'TERNA': 'TRN.XMIL',
            'TRN': 'TRN.XMIL',
            'TRN.MI': 'TRN.XMIL',
            'ITALGAS': 'IG.XMIL',
            'IG': 'IG.XMIL',
            'IG.MI': 'IG.XMIL',
            'MEDIOBANCA': 'MB.XMIL',
            'MB': 'MB.XMIL',
            'MB.MI': 'MB.XMIL',
            'BPER': 'BPE.XMIL',
            'BPE': 'BPE.XMIL',
            'BPE.MI': 'BPE.XMIL',
            'BANCO BPM': 'BAMI.XMIL',
            'BAMI': 'BAMI.XMIL',
            'BAMI.MI': 'BAMI.XMIL',
            'FCA': 'STLAM.XMIL',
            'POSTE': 'PST.XMIL',
            'PST': 'PST.XMIL',
            'PST.MI': 'PST.XMIL',
            'POSTE ITALIANE': 'PST.XMIL',
            'A2A': 'A2A.XMIL',
            'A2A.MI': 'A2A.XMIL',
            'HERA': 'HER.XMIL',
            'HER': 'HER.XMIL',
            'HER.MI': 'HER.XMIL',
            'SAIPEM': 'SPM.XMIL',
            'SPM': 'SPM.XMIL',
            'SPM.MI': 'SPM.XMIL'
        };

        // Exchange to currency mapping
        this.exchangeCurrencyMap = {
            'XMIL': 'EUR',
            'XETRA': 'EUR',
            'XFRA': 'EUR',
            'XPAR': 'EUR',
            'XAMS': 'EUR',
            'XBRU': 'EUR',
            'XLIS': 'EUR',
            'XMAD': 'EUR',
            'XHEL': 'EUR',
            'XDUB': 'EUR',
            'XATH': 'EUR',
            'XLON': 'GBP',
            'XSWX': 'CHF',
            'XSTO': 'SEK',
            'XCSE': 'DKK',
            'XOSL': 'NOK',
            'XWAR': 'PLN',
            'XNYS': 'USD',
            'XNAS': 'USD',
            'XNSE': 'INR',
            'XTSE': 'CAD',
            'XASX': 'AUD',
            'XTKS': 'JPY',
            'XHKG': 'HKD',
            'XSHG': 'CNY',
            'XSHE': 'CNY'
        };
    }

    /**
     * Normalize symbol for Marketstack API
     */
    normalizeSymbol(symbol) {
        const upperSymbol = symbol.toUpperCase().trim();
        
        // Check direct mapping first
        if (this.italianSymbolMap[upperSymbol]) {
            return this.italianSymbolMap[upperSymbol];
        }
        
        // If already has exchange suffix, convert .MI to .XMIL
        if (upperSymbol.endsWith('.MI')) {
            return upperSymbol.replace('.MI', '.XMIL');
        }
        
        // Return as-is for other symbols
        return upperSymbol;
    }

    /**
     * Detect if symbol is likely Italian/European
     */
    isEuropeanSymbol(symbol) {
        const upperSymbol = symbol.toUpperCase();
        return upperSymbol.endsWith('.MI') || 
               upperSymbol.endsWith('.XMIL') ||
               this.italianSymbolMap[upperSymbol] !== undefined ||
               upperSymbol.includes('.XE') || // XETRA
               upperSymbol.includes('.PA') || // Paris
               upperSymbol.includes('.AS') || // Amsterdam
               upperSymbol.includes('.L');    // London
    }

    /**
     * Get currency from exchange code
     */
    getCurrencyFromExchange(exchange) {
        return this.exchangeCurrencyMap[exchange] || 'USD';
    }

    /**
     * Search for symbols
     */
    async search(query) {
        if (!this.apiKey) {
            console.warn('[Marketstack] No API key configured');
            return { success: false, results: [], error: 'No API key' };
        }

        try {
            console.log(`[Marketstack] Searching for: ${query}`);
            
            // Normalize the query for European stocks
            const normalizedQuery = this.normalizeSymbol(query);
            
            const response = await axios.get(`${this.baseUrl}/tickers`, {
                params: {
                    access_key: this.apiKey,
                    search: query, // Use original query for search
                    limit: 10
                },
                timeout: 15000
            });

            this.requestCount++;

            if (!response.data?.data || response.data.data.length === 0) {
                // Try with normalized symbol as direct lookup
                return await this.getTickerInfo(normalizedQuery);
            }

            const results = response.data.data.map(ticker => {
                const exchange = ticker.stock_exchange?.mic || 'UNKNOWN';
                const currency = this.getCurrencyFromExchange(exchange);
                
                return {
                    symbol: ticker.symbol,
                    name: ticker.name,
                    description: ticker.name,
                    type: 'Stock',
                    exchange: exchange,
                    exchangeName: ticker.stock_exchange?.name || exchange,
                    currency: currency,
                    country: ticker.stock_exchange?.country || 'Unknown',
                    isin: null,
                    price: null,
                    change: null,
                    changePercent: null
                };
            });

            return { 
                success: true, 
                results, 
                source: 'marketstack' 
            };

        } catch (error) {
            console.error('[Marketstack] Search error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    /**
     * Get ticker info directly
     */
    async getTickerInfo(symbol) {
        if (!this.apiKey) {
            return { success: false, results: [], error: 'No API key' };
        }

        try {
            const normalizedSymbol = this.normalizeSymbol(symbol);
            console.log(`[Marketstack] Getting ticker info for: ${normalizedSymbol}`);
            
            const response = await axios.get(`${this.baseUrl}/tickers/${normalizedSymbol}`, {
                params: {
                    access_key: this.apiKey
                },
                timeout: 15000
            });

            this.requestCount++;

            if (!response.data) {
                return { success: false, results: [] };
            }

            const ticker = response.data;
            const exchange = ticker.stock_exchange?.mic || 'UNKNOWN';
            const currency = this.getCurrencyFromExchange(exchange);

            const result = {
                symbol: ticker.symbol,
                name: ticker.name,
                description: ticker.name,
                type: 'Stock',
                exchange: exchange,
                exchangeName: ticker.stock_exchange?.name || exchange,
                currency: currency,
                country: ticker.stock_exchange?.country || 'Unknown',
                isin: null,
                price: null,
                change: null,
                changePercent: null
            };

            return { 
                success: true, 
                results: [result], 
                source: 'marketstack' 
            };

        } catch (error) {
            console.error('[Marketstack] Ticker info error:', error.message);
            return { success: false, results: [], error: error.message };
        }
    }

    /**
     * Get real-time/EOD quote for a symbol
     */
    async getQuote(symbol) {
        if (!this.apiKey) {
            return { success: false, error: 'No API key' };
        }

        try {
            const normalizedSymbol = this.normalizeSymbol(symbol);
            console.log(`[Marketstack] Getting quote for: ${normalizedSymbol}`);
            
            // Use EOD endpoint (available on free tier)
            const response = await axios.get(`${this.baseUrl}/eod/latest`, {
                params: {
                    access_key: this.apiKey,
                    symbols: normalizedSymbol
                },
                timeout: 15000
            });

            this.requestCount++;

            const data = response.data?.data?.[0];
            if (!data) {
                console.warn(`[Marketstack] No quote data for ${normalizedSymbol}`);
                return { success: false, error: 'No data' };
            }

            // Extract exchange from symbol
            const exchangeMatch = normalizedSymbol.match(/\.([A-Z]+)$/);
            const exchange = exchangeMatch ? exchangeMatch[1] : 'UNKNOWN';
            const currency = this.getCurrencyFromExchange(exchange);

            // Calculate change
            const change = data.close - data.open;
            const changePercent = data.open > 0 ? ((change / data.open) * 100) : 0;

            return {
                success: true,
                data: {
                    symbol: data.symbol,
                    name: data.symbol.split('.')[0],
                    price: data.close,
                    open: data.open,
                    high: data.high,
                    low: data.low,
                    volume: data.volume,
                    change: change,
                    changePercent: changePercent,
                    currency: currency,
                    exchange: exchange,
                    date: data.date,
                    timestamp: new Date().toISOString()
                },
                source: 'marketstack'
            };

        } catch (error) {
            console.error('[Marketstack] Quote error:', error.message);
            
            // Log more details for debugging
            if (error.response) {
                console.error('[Marketstack] Response status:', error.response.status);
                console.error('[Marketstack] Response data:', JSON.stringify(error.response.data));
            }
            
            return { success: false, error: error.message };
        }
    }

    /**
     * Get historical data
     */
    async getHistoricalData(symbol, period = '1M') {
        if (!this.apiKey) {
            return { success: false, error: 'No API key' };
        }

        try {
            const normalizedSymbol = this.normalizeSymbol(symbol);
            console.log(`[Marketstack] Getting historical data for: ${normalizedSymbol}, period: ${period}`);
            
            // Calculate date range based on period
            const endDate = new Date();
            const startDate = new Date();
            
            switch(period) {
                case '1D': startDate.setDate(endDate.getDate() - 1); break;
                case '1W': startDate.setDate(endDate.getDate() - 7); break;
                case '1M': startDate.setMonth(endDate.getMonth() - 1); break;
                case '3M': startDate.setMonth(endDate.getMonth() - 3); break;
                case '6M': startDate.setMonth(endDate.getMonth() - 6); break;
                case 'YTD': startDate.setMonth(0); startDate.setDate(1); break;
                case '1Y': startDate.setFullYear(endDate.getFullYear() - 1); break;
                case '3Y': startDate.setFullYear(endDate.getFullYear() - 3); break;
                case '5Y': startDate.setFullYear(endDate.getFullYear() - 5); break;
                case 'MAX': startDate.setFullYear(endDate.getFullYear() - 10); break;
                default: startDate.setMonth(endDate.getMonth() - 1);
            }

            const response = await axios.get(`${this.baseUrl}/eod`, {
                params: {
                    access_key: this.apiKey,
                    symbols: normalizedSymbol,
                    date_from: startDate.toISOString().split('T')[0],
                    date_to: endDate.toISOString().split('T')[0],
                    limit: 1000
                },
                timeout: 20000
            });

            this.requestCount++;

            if (!response.data?.data || response.data.data.length === 0) {
                return { success: false, error: 'No historical data' };
            }

            const historicalData = response.data.data
                .map(item => ({
                    date: item.date.split('T')[0],
                    open: item.open,
                    high: item.high,
                    low: item.low,
                    close: item.close,
                    volume: item.volume
                }))
                .sort((a, b) => new Date(a.date) - new Date(b.date));

            return {
                success: true,
                symbol: normalizedSymbol,
                data: historicalData,
                source: 'marketstack'
            };

        } catch (error) {
            console.error('[Marketstack] Historical error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Search by ISIN (limited support)
     */
    async searchByISIN(isin) {
        // Marketstack doesn't support direct ISIN search on free tier
        // Try to search by ISIN as query
        return this.search(isin);
    }

    /**
     * Get remaining API calls
     */
    getRemainingCalls() {
        return Math.max(0, this.monthlyLimit - this.requestCount);
    }
}

module.exports = MarketstackClient;
