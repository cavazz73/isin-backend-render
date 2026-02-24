/**
 * Copyright (c) 2024-2026 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * OpenFIGI Client - Universal ISIN Resolution
 * Uses Bloomberg's free OpenFIGI API to map ISINs to instruments
 * Covers: stocks, bonds, ETFs, funds/OICR, certificates, futures, options
 * All global exchanges including Borsa Italiana
 * 
 * API Docs: https://www.openfigi.com/api
 * Rate limit: 20 requests/minute (no key), 100/min (with key)
 */

const axios = require('axios');

class OpenFigiClient {
    constructor(apiKey = null) {
        this.baseUrl = 'https://api.openfigi.com';
        this.apiKey = apiKey || process.env.OPENFIGI_API_KEY || null;
        this.headers = {
            'Content-Type': 'application/json'
        };
        if (this.apiKey) {
            this.headers['X-OPENFIGI-APIKEY'] = this.apiKey;
        }
        console.log(`[OpenFIGI] Initialized (API key: ${this.apiKey ? 'YES - 100 req/min' : 'NO - 20 req/min'})`);
    }

    /**
     * Map ISIN to financial instrument(s) via OpenFIGI
     * Returns all matching instruments across all exchanges
     */
    async mapISIN(isin) {
        try {
            console.log(`[OpenFIGI] Mapping ISIN: ${isin}`);
            const response = await axios.post(
                `${this.baseUrl}/v3/mapping`,
                [{ idType: 'ID_ISIN', idValue: isin.toUpperCase() }],
                { headers: this.headers, timeout: 10000 }
            );

            const data = response.data;
            if (!data || !data[0] || !data[0].data || data[0].data.length === 0) {
                console.log(`[OpenFIGI] No results for ISIN ${isin}`);
                return { success: false, results: [] };
            }

            const instruments = data[0].data;
            console.log(`[OpenFIGI] Found ${instruments.length} instrument(s) for ${isin}`);

            // Map to our standard format
            const results = instruments.map(inst => ({
                symbol: inst.ticker || '',
                name: inst.name || '',
                type: this._mapSecurityType(inst.securityType, inst.securityType2),
                exchange: inst.exchCode || '',
                exchangeName: inst.marketSector || '',
                mic: inst.exchCode || '',
                figi: inst.figi || '',
                compositeFigi: inst.compositeFIGI || '',
                shareClassFigi: inst.shareClassFIGI || '',
                currency: inst.currency || '',  // non sempre presente, dipende dall'endpoint
                isin: isin.toUpperCase(),
                source: 'openfigi',
                raw: {
                    securityType: inst.securityType || '',
                    securityType2: inst.securityType2 || '',
                    marketSector: inst.marketSector || '',
                    ticker: inst.ticker || '',
                    exchCode: inst.exchCode || '',
                    securityDescription: inst.securityDescription || ''
                }
            }));

            // Deduplicate by exchange and prefer primary listings
            const deduplicated = this._deduplicateResults(results);

            return {
                success: true,
                results: deduplicated,
                metadata: {
                    source: 'openfigi',
                    totalMatches: instruments.length,
                    returnedMatches: deduplicated.length,
                    isin: isin.toUpperCase()
                }
            };
        } catch (error) {
            if (error.response?.status === 429) {
                console.error('[OpenFIGI] Rate limited! Consider adding API key');
            } else {
                console.error(`[OpenFIGI] Error mapping ISIN: ${error.message}`);
            }
            return { success: false, results: [], error: error.message };
        }
    }

    /**
     * Search by text query (name/ticker) via OpenFIGI search endpoint
     * Only works with API key
     */
    async search(query) {
        try {
            console.log(`[OpenFIGI] Text search: "${query}"`);
            const response = await axios.get(
                `${this.baseUrl}/v3/search`,
                {
                    params: { query: query, start: 0, maxResults: 10 },
                    headers: this.headers,
                    timeout: 10000
                }
            );

            const data = response.data;
            if (!data || !data.data || data.data.length === 0) {
                return { success: false, results: [] };
            }

            const results = data.data.map(inst => ({
                symbol: inst.ticker || '',
                name: inst.name || '',
                type: this._mapSecurityType(inst.securityType, inst.securityType2),
                exchange: inst.exchCode || '',
                figi: inst.figi || '',
                isin: '', // search doesn't return ISIN directly
                source: 'openfigi',
                raw: {
                    securityType: inst.securityType || '',
                    securityType2: inst.securityType2 || '',
                    marketSector: inst.marketSector || ''
                }
            }));

            console.log(`[OpenFIGI] Search found ${results.length} results`);
            return {
                success: true,
                results: results,
                metadata: { source: 'openfigi', query }
            };
        } catch (error) {
            console.error(`[OpenFIGI] Search error: ${error.message}`);
            return { success: false, results: [], error: error.message };
        }
    }

    /**
     * Map multiple ISINs in one batch request (max 100 per call)
     */
    async mapBatch(isins) {
        try {
            const jobs = isins.map(isin => ({ idType: 'ID_ISIN', idValue: isin.toUpperCase() }));
            const response = await axios.post(
                `${this.baseUrl}/v3/mapping`,
                jobs,
                { headers: this.headers, timeout: 15000 }
            );

            const results = {};
            response.data.forEach((item, index) => {
                const isin = isins[index];
                if (item.data && item.data.length > 0) {
                    results[isin] = item.data.map(inst => ({
                        symbol: inst.ticker || '',
                        name: inst.name || '',
                        type: this._mapSecurityType(inst.securityType, inst.securityType2),
                        exchange: inst.exchCode || '',
                        figi: inst.figi || '',
                        currency: inst.currency || ''
                    }));
                } else {
                    results[isin] = [];
                }
            });

            console.log(`[OpenFIGI] Batch mapped ${Object.keys(results).length} ISINs`);
            return { success: true, results };
        } catch (error) {
            console.error(`[OpenFIGI] Batch error: ${error.message}`);
            return { success: false, results: {}, error: error.message };
        }
    }

    /**
     * Map OpenFIGI security types to our standard types
     */
    _mapSecurityType(secType, secType2) {
        const type1 = (secType || '').toUpperCase();
        const type2 = (secType2 || '').toUpperCase();

        // Common Stock / Equity
        if (type1 === 'COMMON STOCK' || type1 === 'EQ' || type2 === 'COMMON STOCK') return 'Stock';
        if (type1 === 'DEPOSITARY RECEIPT' || type1 === 'ADR') return 'ADR';
        if (type1 === 'PREFERRED STOCK' || type1 === 'PREFERRED') return 'Preferred';
        if (type1 === 'REIT') return 'REIT';

        // ETF / Funds
        if (type1 === 'ETP' || type1 === 'ETF' || type2 === 'ETF') return 'ETF';
        if (type1 === 'OPEN-END FUND' || type1 === 'MUTUAL FUND' || type2 === 'OPEN-END FUND') return 'Fund';
        if (type1 === 'CLOSED-END FUND') return 'Closed-End Fund';
        if (type1 === 'UNIT' || type1 === 'UNIT TRUST') return 'Fund';

        // Fixed Income
        if (type1 === 'CORP' || type2 === 'CORPORATE BOND') return 'Bond';
        if (type1 === 'GOVT' || type2 === 'GOVERNMENT BOND' || type2 === 'SOVEREIGN') return 'Government Bond';
        if (type1 === 'COVERED' || type2 === 'COVERED BOND') return 'Covered Bond';
        if (type1 === 'ABS' || type1 === 'MBS') return 'Structured Bond';

        // Derivatives / Certificates
        if (type1 === 'INDEX' || type2 === 'INDEX') return 'Index';
        if (type1 === 'WARRANT') return 'Warrant';
        if (type1 === 'OPTION') return 'Option';
        if (type1 === 'FUTURE') return 'Future';
        if (type1 === 'STRUCTURED PRODUCT' || type2 === 'STRUCTURED PRODUCT') return 'Certificate';

        // Commodities / Forex / Crypto
        if (type1 === 'COMMODITY' || type2 === 'COMMODITY') return 'Commodity';
        if (type1 === 'CURRENCY') return 'Currency';

        // Fallback
        if (type1) return type1;
        return 'Unknown';
    }

    /**
     * Deduplicate and prioritize results
     * Prefer: primary exchange > major exchanges > others
     */
    _deduplicateResults(results) {
        if (results.length <= 5) return results;

        // Priority exchanges
        const priorityExchanges = [
            'MI', 'IM',     // Borsa Italiana / Milan
            'US', 'UN', 'UQ', 'UA', 'UP', // NYSE, NASDAQ
            'LN',           // London
            'GR', 'GF',     // Frankfurt
            'FP',           // Euronext Paris
            'NA',           // Euronext Amsterdam
            'SM',           // Madrid
            'SW',           // SIX Swiss
            'JP', 'TY',     // Tokyo
            'HK',           // Hong Kong
        ];

        // Sort by priority
        const sorted = [...results].sort((a, b) => {
            const aIdx = priorityExchanges.indexOf(a.exchange);
            const bIdx = priorityExchanges.indexOf(b.exchange);
            const aPrio = aIdx >= 0 ? aIdx : 999;
            const bPrio = bIdx >= 0 ? bIdx : 999;
            return aPrio - bPrio;
        });

        // Keep max 10 results
        return sorted.slice(0, 10);
    }

    /**
     * Convert OpenFIGI exchange code to Yahoo Finance symbol suffix
     * e.g. ENEL on IM → ENEL.MI, SHEL on LN → SHEL.L
     */
    static exchangeToYahooSuffix(exchCode) {
        const map = {
            // Italy
            'IM': '.MI', 'MI': '.MI',
            // US (no suffix needed)
            'US': '', 'UN': '', 'UQ': '', 'UA': '', 'UP': '', 'UW': '',
            // UK
            'LN': '.L',
            // Germany
            'GR': '.DE', 'GF': '.F', 'GS': '.SG', 'GD': '.DU', 'GH': '.HM', 'GM': '.MU', 'GY': '.DE',
            // France
            'FP': '.PA',
            // Netherlands
            'NA': '.AS',
            // Spain
            'SM': '.MC',
            // Switzerland
            'SW': '.SW',
            // Japan
            'JP': '.T', 'TY': '.T',
            // Hong Kong
            'HK': '.HK',
            // Canada
            'CT': '.TO', 'CN': '.V',
            // Australia
            'AT': '.AX',
            // Singapore
            'SP': '.SI',
            // Korea
            'KS': '.KS', 'KQ': '.KQ',
            // Brazil
            'BZ': '.SA',
            // India
            'IB': '.BO', 'IN': '.NS',
            // Mexico
            'MM': '.MX',
        };
        return map[exchCode] || '';
    }

    /**
     * Get the best Yahoo Finance symbol from OpenFIGI results
     * Prioritizes the primary exchange listing
     */
    static getBestYahooSymbol(results) {
        if (!results || results.length === 0) return null;
        const best = results[0]; // Already sorted by priority
        const suffix = OpenFigiClient.exchangeToYahooSuffix(best.exchange);
        return best.symbol + suffix;
    }
}

module.exports = OpenFigiClient;
