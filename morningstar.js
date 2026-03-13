/**
 * Copyright (c) 2024-2026 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Morningstar Client - Fund/ETF Name Resolution by ISIN
 * Uses Morningstar.it SecuritySearch endpoint (same as website autocomplete)
 * Best source for: fund names, categories, Morningstar IDs
 * 
 * Response format: pipe-delimited text rows
 * Each row: SecId|Name|Type|InternalType|Exchange|Currency|ISIN|Ticker|...
 */

const axios = require('axios');

class MorningstarClient {
    constructor() {
        // Use Italian Morningstar for best coverage of UCITS funds
        this.searchUrl = 'https://www.morningstar.it/it/util/SecuritySearch.ashx';
        console.log('[Morningstar] Initialized (SecuritySearch endpoint)');
    }

    /**
     * Search by ISIN using Morningstar autocomplete endpoint
     * Returns fund name, category, SecId, etc.
     * 
     * @param {string} isin - ISIN code (e.g. "LU1623762843")
     * @returns {object} { success, data: { name, secId, type, isin, exchange, currency } }
     */
    async searchByISIN(isin) {
        try {
            console.log(`[Morningstar] Searching ISIN: ${isin}`);

            const response = await axios.post(
                `${this.searchUrl}?source=nav&moduleId=6&ifIncludeAds=False&usrtType=v`,
                `q=${encodeURIComponent(isin)}&limit=25&timestamp=${Date.now()}&preferedList=`,
                {
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Referer': 'https://www.morningstar.it/',
                        'Origin': 'https://www.morningstar.it',
                        'X-Requested-With': 'XMLHttpRequest'
                    },
                    timeout: 10000
                }
            );

            const text = response.data;
            if (!text || typeof text !== 'string' || text.trim().length === 0) {
                console.log(`[Morningstar] Empty response for ${isin}`);
                return { success: false, data: null };
            }

            // Parse pipe-delimited response
            // Format varies but typically: rows separated by newline, fields by pipe
            // Row: i=SecId|Name|Type|SubType|ExchId|Ticker|Currency|ISIN|...
            const rows = text.split('\n').filter(r => r.trim().length > 0);
            
            if (rows.length === 0) {
                console.log(`[Morningstar] No results for ${isin}`);
                return { success: false, data: null };
            }

            // Find the row matching our ISIN
            let bestMatch = null;
            for (const row of rows) {
                const fields = row.split('|');
                // Look for ISIN in the fields
                const hasIsin = fields.some(f => f.trim().toUpperCase() === isin.toUpperCase());
                if (hasIsin && fields.length >= 3) {
                    bestMatch = fields;
                    break;
                }
            }

            // If no exact ISIN match, take first row (likely the best match from search)
            if (!bestMatch && rows.length > 0) {
                bestMatch = rows[0].split('|');
            }

            if (!bestMatch || bestMatch.length < 2) {
                console.log(`[Morningstar] Could not parse response for ${isin}`);
                return { success: false, data: null };
            }

            // Extract data from pipe-delimited fields
            // Typical format: SecId|Name|Type|SubType|ExchId|Ticker|Currency|ISIN|...
            // But positions may vary - use heuristics
            const data = this._parseSearchResult(bestMatch, isin);
            
            if (data && data.name) {
                console.log(`[Morningstar] Found: "${data.name}" (SecId: ${data.secId || 'N/A'})`);
                return { success: true, data };
            }

            console.log(`[Morningstar] Parsed but no name found for ${isin}`);
            return { success: false, data: null };

        } catch (error) {
            if (error.response?.status === 403 || error.response?.status === 429) {
                console.error(`[Morningstar] Rate limited or blocked: ${error.response.status}`);
            } else {
                console.error(`[Morningstar] Search error: ${error.message}`);
            }
            return { success: false, data: null, error: error.message };
        }
    }

    /**
     * Parse pipe-delimited search result row into structured data
     * Morningstar format is not strictly documented - use heuristics
     */
    _parseSearchResult(fields, isin) {
        // Clean fields
        const clean = fields.map(f => (f || '').trim());
        
        // Find the name field: longest string that contains spaces and isn't an ISIN/ID
        let name = null;
        let secId = null;
        let type = null;
        let currency = null;
        let exchange = null;

        for (const field of clean) {
            if (!field || field.length === 0) continue;
            
            // Detect ISIN (skip)
            if (/^[A-Z]{2}[A-Z0-9]{9,10}$/.test(field)) continue;
            
            // Detect Morningstar SecId (format: F00000XXXX or 0P0000XXXX)
            if (/^[A-Z0-9]{10}$/.test(field) || /^0P[A-Z0-9]{8}$/.test(field) || /^F[A-Z0-9]{9}$/.test(field)) {
                if (!secId) secId = field;
                continue;
            }
            
            // Detect currency (3 uppercase letters)
            if (/^[A-Z]{3}$/.test(field) && ['EUR', 'USD', 'GBP', 'CHF', 'JPY', 'GBX', 'SEK', 'NOK', 'DKK'].includes(field)) {
                currency = field;
                continue;
            }

            // Detect type keywords
            if (['FO', 'FE', 'ET', 'ST', 'CE', 'BO'].includes(field)) {
                type = this._mapType(field);
                continue;
            }

            // Name: contains spaces, longer than 5 chars
            if (field.includes(' ') && field.length > 5 && !name) {
                name = field;
            }
        }

        // Fallback: if no name found with spaces, try the second field (common position)
        if (!name && clean.length > 1 && clean[1].length > 3) {
            name = clean[1];
        }

        return {
            name,
            secId,
            type,
            currency,
            exchange,
            isin: isin.toUpperCase(),
            source: 'morningstar'
        };
    }

    /**
     * Map Morningstar type codes to readable types
     */
    _mapType(code) {
        const map = {
            'FO': 'Fund',           // Open-End Fund
            'FE': 'ETF',            // Exchange-Traded Fund
            'ET': 'ETF',
            'ST': 'Stock',
            'CE': 'Closed-End Fund',
            'BO': 'Bond'
        };
        return map[code] || code;
    }
}

module.exports = MorningstarClient;
