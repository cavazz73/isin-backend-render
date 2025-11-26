/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Financial API Routes v2.1
 * Enhanced support for European/Italian stocks
 */

const express = require('express');
const router = express.Router();
const DataAggregator = require('./dataAggregator');

// Initialize Data Aggregator with all API keys
const aggregator = new DataAggregator({
    finnhubKey: process.env.FINNHUB_API_KEY,
    alphavantageKey: process.env.ALPHA_VANTAGE_API_KEY,
    marketstackKey: process.env.MARKETSTACK_API_KEY
});

console.log('[API] Data Aggregator initialized with sources: Yahoo, Marketstack, Finnhub, Alpha Vantage');

/**
 * Detect query type: ISIN, symbol, or company name
 */
function detectQueryType(query) {
    const cleaned = query.trim().toUpperCase();
    
    // ISIN: 12 characters, starts with 2 letters
    if (/^[A-Z]{2}[A-Z0-9]{10}$/.test(cleaned)) {
        return 'isin';
    }
    
    // Symbol: Usually 1-5 letters, may include .
    if (/^[A-Z]{1,5}(\.[A-Z]{1,2})?$/.test(cleaned)) {
        return 'symbol';
    }
    
    // Otherwise assume company name
    return 'name';
}

// ===================================
// UNIFIED SEARCH
// ===================================
router.get('/search', async (req, res) => {
    try {
        const { q } = req.query;
        
        if (!q) {
            return res.status(400).json({
                success: false,
                error: 'Query parameter "q" is required'
            });
        }

        console.log(`[API] Search request: "${q}"`);

        // Detect query type
        const queryType = detectQueryType(q);
        console.log(`[API] Query type detected: ${queryType}`);

        let result;

        if (queryType === 'isin') {
            result = await aggregator.searchByISIN(q);
        } else {
            result = await aggregator.search(q);
        }

        if (!result.success) {
            return res.status(404).json({
                success: false,
                error: 'No results found',
                query: q,
                queryType: queryType
            });
        }

        res.json({
            success: true,
            results: result.results,
            metadata: {
                ...result.metadata,
                query: q,
                queryType: queryType
            }
        });

    } catch (error) {
        console.error('[API] Search error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// REAL-TIME QUOTE
// ===================================
router.get('/quote/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        
        console.log(`[API] Quote request: ${symbol}`);

        const result = await aggregator.getQuote(symbol);

        if (!result.success) {
            return res.status(404).json({
                success: false,
                error: 'Quote not found',
                symbol: symbol
            });
        }

        res.json({
            success: true,
            data: result.data,
            source: result.source
        });

    } catch (error) {
        console.error('[API] Quote error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// HISTORICAL DATA
// ===================================
router.get('/historical/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        const { period = '1M' } = req.query;

        console.log(`[API] Historical data request: ${symbol}, period: ${period}`);

        const result = await aggregator.getHistoricalData(symbol, period);

        if (!result.success) {
            return res.status(404).json({
                success: false,
                error: 'Historical data not found',
                symbol: symbol,
                period: period
            });
        }

        res.json({
            success: true,
            symbol: result.symbol,
            data: result.data,
            source: result.source,
            period: period
        });

    } catch (error) {
        console.error('[API] Historical data error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// TEST ENDPOINT
// ===================================
router.get('/test', async (req, res) => {
    try {
        const testResults = {
            timestamp: new Date().toISOString(),
            tests: {},
            apiUsage: aggregator.getUsageStats()
        };

        // Test Yahoo Finance
        try {
            const yahooResult = await aggregator.yahoo.search('ENEL');
            testResults.tests.yahoo = {
                status: yahooResult.success ? 'OK' : 'FAIL',
                results: yahooResult.results?.length || 0,
                firstResult: yahooResult.results?.[0]?.symbol || null
            };
        } catch (e) {
            testResults.tests.yahoo = { status: 'ERROR', error: e.message };
        }

        // Test Marketstack (if key configured)
        try {
            if (process.env.MARKETSTACK_API_KEY) {
                const msResult = await aggregator.marketstack.search('ENEL');
                testResults.tests.marketstack = {
                    status: msResult.success ? 'OK' : 'FAIL',
                    results: msResult.results?.length || 0,
                    firstResult: msResult.results?.[0]?.symbol || null
                };
            } else {
                testResults.tests.marketstack = { status: 'SKIPPED', reason: 'No API key' };
            }
        } catch (e) {
            testResults.tests.marketstack = { status: 'ERROR', error: e.message };
        }

        // Test Finnhub
        try {
            const finnhubResult = await aggregator.finnhub.search('AAPL');
            testResults.tests.finnhub = {
                status: finnhubResult.success ? 'OK' : 'FAIL',
                results: finnhubResult.results?.length || 0
            };
        } catch (e) {
            testResults.tests.finnhub = { status: 'ERROR', error: e.message };
        }

        // Test Alpha Vantage
        try {
            const avResult = await aggregator.alphavantage.search('IBM');
            testResults.tests.alphavantage = {
                status: avResult.success ? 'OK' : 'FAIL',
                results: avResult.results?.length || 0
            };
        } catch (e) {
            testResults.tests.alphavantage = { status: 'ERROR', error: e.message };
        }

        // Test Italian stock quote (ENEL.MI)
        try {
            const quoteResult = await aggregator.getQuote('ENEL.MI');
            testResults.tests.italianQuote = {
                status: quoteResult.success ? 'OK' : 'FAIL',
                symbol: quoteResult.data?.symbol || 'N/A',
                price: quoteResult.data?.price || null,
                currency: quoteResult.data?.currency || 'N/A',
                source: quoteResult.source || 'N/A'
            };
        } catch (e) {
            testResults.tests.italianQuote = { status: 'ERROR', error: e.message };
        }

        res.json(testResults);

    } catch (error) {
        res.status(500).json({
            error: error.message
        });
    }
});

// ===================================
// API STATS ENDPOINT
// ===================================
router.get('/stats', (req, res) => {
    res.json({
        success: true,
        usage: aggregator.getUsageStats(),
        timestamp: new Date().toISOString()
    });
});

module.exports = router;
