/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * Financial API Routes v2.1
 * 
 * Enhanced with Twelve Data for European markets
 */

const express = require('express');
const router = express.Router();
const DataAggregator = require('../dataAggregator');

// Initialize Data Aggregator with all API keys
const aggregator = new DataAggregator({
    finnhubKey: process.env.FINNHUB_API_KEY,
    alphavantageKey: process.env.ALPHA_VANTAGE_API_KEY,
    twelveDataKey: process.env.TWELVE_DATA_API_KEY
});

/**
 * Detect query type: ISIN, symbol, or company name
 */
function detectQueryType(query) {
    const cleaned = query.trim().toUpperCase();
    
    // ISIN: 12 characters, starts with 2 letters
    if (/^[A-Z]{2}[A-Z0-9]{10}$/.test(cleaned)) {
        return 'isin';
    }
    
    // Symbol with exchange: ENEL.MI, AAPL:NASDAQ
    if (/^[A-Z0-9]+[.:]([A-Z]+)$/.test(cleaned)) {
        return 'symbol';
    }
    
    // Simple symbol: Usually 1-5 letters
    if (/^[A-Z]{1,5}$/.test(cleaned)) {
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

        if (!result.success || result.results.length === 0) {
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
                error: result.error || 'Quote not found',
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
                error: result.error || 'Historical data not found',
                symbol: symbol,
                period: period
            });
        }

        res.json({
            success: true,
            symbol: result.symbol,
            data: result.data,
            source: result.source,
            period: period,
            currency: result.currency
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
// API USAGE STATS
// ===================================
router.get('/usage', async (req, res) => {
    try {
        const usage = aggregator.getUsageStats();
        res.json({
            success: true,
            usage: usage,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
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
            tests: {}
        };

        // Test Twelve Data (European)
        console.log('[TEST] Testing Twelve Data...');
        try {
            const twelveResult = await aggregator.twelvedata.getQuote('ENEL');
            testResults.tests.twelvedata = {
                status: twelveResult.success ? 'OK' : 'FAIL',
                symbol: 'ENEL',
                price: twelveResult.data?.price,
                currency: twelveResult.data?.currency,
                error: twelveResult.error
            };
        } catch (e) {
            testResults.tests.twelvedata = { status: 'ERROR', error: e.message };
        }

        // Test Yahoo Finance
        console.log('[TEST] Testing Yahoo Finance...');
        try {
            const yahooResult = await aggregator.yahoo.search('AAPL');
            testResults.tests.yahoo = {
                status: yahooResult.success ? 'OK' : 'FAIL',
                results: yahooResult.results?.length || 0
            };
        } catch (e) {
            testResults.tests.yahoo = { status: 'ERROR', error: e.message };
        }

        // Test Finnhub
        console.log('[TEST] Testing Finnhub...');
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
        console.log('[TEST] Testing Alpha Vantage...');
        try {
            const avResult = await aggregator.alphavantage.search('IBM');
            testResults.tests.alphavantage = {
                status: avResult.success ? 'OK' : 'FAIL',
                results: avResult.results?.length || 0
            };
        } catch (e) {
            testResults.tests.alphavantage = { status: 'ERROR', error: e.message };
        }

        // Add usage stats
        testResults.usage = aggregator.getUsageStats();

        res.json(testResults);

    } catch (error) {
        res.status(500).json({
            error: error.message
        });
    }
});

// ===================================
// TEST ITALIAN STOCKS
// ===================================
router.get('/test-italian', async (req, res) => {
    try {
        const italianSymbols = ['ENEL', 'ENI', 'ISP', 'UCG', 'RACE'];
        const results = {};

        for (const symbol of italianSymbols) {
            console.log(`[TEST] Testing Italian stock: ${symbol}`);
            
            try {
                const quote = await aggregator.getQuote(symbol);
                results[symbol] = {
                    success: quote.success,
                    price: quote.data?.price,
                    currency: quote.data?.currency,
                    change: quote.data?.change,
                    changePercent: quote.data?.changePercent,
                    source: quote.source,
                    error: quote.error
                };
            } catch (e) {
                results[symbol] = { success: false, error: e.message };
            }
        }

        res.json({
            success: true,
            timestamp: new Date().toISOString(),
            results: results,
            usage: aggregator.getUsageStats()
        });

    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

module.exports = router;
