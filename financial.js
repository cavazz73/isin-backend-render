/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * Financial API Routes
 * WITH INSTRUMENT DETAILS ENDPOINT (description, fundamentals, etc)
 */

const express = require('express');
const router = express.Router();
const DataAggregator = require('./dataAggregator');

// Initialize Data Aggregator
const aggregator = new DataAggregator({
    finnhubKey: process.env.FINNHUB_API_KEY,
    alphavantageKey: process.env.ALPHA_VANTAGE_API_KEY,
    fmpKey: process.env.FMP_API_KEY
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
// INSTRUMENT DETAILS (Complete with fundamentals)
// ===================================
router.get('/details/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        
        console.log(`[API] Details request: ${symbol}`);

        const result = await aggregator.getInstrumentDetails(symbol);

        if (!result.success) {
            return res.status(404).json({
                success: false,
                error: 'Details not found',
                symbol: symbol
            });
        }

        res.json({
            success: true,
            data: result.data,
            source: result.source,
            fromCache: result.fromCache || false
        });

    } catch (error) {
        console.error('[API] Details error:', error);
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
            tests: {}
        };

        // Test Yahoo Finance
        try {
            const yahooResult = await aggregator.yahoo.search('ENEL');
            testResults.tests.yahoo = {
                status: yahooResult.success ? 'OK' : 'FAIL',
                results: yahooResult.results?.length || 0
            };
        } catch (e) {
            testResults.tests.yahoo = { status: 'ERROR', error: e.message };
        }

        // Test TwelveData
        try {
            const twelveResult = await aggregator.twelvedata.search('AAPL');
            testResults.tests.twelvedata = {
                status: twelveResult.success ? 'OK' : 'FAIL',
                results: twelveResult.results?.length || 0
            };
        } catch (e) {
            testResults.tests.twelvedata = { status: 'ERROR', error: e.message };
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

        // Test FMP
        try {
            const fmpResult = await aggregator.fmp.search('TSLA');
            testResults.tests.fmp = {
                status: fmpResult.success ? 'OK' : 'FAIL',
                results: fmpResult.results?.length || 0
            };
        } catch (e) {
            testResults.tests.fmp = { status: 'ERROR', error: e.message };
        }

        res.json(testResults);

    } catch (error) {
        res.status(500).json({
            error: error.message
        });
    }
});

// ===================================
// CLEAR CACHE
// ===================================
router.delete('/cache', async (req, res) => {
    try {
        const result = await aggregator.clearCache();
        console.log('[API] Cache cleared');
        res.json({ success: true, message: 'Cache cleared', result });
    } catch (error) {
        console.error('[API] Cache clear error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

module.exports = router;
