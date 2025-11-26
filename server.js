/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend - v2.3 Yahoo Finance Optimized
 * - Rate limiting
 * - Caching
 * - No more parallel quote spam
 */

const express = require('express');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3001;

// Initialize Data Aggregator
const DataAggregator = require('./dataAggregator');
const aggregator = new DataAggregator();

// Middleware
app.use(cors());
app.use(express.json());

// Request logging
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
    next();
});

// ===================================
// HEALTH CHECK
// ===================================
app.get('/health', async (req, res) => {
    try {
        const health = await aggregator.healthCheck();
        res.json(health);
    } catch (error) {
        res.json({
            status: 'error',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// ===================================
// UNIFIED SEARCH
// ===================================
app.get('/api/financial/search', async (req, res) => {
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

        if (!result.success || !result.results?.length) {
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
app.get('/api/financial/quote/:symbol', async (req, res) => {
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
            source: result.source || 'yahoo'
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
app.get('/api/financial/historical/:symbol', async (req, res) => {
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
            source: result.source || 'yahoo',
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
// CACHE STATS (Debug)
// ===================================
app.get('/api/debug/cache', (req, res) => {
    res.json(aggregator.getCacheStats());
});

// ===================================
// TEST ENDPOINT
// ===================================
app.get('/api/financial/test', async (req, res) => {
    try {
        const testResults = {
            version: '2.3',
            timestamp: new Date().toISOString(),
            tests: {}
        };

        // Test search
        console.log('[Test] Testing search...');
        const searchResult = await aggregator.search('AAPL');
        testResults.tests.search = {
            status: searchResult.success ? 'OK' : 'FAIL',
            results: searchResult.results?.length || 0
        };

        // Test quote
        console.log('[Test] Testing quote...');
        const quoteResult = await aggregator.getQuote('AAPL');
        testResults.tests.quote = {
            status: quoteResult.success ? 'OK' : 'FAIL',
            price: quoteResult.data?.price || null
        };

        // Test historical
        console.log('[Test] Testing historical...');
        const histResult = await aggregator.getHistoricalData('AAPL', '1M');
        testResults.tests.historical = {
            status: histResult.success ? 'OK' : 'FAIL',
            dataPoints: histResult.data?.length || 0
        };

        // Test Italian stock
        console.log('[Test] Testing Italian stock (ENEL)...');
        const italianResult = await aggregator.search('ENEL');
        testResults.tests.italian = {
            status: italianResult.success ? 'OK' : 'FAIL',
            results: italianResult.results?.length || 0,
            firstSymbol: italianResult.results?.[0]?.symbol || null
        };

        // Cache stats
        testResults.cache = aggregator.getCacheStats();

        res.json(testResults);

    } catch (error) {
        res.status(500).json({
            error: error.message
        });
    }
});

// ===================================
// HELPER FUNCTIONS
// ===================================
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
    
    return 'name';
}

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        success: false,
        error: 'Endpoint not found'
    });
});

// Error handler
app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    res.status(500).json({
        success: false,
        error: 'Internal server error',
        message: err.message
    });
});

// Start server
app.listen(PORT, () => {
    console.log('='.repeat(60));
    console.log('ISIN Research Backend v2.3 - Yahoo Finance Optimized');
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S.');
    console.log('='.repeat(60));
    console.log(`Server running on port ${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/health`);
    console.log(`API endpoint: http://localhost:${PORT}/api/financial/search`);
    console.log('='.repeat(60));
    console.log('Optimizations:');
    console.log('  ✓ Rate limiting (200ms between requests)');
    console.log('  ✓ Caching (5 min TTL)');
    console.log('  ✓ Retry with exponential backoff');
    console.log('  ✓ Single quote per search (not all results)');
    console.log('='.repeat(60));
});

module.exports = app;
