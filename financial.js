/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * Financial API Routes V3.0
 */

const express = require('express');
const router = express.Router();
const DataAggregator = require('./dataAggregator'); // ← FLAT STRUCTURE

// Initialize Data Aggregator V3
const aggregator = new DataAggregator({
    twelveDataKey: process.env.TWELVE_DATA_API_KEY,
    finnhubKey: process.env.FINNHUB_API_KEY,
    alphavantageKey: process.env.ALPHA_VANTAGE_API_KEY
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
// UNIFIED SEARCH V3.0
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

        console.log(`[API v3.0] Search request: "${q}"`);

        // Detect query type
        const queryType = detectQueryType(q);
        console.log(`[API v3.0] Query type: ${queryType}`);

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
                queryType: queryType,
                version: '3.0.0'
            });
        }

        res.json({
            success: true,
            results: result.results,
            metadata: {
                ...result.metadata,
                query: q,
                queryType: queryType,
                version: '3.0.0'
            }
        });

    } catch (error) {
        console.error('[API v3.0] Search error:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            version: '3.0.0'
        });
    }
});

// ===================================
// REAL-TIME QUOTE V3.0
// ===================================
router.get('/quote/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        
        console.log(`[API v3.0] Quote request: ${symbol}`);

        const result = await aggregator.getQuote(symbol);

        if (!result.success) {
            return res.status(404).json({
                success: false,
                error: 'Quote not found',
                symbol: symbol,
                version: '3.0.0'
            });
        }

        res.json({
            success: true,
            data: result.data,
            source: result.source,
            version: '3.0.0'
        });

    } catch (error) {
        console.error('[API v3.0] Quote error:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            version: '3.0.0'
        });
    }
});

// ===================================
// HISTORICAL DATA V3.0
// ===================================
router.get('/historical/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        const { period = '1M' } = req.query;

        console.log(`[API v3.0] Historical request: ${symbol}, period: ${period}`);

        const result = await aggregator.getHistoricalData(symbol, period);

        if (!result.success) {
            return res.status(404).json({
                success: false,
                error: 'Historical data not found',
                symbol: symbol,
                period: period,
                version: '3.0.0'
            });
        }

        res.json({
            success: true,
            symbol: result.symbol,
            data: result.data,
            source: result.source,
            period: period,
            version: '3.0.0'
        });

    } catch (error) {
        console.error('[API v3.0] Historical error:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            version: '3.0.0'
        });
    }
});

// ===================================
// TWELVEDATA USAGE STATISTICS
// ===================================
router.get('/usage', async (req, res) => {
    try {
        const usageStats = aggregator.twelvedata.getUsageStats();
        
        res.json({
            success: true,
            usage: usageStats,
            version: '3.0.0',
            timestamp: new Date().toISOString()
        });

    } catch (error) {
        console.error('[API v3.0] Usage error:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            version: '3.0.0'
        });
    }
});

// ===================================
// HEALTH CHECK V3.0
// ===================================
router.get('/test', async (req, res) => {
    try {
        const health = await aggregator.healthCheck();
        
        res.json({
            success: true,
            health: health,
            version: '3.0.0'
        });

    } catch (error) {
        console.error('[API v3.0] Health check error:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            version: '3.0.0'
        });
    }
});

module.exports = router;
