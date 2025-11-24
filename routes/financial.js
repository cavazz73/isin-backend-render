/**
 * Financial API Routes
 * Copyright (c) 2024-2025 Mutna S.R.L.S.
 */

const express = require('express');
const router = express.Router();
const dataAggregator = require('../services/dataAggregator');

// Search endpoint
router.get('/search', async (req, res) => {
    try {
        const { q } = req.query;

        if (!q || q.trim().length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Query parameter "q" is required'
            });
        }

        console.log(`[API] Search: "${q}"`);
        const result = await dataAggregator.search(q.trim());

        res.json({
            success: true,
            query: q,
            ...result
        });

    } catch (error) {
        console.error('[API] Search error:', error);
        
        let errorData;
        try {
            errorData = JSON.parse(error.message);
        } catch {
            errorData = { message: error.message };
        }

        res.status(500).json({
            success: false,
            error: errorData.message || 'Search failed',
            details: errorData.errors || []
        });
    }
});

// Quote endpoint
router.get('/quote/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;

        if (!symbol || symbol.trim().length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Symbol parameter is required'
            });
        }

        console.log(`[API] Quote: ${symbol}`);
        const result = await dataAggregator.getQuote(symbol.trim().toUpperCase());

        res.json({
            success: true,
            symbol,
            ...result
        });

    } catch (error) {
        console.error('[API] Quote error:', error);
        
        let errorData;
        try {
            errorData = JSON.parse(error.message);
        } catch {
            errorData = { message: error.message };
        }

        res.status(500).json({
            success: false,
            error: errorData.message || 'Quote fetch failed',
            details: errorData.errors || []
        });
    }
});

// Historical data endpoint
router.get('/historical/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        const { period = '1M' } = req.query;

        if (!symbol || symbol.trim().length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Symbol parameter is required'
            });
        }

        const validPeriods = ['1D', '1W', '1M', '3M', '6M', 'YTD', '1Y', '3Y', '5Y', 'MAX'];
        if (!validPeriods.includes(period)) {
            return res.status(400).json({
                success: false,
                error: `Invalid period. Valid: ${validPeriods.join(', ')}`
            });
        }

        console.log(`[API] Historical: ${symbol}, period: ${period}`);
        const result = await dataAggregator.getHistoricalData(symbol.trim().toUpperCase(), period);

        res.json({
            success: true,
            symbol,
            period,
            ...result
        });

    } catch (error) {
        console.error('[API] Historical error:', error);
        
        let errorData;
        try {
            errorData = JSON.parse(error.message);
        } catch {
            errorData = { message: error.message };
        }

        res.status(500).json({
            success: false,
            error: errorData.message || 'Historical data fetch failed',
            details: errorData.errors || []
        });
    }
});

// Stats endpoint
router.get('/stats', (req, res) => {
    try {
        const stats = dataAggregator.getStats();
        
        res.json({
            success: true,
            stats,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('[API] Stats error:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch stats'
        });
    }
});

// Test endpoint
router.get('/test', async (req, res) => {
    try {
        console.log('[API] Testing all sources...');
        const results = await dataAggregator.testAllSources();
        
        res.json({
            success: true,
            results,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('[API] Test error:', error);
        res.status(500).json({
            success: false,
            error: 'Test failed'
        });
    }
});

// Reset stats
router.post('/stats/reset', (req, res) => {
    try {
        dataAggregator.resetStats();
        res.json({
            success: true,
            message: 'Stats reset successfully'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: 'Failed to reset stats'
        });
    }
});

// Clear cache
router.post('/cache/clear', (req, res) => {
    try {
        dataAggregator.clearAllCaches();
        res.json({
            success: true,
            message: 'Cache cleared successfully'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: 'Failed to clear cache'
        });
    }
});

module.exports = router;
