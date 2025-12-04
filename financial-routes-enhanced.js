/**
 * ============================================================================
 * ISIN Research & Compare - Enhanced Financial API Routes
 * Copyright (c) 2025 Mutna S.R.L.S. - All Rights Reserved
 * ============================================================================
 */

const express = require('express');
const router = express.Router();
const EnhancedDataAggregator = require('./dataAggregator-enhanced');

// Initialize enhanced aggregator
const aggregator = new EnhancedDataAggregator({
    finnhubKey: process.env.FINNHUB_API_KEY,
    alphavantageKey: process.env.ALPHAVANTAGE_API_KEY
});

/**
 * GET /api/financial/enhanced-quote/:symbol
 * Get comprehensive quote with all metrics
 */
router.get('/enhanced-quote/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        
        console.log(`[API] Enhanced quote request for: ${symbol}`);
        
        const result = await aggregator.getEnhancedQuote(symbol);
        
        if (result.success) {
            res.json({
                success: true,
                data: result.data,
                source: result.source,
                timestamp: result.timestamp
            });
        } else {
            res.status(404).json({
                success: false,
                error: 'Instrument not found or data unavailable',
                symbol: symbol
            });
        }
    } catch (error) {
        console.error('[API] Enhanced quote error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

/**
 * GET /api/financial/search-enhanced?q=query
 * Search with enhanced results (top 5)
 */
router.get('/search-enhanced', async (req, res) => {
    try {
        const { q } = req.query;
        
        if (!q) {
            return res.status(400).json({
                success: false,
                error: 'Query parameter "q" is required'
            });
        }
        
        console.log(`[API] Enhanced search request for: ${q}`);
        
        const result = await aggregator.searchEnhanced(q);
        
        res.json(result);
    } catch (error) {
        console.error('[API] Enhanced search error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

/**
 * GET /api/financial/quote/:symbol
 * Standard quote (existing - mantiene compatibilità)
 */
router.get('/quote/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        
        const result = await aggregator.getQuote(symbol);
        
        if (result.success) {
            res.json(result);
        } else {
            res.status(404).json({
                success: false,
                error: 'Quote not found',
                symbol: symbol
            });
        }
    } catch (error) {
        console.error('[API] Quote error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error'
        });
    }
});

/**
 * GET /api/financial/search?q=query
 * Standard search (existing)
 */
router.get('/search', async (req, res) => {
    try {
        const { q } = req.query;
        
        if (!q) {
            return res.status(400).json({
                success: false,
                error: 'Query parameter "q" is required'
            });
        }
        
        const result = await aggregator.search(q);
        res.json(result);
    } catch (error) {
        console.error('[API] Search error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error'
        });
    }
});

/**
 * GET /api/financial/historical/:symbol
 * Historical data
 */
router.get('/historical/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        const { period = '1M' } = req.query;
        
        const result = await aggregator.getHistoricalData(symbol, period);
        
        if (result.success) {
            res.json(result);
        } else {
            res.status(404).json({
                success: false,
                error: 'Historical data not found'
            });
        }
    } catch (error) {
        console.error('[API] Historical data error:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error'
        });
    }
});

/**
 * GET /api/health
 * Health check
 */
router.get('/health', async (req, res) => {
    try {
        const health = await aggregator.healthCheck();
        res.json(health);
    } catch (error) {
        res.status(500).json({
            status: 'error',
            error: error.message
        });
    }
});

module.exports = router;
