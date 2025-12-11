/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend - Multi-Source Financial Data API V4.0
 * WITH REDIS CACHE
 */

const express = require('express');
const cors = require('cors');
const DataAggregatorV4 = require('./dataAggregator-v4');

const app = express();
const PORT = process.env.PORT || 3000;

// Initialize Data Aggregator with Redis
const aggregator = new DataAggregatorV4({
    redisUrl: process.env.REDIS_URL,
    twelveDataKey: process.env.TWELVE_DATA_API_KEY,
    finnhubKey: process.env.FINNHUB_API_KEY,
    alphavantageKey: process.env.ALPHA_VANTAGE_API_KEY
});

// Middleware
app.use(cors({
    origin: process.env.ALLOWED_ORIGINS?.split(',') || '*',
    credentials: true
}));
app.use(express.json());

// Request logging
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
    next();
});

// ==================== ENDPOINTS ====================

/**
 * Health Check + Redis Status
 */
app.get('/health', async (req, res) => {
    try {
        const health = await aggregator.healthCheck();
        res.json({
            status: 'ok',
            timestamp: new Date().toISOString(),
            uptime: process.uptime(),
            version: '4.0.0',
            ...health
        });
    } catch (error) {
        res.status(500).json({
            status: 'error',
            error: error.message
        });
    }
});

/**
 * NEW Search endpoint (v4.0)
 * GET /api/search?query=AAPL
 * GET /api/search?q=AAPL (alias)
 */
app.get('/api/search', async (req, res) => {
    try {
        // Accept both 'query' and 'q' parameters
        const query = req.query.query || req.query.q;
        
        if (!query || query.trim().length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Query parameter is required (use ?query=AAPL or ?q=AAPL)'
            });
        }

        const results = await aggregator.search(query);
        res.json(results);
        
    } catch (error) {
        console.error('Search error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * OLD Search endpoint (v3.0 retrocompatibility)
 * GET /api/financial/search?query=AAPL
 * GET /api/financial/search?q=AAPL (alias)
 */
app.get('/api/financial/search', async (req, res) => {
    try {
        // Accept both 'query' and 'q' parameters
        const query = req.query.query || req.query.q;
        
        if (!query || query.trim().length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Query parameter is required (use ?query=AAPL or ?q=AAPL)'
            });
        }

        console.log('[RETROCOMPAT] Using old endpoint /api/financial/search');
        const results = await aggregator.search(query);
        res.json(results);
        
    } catch (error) {
        console.error('Search error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * Search by ISIN
 * GET /api/isin/:isin
 */
app.get('/api/isin/:isin', async (req, res) => {
    try {
        const { isin } = req.params;
        
        // Validate ISIN format (12 alphanumeric characters)
        if (!/^[A-Z]{2}[A-Z0-9]{10}$/i.test(isin)) {
            return res.status(400).json({
                success: false,
                error: 'Invalid ISIN format. Must be 12 alphanumeric characters (e.g. US0378331005)'
            });
        }

        const results = await aggregator.searchByISIN(isin.toUpperCase());
        res.json(results);
        
    } catch (error) {
        console.error('ISIN search error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * Get Quote
 * GET /api/quote/:symbol
 */
app.get('/api/quote/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        
        if (!symbol || symbol.trim().length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Symbol parameter is required'
            });
        }

        const quote = await aggregator.getQuote(symbol);
        res.json(quote);
        
    } catch (error) {
        console.error('Quote error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * Get Historical Data
 * GET /api/historical/:symbol?period=1M
 */
app.get('/api/historical/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        const { period = '1M' } = req.query;
        
        if (!symbol || symbol.trim().length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Symbol parameter is required'
            });
        }

        const data = await aggregator.getHistoricalData(symbol, period);
        res.json(data);
        
    } catch (error) {
        console.error('Historical data error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * GET CACHE STATISTICS
 * GET /api/cache/stats
 */
app.get('/api/cache/stats', async (req, res) => {
    try {
        const stats = await aggregator.getCacheStats();
        res.json({
            success: true,
            cache: stats,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('Cache stats error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * CLEAR CACHE (USE WITH CAUTION!)
 * DELETE /api/cache
 */
app.delete('/api/cache', async (req, res) => {
    try {
        const cleared = await aggregator.clearCache();
        res.json({
            success: cleared,
            message: cleared ? 'Cache cleared successfully' : 'Cache clear failed',
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('Cache clear error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ==================== ERROR HANDLERS ====================

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        success: false,
        error: 'Endpoint not found',
        requestedPath: req.path,
        availableEndpoints: [
            'GET /health',
            'GET /api/search?query=AAPL (or ?q=AAPL)',
            'GET /api/financial/search?query=AAPL (or ?q=AAPL)',
            'GET /api/isin/:isin',
            'GET /api/quote/:symbol',
            'GET /api/historical/:symbol',
            'GET /api/cache/stats',
            'DELETE /api/cache'
        ]
    });
});

// Error handler
app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    res.status(500).json({
        success: false,
        error: 'Internal server error',
        message: process.env.NODE_ENV === 'development' ? err.message : 'Something went wrong'
    });
});

// ==================== START SERVER ====================

app.listen(PORT, () => {
    console.log('='.repeat(70));
    console.log('🚀 ISIN Research Backend V4.0 - WITH REDIS CACHE');
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S. (P.IVA: 04219740364)');
    console.log('='.repeat(70));
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`📍 Health check: http://localhost:${PORT}/health`);
    console.log(`🔍 Search (NEW): http://localhost:${PORT}/api/search?q=AAPL`);
    console.log(`🔍 Search (OLD): http://localhost:${PORT}/api/financial/search?q=AAPL`);
    console.log(`📊 Cache stats: http://localhost:${PORT}/api/cache/stats`);
    console.log('='.repeat(70));
    console.log('📦 Data Sources (Priority Order):');
    console.log('  1️⃣  TwelveData (Primary - 800 req/day - Best for EU)');
    console.log('  2️⃣  Yahoo Finance (Fallback - Unlimited)');
    console.log('  3️⃣  Finnhub (Fallback - 60 req/min)');
    console.log('  4️⃣  Alpha Vantage (Fallback - 25 req/day)');
    console.log('='.repeat(70));
    console.log('⚡ Redis Cache:');
    console.log(`  • Host: capital-swan-9164.upstash.io`);
    console.log(`  • TTL: 5min (prices) | 1h (metrics) | 7d (logos)`);
    console.log(`  • Expected hit rate: 70-80%`);
    console.log('='.repeat(70));
});

module.exports = app;
