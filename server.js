/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend V4.2 - REDIS + BONDS + FULL RETROCOMPATIBILITY
 */

const express = require('express');
const cors = require('cors');
const DataAggregatorV4 = require('./dataAggregator-v4');
const bondsRouter = require('./bondsRouter');

const app = express();
const PORT = process.env.PORT || 10000;

// Initialize Data Aggregator V4 with Redis
const aggregator = new DataAggregatorV4({
    redisUrl: process.env.REDIS_URL,
    twelveDataKey: process.env.TWELVE_DATA_API_KEY,
    finnhubKey: process.env.FINNHUB_API_KEY,
    alphavantageKey: process.env.ALPHA_VANTAGE_API_KEY
});

// Middleware
app.use(cors());
app.use(express.json());

// Request logging
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
    next();
});

// ============================================================================
// HEALTH CHECK
// ============================================================================

app.get('/health', async (req, res) => {
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

app.get('/', (req, res) => {
    res.json({
        name: 'ISIN Research Backend',
        version: '4.2.0-BONDS',
        status: 'operational',
        features: [
            'Multi-source stock data aggregation',
            'Redis caching (70-80% hit rate)',
            'End-of-day bonds data',
            'Full retrocompatibility with v3.0'
        ],
        endpoints: {
            stocks: {
                search: ['/api/search?query=AAPL', '/api/financial/search?q=AAPL'],
                quote: ['/api/quote/:symbol', '/api/financial/details/:symbol'],
                historical: ['/api/historical/:symbol', '/api/financial/historical/:symbol']
            },
            bonds: {
                categories: '/api/bonds/categories',
                byCategory: '/api/bonds/:category?limit=100',
                search: '/api/bonds/search/:isin',
                stats: '/api/bonds/stats'
            },
            cache: '/api/cache/stats',
            health: '/health'
        }
    });
});

// ============================================================================
// V4.0 ENDPOINTS (NEW)
// ============================================================================

// Search stocks
app.get('/api/search', async (req, res) => {
    try {
        const query = req.query.query || req.query.q;
        
        if (!query) {
            return res.status(400).json({
                success: false,
                error: 'Query parameter required (?query=AAPL or ?q=AAPL)'
            });
        }

        const results = await aggregator.search(query);
        res.json(results);
    } catch (error) {
        console.error('[API] Search error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get quote for symbol
app.get('/api/quote/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        const quote = await aggregator.getQuote(symbol);
        
        if (!quote.success) {
            return res.status(404).json(quote);
        }
        
        res.json(quote);
    } catch (error) {
        console.error('[API] Quote error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get historical data
app.get('/api/historical/:symbol', async (req, res) => {
    try {
        const { symbol } = req.params;
        const period = req.query.period || '1M';
        
        const historical = await aggregator.getHistoricalData(symbol, period);
        
        if (!historical.success) {
            return res.status(404).json(historical);
        }
        
        res.json(historical);
    } catch (error) {
        console.error('[API] Historical error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Cache statistics
app.get('/api/cache/stats', async (req, res) => {
    try {
        const stats = await aggregator.getCacheStats();
        res.json({
            success: true,
            cache: stats
        });
    } catch (error) {
        console.error('[API] Cache stats error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ============================================================================
// V3.0 RETROCOMPATIBILITY ENDPOINTS (OLD)
// ============================================================================

app.get('/api/financial/search', async (req, res) => {
    console.log('[RETROCOMPAT] /api/financial/search');
    try {
        const query = req.query.q || req.query.query;
        
        if (!query) {
            return res.status(400).json({
                success: false,
                error: 'Query parameter required (?q=AAPL or ?query=AAPL)'
            });
        }

        const results = await aggregator.search(query);
        res.json(results);
    } catch (error) {
        console.error('[RETROCOMPAT] Search error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

app.get('/api/financial/details/:symbol', async (req, res) => {
    console.log('[RETROCOMPAT] /api/financial/details');
    try {
        const { symbol } = req.params;
        const quote = await aggregator.getQuote(symbol);
        
        if (!quote.success) {
            return res.status(404).json(quote);
        }
        
        res.json(quote);
    } catch (error) {
        console.error('[RETROCOMPAT] Details error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

app.get('/api/financial/historical/:symbol', async (req, res) => {
    console.log('[RETROCOMPAT] /api/financial/historical');
    try {
        const { symbol } = req.params;
        const period = req.query.period || '1M';
        
        const historical = await aggregator.getHistoricalData(symbol, period);
        
        if (!historical.success) {
            return res.status(404).json(historical);
        }
        
        res.json(historical);
    } catch (error) {
        console.error('[RETROCOMPAT] Historical error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ============================================================================
// BONDS ENDPOINTS
// ============================================================================

app.use('/api/bonds', bondsRouter);

// ============================================================================
// ERROR HANDLERS
// ============================================================================

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        success: false,
        error: 'Endpoint not found',
        availableEndpoints: [
            '/health',
            '/api/search?query=AAPL',
            '/api/quote/:symbol',
            '/api/historical/:symbol',
            '/api/bonds/categories',
            '/api/cache/stats'
        ]
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

// ============================================================================
// START SERVER
// ============================================================================

app.listen(PORT, () => {
    console.log('======================================================================');
    console.log('🚀 ISIN Research Backend V4.2 - REDIS + BONDS + RETROCOMPATIBILITY');
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S. (P.IVA: 04219740364)');
    console.log('======================================================================');
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`🏥 Health: /health`);
    console.log(`🔍 Search: /api/search?q=AAPL OR /api/financial/search?q=AAPL`);
    console.log(`📊 Details: /api/quote/:symbol OR /api/financial/details/:symbol`);
    console.log(`📈 Historical: /api/historical/:symbol OR /api/financial/historical/:symbol`);
    console.log(`💾 Cache: /api/cache/stats`);
    console.log(`📋 Bonds Categories: /api/bonds/categories`);
    console.log(`🔗 Bonds by Category: /api/bonds/:category?limit=100`);
    console.log('======================================================================');
    console.log('📦 Data Sources: TwelveData → Yahoo → Finnhub → AlphaVantage');
    console.log('⚡ Redis Cache: 70-80% hit rate expected');
    console.log('📊 Bonds: End-of-day data (updated daily at 18:30 UTC)');
    console.log('======================================================================');
});

module.exports = app;
