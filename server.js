/**
 * ISIN Research API v2.0 - Complete Server
 * Copyright (c) 2024-2025 Mutna S.R.L.S.
 * P.IVA: 04219740364
 */

const express = require('express');
const cors = require('cors');
const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Request logging
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
    next();
});

// Root endpoint
app.get('/', (req, res) => {
    res.json({
        success: true,
        message: 'ISIN Research API v2.0 - Multi-Source Edition',
        company: 'Mutna S.R.L.S.',
        piva: '04219740364',
        version: '2.0.0',
        status: 'operational',
        timestamp: new Date().toISOString(),
        endpoints: {
            search: '/api/financial/search?q={query}',
            quote: '/api/financial/quote/{symbol}',
            historical: '/api/financial/historical/{symbol}?period={period}',
            stats: '/api/financial/stats',
            test: '/api/financial/test',
            health: '/health'
        },
        dataSources: {
            primary: 'Yahoo Finance (unlimited)',
            backup: 'Finnhub (60 calls/min)'
        }
    });
});

// Health check
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        uptime: process.uptime(),
        timestamp: new Date().toISOString(),
        memory: process.memoryUsage()
    });
});

// Financial API routes
try {
    const financialRoutes = require('./routes/financial');
    app.use('/api/financial', financialRoutes);
    console.log('✅ Financial routes loaded');
} catch (error) {
    console.error('❌ Error loading financial routes:', error.message);
    
    // Fallback route
    app.use('/api/financial', (req, res) => {
        res.status(503).json({
            success: false,
            error: 'Financial services temporarily unavailable',
            message: 'Please try again in a moment'
        });
    });
}

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        success: false,
        error: 'Endpoint not found',
        path: req.path,
        availableEndpoints: [
            'GET /',
            'GET /health',
            'GET /api/financial/search?q={query}',
            'GET /api/financial/quote/{symbol}',
            'GET /api/financial/historical/{symbol}?period={period}',
            'GET /api/financial/stats',
            'GET /api/financial/test'
        ]
    });
});

// Global error handler
app.use((err, req, res, next) => {
    console.error('Global error:', err);
    res.status(500).json({
        success: false,
        error: 'Internal server error'
    });
});

// Start server
const PORT = process.env.PORT || 10000;

app.listen(PORT, '0.0.0.0', () => {
    console.log('='.repeat(60));
    console.log('🚀 ISIN Research API v2.0');
    console.log('='.repeat(60));
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`🌍 Environment: ${process.env.NODE_ENV || 'production'}`);
    console.log('📊 Data Sources: Yahoo Finance, Finnhub');
    console.log('='.repeat(60));
    console.log('📡 Endpoints available:');
    console.log('   GET  /');
    console.log('   GET  /health');
    console.log('   GET  /api/financial/search?q={query}');
    console.log('   GET  /api/financial/quote/{symbol}');
    console.log('   GET  /api/financial/historical/{symbol}');
    console.log('   GET  /api/financial/stats');
    console.log('   GET  /api/financial/test');
    console.log('='.repeat(60));
});

module.exports = app;
