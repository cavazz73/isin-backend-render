/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend v3.0 - Multi-Source Financial Data API
 * Primary: TwelveData (European Markets)
 * Fallback: Yahoo Finance, Finnhub, Alpha Vantage
 */

// Load environment variables
require('dotenv').config();

const express = require('express');
const cors = require('cors');
const financialRoutes = require('./financial');  // ✅ FLAT STRUCTURE (no ./routes/)

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Request logging
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
    next();
});

// Health check
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        version: '3.0.0',
        environment: process.env.NODE_ENV || 'production'
    });
});

// API Routes
app.use('/api/financial', financialRoutes);

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        success: false,
        error: 'Endpoint not found',
        availableEndpoints: [
            '/health',
            '/api/financial/search?q=QUERY',
            '/api/financial/quote/:symbol',
            '/api/financial/historical/:symbol?period=1M',
            '/api/financial/usage',
            '/api/financial/test'
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

// Start server
app.listen(PORT, () => {
    console.log('='.repeat(70));
    console.log('🚀 ISIN RESEARCH BACKEND v3.0 - STARTED');
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S. - P.IVA: 04219740364');
    console.log('='.repeat(70));
    console.log(`📡 Server running on port ${PORT}`);
    console.log(`🔍 Health check: http://localhost:${PORT}/health`);
    console.log(`📊 API endpoint: http://localhost:${PORT}/api/financial/search`);
    console.log('='.repeat(70));
    console.log('📈 DATA SOURCES (Priority Order):');
    console.log('  1. TwelveData    → PRIMARY for European markets (800 req/day)');
    console.log('  2. Yahoo Finance → Fallback for US markets (Unlimited)');
    console.log('  3. Finnhub       → Backup (60 req/min)');
    console.log('  4. Alpha Vantage → Last resort (25 req/day)');
    console.log('='.repeat(70));
    console.log('🇮🇹 Italian stocks (ENEL, ENI) → TwelveData → EUR pricing ✅');
    console.log('🇺🇸 US stocks (AAPL, MSFT) → Yahoo Finance → USD pricing ✅');
    console.log('='.repeat(70));
});

module.exports = app;
