/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend - Multi-Source Financial Data API
 * Version: 2.3.0 - Yahoo Finance Primary with Caching
 */

const express = require('express');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Request logging with timestamp
app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
    next();
});

// Health check
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        version: '2.3.0',
        description: 'Yahoo Finance Primary with Caching & Rate Limiting'
    });
});

// API Routes
const financialRoutes = require('./routes/financial');
app.use('/api/financial', financialRoutes);

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
    console.log('');
    console.log('='.repeat(60));
    console.log('ISIN Research Backend v2.3.0');
    console.log('Yahoo Finance Primary with Caching & Rate Limiting');
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S.');
    console.log('='.repeat(60));
    console.log(`Server running on port ${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/health`);
    console.log(`API endpoint: http://localhost:${PORT}/api/financial/search`);
    console.log('='.repeat(60));
    console.log('Data sources (priority order):');
    console.log('  1. Yahoo Finance (Primary - Unlimited with rate limiting)');
    console.log('  2. Finnhub (Fallback for US stocks - 60 req/min)');
    console.log('  3. Alpha Vantage (Historical fallback - 25 req/day)');
    console.log('='.repeat(60));
    console.log('Optimizations enabled:');
    console.log('  ✓ In-memory cache (5 min TTL)');
    console.log('  ✓ Retry with exponential backoff');
    console.log('  ✓ Rate limiting (200ms between requests)');
    console.log('  ✓ Limited enrichment (max 3 results)');
    console.log('='.repeat(60));
    console.log('');
});

module.exports = app;
