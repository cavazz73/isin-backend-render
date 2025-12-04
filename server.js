/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend - Multi-Source Financial Data API
 * Version: 2.1 - RICH METRICS UPDATE
 */

const express = require('express');
const cors = require('cors');
const financialRoutes = require('./financial');

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
        version: '2.1.0'
    });
});

// API Routes
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
    console.log('='.repeat(60));
    console.log('ISIN Research Backend - Multi-Source v2.1 🚀');
    console.log('WITH RICH METRICS: Market Cap, P/E, Logo, Description');
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S.');
    console.log('='.repeat(60));
    console.log(`Server running on port ${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/health`);
    console.log(`API endpoint: http://localhost:${PORT}/api/financial/search`);
    console.log('='.repeat(60));
    console.log('Data sources:');
    console.log('  1. Yahoo Finance (Primary - Unlimited)');
    console.log('  2. Finnhub (Backup - 60 req/min)');
    console.log('  3. Alpha Vantage (Backup - 25 req/day)');
    console.log('='.repeat(60));
});

module.exports = app;
