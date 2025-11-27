/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend - Multi-Source Financial Data API
 * Version: 3.0.0 - TwelveData Integration
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const financialRoutes = require('./financial'); // ← FLAT STRUCTURE!

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
        version: '3.0.0',
        timestamp: new Date().toISOString(),
        uptime: process.uptime()
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
    console.log('ISIN Research Backend v3.0 - TwelveData Integration');
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S.');
    console.log('='.repeat(60));
    console.log(`Server running on port ${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/health`);
    console.log(`API endpoint: http://localhost:${PORT}/api/financial/search`);
    console.log('='.repeat(60));
    console.log('Data sources priority:');
    console.log('  EU Stocks: TwelveData → Yahoo → Finnhub → Alpha Vantage');
    console.log('  US Stocks: Yahoo → TwelveData → Finnhub → Alpha Vantage');
    console.log('='.repeat(60));
    console.log('Environment:');
    console.log(`  TWELVE_DATA_API_KEY: ${process.env.TWELVE_DATA_API_KEY ? 'SET ✓' : 'MISSING ✗'}`);
    console.log(`  FINNHUB_API_KEY: ${process.env.FINNHUB_API_KEY ? 'SET ✓' : 'MISSING ✗'}`);
    console.log(`  ALPHA_VANTAGE_API_KEY: ${process.env.ALPHA_VANTAGE_API_KEY ? 'SET ✓' : 'MISSING ✗'}`);
    console.log('='.repeat(60));
});

module.exports = app;
