/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend - Multi-Source Financial Data API
 * Version: 3.1 - Database Hybrid System + Bond Integration
 */

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const financialRoutes = require('./financial');
const bondsRoutes = require('./bonds'); // ← NUOVO: Import bond routes

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
        version: '3.1.0', // ← Aggiornato
        database: process.env.DATABASE_URL ? 'configured' : 'not configured'
    });
});

// ============================================================================
// DATABASE SETUP ENDPOINT
// ============================================================================
// IMPORTANT: Remove this endpoint after initial database setup!
// Visit: https://isin-backend.onrender.com/setup-database (one time only)
// ============================================================================

app.get('/setup-database', async (req, res) => {
    if (!process.env.DATABASE_URL) {
        return res.status(500).json({
            success: false,
            error: 'DATABASE_URL not configured in environment variables'
        });
    }

    const pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false }
    });

    const schema = `
-- Drop existing tables
DROP TABLE IF EXISTS cache_status CASCADE;
DROP TABLE IF EXISTS quotes CASCADE;
DROP TABLE IF EXISTS instruments CASCADE;

-- Instruments table
CREATE TABLE instruments (
    id SERIAL PRIMARY KEY,
    isin VARCHAR(12) UNIQUE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(255) NOT NULL,
    exchange VARCHAR(50),
    currency VARCHAR(3) DEFAULT 'USD',
    type VARCHAR(50),
    country VARCHAR(2),
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap BIGINT,
    pe_ratio DECIMAL(10, 2),
    dividend_yield DECIMAL(6, 4),
    week_52_high DECIMAL(18, 4),
    week_52_low DECIMAL(18, 4),
    description TEXT,
    logo_url TEXT,
    website VARCHAR(255),
    data_source VARCHAR(50),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Quotes table
CREATE TABLE quotes (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    price DECIMAL(18, 4) NOT NULL,
    change DECIMAL(18, 4),
    change_percent DECIMAL(8, 4),
    volume BIGINT,
    open DECIMAL(18, 4),
    high DECIMAL(18, 4),
    low DECIMAL(18, 4),
    previous_close DECIMAL(18, 4),
    timestamp TIMESTAMP NOT NULL,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(instrument_id, timestamp)
);

-- Cache status table
CREATE TABLE cache_status (
    cache_key VARCHAR(255) PRIMARY KEY,
    instrument_id INTEGER REFERENCES instruments(id) ON DELETE CASCADE,
    cache_type VARCHAR(50) NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    hit_count INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_instruments_isin ON instruments(isin);
CREATE INDEX idx_instruments_symbol ON instruments(symbol);
CREATE INDEX idx_instruments_exchange ON instruments(exchange);
CREATE INDEX idx_quotes_instrument ON quotes(instrument_id);
CREATE INDEX idx_quotes_timestamp ON quotes(timestamp DESC);
CREATE INDEX idx_cache_expires ON cache_status(expires_at);

-- Initial seed data (Italian stocks)
INSERT INTO instruments (isin, symbol, name, exchange, currency, type, country, sector, is_active) VALUES
('IT0003128367', 'ENEL.MI', 'Enel S.p.A.', 'MIL', 'EUR', 'stock', 'IT', 'Utilities', true),
('IT0003132476', 'ENI.MI', 'Eni S.p.A.', 'MIL', 'EUR', 'stock', 'IT', 'Energy', true),
('IT0000072618', 'ISP.MI', 'Intesa Sanpaolo', 'MIL', 'EUR', 'stock', 'IT', 'Financial Services', true),
('IT0005239360', 'UCG.MI', 'UniCredit S.p.A.', 'MIL', 'EUR', 'stock', 'IT', 'Financial Services', true),
('IT0003796171', 'G.MI', 'Generali', 'MIL', 'EUR', 'stock', 'IT', 'Financial Services', true),
('IT0003506190', 'TIT.MI', 'Telecom Italia', 'MIL', 'EUR', 'stock', 'IT', 'Communication Services', true),
('IT0005218380', 'STLAM.MI', 'Stellantis N.V.', 'MIL', 'EUR', 'stock', 'IT', 'Consumer Cyclical', true),
('IT0003242622', 'PRY.MI', 'Prysmian S.p.A.', 'MIL', 'EUR', 'stock', 'IT', 'Industrials', true)
ON CONFLICT (isin) DO NOTHING;
    `;

    try {
        console.log('[DB Setup] Starting database initialization...');
        await pool.query(schema);
        
        const result = await pool.query('SELECT COUNT(*) as count FROM instruments');
        const count = result.rows[0].count;
        
        console.log('[DB Setup] ✅ Database initialized successfully!');
        console.log(`[DB Setup] 📊 Instruments count: ${count}`);
        
        res.json({
            success: true,
            message: '✅ Database setup completed successfully!',
            instruments_count: count,
            tables_created: ['instruments', 'quotes', 'cache_status'],
            indexes_created: 6,
            next_steps: [
                '1. Remove this endpoint from server.js for security',
                '2. Implement cache layer with Redis/Upstash',
                '3. Create smart router for multi-layer data fetching'
            ]
        });
        
    } catch (error) {
        console.error('[DB Setup] ❌ Error:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            details: error.stack
        });
    } finally {
        await pool.end();
    }
});

// API Routes
app.use('/api/financial', financialRoutes);
app.use('/api/bonds', bondsRoutes); // ← NUOVO: Bond routes

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
    console.log('ISIN Research Backend - Multi-Source v3.1'); // ← Aggiornato
    console.log('WITH HYBRID DATABASE SYSTEM + BOND INTEGRATION'); // ← Aggiornato
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S.');
    console.log('='.repeat(60));
    console.log(`Server running on port ${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/health`);
    console.log(`API endpoint: http://localhost:${PORT}/api/financial/search`);
    console.log(`Bonds endpoint: http://localhost:${PORT}/api/bonds/search`); // ← NUOVO
    console.log(`DB Setup: http://localhost:${PORT}/setup-database`);
    console.log('='.repeat(60));
    console.log('Data sources:');
    console.log('  1. PostgreSQL Database (Primary - Local cache)');
    console.log('  2. Yahoo Finance (Fallback - Unlimited)');
    console.log('  3. Finnhub (Fallback - 60 req/min)');
    console.log('  4. Alpha Vantage (Fallback - 25 req/day)');
    console.log('  5. Borsa Italiana (Bonds - Italian Gov Bonds)'); // ← NUOVO
    console.log('='.repeat(60));
    console.log('Database:', process.env.DATABASE_URL ? '✅ Configured' : '⚠️  Not configured');
    console.log('='.repeat(60));
});

module.exports = app;
