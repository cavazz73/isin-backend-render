/**
 * Database Migration Script
 * Run with: node migrate.js
 */

const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

const schema = `
-- ============================================================================
-- ISIN Research & Compare - Database Schema v3.0
-- ============================================================================

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

-- Initial seed data
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

async function migrate() {
    console.log('🚀 Starting database migration...');
    
    try {
        await pool.query(schema);
        
        const result = await pool.query('SELECT COUNT(*) as count FROM instruments');
        console.log('✅ Migration completed successfully!');
        console.log(\`📊 Instruments in database: \${result.rows[0].count}\`);
        
        process.exit(0);
    } catch (error) {
        console.error('❌ Migration failed:', error);
        process.exit(1);
    }
}

migrate();
