/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend - Multi-Source Financial Data API
 * Version: 2.1 - Certificates Update
 */

const express = require('express');
const cors = require('cors');

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
        version: '2.1.0',
        endpoints: {
            financial: '/api/financial',
            certificates: '/api/certificates',
            health: '/health'
        }
    });
});

// ===================================
// LOAD MODULES FROM ROOT
// ===================================

// Financial routes (existing)
let financialRoutes;
try {
    financialRoutes = require('./financial');  // ← ROOT level
    app.use('/api/financial', financialRoutes);
    console.log('✅ Financial module loaded from root');
} catch (error) {
    console.warn('⚠️  Financial module not found:', error.message);
}

// Certificates routes (new)
let certificatesRoutes;
try {
    certificatesRoutes = require('./certificates');  // ← ROOT level
    app.use('/api/certificates', certificatesRoutes);
    console.log('✅ Certificates module loaded from root');
} catch (error) {
    console.error('❌ Certificates module error:', error.message);
    console.log('⚠️  Will use fallback mode with JSON data');
}

// ===================================
// FALLBACK CERTIFICATES ENDPOINT
// ===================================

if (!certificatesRoutes) {
    console.log('📦 Loading certificates from JSON (fallback mode)');
    
    let certificatesData = { certificates: [] };
    try {
        const fs = require('fs');
        const path = require('path');
        const dataPath = path.join(__dirname, 'certificates-data.json');
        const rawData = fs.readFileSync(dataPath, 'utf8');
        certificatesData = JSON.parse(rawData);
        console.log(`✅ Loaded ${certificatesData.certificates.length} certificates from JSON`);
    } catch (error) {
        console.error('❌ Error loading certificates-data.json:', error.message);
    }
    
    // GET /api/certificates - List all
    app.get('/api/certificates', (req, res) => {
        try {
            const { limit = 100, type, issuer, minYield, minBarrier, maxBarrier } = req.query;
            
            let filtered = certificatesData.certificates;
            
            // Apply filters
            if (type) {
                filtered = filtered.filter(c => c.type === type);
            }
            if (issuer) {
                filtered = filtered.filter(c => c.issuer === issuer);
            }
            if (minYield) {
                filtered = filtered.filter(c => c.annual_coupon_yield >= parseFloat(minYield));
            }
            if (minBarrier) {
                filtered = filtered.filter(c => c.barrier_down >= parseFloat(minBarrier));
            }
            if (maxBarrier) {
                filtered = filtered.filter(c => c.barrier_down <= parseFloat(maxBarrier));
            }
            
            const limited = filtered.slice(0, parseInt(limit));
            
            res.json({
                success: true,
                count: limited.length,
                total: filtered.length,
                metadata: certificatesData.metadata || {},
                certificates: limited
            });
        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    });
    
    // GET /api/certificates/:isin - Single certificate
    app.get('/api/certificates/:isin', (req, res) => {
        try {
            const { isin } = req.params;
            const cert = certificatesData.certificates.find(c => 
                c.isin && c.isin.toUpperCase() === isin.toUpperCase()
            );
            
            if (!cert) {
                return res.status(404).json({
                    success: false,
                    error: 'Certificate not found',
                    isin: isin
                });
            }
            
            res.json({ success: true, certificate: cert });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    // GET /api/certificates/search - Search
    app.get('/api/certificates/search', (req, res) => {
        try {
            const { q } = req.query;
            
            if (!q) {
                return res.status(400).json({
                    success: false,
                    error: 'Query parameter "q" is required'
                });
            }
            
            const query = q.toLowerCase();
            const results = certificatesData.certificates.filter(c => {
                return (
                    (c.isin && c.isin.toLowerCase().includes(query)) ||
                    (c.name && c.name.toLowerCase().includes(query)) ||
                    (c.issuer && c.issuer.toLowerCase().includes(query)) ||
                    (c.type && c.type.toLowerCase().includes(query))
                );
            });
            
            res.json({
                success: true,
                count: results.length,
                query: q,
                certificates: results.slice(0, 50)
            });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    // GET /api/certificates/meta/types
    app.get('/api/certificates/meta/types', (req, res) => {
        try {
            const types = [...new Set(certificatesData.certificates
                .map(c => c.type)
                .filter(t => t)
            )];
            res.json({ success: true, count: types.length, types: types.sort() });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    // GET /api/certificates/meta/issuers
    app.get('/api/certificates/meta/issuers', (req, res) => {
        try {
            const issuers = [...new Set(certificatesData.certificates
                .map(c => c.issuer)
                .filter(i => i)
            )];
            res.json({ success: true, count: issuers.length, issuers: issuers.sort() });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    // GET /api/certificates/meta/stats
    app.get('/api/certificates/meta/stats', (req, res) => {
        try {
            const certs = certificatesData.certificates;
            const calculateAvg = (field) => {
                const values = certs.map(c => c[field]).filter(v => v != null && !isNaN(v));
                return values.length > 0 ? values.reduce((a,b) => a+b, 0) / values.length : 0;
            };
            
            res.json({
                success: true,
                statistics: {
                    total_certificates: certs.length,
                    types: [...new Set(certs.map(c => c.type).filter(t => t))].length,
                    issuers: [...new Set(certs.map(c => c.issuer).filter(i => i))].length,
                    avg_annual_yield: parseFloat(calculateAvg('annual_coupon_yield').toFixed(2)),
                    avg_barrier: parseFloat(calculateAvg('barrier_down').toFixed(2)),
                    yield_range: {
                        min: Math.min(...certs.map(c => c.annual_coupon_yield || 0)),
                        max: Math.max(...certs.map(c => c.annual_coupon_yield || 0))
                    },
                    barrier_range: {
                        min: Math.min(...certs.map(c => c.barrier_down || 0)),
                        max: Math.max(...certs.map(c => c.barrier_down || 0))
                    },
                    last_update: certificatesData.metadata?.timestamp || new Date().toISOString()
                }
            });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
}

// ===================================
// 404 HANDLER
// ===================================

app.use((req, res) => {
    res.status(404).json({
        success: false,
        error: 'Endpoint not found',
        path: req.path,
        available_endpoints: [
            '/health',
            '/api/financial/search (if loaded)',
            '/api/certificates',
            '/api/certificates/:isin',
            '/api/certificates/search',
            '/api/certificates/meta/types',
            '/api/certificates/meta/issuers',
            '/api/certificates/meta/stats'
        ]
    });
});

// ===================================
// ERROR HANDLER
// ===================================

app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    res.status(500).json({
        success: false,
        error: 'Internal server error',
        message: err.message
    });
});

// ===================================
// START SERVER
// ===================================

app.listen(PORT, () => {
    console.log('='.repeat(60));
    console.log('ISIN Research Backend - Multi-Source v2.1');
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S.');
    console.log('='.repeat(60));
    console.log(`Server running on port ${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/health`);
    console.log(`API Financial: http://localhost:${PORT}/api/financial`);
    console.log(`API Certificates: http://localhost:${PORT}/api/certificates`);
    console.log('='.repeat(60));
    console.log('Modules loaded:');
    console.log(`  Financial: ${financialRoutes ? '✅ Active' : '⚠️  Not found (optional)'}`);
    console.log(`  Certificates: ${certificatesRoutes ? '✅ Module mode' : '📦 Fallback mode (JSON)'}`);
    console.log('='.repeat(60));
});

module.exports = app;
