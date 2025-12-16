/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Certificates API Routes
 * Serves investment certificates data from scraped JSON
 */

const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');

// ===================================
// LOAD CERTIFICATES DATA
// ===================================

let certificatesData = {
    metadata: {},
    certificates: []
};

function loadCertificatesData() {
    try {
        const dataPath = path.join(__dirname, 'certificates-data.json');
        if (fs.existsSync(dataPath)) {
            const rawData = fs.readFileSync(dataPath, 'utf8');
            certificatesData = JSON.parse(rawData);
            console.log(`✅ Loaded ${certificatesData.certificates.length} certificates`);
        } else {
            console.warn('⚠️  certificates-data.json not found, using empty dataset');
        }
    } catch (error) {
        console.error('❌ Error loading certificates data:', error.message);
    }
}

// Load data on startup
loadCertificatesData();

// Reload data every 6 hours
setInterval(loadCertificatesData, 6 * 60 * 60 * 1000);

// ===================================
// GET ALL CERTIFICATES (with filters)
// ===================================

router.get('/', async (req, res) => {
    try {
        const {
            type,           // Certificate type filter
            issuer,         // Issuer filter
            minYield,       // Minimum annual yield
            maxYield,       // Maximum annual yield
            minBarrier,     // Minimum barrier
            maxBarrier,     // Maximum barrier
            limit = 100     // Results limit
        } = req.query;

        let filtered = [...certificatesData.certificates];

        // Apply filters
        if (type) {
            filtered = filtered.filter(c => 
                c.type && c.type.toLowerCase().includes(type.toLowerCase())
            );
        }

        if (issuer) {
            filtered = filtered.filter(c => 
                c.issuer && c.issuer.toLowerCase().includes(issuer.toLowerCase())
            );
        }

        if (minYield) {
            filtered = filtered.filter(c => 
                c.annual_coupon_yield >= parseFloat(minYield)
            );
        }

        if (maxYield) {
            filtered = filtered.filter(c => 
                c.annual_coupon_yield <= parseFloat(maxYield)
            );
        }

        if (minBarrier) {
            filtered = filtered.filter(c => 
                c.barrier_down >= parseFloat(minBarrier)
            );
        }

        if (maxBarrier) {
            filtered = filtered.filter(c => 
                c.barrier_down <= parseFloat(maxBarrier)
            );
        }

        // Apply limit
        const limited = filtered.slice(0, parseInt(limit));

        res.json({
            success: true,
            count: limited.length,
            total: filtered.length,
            metadata: certificatesData.metadata,
            certificates: limited
        });

    } catch (error) {
        console.error('[Certificates API] Error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// GET CERTIFICATE BY ISIN
// ===================================

router.get('/:isin', async (req, res) => {
    try {
        const { isin } = req.params;
        
        const certificate = certificatesData.certificates.find(c => 
            c.isin && c.isin.toUpperCase() === isin.toUpperCase()
        );

        if (!certificate) {
            return res.status(404).json({
                success: false,
                error: 'Certificate not found',
                isin: isin
            });
        }

        res.json({
            success: true,
            certificate: certificate
        });

    } catch (error) {
        console.error('[Certificates API] Error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// SEARCH CERTIFICATES
// ===================================

router.get('/search', async (req, res) => {
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
            certificates: results.slice(0, 50) // Limit to 50 results
        });

    } catch (error) {
        console.error('[Certificates API] Error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// GET CERTIFICATE TYPES
// ===================================

router.get('/meta/types', async (req, res) => {
    try {
        const types = [...new Set(certificatesData.certificates
            .map(c => c.type)
            .filter(t => t)
        )];

        res.json({
            success: true,
            count: types.length,
            types: types.sort()
        });

    } catch (error) {
        console.error('[Certificates API] Error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// GET CERTIFICATE ISSUERS
// ===================================

router.get('/meta/issuers', async (req, res) => {
    try {
        const issuers = [...new Set(certificatesData.certificates
            .map(c => c.issuer)
            .filter(i => i)
        )];

        res.json({
            success: true,
            count: issuers.length,
            issuers: issuers.sort()
        });

    } catch (error) {
        console.error('[Certificates API] Error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// GET STATISTICS
// ===================================

router.get('/meta/stats', async (req, res) => {
    try {
        const certs = certificatesData.certificates;
        
        const stats = {
            total_certificates: certs.length,
            
            types: [...new Set(certs.map(c => c.type).filter(t => t))].length,
            issuers: [...new Set(certs.map(c => c.issuer).filter(i => i))].length,
            
            avg_annual_yield: calculateAverage(certs, 'annual_coupon_yield'),
            avg_barrier: calculateAverage(certs, 'barrier_down'),
            
            yield_range: {
                min: Math.min(...certs.map(c => c.annual_coupon_yield || 0)),
                max: Math.max(...certs.map(c => c.annual_coupon_yield || 0))
            },
            
            barrier_range: {
                min: Math.min(...certs.map(c => c.barrier_down || 0)),
                max: Math.max(...certs.map(c => c.barrier_down || 0))
            },
            
            last_update: certificatesData.metadata.timestamp
        };

        res.json({
            success: true,
            statistics: stats
        });

    } catch (error) {
        console.error('[Certificates API] Error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// UTILITY FUNCTIONS
// ===================================

function calculateAverage(arr, field) {
    const values = arr.map(item => item[field]).filter(v => v != null && !isNaN(v));
    if (values.length === 0) return 0;
    const sum = values.reduce((a, b) => a + b, 0);
    return parseFloat((sum / values.length).toFixed(2));
}

module.exports = router;
