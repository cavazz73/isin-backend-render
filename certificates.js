/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Certificates API Routes
 * Serves certificates data from certificates-data.json (no live scraping)
 */

const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');

// ===================================
// LOAD CERTIFICATES DATA
// ===================================

let certificatesData = {
    lastUpdate: null,
    totalCertificates: 0,
    categories: {},
    certificates: []
};

function loadCertificatesData() {
    try {
        const dataPath = path.join(__dirname, 'data', 'certificates', 'certificates-data.json');
        
        if (fs.existsSync(dataPath)) {
            const rawData = fs.readFileSync(dataPath, 'utf8');
            certificatesData = JSON.parse(rawData);
            
            console.log(`✅ [CERTIFICATES] Loaded ${certificatesData.certificates.length} certificates`);
            console.log(`   Last update: ${certificatesData.lastUpdate}`);
            console.log(`   Categories: ${Object.keys(certificatesData.categories || {}).length}`);
        } else {
            console.warn('⚠️  [CERTIFICATES] certificates-data.json not found at:', dataPath);
        }
    } catch (error) {
        console.error('❌ [CERTIFICATES] Error loading data:', error.message);
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
            type,           // Certificate type (phoenixMemory, cashCollect, bonusCap, express)
            issuer,         // Issuer filter
            minCoupon,      // Minimum coupon
            maxCoupon,      // Maximum coupon
            minYield,       // Minimum annual yield
            maxYield,       // Maximum annual yield
            limit = 100     // Results limit
        } = req.query;

        let filtered = [...certificatesData.certificates];

        // Apply filters
        if (type) {
            filtered = filtered.filter(c => 
                c.type && c.type.toLowerCase() === type.toLowerCase()
            );
        }

        if (issuer) {
            filtered = filtered.filter(c => 
                c.issuer && c.issuer.toLowerCase().includes(issuer.toLowerCase())
            );
        }

        if (minCoupon) {
            filtered = filtered.filter(c => 
                c.coupon && c.coupon >= parseFloat(minCoupon)
            );
        }

        if (maxCoupon) {
            filtered = filtered.filter(c => 
                c.coupon && c.coupon <= parseFloat(maxCoupon)
            );
        }

        if (minYield) {
            filtered = filtered.filter(c => 
                c.annual_coupon_yield && c.annual_coupon_yield >= parseFloat(minYield)
            );
        }

        if (maxYield) {
            filtered = filtered.filter(c => 
                c.annual_coupon_yield && c.annual_coupon_yield <= parseFloat(maxYield)
            );
        }

        // Apply limit
        const limitNum = parseInt(limit);
        if (limitNum > 0) {
            filtered = filtered.slice(0, limitNum);
        }

        res.json({
            success: true,
            count: filtered.length,
            totalAvailable: certificatesData.certificates.length,
            lastUpdate: certificatesData.lastUpdate,
            certificates: filtered
        });

    } catch (error) {
        console.error('❌ [CERTIFICATES] Error in GET /:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
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
        console.error('❌ [CERTIFICATES] Error in GET /:isin:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// ===================================
// SEARCH CERTIFICATES
// ===================================

router.get('/search/query', async (req, res) => {
    try {
        const { q } = req.query;
        
        if (!q || q.length < 2) {
            return res.json({
                success: true,
                count: 0,
                certificates: []
            });
        }

        const query = q.toLowerCase();
        const results = certificatesData.certificates.filter(c => 
            (c.isin && c.isin.toLowerCase().includes(query)) ||
            (c.name && c.name.toLowerCase().includes(query)) ||
            (c.issuer && c.issuer.toLowerCase().includes(query))
        );

        res.json({
            success: true,
            count: results.length,
            query: q,
            certificates: results.slice(0, 50) // Limit search results
        });

    } catch (error) {
        console.error('❌ [CERTIFICATES] Error in search:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            message: error.message
        });
    }
});

// ===================================
// GET CATEGORIES
// ===================================

router.get('/meta/categories', async (req, res) => {
    try {
        res.json({
            success: true,
            lastUpdate: certificatesData.lastUpdate,
            categories: certificatesData.categories || {}
        });
    } catch (error) {
        console.error('❌ [CERTIFICATES] Error in categories:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error'
        });
    }
});

// ===================================
// GET STATS
// ===================================

router.get('/meta/stats', async (req, res) => {
    try {
        const stats = {
            totalCertificates: certificatesData.certificates.length,
            lastUpdate: certificatesData.lastUpdate,
            categoriesCount: Object.keys(certificatesData.categories || {}).length,
            byType: {}
        };

        // Count by type
        certificatesData.certificates.forEach(c => {
            const type = c.type || 'other';
            stats.byType[type] = (stats.byType[type] || 0) + 1;
        });

        res.json({
            success: true,
            stats: stats
        });

    } catch (error) {
        console.error('❌ [CERTIFICATES] Error in stats:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error'
        });
    }
});

// ===================================
// RELOAD DATA (manual trigger)
// ===================================

router.post('/admin/reload', async (req, res) => {
    try {
        loadCertificatesData();
        res.json({
            success: true,
            message: 'Certificates data reloaded',
            count: certificatesData.certificates.length
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

module.exports = router;
