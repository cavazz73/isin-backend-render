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
        const dataPath = path.join(__dirname, 'data', 'certificates-data.json');
        
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
            // Filters
            type,           // Certificate type
            issuer,         // Issuer filter
            minCoupon,      // Minimum coupon
            maxCoupon,      // Maximum coupon
            minYield,       // Minimum annual yield
            maxYield,       // Maximum annual yield
            search,         // Search in ISIN/Name
            
            // Pagination
            page = 1,       // Current page
            limit = 20,     // Items per page
            
            // Sorting
            sortBy = 'name',     // Sort field
            sortOrder = 'asc'    // Sort order (asc/desc)
        } = req.query;

        // Parse pagination params
        const pageNum = Math.max(1, parseInt(page));
        const limitNum = Math.min(Math.max(1, parseInt(limit)), 100); // Max 100 per page
        const offset = (pageNum - 1) * limitNum;

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
            const minYieldVal = parseFloat(minYield);
            filtered = filtered.filter(c => {
                const yieldVal = c.annual_coupon_yield || c.change_percent || 0;
                return yieldVal >= minYieldVal;
            });
        }

        if (maxYield) {
            const maxYieldVal = parseFloat(maxYield);
            filtered = filtered.filter(c => {
                const yieldVal = c.annual_coupon_yield || c.change_percent || 0;
                return yieldVal <= maxYieldVal;
            });
        }

        // Search filter
        if (search && search.trim().length > 0) {
            const searchLower = search.toLowerCase().trim();
            filtered = filtered.filter(c => 
                (c.isin && c.isin.toLowerCase().includes(searchLower)) ||
                (c.name && c.name.toLowerCase().includes(searchLower)) ||
                (c.symbol && c.symbol.toLowerCase().includes(searchLower)) ||
                (c.issuer && c.issuer.toLowerCase().includes(searchLower))
            );
        }

        // Sorting
        filtered.sort((a, b) => {
            let aVal, bVal;

            switch(sortBy) {
                case 'price':
                    aVal = a.last_price || a.price || 0;
                    bVal = b.last_price || b.price || 0;
                    break;
                case 'yield':
                    aVal = a.annual_coupon_yield || a.change_percent || 0;
                    bVal = b.annual_coupon_yield || b.change_percent || 0;
                    break;
                case 'coupon':
                    aVal = a.coupon || 0;
                    bVal = b.coupon || 0;
                    break;
                case 'issuer':
                    aVal = (a.issuer || '').toLowerCase();
                    bVal = (b.issuer || '').toLowerCase();
                    break;
                case 'name':
                default:
                    aVal = (a.name || '').toLowerCase();
                    bVal = (b.name || '').toLowerCase();
                    break;
            }

            if (sortOrder === 'desc') {
                return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
            } else {
                return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
            }
        });

        // Calculate pagination
        const total = filtered.length;
        const totalPages = Math.ceil(total / limitNum);
        const paginated = filtered.slice(offset, offset + limitNum);

        res.json({
            success: true,
            count: paginated.length,
            total: total,
            totalAvailable: certificatesData.certificates.length,
            lastUpdate: certificatesData.lastUpdate,
            certificates: paginated,
            
            // Pagination metadata
            pagination: {
                page: pageNum,
                limit: limitNum,
                totalPages: totalPages,
                hasNext: pageNum < totalPages,
                hasPrev: pageNum > 1,
                nextPage: pageNum < totalPages ? pageNum + 1 : null,
                prevPage: pageNum > 1 ? pageNum - 1 : null
            },
            
            // Applied filters (for frontend)
            filters: {
                type: type || null,
                issuer: issuer || null,
                minCoupon: minCoupon || null,
                maxCoupon: maxCoupon || null,
                minYield: minYield || null,
                maxYield: maxYield || null,
                search: search || null
            },
            
            // Applied sorting (for frontend)
            sort: {
                by: sortBy,
                order: sortOrder
            }
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
