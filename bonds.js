/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Bonds API Routes
 * Serves bonds data from bonds-data.json (no live scraping)
 */

const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');

// ===================================
// LOAD BONDS DATA
// ===================================

let bondsData = {
    lastUpdate: null,
    categories: {}
};

let allBonds = [];

function loadBondsData() {
    try {
        const dataPath = path.join(__dirname, 'data', 'bonds-data.json');
        if (fs.existsSync(dataPath)) {
            const rawData = fs.readFileSync(dataPath, 'utf8');
            bondsData = JSON.parse(rawData);
            
            // Flatten all bonds from all categories
            allBonds = [];
            Object.keys(bondsData.categories || {}).forEach(catKey => {
                const category = bondsData.categories[catKey];
                if (category.bonds && Array.isArray(category.bonds)) {
                    allBonds = allBonds.concat(category.bonds.map(b => ({
                        ...b,
                        categoryId: catKey,
                        categoryName: category.name
                    })));
                }
            });
            
            console.log(`✅ Loaded ${allBonds.length} bonds from ${Object.keys(bondsData.categories || {}).length} categories`);
        } else {
            console.warn('⚠️  bonds-data.json not found');
        }
    } catch (error) {
        console.error('❌ Error loading bonds data:', error.message);
    }
}

// Load data on startup
loadBondsData();

// Reload data every 6 hours
setInterval(loadBondsData, 6 * 60 * 60 * 1000);

// ===================================
// GET ALL BONDS (with filters)
// ===================================

router.get('/', async (req, res) => {
    try {
        const {
            category,       // Category filter (e.g. gov-it-btp)
            type,           // Type filter (e.g. BTP)
            minYield,       // Minimum yield
            maxYield,       // Maximum yield
            minCoupon,      // Minimum coupon
            maxCoupon,      // Maximum coupon
            limit = 100     // Results limit
        } = req.query;

        let filtered = [...allBonds];

        // Apply filters
        if (category) {
            filtered = filtered.filter(b => b.categoryId === category);
        }

        if (type) {
            filtered = filtered.filter(b => 
                b.type && b.type.toLowerCase() === type.toLowerCase()
            );
        }

        if (minYield) {
            filtered = filtered.filter(b => 
                b.yield >= parseFloat(minYield)
            );
        }

        if (maxYield) {
            filtered = filtered.filter(b => 
                b.yield <= parseFloat(maxYield)
            );
        }

        if (minCoupon) {
            filtered = filtered.filter(b => 
                b.coupon >= parseFloat(minCoupon)
            );
        }

        if (maxCoupon) {
            filtered = filtered.filter(b => 
                b.coupon <= parseFloat(maxCoupon)
            );
        }

        // Apply limit
        const limited = filtered.slice(0, parseInt(limit));

        res.json({
            success: true,
            count: limited.length,
            total: filtered.length,
            bonds: limited,
            lastUpdate: bondsData.lastUpdate
        });

    } catch (error) {
        console.error('[Bonds API] Error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// SEARCH BONDS BY CATEGORY
// ===================================

router.get('/search', async (req, res) => {
    try {
        const { category, limit = 100, q } = req.query;
        
        console.log(`[BONDS] Search request: category=${category}, limit=${limit}`);

        let filtered = [...allBonds];

        // Filter by category
        if (category) {
            filtered = filtered.filter(b => b.categoryId === category);
        }

        // Filter by search query
        if (q) {
            const query = q.toLowerCase();
            filtered = filtered.filter(b => {
                return (
                    (b.isin && b.isin.toLowerCase().includes(query)) ||
                    (b.name && b.name.toLowerCase().includes(query)) ||
                    (b.type && b.type.toLowerCase().includes(query))
                );
            });
        }

        // Apply limit
        const limited = filtered.slice(0, parseInt(limit));

        console.log(`[BONDS] Returning ${limited.length} bonds for category ${category || 'all'}`);

        res.json({
            success: true,
            category: category || 'all',
            count: limited.length,
            total: filtered.length,
            bonds: limited,
            lastUpdate: bondsData.lastUpdate
        });

    } catch (error) {
        console.error('[BONDS] Search error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// GET BOND BY ISIN
// ===================================

router.get('/:isin', async (req, res) => {
    try {
        const { isin } = req.params;
        
        const bond = allBonds.find(b => 
            b.isin && b.isin.toUpperCase() === isin.toUpperCase()
        );

        if (!bond) {
            return res.status(404).json({
                success: false,
                error: 'Bond not found',
                isin: isin
            });
        }

        res.json({
            success: true,
            bond: bond
        });

    } catch (error) {
        console.error('[Bonds API] Error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// GET CATEGORIES
// ===================================

router.get('/meta/categories', async (req, res) => {
    try {
        const categories = Object.keys(bondsData.categories || {}).map(key => ({
            id: key,
            name: bondsData.categories[key].name || key,
            description: bondsData.categories[key].description || '',
            count: bondsData.categories[key].bonds ? bondsData.categories[key].bonds.length : 0
        }));

        res.json({
            success: true,
            count: categories.length,
            categories: categories
        });

    } catch (error) {
        console.error('[Bonds API] Error:', error);
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
        const stats = {
            total_bonds: allBonds.length,
            categories: Object.keys(bondsData.categories || {}).length,
            
            avg_yield: calculateAverage(allBonds, 'yield'),
            avg_coupon: calculateAverage(allBonds, 'coupon'),
            
            yield_range: {
                min: Math.min(...allBonds.map(b => b.yield || 0)),
                max: Math.max(...allBonds.map(b => b.yield || 0))
            },
            
            coupon_range: {
                min: Math.min(...allBonds.map(b => b.coupon || 0)),
                max: Math.max(...allBonds.map(b => b.coupon || 0))
            },
            
            last_update: bondsData.lastUpdate
        };

        res.json({
            success: true,
            statistics: stats
        });

    } catch (error) {
        console.error('[Bonds API] Error:', error);
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
