/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Bond API Routes - Italian Government Bonds
 */

const express = require('express');
const router = express.Router();
const BorsaItalianaScraper = require('./borsaItalianaScraper');

// Initialize Bond Scraper
const bondScraper = new BorsaItalianaScraper();

// ===================================
// SEARCH BONDS BY CATEGORY
// ===================================
router.get('/search', async (req, res) => {
    try {
        const { category = 'btp', limit = 20 } = req.query;
        
        console.log(`[BONDS] Search request: category=${category}, limit=${limit}`);

        // Validate category
        const validCategories = ['btp', 'bot', 'cct', 'ctz'];
        if (!validCategories.includes(category.toLowerCase())) {
            return res.status(400).json({
                success: false,
                error: `Invalid category. Valid options: ${validCategories.join(', ')}`
            });
        }

        // Search bonds
        const results = await bondScraper.searchBonds(category, parseInt(limit));
        
        res.json({
            success: true,
            category: results.category,
            count: results.count,
            bonds: results.bonds,
            disclaimer: results.disclaimer,
            timestamp: new Date().toISOString()
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
// GET BOND DETAILS BY ISIN
// ===================================
router.get('/details/:isin', async (req, res) => {
    try {
        const { isin } = req.params;
        const { category = 'btp' } = req.query;
        
        console.log(`[BONDS] Details request: ISIN=${isin}, category=${category}`);

        // Validate ISIN format
        if (!/^[A-Z]{2}[A-Z0-9]{10}$/.test(isin.toUpperCase())) {
            return res.status(400).json({
                success: false,
                error: 'Invalid ISIN format'
            });
        }

        // Get bond details
        const details = await bondScraper.getBondDetails(isin, category);
        
        if (!details) {
            return res.status(404).json({
                success: false,
                error: 'Bond not found'
            });
        }

        res.json({
            success: true,
            bond: details,
            timestamp: new Date().toISOString()
        });

    } catch (error) {
        console.error('[BONDS] Details error:', error);
        res.status(404).json({
            success: false,
            error: 'Bond not found or temporarily unavailable'
        });
    }
});

// ===================================
// GET ALL BTP (Quick Access)
// ===================================
router.get('/btp', async (req, res) => {
    try {
        const { limit = 20 } = req.query;
        const results = await bondScraper.searchBonds('btp', parseInt(limit));
        
        res.json({
            success: true,
            ...results
        });
    } catch (error) {
        console.error('[BONDS] BTP error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// GET ALL BOT (Quick Access)
// ===================================
router.get('/bot', async (req, res) => {
    try {
        const { limit = 20 } = req.query;
        const results = await bondScraper.searchBonds('bot', parseInt(limit));
        
        res.json({
            success: true,
            ...results
        });
    } catch (error) {
        console.error('[BONDS] BOT error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// CACHE STATISTICS
// ===================================
router.get('/cache-stats', (req, res) => {
    try {
        const stats = bondScraper.getCacheStats();
        
        res.json({
            success: true,
            cache: stats,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// CLEAR CACHE (Admin)
// ===================================
router.post('/clear-cache', (req, res) => {
    try {
        bondScraper.clearCache();
        
        res.json({
            success: true,
            message: 'Bond cache cleared successfully'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// TEST ENDPOINT
// ===================================
router.get('/test', async (req, res) => {
    try {
        console.log('[BONDS] Running test...');
        
        // Test BTP search
        const btpTest = await bondScraper.searchBonds('btp', 3);
        
        res.json({
            success: true,
            timestamp: new Date().toISOString(),
            test_results: {
                btp_search: {
                    status: btpTest.count > 0 ? 'OK' : 'FAIL',
                    bonds_found: btpTest.count,
                    sample: btpTest.bonds[0] || null
                },
                cache: bondScraper.getCacheStats()
            }
        });
        
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

module.exports = router;
