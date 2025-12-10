/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Bond API Routes - Complete System
 * Supporta: Governativi IT/EU, Sovranazionali, Corporate
 */

const express = require('express');
const router = express.Router();
const BondScraperComplete = require('./bondScraperComplete');

// Initialize Bond Scraper
const bondScraper = new BondScraperComplete();

// ===================================
// GET AVAILABLE CATEGORIES
// ===================================
router.get('/categories', (req, res) => {
    try {
        const categories = bondScraper.getCategories();
        
        // Raggruppa per tipo
        const grouped = {
            governativi_it: [],
            governativi_eu: [],
            sovranazionali: [],
            corporate: []
        };
        
        categories.forEach(cat => {
            if (cat.type === 'governativo') {
                grouped.governativi_it.push(cat);
            } else if (cat.type === 'euro-gov') {
                grouped.governativi_eu.push(cat);
            } else if (cat.type === 'sovranazionale') {
                grouped.sovranazionali.push(cat);
            } else if (cat.type === 'corporate') {
                grouped.corporate.push(cat);
            }
        });
        
        res.json({
            success: true,
            total: categories.length,
            categories: grouped,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('[BONDS] Categories error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// SEARCH BY CATEGORY
// ===================================
router.get('/search', async (req, res) => {
    try {
        const { category, limit = 50 } = req.query;
        
        if (!category) {
            return res.status(400).json({
                success: false,
                error: 'Parameter "category" is required'
            });
        }
        
        console.log(`[BONDS] Search: category=${category}, limit=${limit}`);
        
        const results = await bondScraper.searchBonds(category, parseInt(limit));
        
        if (!results.success) {
            return res.status(404).json({
                success: false,
                error: results.error || 'Bonds not found'
            });
        }
        
        res.json({
            success: true,
            ...results,
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
// ADVANCED FILTER
// ===================================
router.get('/filter', async (req, res) => {
    try {
        const { 
            category, 
            minYield, 
            maxYield, 
            minPrice, 
            maxPrice, 
            sortBy 
        } = req.query;
        
        if (!category) {
            return res.status(400).json({
                success: false,
                error: 'Parameter "category" is required'
            });
        }
        
        const filters = {};
        if (minYield) filters.minYield = parseFloat(minYield);
        if (maxYield) filters.maxYield = parseFloat(maxYield);
        if (minPrice) filters.minPrice = parseFloat(minPrice);
        if (maxPrice) filters.maxPrice = parseFloat(maxPrice);
        if (sortBy) filters.sortBy = sortBy;
        
        console.log(`[BONDS] Filter: category=${category}, filters=`, filters);
        
        const results = await bondScraper.filterBonds(category, filters);
        
        res.json({
            success: true,
            ...results,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('[BONDS] Filter error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// SEARCH BY ISIN
// ===================================
router.get('/isin/:isin', async (req, res) => {
    try {
        const { isin } = req.params;
        
        // Validate ISIN format
        if (!/^[A-Z]{2}[A-Z0-9]{10}$/.test(isin.toUpperCase())) {
            return res.status(400).json({
                success: false,
                error: 'Invalid ISIN format'
            });
        }
        
        console.log(`[BONDS] ISIN lookup: ${isin}`);
        
        const result = await bondScraper.searchByISIN(isin);
        
        if (!result.success) {
            return res.status(404).json({
                success: false,
                error: result.error || 'Bond not found'
            });
        }
        
        res.json({
            success: true,
            bond: result.bond,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('[BONDS] ISIN error:', error);
        res.status(404).json({
            success: false,
            error: 'Bond not found'
        });
    }
});

// ===================================
// OVERVIEW (All Categories)
// ===================================
router.get('/overview', async (req, res) => {
    try {
        const { limit = 5 } = req.query;
        
        console.log('[BONDS] Generating overview...');
        
        const overview = await bondScraper.searchAll(parseInt(limit));
        
        res.json({
            success: true,
            ...overview
        });
        
    } catch (error) {
        console.error('[BONDS] Overview error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ===================================
// QUICK ACCESS - GOVERNATIVI IT
// ===================================
router.get('/gov-it/btp', async (req, res) => {
    try {
        const { limit = 30 } = req.query;
        const results = await bondScraper.searchBonds('gov-it-btp', parseInt(limit));
        res.json({ success: true, ...results });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/gov-it/bot', async (req, res) => {
    try {
        const { limit = 30 } = req.query;
        const results = await bondScraper.searchBonds('gov-it-bot', parseInt(limit));
        res.json({ success: true, ...results });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/gov-it/cct', async (req, res) => {
    try {
        const { limit = 30 } = req.query;
        const results = await bondScraper.searchBonds('gov-it-cct', parseInt(limit));
        res.json({ success: true, ...results });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/gov-it/ctz', async (req, res) => {
    try {
        const { limit = 30 } = req.query;
        const results = await bondScraper.searchBonds('gov-it-ctz', parseInt(limit));
        res.json({ success: true, ...results });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ===================================
// QUICK ACCESS - GOVERNATIVI EU
// ===================================
router.get('/gov-eu/germany', async (req, res) => {
    try {
        const { limit = 30 } = req.query;
        const results = await bondScraper.searchBonds('gov-eu-germany', parseInt(limit));
        res.json({ success: true, ...results });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/gov-eu/france', async (req, res) => {
    try {
        const { limit = 30 } = req.query;
        const results = await bondScraper.searchBonds('gov-eu-france', parseInt(limit));
        res.json({ success: true, ...results });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/gov-eu/spain', async (req, res) => {
    try {
        const { limit = 30 } = req.query;
        const results = await bondScraper.searchBonds('gov-eu-spain', parseInt(limit));
        res.json({ success: true, ...results });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ===================================
// SOVRANAZIONALI
// ===================================
router.get('/supranational', async (req, res) => {
    try {
        const { limit = 30 } = req.query;
        const results = await bondScraper.searchBonds('supranational', parseInt(limit));
        res.json({ success: true, ...results });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ===================================
// CORPORATE
// ===================================
router.get('/corporate', async (req, res) => {
    try {
        const { limit = 30 } = req.query;
        const results = await bondScraper.searchBonds('corporate-all', parseInt(limit));
        res.json({ success: true, ...results });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ===================================
// CACHE MANAGEMENT
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
        console.log('[BONDS] Running comprehensive test...');
        
        const testResults = {
            timestamp: new Date().toISOString(),
            tests: {}
        };
        
        // Test 1: Categories
        try {
            const categories = bondScraper.getCategories();
            testResults.tests.categories = {
                status: 'OK',
                count: categories.length
            };
        } catch (e) {
            testResults.tests.categories = { status: 'ERROR', error: e.message };
        }
        
        // Test 2: BTP
        try {
            const btps = await bondScraper.searchBonds('gov-it-btp', 3);
            testResults.tests.btp = {
                status: btps.bonds.length > 0 ? 'OK' : 'FAIL',
                bonds_found: btps.count,
                sample: btps.bonds[0] || null
            };
        } catch (e) {
            testResults.tests.btp = { status: 'ERROR', error: e.message };
        }
        
        // Test 3: Germania
        try {
            const germany = await bondScraper.searchBonds('gov-eu-germany', 3);
            testResults.tests.germany = {
                status: germany.bonds.length > 0 ? 'OK' : 'FAIL',
                bonds_found: germany.count
            };
        } catch (e) {
            testResults.tests.germany = { status: 'ERROR', error: e.message };
        }
        
        // Test 4: Cache
        testResults.tests.cache = bondScraper.getCacheStats();
        
        res.json({
            success: true,
            ...testResults
        });
        
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

module.exports = router;
