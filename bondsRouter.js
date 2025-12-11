/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Bonds Router - Backend Endpoints for Bonds Data
 * WITH FULL RETROCOMPATIBILITY FOR OLD CATEGORY NAMES
 */

const express = require('express');
const fs = require('fs').promises;
const path = require('path');

const router = express.Router();

// Path to bonds data file
const BONDS_DATA_PATH = path.join(__dirname, 'data', 'bonds-data.json');

// RETROCOMPATIBILITY MAPPING: old names → new names
const CATEGORY_MAPPING = {
    // Old names (from frontend)
    'it-btp': 'it-governativi',
    'it-bot': 'it-governativi',
    'it-cct': 'it-governativi',
    'it-ctz': 'it-governativi',
    'gov-it-btp': 'it-governativi',
    
    'eu-governativi-europa': 'eu-governativi',
    'gov-eu': 'eu-governativi',
    
    'sovranazionali': 'sovranazionali',  // Already correct
    'supranational': 'sovranazionali',
    
    'corporate': 'corporate',  // Already correct
    
    // New names (if called directly)
    'it-governativi': 'it-governativi',
    'eu-governativi': 'eu-governativi'
};

/**
 * Normalize category name using mapping
 */
function normalizeCategory(category) {
    const normalized = CATEGORY_MAPPING[category.toLowerCase()];
    if (!normalized) {
        return null;  // Invalid category
    }
    return normalized;
}

/**
 * Load bonds data from file
 */
async function loadBondsData() {
    try {
        const data = await fs.readFile(BONDS_DATA_PATH, 'utf8');
        return JSON.parse(data);
    } catch (error) {
        console.error('[BondsRouter] Error loading bonds data:', error.message);
        return null;
    }
}

/**
 * GET /api/bonds/categories
 * Get list of bond categories with counts
 */
router.get('/categories', async (req, res) => {
    try {
        const bondsData = await loadBondsData();
        
        if (!bondsData) {
            return res.status(500).json({
                success: false,
                error: 'Bonds data not available'
            });
        }

        const categories = Object.entries(bondsData.categories).map(([key, data]) => ({
            id: key,
            name: data.name,
            description: data.description,
            count: data.count || data.bonds.length
        }));

        res.json({
            success: true,
            categories: categories,
            lastUpdate: bondsData.lastUpdate
        });

    } catch (error) {
        console.error('[BondsRouter] Error in /categories:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * GET /api/bonds/stats
 * Get bonds statistics
 */
router.get('/stats', async (req, res) => {
    try {
        const bondsData = await loadBondsData();
        
        if (!bondsData) {
            return res.status(500).json({
                success: false,
                error: 'Bonds data not available'
            });
        }

        res.json({
            success: true,
            statistics: bondsData.statistics,
            lastUpdate: bondsData.lastUpdate,
            categoryMapping: CATEGORY_MAPPING  // Include mapping for reference
        });

    } catch (error) {
        console.error('[BondsRouter] Error in /stats:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * GET /api/bonds/search (without ISIN)
 * Returns help message about how to use search endpoint
 */
router.get('/search', async (req, res) => {
    console.log('[BondsRouter] Search endpoint called without ISIN');
    
    res.status(400).json({
        success: false,
        error: 'ISIN required for search',
        usage: 'Use /api/bonds/search/:isin to search by ISIN',
        example: '/api/bonds/search/IT0005508251',
        hint: 'To get all bonds in a category, use /api/bonds/:category?limit=100',
        availableCategories: [
            'it-governativi (or it-btp, it-bot, it-cct, it-ctz)',
            'eu-governativi (or eu-governativi-europa)',
            'sovranazionali (or supranational)',
            'corporate'
        ]
    });
});

/**
 * GET /api/bonds/search/:isin
 * Search bond by ISIN
 */
router.get('/search/:isin', async (req, res) => {
    try {
        const { isin } = req.params;
        
        console.log(`[BondsRouter] Searching for ISIN: ${isin}`);

        const bondsData = await loadBondsData();
        
        if (!bondsData) {
            return res.status(500).json({
                success: false,
                error: 'Bonds data not available'
            });
        }

        // Search across all categories
        let foundBond = null;
        let foundCategory = null;

        for (const [categoryKey, categoryData] of Object.entries(bondsData.categories)) {
            const bond = categoryData.bonds.find(b => b.isin === isin.toUpperCase());
            if (bond) {
                foundBond = bond;
                foundCategory = categoryKey;
                break;
            }
        }

        if (!foundBond) {
            console.log(`[BondsRouter] ISIN not found: ${isin}`);
            return res.status(404).json({
                success: false,
                error: `Bond with ISIN '${isin}' not found`
            });
        }

        console.log(`[BondsRouter] Found bond in category: ${foundCategory}`);

        res.json({
            success: true,
            bond: foundBond,
            category: foundCategory,
            lastUpdate: bondsData.lastUpdate
        });

    } catch (error) {
        console.error('[BondsRouter] Error in /search/:isin:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * GET /api/bonds/:category
 * Get all bonds in a specific category
 * Query params: limit (default 100), offset (default 0)
 * SUPPORTS OLD AND NEW CATEGORY NAMES
 * 
 * NOTE: This MUST be the last route because it's a catch-all!
 */
router.get('/:category', async (req, res) => {
    try {
        const { category } = req.params;
        const limit = parseInt(req.query.limit) || 100;
        const offset = parseInt(req.query.offset) || 0;

        console.log(`[BondsRouter] Request for category: ${category}`);

        // Normalize category name (old → new)
        const normalizedCategory = normalizeCategory(category);
        
        if (!normalizedCategory) {
            return res.status(404).json({
                success: false,
                error: `Unknown category '${category}'`,
                hint: 'Available categories: it-governativi, eu-governativi, sovranazionali, corporate',
                retrocompatible: 'Also accepts: it-btp, it-bot, it-cct, it-ctz, gov-it-btp, eu-governativi-europa, gov-eu, supranational'
            });
        }

        console.log(`[BondsRouter] Normalized to: ${normalizedCategory}`);

        const bondsData = await loadBondsData();
        
        if (!bondsData) {
            return res.status(500).json({
                success: false,
                error: 'Bonds data not available'
            });
        }

        const categoryData = bondsData.categories[normalizedCategory];
        
        if (!categoryData) {
            return res.status(404).json({
                success: false,
                error: `Category '${normalizedCategory}' not found in database`,
                availableCategories: Object.keys(bondsData.categories)
            });
        }

        // Pagination
        const totalBonds = categoryData.bonds.length;
        const paginatedBonds = categoryData.bonds.slice(offset, offset + limit);

        console.log(`[BondsRouter] Returning ${paginatedBonds.length} bonds from ${totalBonds} total`);

        res.json({
            success: true,
            category: {
                id: normalizedCategory,
                requestedAs: category,  // Show what user requested
                name: categoryData.name,
                description: categoryData.description
            },
            bonds: paginatedBonds,
            pagination: {
                total: totalBonds,
                limit: limit,
                offset: offset,
                returned: paginatedBonds.length
            },
            lastUpdate: bondsData.lastUpdate
        });

    } catch (error) {
        console.error('[BondsRouter] Error in /:category:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

module.exports = router;
