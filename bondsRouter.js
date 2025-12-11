/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Bonds Router - Backend Endpoints for Bonds Data
 */

const express = require('express');
const fs = require('fs').promises;
const path = require('path');

const router = express.Router();

// Path to bonds data file
const BONDS_DATA_PATH = path.join(__dirname, 'data', 'bonds-data.json');

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
 * GET /api/bonds/:category
 * Get all bonds in a specific category
 * Query params: limit (default 100), offset (default 0)
 */
router.get('/:category', async (req, res) => {
    try {
        const { category } = req.params;
        const limit = parseInt(req.query.limit) || 100;
        const offset = parseInt(req.query.offset) || 0;

        const bondsData = await loadBondsData();
        
        if (!bondsData) {
            return res.status(500).json({
                success: false,
                error: 'Bonds data not available'
            });
        }

        const categoryData = bondsData.categories[category];
        
        if (!categoryData) {
            return res.status(404).json({
                success: false,
                error: `Category '${category}' not found`,
                availableCategories: Object.keys(bondsData.categories)
            });
        }

        // Pagination
        const totalBonds = categoryData.bonds.length;
        const paginatedBonds = categoryData.bonds.slice(offset, offset + limit);

        res.json({
            success: true,
            category: {
                id: category,
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

/**
 * GET /api/bonds/search/:isin
 * Search bond by ISIN
 */
router.get('/search/:isin', async (req, res) => {
    try {
        const { isin } = req.params;

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
            return res.status(404).json({
                success: false,
                error: `Bond with ISIN '${isin}' not found`
            });
        }

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
            lastUpdate: bondsData.lastUpdate
        });

    } catch (error) {
        console.error('[BondsRouter] Error in /stats:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

module.exports = router;
