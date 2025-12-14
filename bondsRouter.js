/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Bonds Router - SIMPLE VERSION
 * Matches the structure created by bonds-scraper-simple.js
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
 * GET /api/bonds/search
 */
router.get('/search', async (req, res) => {
    try {
        const category = req.query.category;
        const limit = parseInt(req.query.limit) || 100;
        const offset = parseInt(req.query.offset) || 0;

        console.log(`[BondsRouter] Search with category: ${category}, limit: ${limit}`);
        console.log(`[BondsRouter] Query params:`, JSON.stringify(req.query));

        if (!category) {
            return res.status(400).json({
                success: false,
                error: 'Category parameter required'
            });
        }

        const bondsData = await loadBondsData();
        
        if (!bondsData) {
            return res.status(500).json({
                success: false,
                error: 'Bonds data not available'
            });
        }

        // Get bonds directly from the category
        let bonds = [];
        let categoryName = '';
        let categoryDescription = '';
        
        if (bondsData.categories && bondsData.categories[category]) {
            const cat = bondsData.categories[category];
            bonds = cat.bonds || [];
            categoryName = cat.name || category;
            categoryDescription = cat.description || '';
        } else {
            // Category not found - return empty
            console.log(`[BondsRouter] Category '${category}' not found`);
            categoryName = category;
            categoryDescription = 'Category not found';
        }

        // Apply filters
        const yieldMin = parseFloat(req.query.yield_min || req.query.yieldMin);
        const yieldMax = parseFloat(req.query.yield_max || req.query.yieldMax);
        
        if (!isNaN(yieldMin)) {
            bonds = bonds.filter(b => b.yield >= yieldMin);
            console.log(`[BondsRouter] Applied yield_min filter: >= ${yieldMin}%`);
        }
        
        if (!isNaN(yieldMax)) {
            bonds = bonds.filter(b => b.yield <= yieldMax);
            console.log(`[BondsRouter] Applied yield_max filter: <= ${yieldMax}%`);
        }

        // Apply sorting
        const sortBy = req.query.sort_by || req.query.sortBy || 'yield_desc';
        const normalizedSortBy = sortBy.replace(/-/g, '_');
        
        switch(normalizedSortBy) {
            case 'yield_desc':
                bonds.sort((a, b) => (b.yield || 0) - (a.yield || 0));
                break;
            case 'yield_asc':
                bonds.sort((a, b) => (a.yield || 0) - (b.yield || 0));
                break;
            case 'maturity_asc':
                bonds.sort((a, b) => (a.maturity || '').localeCompare(b.maturity || ''));
                break;
            case 'maturity_desc':
                bonds.sort((a, b) => (b.maturity || '').localeCompare(a.maturity || ''));
                break;
            case 'price_asc':
                bonds.sort((a, b) => (a.price || 0) - (b.price || 0));
                break;
            case 'price_desc':
                bonds.sort((a, b) => (b.price || 0) - (a.price || 0));
                break;
        }

        console.log(`[BondsRouter] Filters - yieldMin: ${yieldMin}, yieldMax: ${yieldMax}, sortBy: ${normalizedSortBy}`);
        console.log(`[BondsRouter] Applied sorting: ${normalizedSortBy}`);

        // Apply pagination
        const totalBonds = bonds.length;
        const paginatedBonds = bonds.slice(offset, offset + limit);

        console.log(`[BondsRouter] Returning ${paginatedBonds.length} bonds from ${totalBonds} total for ${category}`);

        res.json({
            success: true,
            category: {
                id: category,
                name: categoryName,
                description: categoryDescription
            },
            bonds: paginatedBonds,
            filters: {
                yieldMin: !isNaN(yieldMin) ? yieldMin : null,
                yieldMax: !isNaN(yieldMax) ? yieldMax : null,
                sortBy: normalizedSortBy
            },
            pagination: {
                total: totalBonds,
                limit: limit,
                offset: offset,
                returned: paginatedBonds.length
            },
            lastUpdate: bondsData.lastUpdate
        });

    } catch (error) {
        console.error('[BondsRouter] Error in /search:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * GET /api/bonds/categories
 * Return list of all available categories
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

        const categories = [];
        
        if (bondsData.categories) {
            Object.keys(bondsData.categories).forEach(catId => {
                const cat = bondsData.categories[catId];
                categories.push({
                    id: catId,
                    name: cat.name,
                    description: cat.description,
                    count: cat.count || (cat.bonds ? cat.bonds.length : 0)
                });
            });
        }

        res.json({
            success: true,
            categories: categories,
            statistics: bondsData.statistics || {},
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

module.exports = router;
