/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Bonds Router - Backend Endpoints for Bonds Data
 * MATCHES FRONTEND EXPECTATIONS: /api/bonds/search?category=XXX
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
 * Filter bonds by country code
 */
function filterByCountry(bonds, countryCode) {
    return bonds.filter(bond => bond.country === countryCode);
}

/**
 * Filter bonds by type
 */
function filterByType(bonds, type) {
    return bonds.filter(bond => bond.type === type);
}

/**
 * GET /api/bonds/search
 * Main endpoint that frontend uses with ?category= query parameter
 */
router.get('/search', async (req, res) => {
    try {
        const category = req.query.category;
        const limit = parseInt(req.query.limit) || 100;
        const offset = parseInt(req.query.offset) || 0;

        console.log(`[BondsRouter] Search with category: ${category}, limit: ${limit}`);

        if (!category) {
            return res.status(400).json({
                success: false,
                error: 'Category parameter required',
                usage: '/api/bonds/search?category=gov-it-btp&limit=100'
            });
        }

        const bondsData = await loadBondsData();
        
        if (!bondsData) {
            return res.status(500).json({
                success: false,
                error: 'Bonds data not available'
            });
        }

        let bonds = [];
        let categoryName = '';
        let categoryDescription = '';

        // Map frontend category to backend data with filtering
        switch(category) {
            // Italian Government Bonds
            case 'gov-it-btp':
                bonds = filterByType(bondsData.categories['it-governativi'].bonds, 'BTP');
                categoryName = 'BTP - Buoni Tesoro Poliennali';
                categoryDescription = 'Titoli di Stato italiani a medio-lungo termine';
                break;
            
            case 'gov-it-bot':
                bonds = filterByType(bondsData.categories['it-governativi'].bonds, 'BOT');
                categoryName = 'BOT - Buoni Ordinari Tesoro';
                categoryDescription = 'Titoli di Stato italiani a breve termine';
                break;
            
            case 'gov-it-cct':
                bonds = filterByType(bondsData.categories['it-governativi'].bonds, 'CCT');
                categoryName = 'CCT - Certificati Credito Tesoro';
                categoryDescription = 'Titoli di Stato italiani a tasso variabile';
                break;
            
            case 'gov-it-ctz':
                bonds = filterByType(bondsData.categories['it-governativi'].bonds, 'CTZ');
                categoryName = 'CTZ - Certificati Tesoro Zero Coupon';
                categoryDescription = 'Titoli di Stato italiani zero coupon';
                break;

            // EU Government Bonds by Country
            case 'gov-eu-germany':
                bonds = filterByCountry(bondsData.categories['eu-governativi'].bonds, 'DE');
                categoryName = 'Bund Germania';
                categoryDescription = 'Titoli di Stato tedeschi';
                break;
            
            case 'gov-eu-france':
                bonds = filterByCountry(bondsData.categories['eu-governativi'].bonds, 'FR');
                categoryName = 'OAT Francia';
                categoryDescription = 'Titoli di Stato francesi';
                break;
            
            case 'gov-eu-spain':
                bonds = filterByCountry(bondsData.categories['eu-governativi'].bonds, 'ES');
                categoryName = 'Bonos Spagna';
                categoryDescription = 'Titoli di Stato spagnoli';
                break;
            
            case 'gov-eu-netherlands':
                bonds = filterByCountry(bondsData.categories['eu-governativi'].bonds, 'NL');
                categoryName = 'DSL Paesi Bassi';
                categoryDescription = 'Titoli di Stato olandesi';
                break;
            
            case 'gov-eu-belgium':
                bonds = filterByCountry(bondsData.categories['eu-governativi'].bonds, 'BE');
                categoryName = 'OLO Belgio';
                categoryDescription = 'Titoli di Stato belgi';
                break;
            
            case 'gov-eu-austria':
                bonds = filterByCountry(bondsData.categories['eu-governativi'].bonds, 'AT');
                categoryName = 'RAGB Austria';
                categoryDescription = 'Titoli di Stato austriaci';
                break;
            
            case 'gov-eu-portugal':
                bonds = filterByCountry(bondsData.categories['eu-governativi'].bonds, 'PT');
                categoryName = 'PGB Portogallo';
                categoryDescription = 'Titoli di Stato portoghesi';
                break;
            
            case 'gov-eu-ireland':
                bonds = filterByCountry(bondsData.categories['eu-governativi'].bonds, 'IE');
                categoryName = 'IRISH Irlanda';
                categoryDescription = 'Titoli di Stato irlandesi';
                break;
            
            case 'gov-eu-finland':
                bonds = filterByCountry(bondsData.categories['eu-governativi'].bonds, 'FI');
                categoryName = 'FINNISH Finlandia';
                categoryDescription = 'Titoli di Stato finlandesi';
                break;

            // Supranational
            case 'supranational':
                bonds = bondsData.categories['sovranazionali'].bonds;
                categoryName = 'Sovranazionali';
                categoryDescription = 'BEI, EFSF, ESM';
                break;

            // Corporate
            case 'corporate-all':
                bonds = bondsData.categories['corporate'].bonds;
                categoryName = 'Corporate';
                categoryDescription = 'Obbligazioni societarie';
                break;

            default:
                console.log(`[BondsRouter] Unknown category: ${category}`);
                return res.status(404).json({
                    success: false,
                    error: `Unknown category: ${category}`,
                    availableCategories: [
                        'gov-it-btp', 'gov-it-bot', 'gov-it-cct', 'gov-it-ctz',
                        'gov-eu-germany', 'gov-eu-france', 'gov-eu-spain',
                        'gov-eu-netherlands', 'gov-eu-belgium', 'gov-eu-austria',
                        'gov-eu-portugal', 'gov-eu-ireland', 'gov-eu-finland',
                        'supranational', 'corporate-all'
                    ]
                });
        }

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
