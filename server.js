/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend - Multi-Source Financial Data API
 * Version: 2.2
 */

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(cors());
app.use(express.json());

// Request logging
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
    next();
});

// ===================================
// LOAD MODULES WITH FALLBACK
// ===================================

let financialModule = null;
let certificatesModule = null;
let bondsModule = null;

// Try to load Financial module (stocks, quotes, etc)
try {
    financialModule = require('./financial');
    console.log('✅ Financial module loaded from root');
} catch (error) {
    console.warn('⚠️  Financial module not found:', error.message);
}

// Try to load Certificates module
try {
    certificatesModule = require('./certificates');
    console.log('✅ Certificates module loaded from root');
} catch (error) {
    console.warn('⚠️  Certificates module not found:', error.message);
}

// Try to load Bonds module (if exists)
try {
    bondsModule = require('./bonds');
    console.log('✅ Bonds module loaded from root');
} catch (error) {
    // Bonds module is optional, try inline fallback
    try {
        const bondsDataPath = path.join(__dirname, 'bonds-data.json');
        if (fs.existsSync(bondsDataPath)) {
            console.log('✅ Using bonds-data.json fallback');
            bondsModule = createBondsModuleFromJSON(bondsDataPath);
        } else {
            console.warn('⚠️  Bonds module not found (optional)');
        }
    } catch (fallbackError) {
        console.warn('⚠️  Bonds data not available');
    }
}

// ===================================
// HEALTH CHECK
// ===================================

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        version: '2.2.0',
        modules: {
            financial: financialModule ? 'loaded' : 'not available',
            certificates: certificatesModule ? 'loaded' : 'not available',
            bonds: bondsModule ? 'loaded' : 'not available'
        }
    });
});

// ===================================
// API ROUTES
// ===================================

// Financial API (stocks, quotes, charts)
if (financialModule) {
    app.use('/api/financial', financialModule);
} else {
    app.use('/api/financial', (req, res) => {
        res.status(503).json({
            success: false,
            error: 'Financial module not available',
            message: 'Please check server configuration'
        });
    });
}

// Certificates API
if (certificatesModule) {
    app.use('/api/certificates', certificatesModule);
} else {
    app.use('/api/certificates', (req, res) => {
        res.status(503).json({
            success: false,
            error: 'Certificates module not available',
            message: 'Please check server configuration'
        });
    });
}

// Bonds API
if (bondsModule) {
    app.use('/api/bonds', bondsModule);
} else {
    app.use('/api/bonds', (req, res) => {
        res.status(503).json({
            success: false,
            error: 'Bonds module not available',
            message: 'Bonds data not found'
        });
    });
}

// ===================================
// ERROR HANDLERS
// ===================================

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        success: false,
        error: 'Endpoint not found',
        path: req.path
    });
});

// Global error handler
app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    res.status(500).json({
        success: false,
        error: 'Internal server error',
        message: err.message
    });
});

// ===================================
// START SERVER
// ===================================

app.listen(PORT, () => {
    console.log('='.repeat(60));
    console.log('ISIN Research Backend - Multi-Source v2.2');
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S.');
    console.log('='.repeat(60));
    console.log(`Server running on port ${PORT}`);
    console.log(`Health check: http://localhost:${PORT}/health`);
    console.log(`API Financial: http://localhost:${PORT}/api/financial`);
    console.log(`API Certificates: http://localhost:${PORT}/api/certificates`);
    console.log(`API Bonds: http://localhost:${PORT}/api/bonds`);
    console.log('='.repeat(60));
    console.log('Modules loaded:');
    console.log(`  Financial: ${financialModule ? '✅ Active' : '⚠️  Not found (optional)'}`);
    console.log(`  Certificates: ${certificatesModule ? '✅ Module mode' : '⚠️  Not found'}`);
    console.log(`  Bonds: ${bondsModule ? '✅ Active' : '⚠️  Not found (optional)'}`);
    console.log('='.repeat(60));
});

// ===================================
// UTILITY: Create Bonds Module from JSON
// ===================================

function createBondsModuleFromJSON(dataPath) {
    const express = require('express');
    const router = express.Router();
    
    let allBonds = [];
    let categoriesData = {};
    
    try {
        const rawData = fs.readFileSync(dataPath, 'utf8');
        const data = JSON.parse(rawData);
        
        // Check structure type
        if (data.bonds) {
            // Simple structure: { bonds: [...] }
            allBonds = data.bonds;
            console.log(`✅ Loaded ${allBonds.length} bonds (simple structure)`);
        } else if (data.categories) {
            // Categories structure: { categories: { cat1: { bonds: [...] }, ... } }
            categoriesData = data.categories;
            
            // Flatten all bonds from all categories
            Object.keys(categoriesData).forEach(catKey => {
                const category = categoriesData[catKey];
                if (category.bonds && Array.isArray(category.bonds)) {
                    allBonds = allBonds.concat(category.bonds);
                }
            });
            
            console.log(`✅ Loaded ${allBonds.length} bonds from ${Object.keys(categoriesData).length} categories`);
        } else {
            console.warn('⚠️  Unknown bonds data structure');
        }
    } catch (error) {
        console.error('❌ Error loading bonds data:', error.message);
    }
    
    // GET all bonds
    router.get('/', (req, res) => {
        const { category, limit = 100 } = req.query;
        
        let filtered = allBonds;
        
        // Filter by category if specified
        if (category && categoriesData[category]) {
            filtered = categoriesData[category].bonds || [];
        }
        
        // Apply limit
        const limited = filtered.slice(0, parseInt(limit));
        
        res.json({
            success: true,
            count: limited.length,
            total: filtered.length,
            bonds: limited
        });
    });
    
    // GET bond by ISIN
    router.get('/:isin', (req, res) => {
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
    });
    
    // Search bonds
    router.get('/search', (req, res) => {
        const { q } = req.query;
        
        if (!q) {
            return res.status(400).json({
                success: false,
                error: 'Query parameter "q" is required'
            });
        }
        
        const query = q.toLowerCase();
        const results = allBonds.filter(b => {
            return (
                (b.isin && b.isin.toLowerCase().includes(query)) ||
                (b.name && b.name.toLowerCase().includes(query)) ||
                (b.issuer && b.issuer.toLowerCase().includes(query)) ||
                (b.type && b.type.toLowerCase().includes(query))
            );
        });
        
        res.json({
            success: true,
            count: results.length,
            query: q,
            bonds: results.slice(0, 50)
        });
    });
    
    // GET categories
    router.get('/meta/categories', (req, res) => {
        const categories = Object.keys(categoriesData).map(key => ({
            id: key,
            name: categoriesData[key].name || key,
            description: categoriesData[key].description || '',
            count: categoriesData[key].bonds ? categoriesData[key].bonds.length : 0
        }));
        
        res.json({
            success: true,
            count: categories.length,
            categories: categories
        });
    });
    
    return router;
}

module.exports = app;
