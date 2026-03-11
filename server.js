/**
 * Copyright (c) 2024-2026 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ISIN Research Backend - Multi-Source Financial Data API
 * Version: 6.0.0 - With Stripe Payments
 */

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(cors());

// JSON parsing - skip for Stripe webhook (needs raw body)
app.use((req, res, next) => {
    if (req.originalUrl === '/api/stripe/webhook') {
        next();
    } else {
        express.json({ limit: '5mb' })(req, res, next);
    }
});

// Request logging
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
    next();
});

// Health check
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        version: '6.0.0',
        endpoints: {
            financial: '/api/financial',
            certificates: '/api/certificates',
            bonds: '/api/bonds',
            ai: '/api/ai',
            stripe: '/api/stripe',
            health: '/health'
        }
    });
});

// ===================================
// LOAD AI MODULE
// ===================================

let aiRoutes;
try {
    aiRoutes = require('./ai');
    app.use('/api/ai', aiRoutes);
    console.log('✅ AI module loaded');
} catch (error) {
    console.warn('⚠️  AI module not found:', error.message);
}

// ===================================
// LOAD STRIPE MODULE
// ===================================

let stripeRoutes;
try {
    stripeRoutes = require('./stripe');
    app.use('/api/stripe', stripeRoutes);
    console.log('✅ Stripe module loaded');
} catch (error) {
    console.warn('⚠️  Stripe module not loaded:', error.message);
}

// ===================================
// LOAD FINANCIAL MODULE
// ===================================

let financialRoutes;
try {
    financialRoutes = require('./financial');
    app.use('/api/financial', financialRoutes);
    console.log('✅ Financial module loaded');
} catch (error) {
    console.warn('⚠️  Financial module not found:', error.message);
}

// ===================================
// LOAD CERTIFICATES MODULE
// ===================================

let certificatesRoutes;
try {
    certificatesRoutes = require('./certificates');
    app.use('/api/certificates', certificatesRoutes);
    console.log('✅ Certificates module loaded');
} catch (error) {
    console.warn('⚠️  Certificates module not found:', error.message);
}

// ===================================
// LOAD BONDS MODULE (with fallback)
// ===================================

let bondsRoutes;
try {
    bondsRoutes = require('./bonds');
    app.use('/api/bonds', bondsRoutes);
    console.log('✅ Bonds module loaded');
} catch (error) {
    console.log('📦 Bonds module not found, using JSON fallback');
    
    const bondsRouter = express.Router();
    let bondsData = [];
    let categoriesData = {};
    
    try {
        const dataPath = path.join(__dirname, 'bonds-data.json');
        if (fs.existsSync(dataPath)) {
            const rawData = fs.readFileSync(dataPath, 'utf8');
            const data = JSON.parse(rawData);
            
            if (data.bonds) {
                bondsData = data.bonds;
                console.log(`✅ Loaded ${bondsData.length} bonds from JSON`);
            } else if (data.categories) {
                categoriesData = data.categories;
                Object.keys(categoriesData).forEach(catKey => {
                    const category = categoriesData[catKey];
                    if (category.bonds && Array.isArray(category.bonds)) {
                        bondsData = bondsData.concat(category.bonds);
                    }
                });
                console.log(`✅ Loaded ${bondsData.length} bonds from ${Object.keys(categoriesData).length} categories`);
            }
        } else {
            console.warn('⚠️  bonds-data.json not found');
        }
    } catch (error) {
        console.error('❌ Error loading bonds data:', error.message);
    }
    
    bondsRouter.get('/', (req, res) => {
        const { limit = 100, category } = req.query;
        let filtered = bondsData;
        if (category && categoriesData[category]) {
            filtered = categoriesData[category].bonds || [];
        }
        const limited = filtered.slice(0, parseInt(limit));
        res.json({ success: true, count: limited.length, total: filtered.length, bonds: limited });
    });
    
    bondsRouter.get('/:isin', (req, res) => {
        const { isin } = req.params;
        const bond = bondsData.find(b => b.isin && b.isin.toUpperCase() === isin.toUpperCase());
        if (!bond) {
            return res.status(404).json({ success: false, error: 'Bond not found', isin });
        }
        res.json({ success: true, bond });
    });
    
    bondsRouter.get('/search', (req, res) => {
        const { q } = req.query;
        if (!q) return res.status(400).json({ success: false, error: 'Query "q" required' });
        const query = q.toLowerCase();
        const results = bondsData.filter(b =>
            (b.isin && b.isin.toLowerCase().includes(query)) ||
            (b.name && b.name.toLowerCase().includes(query)) ||
            (b.type && b.type.toLowerCase().includes(query))
        );
        res.json({ success: true, count: results.length, query: q, bonds: results.slice(0, 50) });
    });
    
    bondsRouter.get('/meta/categories', (req, res) => {
        const categories = Object.keys(categoriesData).map(key => ({
            id: key,
            name: categoriesData[key].name || key,
            description: categoriesData[key].description || '',
            count: categoriesData[key].bonds ? categoriesData[key].bonds.length : 0
        }));
        res.json({ success: true, count: categories.length, categories });
    });
    
    app.use('/api/bonds', bondsRouter);
}

// ===================================
// 404 HANDLER
// ===================================

app.use((req, res) => {
    res.status(404).json({ success: false, error: 'Endpoint not found', path: req.path });
});

// ===================================
// ERROR HANDLER
// ===================================

app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    res.status(500).json({ success: false, error: 'Internal server error', message: err.message });
});

// ===================================
// START SERVER
// ===================================

app.listen(PORT, () => {
    console.log('='.repeat(60));
    console.log('ISIN Research Backend v6.0.0 - With Stripe');
    console.log('Copyright (c) 2024-2026 Mutna S.R.L.S.');
    console.log('='.repeat(60));
    console.log(`Server running on port ${PORT}`);
    console.log('Modules:');
    console.log(`  Financial:    ${financialRoutes ? '✅' : '⚠️  Not found'}`);
    console.log(`  Certificates: ${certificatesRoutes ? '✅' : '⚠️  Not found'}`);
    console.log(`  Bonds:        ${bondsRoutes ? '✅ Module' : '📦 JSON fallback'}`);
    console.log(`  AI:           ${aiRoutes ? '✅' : '⚠️  Not found'}`);
    console.log(`  Stripe:       ${stripeRoutes ? '✅' : '⚠️  Not configured'}`);
    console.log('='.repeat(60));
});

module.exports = app;
