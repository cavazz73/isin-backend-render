/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Certificates API - v3.0 NO FILTER
 * Serves ALL certificates without aggressive filtering
 */

const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');

// ===================================
// DATA STORAGE
// ===================================

let certificatesData = {
    metadata: {},
    certificates: []
};

// ===================================
// LOAD DATA FUNCTION
// ===================================

function loadCertificatesData() {
    try {
        // Try multiple paths
        const paths = [
            path.join(__dirname, 'data', 'certificates-data.json'),
            path.join(__dirname, 'certificates-data.json'),
            './data/certificates-data.json',
            './certificates-data.json'
        ];
        
        let loaded = false;
        
        for (const dataPath of paths) {
            try {
                if (fs.existsSync(dataPath)) {
                    console.log(`[CERTIFICATES] Trying path: ${dataPath}`);
                    const rawData = fs.readFileSync(dataPath, 'utf8');
                    const parsed = JSON.parse(rawData);
                    
                    // Get certificates array - NO FILTERING!
                    const certs = parsed.certificates || [];
                    
                    certificatesData = {
                        metadata: parsed.metadata || parsed,
                        certificates: certs
                    };
                    
                    console.log(`✅ [CERTIFICATES] Loaded ${certs.length} certificates from ${dataPath}`);
                    
                    // Log stats
                    const issuers = [...new Set(certs.map(c => c.issuer).filter(i => i))];
                    console.log(`   Issuers: ${issuers.slice(0, 5).join(', ')}${issuers.length > 5 ? '...' : ''}`);
                    
                    const withScenario = certs.filter(c => c.scenario_analysis).length;
                    console.log(`   With scenario analysis: ${withScenario}/${certs.length}`);
                    
                    loaded = true;
                    break;
                }
            } catch (pathError) {
                console.log(`   Path ${dataPath} failed: ${pathError.message}`);
            }
        }
        
        if (!loaded) {
            console.warn('⚠️ [CERTIFICATES] No data file found');
        }
        
    } catch (error) {
        console.error('❌ [CERTIFICATES] Error loading data:', error.message);
    }
}

// Load on startup
loadCertificatesData();

// Reload every 6 hours
setInterval(loadCertificatesData, 6 * 60 * 60 * 1000);

// ===================================
// GET ALL CERTIFICATES
// ===================================

router.get('/', (req, res) => {
    try {
        const { 
            type, 
            issuer, 
            minYield, 
            maxYield, 
            minBarrier, 
            maxBarrier,
            category,
            limit = 500 
        } = req.query;
        
        let filtered = [...certificatesData.certificates];
        
        // Apply filters only if specified and not default
        if (type && type !== 'Tutti i tipi' && type !== '') {
            filtered = filtered.filter(c => 
                c.type && c.type.toLowerCase().includes(type.toLowerCase())
            );
        }
        
        if (issuer && issuer !== 'Tutti gli emittenti' && issuer !== '') {
            filtered = filtered.filter(c => 
                c.issuer && c.issuer.toLowerCase().includes(issuer.toLowerCase())
            );
        }
        
        if (category && category !== 'all' && category !== '') {
            filtered = filtered.filter(c => 
                c.underlying_category && c.underlying_category.toLowerCase() === category.toLowerCase()
            );
        }
        
        if (minYield && parseFloat(minYield) > 0) {
            filtered = filtered.filter(c => 
                (c.annual_coupon_yield || 0) >= parseFloat(minYield)
            );
        }
        
        if (maxYield && parseFloat(maxYield) < 100) {
            filtered = filtered.filter(c => 
                (c.annual_coupon_yield || 100) <= parseFloat(maxYield)
            );
        }
        
        if (minBarrier && parseFloat(minBarrier) > 0) {
            filtered = filtered.filter(c => 
                (c.barrier_down || c.barrier || 0) >= parseFloat(minBarrier)
            );
        }
        
        if (maxBarrier && parseFloat(maxBarrier) < 100) {
            filtered = filtered.filter(c => 
                (c.barrier_down || c.barrier || 100) <= parseFloat(maxBarrier)
            );
        }
        
        // Apply limit
        const limited = filtered.slice(0, parseInt(limit));
        
        res.json({
            success: true,
            count: limited.length,
            total: certificatesData.certificates.length,
            metadata: certificatesData.metadata,
            certificates: limited
        });
        
    } catch (error) {
        console.error('[CERTIFICATES] Error:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            certificates: []
        });
    }
});

// ===================================
// META ENDPOINTS (must be before /:isin)
// ===================================

router.get('/meta/types', (req, res) => {
    try {
        const types = [...new Set(
            certificatesData.certificates
                .map(c => c.type)
                .filter(t => t)
        )].sort();
        
        res.json({
            success: true,
            count: types.length,
            types: types
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message, types: [] });
    }
});

router.get('/meta/issuers', (req, res) => {
    try {
        const issuers = [...new Set(
            certificatesData.certificates
                .map(c => c.issuer)
                .filter(i => i)
        )].sort();
        
        res.json({
            success: true,
            count: issuers.length,
            issuers: issuers
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message, issuers: [] });
    }
});

router.get('/meta/categories', (req, res) => {
    try {
        const categories = {};
        certificatesData.certificates.forEach(c => {
            const cat = c.underlying_category || 'other';
            categories[cat] = (categories[cat] || 0) + 1;
        });
        
        res.json({
            success: true,
            categories: categories
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/meta/stats', (req, res) => {
    try {
        const certs = certificatesData.certificates;
        
        const avgYield = certs.length > 0 
            ? certs.reduce((sum, c) => sum + (c.annual_coupon_yield || 0), 0) / certs.length 
            : 0;
        
        res.json({
            success: true,
            statistics: {
                total_certificates: certs.length,
                types: [...new Set(certs.map(c => c.type).filter(t => t))].length,
                issuers: [...new Set(certs.map(c => c.issuer).filter(i => i))].length,
                with_scenario: certs.filter(c => c.scenario_analysis).length,
                avg_annual_yield: parseFloat(avgYield.toFixed(2)),
                last_update: certificatesData.metadata.timestamp || certificatesData.metadata.lastUpdate
            }
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/search', (req, res) => {
    try {
        const { q } = req.query;
        
        if (!q) {
            return res.status(400).json({
                success: false,
                error: 'Query parameter "q" is required'
            });
        }
        
        const query = q.toLowerCase();
        
        const results = certificatesData.certificates.filter(c => {
            return (
                (c.isin && c.isin.toLowerCase().includes(query)) ||
                (c.name && c.name.toLowerCase().includes(query)) ||
                (c.issuer && c.issuer.toLowerCase().includes(query)) ||
                (c.type && c.type.toLowerCase().includes(query)) ||
                (c.underlying_name && c.underlying_name.toLowerCase().includes(query)) ||
                (c.underlying && c.underlying.toLowerCase().includes(query))
            );
        });
        
        res.json({
            success: true,
            count: results.length,
            query: q,
            certificates: results.slice(0, 50)
        });
        
    } catch (error) {
        res.status(500).json({ success: false, error: error.message, certificates: [] });
    }
});

router.post('/reload', (req, res) => {
    loadCertificatesData();
    res.json({ 
        success: true, 
        count: certificatesData.certificates.length,
        message: 'Data reloaded'
    });
});

// ===================================
// GET BY ISIN (must be LAST)
// ===================================

router.get('/:isin', (req, res) => {
    try {
        const { isin } = req.params;
        
        // Skip meta endpoints
        if (['meta', 'search', 'reload'].includes(isin)) {
            return res.status(404).json({ success: false, error: 'Use correct endpoint' });
        }
        
        const cert = certificatesData.certificates.find(c => 
            c.isin && c.isin.toUpperCase() === isin.toUpperCase()
        );
        
        if (!cert) {
            return res.status(404).json({
                success: false,
                error: 'Certificate not found',
                isin: isin
            });
        }
        
        // Return the full certificate with all data
        res.json({
            success: true,
            certificate: cert,
            // Also return at top level for backwards compatibility
            ...cert
        });
        
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

module.exports = router;
