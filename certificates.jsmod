/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Certificates API - FIXED VERSION v2.0
 * All endpoints required by frontend
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
            if (fs.existsSync(dataPath)) {
                console.log(`[CERTIFICATES] Trying path: ${dataPath}`);
                const rawData = fs.readFileSync(dataPath, 'utf8');
                const parsed = JSON.parse(rawData);
                
                // Get certificates array (handle different structures)
                let certs = parsed.certificates || [];
                
                // Minimal filtering - only remove completely broken entries
                const cleanCerts = certs.filter(c => {
                    // Must have ISIN
                    if (!c.isin) return false;
                    
                    // Must have some basic data
                    if (!c.name && !c.issuer && !c.type) return false;
                    
                    return true;
                });
                
                certificatesData = {
                    metadata: parsed.metadata || {},
                    certificates: cleanCerts
                };
                
                console.log(`✅ [CERTIFICATES] Loaded ${cleanCerts.length} certificates from ${dataPath}`);
                loaded = true;
                break;
            }
        }
        
        if (!loaded) {
            console.warn('⚠️ [CERTIFICATES] No data file found, using empty dataset');
            console.warn('   Searched paths:', paths);
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
            limit = 500 
        } = req.query;
        
        let filtered = [...certificatesData.certificates];
        
        // Apply filters
        if (type) {
            filtered = filtered.filter(c => 
                c.type && c.type.toLowerCase().includes(type.toLowerCase())
            );
        }
        
        if (issuer) {
            filtered = filtered.filter(c => 
                c.issuer && c.issuer.toLowerCase().includes(issuer.toLowerCase())
            );
        }
        
        if (minYield) {
            const min = parseFloat(minYield);
            filtered = filtered.filter(c => 
                (c.annual_coupon_yield || 0) >= min
            );
        }
        
        if (maxYield) {
            const max = parseFloat(maxYield);
            filtered = filtered.filter(c => 
                (c.annual_coupon_yield || 100) <= max
            );
        }
        
        if (minBarrier) {
            const min = parseFloat(minBarrier);
            filtered = filtered.filter(c => 
                (c.barrier_down || c.barrier || 0) >= min
            );
        }
        
        if (maxBarrier) {
            const max = parseFloat(maxBarrier);
            filtered = filtered.filter(c => 
                (c.barrier_down || c.barrier || 100) <= max
            );
        }
        
        // Apply limit
        const limited = filtered.slice(0, parseInt(limit));
        
        res.json({
            success: true,
            count: limited.length,
            total: filtered.length,
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
// GET CERTIFICATE BY ISIN
// ===================================

router.get('/meta/types', (req, res) => {
    // This must come BEFORE /:isin route
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
    // This must come BEFORE /:isin route
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

router.get('/meta/stats', (req, res) => {
    try {
        const certs = certificatesData.certificates;
        
        const avgYield = certs.length > 0 
            ? certs.reduce((sum, c) => sum + (c.annual_coupon_yield || 0), 0) / certs.length 
            : 0;
            
        const avgBarrier = certs.length > 0 
            ? certs.reduce((sum, c) => sum + (c.barrier_down || c.barrier || 0), 0) / certs.length 
            : 0;
        
        res.json({
            success: true,
            statistics: {
                total_certificates: certs.length,
                types: [...new Set(certs.map(c => c.type).filter(t => t))].length,
                issuers: [...new Set(certs.map(c => c.issuer).filter(i => i))].length,
                avg_annual_yield: parseFloat(avgYield.toFixed(2)),
                avg_barrier: parseFloat(avgBarrier.toFixed(2)),
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
                (c.underlying_name && c.underlying_name.toLowerCase().includes(query))
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

// This must be LAST - catch-all for ISIN lookups
router.get('/:isin', (req, res) => {
    try {
        const { isin } = req.params;
        
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
        
        res.json({
            success: true,
            certificate: cert
        });
        
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

module.exports = router;
