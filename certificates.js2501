/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * Certificates API - FIXED VERSION
 * Include filtro anti-corruzione dati
 */

const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');

let certificatesData = {
    metadata: {},
    certificates: []
};

function loadCertificatesData() {
    try {
        const dataPath = path.join(__dirname, 'data', 'certificates-data.json');
        if (fs.existsSync(dataPath)) {
            const rawData = fs.readFileSync(dataPath, 'utf8');
            const parsed = JSON.parse(rawData);
            
            // 🔥 FILTRO SICUREZZA 🔥
            // Rimuoviamo certificati con prezzo nullo o date assurde
            const cleanCerts = (parsed.certificates || []).filter(c => {
                const hasPrice = c.price !== null && c.price > 0;
                const validDate = c.maturity_date && !c.maturity_date.includes("1900");
                return hasPrice && validDate;
            });

            certificatesData = {
                metadata: parsed.metadata || {},
                certificates: cleanCerts
            };
            
            console.log(`✅ [CERTIFICATES] Caricati ${cleanCerts.length} certificati validi.`);
        } else {
            console.warn('⚠️ data/certificates-data.json non trovato.');
        }
    } catch (error) {
        console.error('❌ Errore caricamento certificati:', error.message);
    }
}

// Carica all'avvio
loadCertificatesData();

// Endpoint lista
router.get('/', (req, res) => {
    res.json({
        success: true,
        count: certificatesData.certificates.length,
        certificates: certificatesData.certificates
    });
});

// Endpoint dettaglio
router.get('/:isin', (req, res) => {
    const cert = certificatesData.certificates.find(c => c.isin === req.params.isin);
    if (cert) {
        // Se lo scenario_analysis manca nel JSON, lo calcoliamo al volo (fallback)
        if (!cert.scenario_analysis || cert.scenario_analysis.length === 0) {
           // Qui potresti aggiungere un calcolo fallback Node.js se necessario,
           // ma il nuovo scraper Python lo fa già.
        }
        res.json(cert);
    } else {
        res.status(404).json({ success: false, error: 'Certificato non trovato' });
    }
});

// Endpoint ricarica forzata (utile se aggiorni il file manualmente)
router.post('/reload', (req, res) => {
    loadCertificatesData();
    res.json({ success: true, count: certificatesData.certificates.length });
});

module.exports = router;
