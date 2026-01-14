/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * Financial API Routes
 * WITH INSTRUMENT DETAILS ENDPOINT (description, fundamentals, etc)
 */

const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const util = require('util');
const execPromise = util.promisify(exec);
const DataAggregator = require('./dataAggregator');

const aggregator = new DataAggregator();

// --- CARICAMENTO DATI LOCALI ---
function searchLocalFile(filename, query, typeLabel) {
    try {
        const filePath = path.join(__dirname, 'data', filename);
        if (!fs.existsSync(filePath)) return [];
        
        const raw = fs.readFileSync(filePath, 'utf8');
        const data = JSON.parse(raw);
        let items = [];

        // Logica per estrarre array da strutture diverse
        if (data.certificates) items = data.certificates;
        else if (data.categories) {
            Object.values(data.categories).forEach(cat => {
                if (cat.bonds) items.push(...cat.bonds);
            });
        } else if (Array.isArray(data)) {
            items = data;
        }

        const q = query.toUpperCase();
        return items.filter(i => 
            (i.isin && i.isin.includes(q)) || 
            (i.name && i.name.toUpperCase().includes(q))
        ).slice(0, 5).map(item => ({
            symbol: item.isin,
            name: item.name,
            type: typeLabel,
            price: item.price || item.bid_price || 0,
            currency: item.currency || 'EUR',
            source: 'Local DB',
            score: item.isin === q ? 100 : 50
        }));
    } catch (e) { return []; }
}

// --- ENDPOINT RICERCA INTELLIGENTE ---
router.get('/search', async (req, res) => {
    const query = req.query.q;
    if (!query || query.length < 2) return res.json({ success: false, error: 'Query too short' });

    const qUpper = query.toUpperCase();
    console.log(`🔍 SEARCH: "${qUpper}"`);

    try {
        // 1. CERCA NEL DB LOCALE (Velocissimo)
        const localCerts = searchLocalFile('certificates-data.json', qUpper, 'CERTIFICATE');
        const localBonds = searchLocalFile('bonds-data.json', qUpper, 'BOND');
        
        let results = [...localCerts, ...localBonds];

        // 2. LIVE FALLBACK: Se è un ISIN e non è nel DB, vai su Internet
        const isISIN = /^[A-Z]{2}[A-Z0-9]{9}\d$/.test(qUpper);
        
        // Se non abbiamo trovato l'ISIN esatto localmente
        const exactMatch = results.find(r => r.symbol === qUpper);
        
        if (isISIN && !exactMatch) {
            console.log(`⚠️ ISIN ${qUpper} non trovato localmente. Attivo Scraper Live...`);
            try {
                // Chiama Python: "Cercami questo ISIN ovunque (Certificati o Bond)"
                const { stdout } = await execPromise(`python production_scraper.py --isin ${qUpper}`, { timeout: 20000 });
                
                if (stdout.trim()) {
                    const liveData = JSON.parse(stdout);
                    if (!liveData.error) {
                        results.unshift({
                            symbol: liveData.isin,
                            name: liveData.name,
                            type: liveData.type,   // Python ci dirà se è CERTIFICATE o BOND
                            price: liveData.price,
                            currency: liveData.currency,
                            source: liveData.source, // Es: "Teleborsa (Live)"
                            score: 100
                        });
                        console.log(`✅ Trovato live: ${liveData.name}`);
                    }
                }
            } catch (err) {
                console.warn("Live scraper nullo o timeout:", err.message);
            }
        }

        // 3. AGGIUNGI AZIONI (Yahoo/Finnhub)
        // Se non è un ISIN o se vogliamo completare la ricerca
        if (!isISIN || results.length === 0) {
            try {
                const apiRes = await aggregator.search(query);
                if (apiRes && apiRes.results) {
                    // Evita duplicati
                    const newItems = apiRes.results.filter(ext => !results.some(loc => loc.symbol === ext.symbol));
                    results.push(...newItems);
                }
            } catch (e) { console.warn("API Esterne saltate"); }
        }

        res.json({
            success: true,
            count: results.length,
            results: results.slice(0, 15)
        });

    } catch (error) {
        console.error('Search Critical Error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

module.exports = router;
