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

// Inizializza aggregatore esterno (Yahoo/Finnhub)
const aggregator = new DataAggregator();

// --- FUNZIONE: Carica dati dai file JSON locali ---
function searchLocalDatabase(filename, query, typeLabel) {
    try {
        const filePath = path.join(__dirname, 'data', filename);
        if (!fs.existsSync(filePath)) return [];

        const raw = fs.readFileSync(filePath, 'utf8');
        const data = JSON.parse(raw);
        
        // Normalizza i dati: i bond a volte sono nidificati, i certificati sono liste piatte
        let items = [];
        
        if (filename.includes('certificates')) {
            items = data.certificates || [];
        } else if (filename.includes('bonds')) {
            // Se i bonds sono divisi per categorie, li appiattiamo
            if (data.categories) {
                Object.values(data.categories).forEach(cat => {
                    if (cat.bonds) items.push(...cat.bonds);
                });
            } else if (Array.isArray(data)) {
                items = data;
            }
        }

        const q = query.toUpperCase();
        
        // Filtro
        return items.filter(item => {
            const isin = (item.isin || "").toUpperCase();
            const name = (item.name || "").toUpperCase();
            return isin.includes(q) || name.includes(q);
        }).slice(0, 5).map(item => ({
            symbol: item.isin,         // Standardizziamo su 'symbol' per il frontend
            name: item.name,
            type: typeLabel,           // 'CERTIFICATE' o 'BOND'
            price: item.price || item.bid_price || 0,
            currency: item.currency || 'EUR',
            source: 'Local DB',
            score: item.isin === q ? 100 : 50 // Priorità ai match esatti
        }));

    } catch (e) {
        console.error(`Errore lettura ${filename}:`, e.message);
        return [];
    }
}

// --- ENDPOINT RICERCA UNIFICATA ---
router.get('/search', async (req, res) => {
    const query = req.query.q;
    if (!query || query.length < 2) return res.json({ success: false, error: 'Query too short' });

    const qUpper = query.toUpperCase();
    console.log(`🔍 [SEARCH] Query: "${qUpper}"`);

    // 1. CERCA LOCALMENTE (Certificati e Bond)
    const localCerts = searchLocalDatabase('certificates-data.json', qUpper, 'CERTIFICATE');
    const localBonds = searchLocalDatabase('bonds-data.json', qUpper, 'BOND');
    
    let results = [...localCerts, ...localBonds];

    // 2. SE È UN ISIN E NON LO TROVIAMO -> LIVE SCRAPING (Python)
    const isISIN = /^[A-Z]{2}[A-Z0-9]{9}\d$/.test(qUpper);
    const exactMatchFound = results.some(r => r.symbol === qUpper);

    if (isISIN && !exactMatchFound) {
        console.log(`⚠️ ISIN sconosciuto localmente. Avvio Hunter Python...`);
        try {
            // Chiama lo scraper in modalità "ISIN singolo"
            // Timeout 15 secondi per non far aspettare troppo l'utente
            const { stdout } = await execPromise(`python production_scraper.py --isin ${qUpper}`, { timeout: 15000 });
            
            if (stdout.trim()) {
                try {
                    const liveData = JSON.parse(stdout);
                    if (!liveData.error) {
                        results.unshift({
                            symbol: liveData.isin,
                            name: liveData.name,
                            type: liveData.type, // CERTIFICATE (o altro se espandiamo python)
                            price: liveData.price,
                            currency: liveData.currency,
                            source: 'Live Web',
                            score: 100
                        });
                    }
                } catch (jsonErr) {
                    console.warn("Output Python non valido:", stdout);
                }
            }
        } catch (err) {
            console.warn("Hunter Python timeout o errore:", err.message);
        }
    }

    // 3. CERCA ESTERNAMENTE (Azioni/ETF via Yahoo/Finnhub)
    // Solo se non è un ISIN palese (per risparmiare chiamate API) o se l'utente vuole tutto
    if (!isISIN || results.length === 0) {
        try {
            const externalResults = await aggregator.search(query);
            if (externalResults && externalResults.results) {
                // Aggiungiamo solo se non duplicano (per ISIN rari che Yahoo conosce)
                const newItems = externalResults.results.filter(ext => 
                    !results.some(loc => loc.symbol === ext.symbol)
                );
                results.push(...newItems);
            }
        } catch (e) {
            console.warn("API Esterne saltate o errore:", e.message);
        }
    }

    // 4. ORDINAMENTO E RISPOSTA
    // Mette in cima i match esatti e i dati locali
    results.sort((a, b) => (b.score || 0) - (a.score || 0));

    res.json({
        success: true,
        count: results.length,
        results: results.slice(0, 15) // Top 15 risultati misti
    });
});

module.exports = router;
