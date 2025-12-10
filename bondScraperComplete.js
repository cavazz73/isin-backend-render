/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * FIXED Bond Scraper - Borsa Italiana (Parsing reale)
 */

const axios = require('axios');
const cheerio = require('cheerio');

class BondScraperComplete {
    constructor() {
        this.baseUrl = 'https://www.borsaitaliana.it';
        
        // Categorie con URL CORRETTI
        this.categories = {
            // TITOLI DI STATO ITALIANI (funzionano già)
            'gov-it-btp': { path: '/borsa/obbligazioni/mot/btp/lista.html', name: 'BTP - Buoni Tesoro Poliennali', type: 'governativo' },
            'gov-it-bot': { path: '/borsa/obbligazioni/mot/bot/lista.html', name: 'BOT - Buoni Ordinari Tesoro', type: 'governativo' },
            'gov-it-cct': { path: '/borsa/obbligazioni/mot/cct/lista.html', name: 'CCT - Certificati Credito Tesoro', type: 'governativo' },
            'gov-it-ctz': { path: '/borsa/obbligazioni/mot/ctz/lista.html', name: 'CTZ - Certificati Tesoro Zero Coupon', type: 'governativo' },
            
            // EURO-OBBLIGAZIONI (tutti nella stessa pagina, filtro lato codice)
            'gov-eu-all': { path: '/borsa/obbligazioni/mot/euro-obbligazioni/lista.html', name: 'Euro-Obbligazioni (Tutte)', type: 'euro-gov' },
            'gov-eu-germany': { path: '/borsa/obbligazioni/mot/euro-obbligazioni/lista.html', name: 'Germania', type: 'euro-gov', filter: 'DE' },
            'gov-eu-france': { path: '/borsa/obbligazioni/mot/euro-obbligazioni/lista.html', name: 'Francia', type: 'euro-gov', filter: 'FR' },
            'gov-eu-spain': { path: '/borsa/obbligazioni/mot/euro-obbligazioni/lista.html', name: 'Spagna', type: 'euro-gov', filter: 'ES' },
            'gov-eu-netherlands': { path: '/borsa/obbligazioni/mot/euro-obbligazioni/lista.html', name: 'Olanda', type: 'euro-gov', filter: 'NL' },
            'gov-eu-belgium': { path: '/borsa/obbligazioni/mot/euro-obbligazioni/lista.html', name: 'Belgio', type: 'euro-gov', filter: 'BE' },
            'gov-eu-austria': { path: '/borsa/obbligazioni/mot/euro-obbligazioni/lista.html', name: 'Austria', type: 'euro-gov', filter: 'AT' },
            'gov-eu-portugal': { path: '/borsa/obbligazioni/mot/euro-obbligazioni/lista.html', name: 'Portogallo', type: 'euro-gov', filter: 'PT' },
            'gov-eu-ireland': { path: '/borsa/obbligazioni/mot/euro-obbligazioni/lista.html', name: 'Irlanda', type: 'euro-gov', filter: 'IE' },
            'gov-eu-finland': { path: '/borsa/obbligazioni/mot/euro-obbligazioni/lista.html', name: 'Finlandia', type: 'euro-gov', filter: 'FI' },
            
            // SOVRANAZIONALI
            'supranational': { path: '/borsa/obbligazioni/mot/obbligazioni-sovranazionali/lista.html', name: 'Sovranazionali (EFSF, ESM, BEI)', type: 'sovranazionale' },
            
            // CORPORATE
            'corporate-all': { path: '/borsa/obbligazioni/mot/obbligazioni-euro/lista.html', name: 'Corporate - Tutte', type: 'corporate' },
        };
        
        this.lastRequest = 0;
        this.minDelay = 2000;
        this.cache = new Map();
        this.cacheDuration = 60 * 60 * 1000;
    }

    async rateLimit() {
        const now = Date.now();
        const timeSinceLastRequest = now - this.lastRequest;
        if (timeSinceLastRequest < this.minDelay) {
            await new Promise(resolve => setTimeout(resolve, this.minDelay - timeSinceLastRequest));
        }
        this.lastRequest = Date.now();
    }

    getFromCache(key) {
        const cached = this.cache.get(key);
        if (cached && Date.now() - cached.timestamp < this.cacheDuration) {
            return cached.data;
        }
        return null;
    }

    setCache(key, data) {
        this.cache.set(key, { data, timestamp: Date.now() });
    }

    /**
     * Parse BTP/BOT/CCT/CTZ (italiani) - Struttura standard
     */
    parseItalianBonds($, category, limit) {
        const bonds = [];
        
        $('table tbody tr, table.m-table tbody tr').each((i, row) => {
            if (bonds.length >= limit) return false;

            const $row = $(row);
            const cells = $row.find('td');
            
            if (cells.length >= 4) {
                const name = $(cells[0]).text().trim();
                const isin = $(cells[1]).text().trim();
                
                if (!isin || isin.length < 10) return; // Skip invalid
                
                const bond = {
                    name: name || 'N/A',
                    isin: isin,
                    price: this.parsePrice($(cells[2]).text()),
                    change: this.parseChange($(cells[3]).text()),
                    changePercent: this.parseChangePercent($(cells[3]).text()),
                    yield: cells.length > 4 ? this.parseYield($(cells[4]).text()) : null,
                    volume: cells.length > 5 ? this.parseVolume($(cells[5]).text()) : null,
                    category: category.key,
                    categoryName: category.name,
                    type: category.type,
                    currency: 'EUR',
                    country: this.detectCountry(isin),
                    source: 'Borsa Italiana',
                    dataDelay: '15min',
                    timestamp: new Date().toISOString()
                };
                bonds.push(bond);
            }
        });
        
        return bonds;
    }

    /**
     * Parse Euro-obbligazioni - Struttura DIVERSA con link
     */
    parseEuropeanBonds($, category, limit) {
        const bonds = [];
        
        // La struttura è: ogni riga ha un link con ISIN nel href
        $('a[href*="/scheda/"]').each((i, elem) => {
            if (bonds.length >= limit) return false;
            
            const $link = $(elem);
            const href = $link.attr('href');
            
            // Estrai ISIN dall'URL (es. /scheda/AT0000383864.html)
            const isinMatch = href.match(/\/([A-Z]{2}[A-Z0-9]{10})\./);
            if (!isinMatch) return;
            
            const isin = isinMatch[1];
            
            // Applica filtro paese se specificato
            if (category.filter && !isin.startsWith(category.filter)) {
                return; // Skip
            }
            
            // Trova la riga parent
            const $row = $link.closest('tr');
            if ($row.length === 0) return;
            
            // Nome bond (testo del link, pulito)
            const name = $link.text().trim().replace(/\s+/g, ' ');
            
            // Cerca celle nella riga
            const cells = $row.find('td');
            
            // Parsing dati (ultima, cedola, scadenza sono in celle specifiche)
            let price = null;
            let coupon = null;
            
            cells.each((idx, cell) => {
                const text = $(cell).text().trim();
                // Cerca numero che sembra un prezzo (es. "100,816")
                if (text.match(/^\d+[,\.]\d+$/)) {
                    price = this.parsePrice(text);
                }
                // Cerca cedola (es. "6,25")
                if (text.match(/^\d+[,\.]\d{2}$/) && !price) {
                    coupon = this.parseYield(text);
                }
            });
            
            const bond = {
                name: name || 'N/A',
                isin: isin,
                price: price,
                change: null,  // Non disponibile in questa vista
                changePercent: null,
                yield: coupon,
                volume: null,  // Non disponibile
                category: category.key,
                categoryName: category.name,
                type: category.type,
                currency: 'EUR',
                country: this.detectCountry(isin),
                source: 'Borsa Italiana',
                dataDelay: '15min',
                timestamp: new Date().toISOString()
            };
            
            bonds.push(bond);
        });
        
        return bonds;
    }

    /**
     * Cerca bond per categoria
     */
    async searchBonds(categoryKey, limit = 50) {
        try {
            const cacheKey = `search_${categoryKey}_${limit}`;
            const cached = this.getFromCache(cacheKey);
            if (cached) return cached;

            const category = this.categories[categoryKey];
            if (!category) {
                throw new Error(`Categoria non valida: ${categoryKey}`);
            }

            await this.rateLimit();

            const url = `${this.baseUrl}${category.path}`;
            console.log(`[BondScraper] Fetching: ${url}`);
            
            const response = await axios.get(url, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml',
                    'Accept-Language': 'it-IT,it;q=0.9,en;q=0.8'
                },
                timeout: 15000
            });

            const $ = cheerio.load(response.data);
            let bonds = [];

            // Seleziona parser in base al tipo
            if (category.type === 'governativo') {
                // Italiani: parsing standard
                bonds = this.parseItalianBonds($, { ...category, key: categoryKey }, limit);
            } else if (category.type === 'euro-gov' || category.type === 'sovranazionale' || category.type === 'corporate') {
                // Europei/Sovranazionali/Corporate: parsing con link
                bonds = this.parseEuropeanBonds($, { ...category, key: categoryKey }, limit);
            }

            const result = {
                success: true,
                category: categoryKey,
                categoryName: category.name,
                type: category.type,
                count: bonds.length,
                bonds: bonds,
                disclaimer: 'Dati forniti da Borsa Italiana con delay di 15 minuti. Prezzi puramente indicativi.'
            };

            this.setCache(cacheKey, result);
            return result;

        } catch (error) {
            console.error(`[BondScraper] Errore ${categoryKey}:`, error.message);
            return {
                success: false,
                category: categoryKey,
                error: error.message,
                bonds: []
            };
        }
    }

    async searchByISIN(isin) {
        try {
            const country = isin.substring(0, 2);
            let categoryKey = null;

            if (country === 'IT') {
                categoryKey = 'gov-it-btp';
            } else if (country === 'DE') {
                categoryKey = 'gov-eu-germany';
            } else if (country === 'FR') {
                categoryKey = 'gov-eu-france';
            } else if (country === 'ES') {
                categoryKey = 'gov-eu-spain';
            } else {
                categoryKey = 'gov-eu-all';
            }

            const results = await this.searchBonds(categoryKey, 200);
            const bond = results.bonds.find(b => b.isin === isin);

            if (!bond) {
                throw new Error('Bond non trovato');
            }

            return { success: true, bond: bond };

        } catch (error) {
            console.error(`[BondScraper] ISIN ${isin}:`, error.message);
            return { success: false, error: error.message };
        }
    }

    async filterBonds(categoryKey, filters = {}) {
        const results = await this.searchBonds(categoryKey, 200);
        let filtered = results.bonds;

        if (filters.minYield) filtered = filtered.filter(b => b.yield >= filters.minYield);
        if (filters.maxYield) filtered = filtered.filter(b => b.yield <= filters.maxYield);
        if (filters.minPrice) filtered = filtered.filter(b => b.price >= filters.minPrice);
        if (filters.maxPrice) filtered = filtered.filter(b => b.price <= filters.maxPrice);

        if (filters.sortBy) {
            switch(filters.sortBy) {
                case 'yield-desc': filtered.sort((a, b) => (b.yield || 0) - (a.yield || 0)); break;
                case 'yield-asc': filtered.sort((a, b) => (a.yield || 0) - (b.yield || 0)); break;
                case 'price-desc': filtered.sort((a, b) => (b.price || 0) - (a.price || 0)); break;
                case 'price-asc': filtered.sort((a, b) => (a.price || 0) - (b.price || 0)); break;
            }
        }

        return { success: true, category: categoryKey, count: filtered.length, bonds: filtered, filters: filters };
    }

    getCategories() {
        return Object.entries(this.categories).map(([key, value]) => ({
            key: key,
            name: value.name,
            type: value.type
        }));
    }

    parsePrice(text) {
        const match = text.match(/[\d.,]+/);
        return match ? parseFloat(match[0].replace(',', '.')) : null;
    }

    parseChange(text) {
        const match = text.match(/[-+]?[\d.,]+/);
        return match ? parseFloat(match[0].replace(',', '.')) : null;
    }

    parseChangePercent(text) {
        const matches = text.match(/[-+]?[\d.,]+/g);
        return matches && matches.length > 1 ? parseFloat(matches[1].replace(',', '.')) : null;
    }

    parseYield(text) {
        const match = text.match(/[\d.,]+/);
        return match ? parseFloat(match[0].replace(',', '.')) : null;
    }

    parseVolume(text) {
        const cleaned = text.replace(/[^\d]/g, '');
        return cleaned ? parseInt(cleaned) : null;
    }

    detectCountry(isin) {
        const countryCode = isin.substring(0, 2);
        const countries = {
            'IT': 'Italia', 'DE': 'Germania', 'FR': 'Francia', 'ES': 'Spagna',
            'NL': 'Olanda', 'BE': 'Belgio', 'AT': 'Austria', 'PT': 'Portogallo',
            'IE': 'Irlanda', 'FI': 'Finlandia', 'EU': 'Europeo'
        };
        return countries[countryCode] || countryCode;
    }

    clearCache() {
        this.cache.clear();
    }

    getCacheStats() {
        return {
            size: this.cache.size,
            duration: `${this.cacheDuration / 1000 / 60} minuti`,
            entries: Array.from(this.cache.keys())
        };
    }
}

module.exports = BondScraperComplete;

// Test
if (require.main === module) {
    (async () => {
        const scraper = new BondScraperComplete();
        
        console.log('🔍 Test Bond Scraper FIXED\n');
        
        try {
            // Test BTP
            console.log('1️⃣ Test BTP (italiani)...');
            const btps = await scraper.searchBonds('gov-it-btp', 5);
            console.log(`   ✅ ${btps.count} BTP trovati`);
            if (btps.bonds[0]) console.log(`   Primo: ${btps.bonds[0].name}`);
            
            // Test Germania
            console.log('\n2️⃣ Test Germania (europei)...');
            const germany = await scraper.searchBonds('gov-eu-germany', 5);
            console.log(`   ✅ ${germany.count} bond tedeschi trovati`);
            if (germany.bonds[0]) console.log(`   Primo: ${germany.bonds[0].name}`);
            
            // Test Austria
            console.log('\n3️⃣ Test Austria (europei)...');
            const austria = await scraper.searchBonds('gov-eu-austria', 5);
            console.log(`   ✅ ${austria.count} bond austriaci trovati`);
            if (austria.bonds[0]) console.log(`   Primo: ${austria.bonds[0].name}`);
            
            // Test Sovranazionali
            console.log('\n4️⃣ Test Sovranazionali...');
            const sovr = await scraper.searchBonds('supranational', 5);
            console.log(`   ✅ ${sovr.count} bond sovranazionali trovati`);
            if (sovr.bonds[0]) console.log(`   Primo: ${sovr.bonds[0].name}`);
            
        } catch (error) {
            console.error('❌ Errore test:', error.message);
        }
    })();
}
