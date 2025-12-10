/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * COMPLETE Bond Scraper - Borsa Italiana
 * Supporta: Governativi IT, Euro-obbligazioni, Corporate, Sovranazionali
 */

const axios = require('axios');
const cheerio = require('cheerio');

class BondScraperComplete {
    constructor() {
        this.baseUrl = 'https://www.borsaitaliana.it';
        
        // Tutte le categorie disponibili
        this.categories = {
            // TITOLI DI STATO ITALIANI
            'gov-it-btp': { path: '/borsa/obbligazioni/mot/btp/lista.html', name: 'BTP - Buoni Tesoro Poliennali', type: 'governativo' },
            'gov-it-bot': { path: '/borsa/obbligazioni/mot/bot/lista.html', name: 'BOT - Buoni Ordinari Tesoro', type: 'governativo' },
            'gov-it-cct': { path: '/borsa/obbligazioni/mot/cct/lista.html', name: 'CCT - Certificati Credito Tesoro', type: 'governativo' },
            'gov-it-ctz': { path: '/borsa/obbligazioni/mot/ctz/lista.html', name: 'CTZ - Certificati Tesoro Zero Coupon', type: 'governativo' },
            
            // EURO-OBBLIGAZIONI (Governi Europei)
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
        
        // Rate limiting: 1 richiesta ogni 2 secondi
        this.lastRequest = 0;
        this.minDelay = 2000;
        
        // Cache 1 ora
        this.cache = new Map();
        this.cacheDuration = 60 * 60 * 1000;
    }

    /**
     * Rate limiting
     */
    async rateLimit() {
        const now = Date.now();
        const timeSinceLastRequest = now - this.lastRequest;
        
        if (timeSinceLastRequest < this.minDelay) {
            await new Promise(resolve => 
                setTimeout(resolve, this.minDelay - timeSinceLastRequest)
            );
        }
        
        this.lastRequest = Date.now();
    }

    /**
     * Cache management
     */
    getFromCache(key) {
        const cached = this.cache.get(key);
        if (cached && Date.now() - cached.timestamp < this.cacheDuration) {
            return cached.data;
        }
        return null;
    }

    setCache(key, data) {
        this.cache.set(key, {
            data,
            timestamp: Date.now()
        });
    }

    /**
     * Cerca bond per categoria
     * @param {string} categoryKey - Chiave categoria (es. 'gov-it-btp')
     * @param {number} limit - Numero massimo risultati
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
                    'Accept-Language': 'it-IT,it;q=0.9,en;q=0.8',
                    'Cache-Control': 'no-cache'
                },
                timeout: 15000
            });

            const $ = cheerio.load(response.data);
            const bonds = [];

            // Parsing tabella bond (struttura Borsa Italiana)
            $('table.m-table tbody tr, table tbody tr').each((i, row) => {
                if (bonds.length >= limit) return false;

                const $row = $(row);
                const cells = $row.find('td');
                
                if (cells.length >= 4) {
                    const name = $(cells[0]).text().trim();
                    const isin = $(cells[1]).text().trim();
                    
                    // Filtro per paese (se specificato)
                    if (category.filter && !isin.startsWith(category.filter)) {
                        return; // Skip
                    }
                    
                    const bond = {
                        name: name,
                        isin: isin,
                        price: this.parsePrice($(cells[2]).text()),
                        change: this.parseChange($(cells[3]).text()),
                        changePercent: this.parseChangePercent($(cells[3]).text()),
                        yield: cells.length > 4 ? this.parseYield($(cells[4]).text()) : null,
                        volume: cells.length > 5 ? this.parseVolume($(cells[5]).text()) : null,
                        
                        // Metadata
                        category: categoryKey,
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

    /**
     * Cerca in TUTTE le categorie (overview completo)
     */
    async searchAll(limit = 10) {
        const results = {};
        
        for (const [key, category] of Object.entries(this.categories)) {
            try {
                const data = await this.searchBonds(key, limit);
                results[key] = {
                    name: category.name,
                    type: category.type,
                    count: data.count,
                    topBonds: data.bonds.slice(0, 5) // Top 5 per categoria
                };
            } catch (error) {
                console.error(`[BondScraper] Skip ${key}:`, error.message);
                results[key] = { error: error.message };
            }
        }
        
        return {
            success: true,
            timestamp: new Date().toISOString(),
            categories: results
        };
    }

    /**
     * Cerca per ISIN specifico
     */
    async searchByISIN(isin) {
        try {
            // Rileva categoria da ISIN
            const country = isin.substring(0, 2);
            let categoryKey = null;

            if (country === 'IT') {
                categoryKey = 'gov-it-btp'; // Default italiano
            } else if (country === 'DE') {
                categoryKey = 'gov-eu-germany';
            } else if (country === 'FR') {
                categoryKey = 'gov-eu-france';
            } else if (country === 'ES') {
                categoryKey = 'gov-eu-spain';
            }

            if (!categoryKey) {
                throw new Error('Paese non supportato');
            }

            const results = await this.searchBonds(categoryKey, 100);
            const bond = results.bonds.find(b => b.isin === isin);

            if (!bond) {
                throw new Error('Bond non trovato');
            }

            return {
                success: true,
                bond: bond
            };

        } catch (error) {
            console.error(`[BondScraper] ISIN ${isin}:`, error.message);
            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Filtra bond per criteri
     */
    async filterBonds(categoryKey, filters = {}) {
        const results = await this.searchBonds(categoryKey, 200);
        
        let filtered = results.bonds;

        // Filtro per yield minimo/massimo
        if (filters.minYield) {
            filtered = filtered.filter(b => b.yield >= filters.minYield);
        }
        if (filters.maxYield) {
            filtered = filtered.filter(b => b.yield <= filters.maxYield);
        }

        // Filtro per prezzo
        if (filters.minPrice) {
            filtered = filtered.filter(b => b.price >= filters.minPrice);
        }
        if (filters.maxPrice) {
            filtered = filtered.filter(b => b.price <= filters.maxPrice);
        }

        // Ordinamento
        if (filters.sortBy) {
            switch(filters.sortBy) {
                case 'yield-desc':
                    filtered.sort((a, b) => (b.yield || 0) - (a.yield || 0));
                    break;
                case 'yield-asc':
                    filtered.sort((a, b) => (a.yield || 0) - (b.yield || 0));
                    break;
                case 'price-desc':
                    filtered.sort((a, b) => (b.price || 0) - (a.price || 0));
                    break;
                case 'price-asc':
                    filtered.sort((a, b) => (a.price || 0) - (b.price || 0));
                    break;
            }
        }

        return {
            success: true,
            category: categoryKey,
            count: filtered.length,
            bonds: filtered,
            filters: filters
        };
    }

    /**
     * Ottieni lista categorie disponibili
     */
    getCategories() {
        return Object.entries(this.categories).map(([key, value]) => ({
            key: key,
            name: value.name,
            type: value.type
        }));
    }

    /**
     * Parsing helpers
     */
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
            'IT': 'Italia',
            'DE': 'Germania',
            'FR': 'Francia',
            'ES': 'Spagna',
            'NL': 'Olanda',
            'BE': 'Belgio',
            'AT': 'Austria',
            'PT': 'Portogallo',
            'IE': 'Irlanda',
            'FI': 'Finlandia',
            'EU': 'Europeo'
        };
        return countries[countryCode] || countryCode;
    }

    /**
     * Clear cache
     */
    clearCache() {
        this.cache.clear();
    }

    /**
     * Cache stats
     */
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
        
        console.log('🔍 Test Bond Scraper Complete\n');
        
        try {
            // Test 1: Categorie disponibili
            console.log('1️⃣ Categorie disponibili:');
            const categories = scraper.getCategories();
            console.log(`   ✅ ${categories.length} categorie`);
            categories.forEach(cat => {
                console.log(`   - ${cat.key}: ${cat.name} (${cat.type})`);
            });
            console.log('');
            
            // Test 2: BTP
            console.log('2️⃣ Ricerca BTP...');
            const btps = await scraper.searchBonds('gov-it-btp', 5);
            console.log(`   ✅ Trovati ${btps.count} BTP`);
            if (btps.bonds.length > 0) {
                console.log(`   Primo: ${btps.bonds[0].name} - €${btps.bonds[0].price}`);
            }
            console.log('');
            
            // Test 3: Germania
            console.log('3️⃣ Ricerca Bond Germania...');
            const germany = await scraper.searchBonds('gov-eu-germany', 5);
            console.log(`   ✅ Trovati ${germany.count} bond tedeschi`);
            console.log('');
            
            // Test 4: Filtro yield
            console.log('4️⃣ Filtro BTP yield > 3%...');
            const filtered = await scraper.filterBonds('gov-it-btp', {
                minYield: 3.0,
                sortBy: 'yield-desc'
            });
            console.log(`   ✅ Trovati ${filtered.count} bond con yield > 3%`);
            console.log('');
            
            // Test 5: Cache stats
            console.log('5️⃣ Cache Statistics:');
            console.log(scraper.getCacheStats());
            
        } catch (error) {
            console.error('❌ Errore test:', error.message);
        }
    })();
}
