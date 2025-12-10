/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * ULTRA-ROBUST Bond Scraper - Multiple parsing strategies
 */

const axios = require('axios');
const cheerio = require('cheerio');

class BondScraperComplete {
    constructor() {
        this.baseUrl = 'https://www.borsaitaliana.it';
        
        this.categories = {
            // TITOLI DI STATO ITALIANI
            'gov-it-btp': { path: '/borsa/obbligazioni/mot/btp/lista.html', name: 'BTP - Buoni Tesoro Poliennali', type: 'governativo' },
            'gov-it-bot': { path: '/borsa/obbligazioni/mot/bot/lista.html', name: 'BOT - Buoni Ordinari Tesoro', type: 'governativo' },
            'gov-it-cct': { path: '/borsa/obbligazioni/mot/cct/lista.html', name: 'CCT - Certificati Credito Tesoro', type: 'governativo' },
            'gov-it-ctz': { path: '/borsa/obbligazioni/mot/ctz/lista.html', name: 'CTZ - Certificati Tesoro Zero Coupon', type: 'governativo' },
            
            // EURO-OBBLIGAZIONI
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
     * Parse italiani - Struttura standard tabella
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
                
                if (!isin || isin.length < 10) return;
                
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
     * Parse europei - MULTIPLE STRATEGIES
     */
    parseEuropeanBonds($, category, limit) {
        const bonds = [];
        const seen = new Set(); // Evita duplicati
        
        console.log(`[Parser] Starting European bonds parsing for ${category.name}`);
        
        // STRATEGIA 1: Cerca pattern ISIN nel testo
        const html = $.html();
        const isinPattern = /([A-Z]{2}[A-Z0-9]{10})/g;
        let match;
        
        while ((match = isinPattern.exec(html)) !== null && bonds.length < limit) {
            const isin = match[1];
            
            // Filtro paese
            if (category.filter && !isin.startsWith(category.filter)) {
                continue;
            }
            
            // Evita duplicati
            if (seen.has(isin)) {
                continue;
            }
            seen.add(isin);
            
            // Cerca nome bond nel contesto vicino all'ISIN
            const contextStart = Math.max(0, match.index - 200);
            const contextEnd = Math.min(html.length, match.index + 200);
            const context = html.substring(contextStart, contextEnd);
            
            // Estrai nome (cerca pattern common per bond austriaci/tedeschi/etc)
            let name = 'N/A';
            const namePatterns = [
                /(?:Austria|Germany|France|Spain|Netherlands|Belgium|Portugal|Ireland|Finland)[^<>]*?(?:Tf|Mz)[^<>]*?(?:\d+[,.]?\d*%?)[^<>]*?(?:\d{4})/i,
                /([^<>]{20,100})\s*Ultimo:/i
            ];
            
            for (const pattern of namePatterns) {
                const nameMatch = context.match(pattern);
                if (nameMatch) {
                    name = nameMatch[0].replace(/\s+/g, ' ').trim();
                    // Pulisci HTML tags
                    name = name.replace(/<[^>]*>/g, '').trim();
                    if (name.length > 10 && name.length < 100) {
                        break;
                    }
                }
            }
            
            // Cerca prezzo e cedola nel contesto
            let price = null;
            let coupon = null;
            
            const numbers = context.match(/\d+[,\.]\d{2,}/g);
            if (numbers && numbers.length > 0) {
                // Primo numero potrebbe essere prezzo
                price = this.parsePrice(numbers[0]);
                // Secondo potrebbe essere cedola
                if (numbers.length > 1) {
                    const possibleCoupon = this.parseYield(numbers[1]);
                    if (possibleCoupon && possibleCoupon < 20) { // Cedole realistiche < 20%
                        coupon = possibleCoupon;
                    }
                }
            }
            
            const bond = {
                name: name,
                isin: isin,
                price: price,
                change: null,
                changePercent: null,
                yield: coupon,
                volume: null,
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
            console.log(`[Parser] Found bond: ${isin} - ${name.substring(0, 30)}...`);
        }
        
        console.log(`[Parser] Total bonds found: ${bonds.length}`);
        return bonds;
    }

    async searchBonds(categoryKey, limit = 50) {
        try {
            const cacheKey = `search_${categoryKey}_${limit}`;
            const cached = this.getFromCache(cacheKey);
            if (cached) {
                console.log(`[Cache] Returning cached data for ${categoryKey}`);
                return cached;
            }

            const category = this.categories[categoryKey];
            if (!category) {
                throw new Error(`Categoria non valida: ${categoryKey}`);
            }

            await this.rateLimit();

            const url = `${this.baseUrl}${category.path}`;
            console.log(`[HTTP] Fetching: ${url}`);
            
            const response = await axios.get(url, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'it-IT,it;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                },
                timeout: 20000,
                maxRedirects: 5
            });

            console.log(`[HTTP] Response status: ${response.status}, size: ${response.data.length} bytes`);

            const $ = cheerio.load(response.data);
            let bonds = [];

            if (category.type === 'governativo') {
                bonds = this.parseItalianBonds($, { ...category, key: categoryKey }, limit);
            } else {
                bonds = this.parseEuropeanBonds($, { ...category, key: categoryKey }, limit);
            }

            const result = {
                success: true,
                category: categoryKey,
                categoryName: category.name,
                type: category.type,
                count: bonds.length,
                bonds: bonds,
                disclaimer: 'Dati forniti da Borsa Italiana con delay di 15 minuti. Prezzi puramente indicativi.',
                debug: {
                    url: url,
                    htmlSize: response.data.length,
                    parserUsed: category.type === 'governativo' ? 'italian' : 'european'
                }
            };

            if (bonds.length > 0) {
                this.setCache(cacheKey, result);
            }
            
            return result;

        } catch (error) {
            console.error(`[ERROR] ${categoryKey}:`, error.message);
            return {
                success: false,
                category: categoryKey,
                error: error.message,
                bonds: [],
                debug: {
                    errorStack: error.stack
                }
            };
        }
    }

    async searchByISIN(isin) {
        try {
            const country = isin.substring(0, 2);
            let categoryKey = null;

            if (country === 'IT') categoryKey = 'gov-it-btp';
            else if (country === 'DE') categoryKey = 'gov-eu-germany';
            else if (country === 'FR') categoryKey = 'gov-eu-france';
            else if (country === 'ES') categoryKey = 'gov-eu-spain';
            else categoryKey = 'gov-eu-all';

            const results = await this.searchBonds(categoryKey, 200);
            const bond = results.bonds.find(b => b.isin === isin);

            if (!bond) throw new Error('Bond non trovato');

            return { success: true, bond: bond };

        } catch (error) {
            console.error(`[ISIN] ${isin}:`, error.message);
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
            'IE': 'Irlanda', 'FI': 'Finlandia', 'EU': 'Europeo', 'XS': 'Internazionale'
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

// Test standalone
if (require.main === module) {
    (async () => {
        const scraper = new BondScraperComplete();
        console.log('🧪 Testing ULTRA-ROBUST Parser\n');
        
        const germany = await scraper.searchBonds('gov-eu-germany', 10);
        console.log(`\n✅ Germania: ${germany.count} bonds`);
        if (germany.bonds[0]) {
            console.log(`   Sample: ${germany.bonds[0].isin} - ${germany.bonds[0].name}`);
        }
    })();
}
