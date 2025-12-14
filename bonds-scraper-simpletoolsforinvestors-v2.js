/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * SimpleTools For Investors - Enhanced Bond Scraper v2
 * Better parsing logic and country detection
 */

const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs').promises;

class SimpleToolsBondScraperV2 {
    constructor() {
        this.baseUrl = 'https://www.simpletoolsforinvestors.eu/monitor_info.php';
        this.categories = this.defineCategories();
    }

    defineCategories() {
        return {
            // Italian Government Bonds - Use dedicated monitors
            'gov-it-btp': {
                name: 'BTP - Buoni Tesoro Poliennali',
                description: 'Titoli di Stato italiani a medio-lungo termine',
                monitor: 'italia',
                filter: bond => this.isBondType(bond, 'BTP')
            },
            'gov-it-bot': {
                name: 'BOT - Buoni Ordinari del Tesoro',
                description: 'Titoli di Stato italiani a breve termine',
                monitor: 'buoni_ordinari',
                filter: () => true // All bonds from this monitor
            },
            'gov-it-cct': {
                name: 'CCT - Certificati di Credito del Tesoro',
                description: 'Titoli di Stato italiani a tasso variabile',
                monitor: 'italia',
                filter: bond => this.isBondType(bond, 'CCT')
            },
            'gov-it-ctz': {
                name: 'CTZ - Certificati del Tesoro Zero Coupon',
                description: 'Titoli di Stato italiani zero coupon',
                monitor: 'italia',
                filter: bond => this.isBondType(bond, 'CTZ')
            },

            // European Government Bonds - Dedicated monitors
            'gov-eu-germany': {
                name: 'Germania - Bund',
                description: 'Titoli di Stato tedeschi',
                monitor: 'germania',
                filter: () => true
            },
            'gov-eu-france': {
                name: 'Francia - OAT',
                description: 'Titoli di Stato francesi',
                monitor: 'francia',
                filter: () => true
            },
            
            // European countries from altri_europa monitor
            'gov-eu-spain': {
                name: 'Spagna - Bonos',
                description: 'Titoli di Stato spagnoli',
                monitor: 'altri_europa',
                filter: bond => this.isCountry(bond, ['SPAGNA', 'SPAIN', 'ES000'])
            },
            'gov-eu-netherlands': {
                name: 'Paesi Bassi - DSL',
                description: 'Titoli di Stato olandesi',
                monitor: 'altri_europa',
                filter: bond => this.isCountry(bond, ['PAESI BASSI', 'NETHERLANDS', 'OLANDA', 'NL000'])
            },
            'gov-eu-ireland': {
                name: 'Irlanda - IRISH',
                description: 'Titoli di Stato irlandesi',
                monitor: 'altri_europa',
                filter: bond => this.isCountry(bond, ['IRLANDA', 'IRELAND', 'IE000'])
            },
            'gov-eu-portugal': {
                name: 'Portogallo - OT',
                description: 'Titoli di Stato portoghesi',
                monitor: 'altri_europa',
                filter: bond => this.isCountry(bond, ['PORTOGALLO', 'PORTUGAL', 'PTOTE'])
            },
            'gov-eu-austria': {
                name: 'Austria - RAGB',
                description: 'Titoli di Stato austriaci',
                monitor: 'altri_europa',
                filter: bond => this.isCountry(bond, ['AUSTRIA', 'AT0000'])
            },
            'gov-eu-finland': {
                name: 'Finlandia - Finnish',
                description: 'Titoli di Stato finlandesi',
                monitor: 'altri_europa',
                filter: bond => this.isCountry(bond, ['FINLANDIA', 'FINLAND', 'FI4000'])
            },
            'gov-eu-belgium': {
                name: 'Belgio - OLO',
                description: 'Titoli di Stato belgi',
                monitor: 'altri_europa',
                filter: bond => this.isCountry(bond, ['BELGIO', 'BELGIUM', 'BELGIQUE', 'BE0000'])
            },

            // Supranational
            'supranational': {
                name: 'Sovranazionali',
                description: 'Obbligazioni emesse da enti sovranazionali (BEI, ESM, etc)',
                monitor: ['sovranazionali', 'bei'],
                filter: () => true
            },

            // Corporate
            'corporate-all': {
                name: 'Corporate',
                description: 'Obbligazioni societarie',
                monitor: 'corporate',
                filter: () => true
            }
        };
    }

    /**
     * Check if bond is specific type (BTP, BOT, CCT, CTZ)
     */
    isBondType(bond, type) {
        const name = (bond.name || '').toUpperCase();
        const isin = (bond.isin || '').toUpperCase();
        
        // Check name contains type
        if (name.includes(type)) return true;
        
        // BOT are typically short maturity Italian bonds with IT ISIN
        if (type === 'BOT' && isin.startsWith('IT') && bond.maturity) {
            const maturityDate = new Date(bond.maturity);
            const now = new Date();
            const monthsDiff = (maturityDate - now) / (1000 * 60 * 60 * 24 * 30);
            return monthsDiff <= 12; // BOT are < 12 months
        }
        
        return false;
    }

    /**
     * Check if bond is from specific country
     */
    isCountry(bond, keywords) {
        const name = (bond.name || '').toUpperCase();
        const isin = (bond.isin || '').toUpperCase();
        
        return keywords.some(keyword => 
            name.includes(keyword.toUpperCase()) || 
            isin.includes(keyword.toUpperCase())
        );
    }

    /**
     * Scrape a single monitor with enhanced parsing
     */
    async scrapeMonitor(monitorName) {
        try {
            const url = `${this.baseUrl}?monitor=${monitorName}&yieldtype=G&timescale=DUR`;
            console.log(`[Scraper] Fetching ${monitorName}...`);

            const response = await axios.get(url, {
                timeout: 30000,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
            });

            const $ = cheerio.load(response.data);
            const bonds = [];

            // Find all rows in tables
            $('table tr').each((i, row) => {
                const cells = $(row).find('td');
                
                // Need at least ISIN, name, currency, maturity, price, yield columns
                if (cells.length < 8) return;

                const isinText = $(cells[0]).text().trim();
                
                // Valid ISIN format check (relaxed for debugging)
                if (!isinText || isinText.length < 10) return;
                
                // More lenient ISIN check
                const isin = isinText.toUpperCase();
                if (!/^[A-Z]{2}/.test(isin)) return; // Must start with 2 letters

                const name = $(cells[2]).text().trim();
                if (!name) return; // Must have a name

                const currency = $(cells[3]).text().trim();
                const maturity = $(cells[4]).text().trim();
                
                // Price usually in column 8 or 9
                let priceText = $(cells[8]).text().trim();
                if (!priceText || isNaN(parseFloat(priceText.replace(',', '.')))) {
                    priceText = $(cells[9]).text().trim();
                }
                
                // Yield usually in column 12 or 13
                let yieldText = $(cells[12]).text().trim();
                if (!yieldText || isNaN(parseFloat(yieldText.replace(',', '.')))) {
                    yieldText = $(cells[13]).text().trim();
                }

                // Extract coupon from name
                const couponMatch = name.match(/(\d+[,\.]\d+)%/);
                const coupon = couponMatch ? parseFloat(couponMatch[1].replace(',', '.')) : 0;

                // Determine country from ISIN
                const country = isin.substring(0, 2);

                // Determine bond type
                let type = 'BOND';
                if (name.includes('BTP')) type = 'BTP';
                else if (name.includes('BOT')) type = 'BOT';
                else if (name.includes('CCT')) type = 'CCT';
                else if (name.includes('CTZ')) type = 'CTZ';

                const bond = {
                    isin,
                    name,
                    type,
                    country,
                    currency: currency || 'EUR',
                    maturity: this.parseDate(maturity),
                    coupon,
                    yield: parseFloat(yieldText.replace(',', '.')) || 0,
                    price: parseFloat(priceText.replace(',', '.')) || 0,
                    change: '+0.00',
                    lastUpdate: new Date().toISOString().split('T')[0]
                };

                bonds.push(bond);
            });

            console.log(`[Scraper] Found ${bonds.length} bonds in ${monitorName}`);
            
            // Debug: Show sample
            if (bonds.length > 0) {
                console.log(`[Scraper] Sample from ${monitorName}:`, {
                    isin: bonds[0].isin,
                    name: bonds[0].name.substring(0, 30),
                    type: bonds[0].type,
                    country: bonds[0].country
                });
            }
            
            return bonds;

        } catch (error) {
            console.error(`[Scraper] Error fetching ${monitorName}:`, error.message);
            return [];
        }
    }

    /**
     * Parse date from Italian format
     */
    parseDate(dateStr) {
        if (!dateStr) return '';
        
        // Already in YYYY-MM-DD format
        if (dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) return dateStr;
        
        // DD/MM/YYYY format
        const parts = dateStr.split('/');
        if (parts.length === 3) {
            const [day, month, year] = parts;
            return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
        }
        
        return dateStr;
    }

    /**
     * Scrape all categories
     */
    async scrapeAll() {
        console.log('[Scraper] Starting complete scrape...');
        console.log('[Scraper] Using SimpleTools For Investors data');
        
        const result = {
            lastUpdate: new Date().toISOString(),
            categories: {},
            statistics: {
                totalBonds: 0,
                totalCategories: 0
            }
        };

        // Group categories by monitor to avoid duplicate requests
        const monitorMap = new Map();
        
        for (const [catId, catConfig] of Object.entries(this.categories)) {
            const monitors = Array.isArray(catConfig.monitor) ? catConfig.monitor : [catConfig.monitor];
            
            for (const monitor of monitors) {
                if (!monitorMap.has(monitor)) {
                    monitorMap.set(monitor, []);
                }
                monitorMap.get(monitor).push({ catId, catConfig });
            }
        }

        console.log(`[Scraper] Will fetch ${monitorMap.size} unique monitors`);

        // Scrape each unique monitor once
        const monitorData = new Map();
        
        for (const [monitor, categories] of monitorMap.entries()) {
            const bonds = await this.scrapeMonitor(monitor);
            monitorData.set(monitor, bonds);
            
            // Rate limiting - wait 2 seconds between requests
            await new Promise(resolve => setTimeout(resolve, 2000));
        }

        // Process each category
        for (const [catId, catConfig] of Object.entries(this.categories)) {
            const monitors = Array.isArray(catConfig.monitor) ? catConfig.monitor : [catConfig.monitor];
            
            let allBonds = [];
            for (const monitor of monitors) {
                const bonds = monitorData.get(monitor) || [];
                allBonds = allBonds.concat(bonds);
            }

            // Apply category filter
            const filteredBonds = allBonds.filter(catConfig.filter);

            // Sort by yield descending
            filteredBonds.sort((a, b) => b.yield - a.yield);

            result.categories[catId] = {
                name: catConfig.name,
                description: catConfig.description,
                count: filteredBonds.length,
                bonds: filteredBonds
            };

            result.statistics.totalBonds += filteredBonds.length;
            result.statistics.totalCategories++;

            console.log(`[Scraper] ${catId}: ${filteredBonds.length} bonds`);
        }

        console.log(`[Scraper] ========================================`);
        console.log(`[Scraper] COMPLETE! Total: ${result.statistics.totalBonds} bonds in ${result.statistics.totalCategories} categories`);
        console.log(`[Scraper] ========================================`);
        
        return result;
    }

    /**
     * Save data to JSON file
     */
    async saveToFile(data, filename = 'bonds-data.json') {
        try {
            const dir = filename.includes('/') ? filename.substring(0, filename.lastIndexOf('/')) : '.';
            
            // Ensure directory exists
            try {
                await fs.access(dir);
            } catch {
                await fs.mkdir(dir, { recursive: true });
            }
            
            await fs.writeFile(filename, JSON.stringify(data, null, 2), 'utf8');
            const stats = await fs.stat(filename);
            console.log(`[Scraper] Data saved to ${filename} (${(stats.size / 1024).toFixed(2)} KB)`);
            return true;
        } catch (error) {
            console.error(`[Scraper] Error saving file:`, error.message);
            return false;
        }
    }
}

// Run if called directly
if (require.main === module) {
    (async () => {
        const scraper = new SimpleToolsBondScraperV2();
        const data = await scraper.scrapeAll();
        
        // Save to data/ directory
        const saved = await scraper.saveToFile(data, 'data/bonds-data.json');
        
        if (saved) {
            console.log('[Scraper] ✓ SUCCESS - Ready for deployment');
        } else {
            console.error('[Scraper] ✗ FAILED - Check errors above');
            process.exit(1);
        }
    })();
}

module.exports = SimpleToolsBondScraperV2;
