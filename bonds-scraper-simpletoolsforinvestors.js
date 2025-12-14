/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * SimpleTools For Investors - Complete Bond Scraper
 * Scrapes all bond categories from simpletoolsforinvestors.eu
 */

const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs').promises;

class SimpleToolsBondScraper {
    constructor() {
        this.baseUrl = 'https://www.simpletoolsforinvestors.eu/monitor_info.php';
        this.categories = this.defineCategories();
    }

    defineCategories() {
        return {
            // Italian Government Bonds
            'gov-it-btp': {
                name: 'BTP - Buoni Tesoro Poliennali',
                description: 'Titoli di Stato italiani a medio-lungo termine',
                monitor: 'italia',
                filter: bond => bond.type === 'BTP'
            },
            'gov-it-bot': {
                name: 'BOT - Buoni Ordinari del Tesoro',
                description: 'Titoli di Stato italiani a breve termine',
                monitor: 'buoni_ordinari',
                filter: bond => bond.type === 'BOT'
            },
            'gov-it-cct': {
                name: 'CCT - Certificati di Credito del Tesoro',
                description: 'Titoli di Stato italiani a tasso variabile',
                monitor: 'italia',
                filter: bond => bond.type === 'CCT'
            },
            'gov-it-ctz': {
                name: 'CTZ - Certificati del Tesoro Zero Coupon',
                description: 'Titoli di Stato italiani zero coupon',
                monitor: 'italia',
                filter: bond => bond.type === 'CTZ'
            },

            // European Government Bonds
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
            'gov-eu-spain': {
                name: 'Spagna - Bonos',
                description: 'Titoli di Stato spagnoli',
                monitor: 'altri_europa',
                filter: bond => bond.name && bond.name.toUpperCase().includes('SPAGNA')
            },
            'gov-eu-netherlands': {
                name: 'Paesi Bassi - DSL',
                description: 'Titoli di Stato olandesi',
                monitor: 'altri_europa',
                filter: bond => bond.name && bond.name.toUpperCase().includes('PAESI BASSI')
            },
            'gov-eu-ireland': {
                name: 'Irlanda - IRISH',
                description: 'Titoli di Stato irlandesi',
                monitor: 'altri_europa',
                filter: bond => bond.name && bond.name.toUpperCase().includes('IRLANDA')
            },
            'gov-eu-portugal': {
                name: 'Portogallo - OT',
                description: 'Titoli di Stato portoghesi',
                monitor: 'altri_europa',
                filter: bond => bond.name && bond.name.toUpperCase().includes('PORTOGALLO')
            },
            'gov-eu-austria': {
                name: 'Austria - RAGB',
                description: 'Titoli di Stato austriaci',
                monitor: 'altri_europa',
                filter: bond => bond.name && bond.name.toUpperCase().includes('AUSTRIA')
            },
            'gov-eu-finland': {
                name: 'Finlandia - Finnish',
                description: 'Titoli di Stato finlandesi',
                monitor: 'altri_europa',
                filter: bond => bond.name && bond.name.toUpperCase().includes('FINLANDIA')
            },
            'gov-eu-belgium': {
                name: 'Belgio - OLO',
                description: 'Titoli di Stato belgi',
                monitor: 'altri_europa',
                filter: bond => bond.name && bond.name.toUpperCase().includes('BELGIO')
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
     * Scrape a single monitor
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

            // Find the main data table
            $('table').each((i, table) => {
                $(table).find('tr').each((j, row) => {
                    const cells = $(row).find('td');
                    
                    // Skip header rows and rows with too few cells
                    if (cells.length < 10) return;

                    const isin = $(cells[0]).text().trim();
                    
                    // Valid ISIN format check
                    if (!/^[A-Z]{2}[A-Z0-9]{10}$/.test(isin)) return;

                    const name = $(cells[2]).text().trim();
                    const currency = $(cells[3]).text().trim();
                    const maturity = $(cells[4]).text().trim();
                    const priceText = $(cells[8]).text().trim().replace(',', '.');
                    const yieldText = $(cells[12]).text().trim().replace(',', '.');

                    // Extract coupon from name (e.g., "BTP 01/10/2055 4,65%")
                    const couponMatch = name.match(/(\d+[,\.]\d+)%/);
                    const coupon = couponMatch ? parseFloat(couponMatch[1].replace(',', '.')) : 0;

                    // Determine bond type from name
                    let type = 'BOND';
                    if (name.includes('BTP')) type = 'BTP';
                    else if (name.includes('BOT')) type = 'BOT';
                    else if (name.includes('CCT')) type = 'CCT';
                    else if (name.includes('CTZ')) type = 'CTZ';

                    const bond = {
                        isin,
                        name,
                        type,
                        currency: currency || 'EUR',
                        maturity: this.parseDate(maturity),
                        coupon,
                        yield: parseFloat(yieldText) || 0,
                        price: parseFloat(priceText) || 0,
                        change: '+0.00',
                        lastUpdate: new Date().toISOString().split('T')[0]
                    };

                    bonds.push(bond);
                });
            });

            console.log(`[Scraper] Found ${bonds.length} bonds in ${monitorName}`);
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
        
        // Format: DD/MM/YYYY or YYYY-MM-DD
        if (dateStr.includes('-')) return dateStr;
        
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

        console.log(`[Scraper] Complete! Total: ${result.statistics.totalBonds} bonds in ${result.statistics.totalCategories} categories`);
        return result;
    }

    /**
     * Save data to JSON file
     */
    async saveToFile(data, filename = 'bonds-data.json') {
        try {
            await fs.writeFile(filename, JSON.stringify(data, null, 2), 'utf8');
            console.log(`[Scraper] Data saved to ${filename}`);
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
        const scraper = new SimpleToolsBondScraper();
        const data = await scraper.scrapeAll();
        await scraper.saveToFile(data, 'data/bonds-data.json');
    })();
}

module.exports = SimpleToolsBondScraper;
