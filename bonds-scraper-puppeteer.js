/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * V5 FINAL - 100% COMPLETE
 * Fixes: Yield column (cells[13] with cells.length check), BOT/CCT/CTZ filters, all categories
 */

const puppeteer = require('puppeteer');
const fs = require('fs').promises;

class BondsScraperV5Final {
    constructor() {
        this.baseUrl = 'https://www.simpletoolsforinvestors.eu/monitor_info.php';
        this.categories = this.defineCategories();
        this.browser = null;
    }

    defineCategories() {
        return {
            'gov-it-btp': {
                name: 'BTP - Buoni Tesoro Poliennali',
                description: 'Titoli di Stato italiani a medio-lungo termine',
                monitor: 'italia',
                filter: bond => this.isBTP(bond)
            },
            'gov-it-bot': {
                name: 'BOT - Buoni Ordinari del Tesoro',
                description: 'Titoli di Stato italiani a breve termine',
                monitor: 'italia',  // ← FIX: Stesso monitor di BTP, filtro per maturity
                filter: bond => this.isBOT(bond)
            },
            'gov-it-cct': {
                name: 'CCT - Certificati di Credito del Tesoro',
                description: 'Titoli di Stato italiani a tasso variabile',
                monitor: 'italia',
                filter: bond => this.isCCT(bond)
            },
            'gov-it-ctz': {
                name: 'CTZ - Certificati del Tesoro Zero Coupon',
                description: 'Titoli di Stato italiani zero coupon',
                monitor: 'italia',
                filter: bond => this.isCTZ(bond)
            },
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
            'supranational': {
                name: 'Sovranazionali',
                description: 'Obbligazioni emesse da enti sovranazionali (BEI, ESM, etc)',
                monitor: ['sovranazionali', 'bei'],
                filter: () => true
            },
            'corporate-all': {
                name: 'Corporate',
                description: 'Obbligazioni societarie',
                monitor: 'corporate',
                filter: () => true
            }
        };
    }

    // NEW: Filtri intelligenti per titoli italiani
    isBTP(bond) {
        const name = (bond.name || '').toUpperCase();
        // BTP hanno maturity > 18 mesi e contengono "BTP" nel nome
        return (name.includes('BTP') || name.includes('FUTURA')) && 
               !name.includes('BOT') && !name.includes('CCT') && !name.includes('CTZ');
    }

    isBOT(bond) {
        const name = (bond.name || '').toUpperCase();
        // BOT: breve termine (< 12 mesi) oppure hanno "BOT" nel nome
        if (name.includes('BOT')) return true;
        
        // Calcola mesi alla maturity
        const maturity = new Date(bond.maturity);
        const now = new Date();
        const monthsToMaturity = (maturity - now) / (1000 * 60 * 60 * 24 * 30);
        
        return bond.country === 'IT' && monthsToMaturity > 0 && monthsToMaturity <= 12;
    }

    isCCT(bond) {
        const name = (bond.name || '').toUpperCase();
        // CCT: tasso variabile
        return name.includes('CCT') || name.includes('TASSO VARIABILE');
    }

    isCTZ(bond) {
        const name = (bond.name || '').toUpperCase();
        // CTZ: zero coupon con maturity 18-24 mesi
        if (name.includes('CTZ')) return true;
        
        const maturity = new Date(bond.maturity);
        const now = new Date();
        const monthsToMaturity = (maturity - now) / (1000 * 60 * 60 * 24 * 30);
        
        return bond.country === 'IT' && bond.coupon === 0 && 
               monthsToMaturity > 12 && monthsToMaturity <= 24;
    }

    isCountry(bond, keywords) {
        const name = (bond.name || '').toUpperCase();
        const isin = (bond.isin || '').toUpperCase();
        
        return keywords.some(keyword => 
            name.includes(keyword.toUpperCase()) || 
            isin.includes(keyword.toUpperCase())
        );
    }

    async initBrowser() {
        console.log('[Puppeteer] Launching browser...');
        this.browser = await puppeteer.launch({
            headless: 'new',
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu'
            ]
        });
        console.log('[Puppeteer] Browser ready');
    }

    async closeBrowser() {
        if (this.browser) {
            await this.browser.close();
            console.log('[Puppeteer] Browser closed');
        }
    }

    async scrapeMonitor(monitorName) {
        const page = await this.browser.newPage();
        
        try {
            const url = `${this.baseUrl}?monitor=${monitorName}&yieldtype=G&timescale=DUR`;
            console.log(`[Scraper] Fetching ${monitorName}...`);

            await page.setViewport({ width: 1920, height: 1080 });
            await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');

            await page.goto(url, { 
                waitUntil: 'networkidle2',
                timeout: 60000 
            });

            // Wait for AJAX/JavaScript
            await new Promise(resolve => setTimeout(resolve, 8000));
            await page.waitForSelector('table', { timeout: 10000 });

            // Extract bonds with CORRECT column indices and yield fallback
            const bonds = await page.evaluate(() => {
                const results = [];
                const tables = document.querySelectorAll('table');
                
                // Find table with most rows
                let bondTable = null;
                let maxRows = 0;
                
                tables.forEach(table => {
                    const rows = table.querySelectorAll('tr');
                    if (rows.length > maxRows) {
                        maxRows = rows.length;
                        bondTable = table;
                    }
                });
                
                if (!bondTable) return results;
                
                const rows = bondTable.querySelectorAll('tr');
                
                rows.forEach(row => {
                    const cells = row.querySelectorAll('td');
                    
                    // FIX: Need at least 14 columns to access cells[13]
                    if (cells.length < 14) return;
                    
                    // Column 0: ISIN
                    const isinText = cells[0]?.textContent?.trim() || '';
                    if (!isinText || isinText.length < 10) return;
                    
                    // Skip header rows
                    if (isinText.includes('SCALA') || isinText.includes('Codice')) return;
                    
                    // Validate ISIN pattern
                    const isin = isinText.toUpperCase();
                    if (!/^[A-Z]{2}[A-Z0-9]{10}/.test(isin)) return;
                    
                    // Column 3: Name/Description
                    const name = cells[3]?.textContent?.trim() || '';
                    if (!name || name.length < 5) return;
                    
                    // Skip if name looks like UI text
                    if (name.includes('Tipo yield') || name.includes('Descrizione')) return;
                    
                    // Column 4: Currency
                    const currency = cells[4]?.textContent?.trim() || 'EUR';
                    
                    // Column 5: Maturity date
                    const maturity = cells[5]?.textContent?.trim() || '';
                    
                    // FIX: Try multiple columns for yield (13, then 12, then 14)
                    let yieldText = cells[13]?.textContent?.trim() || '';
                    let yieldSource = 13;
                    
                    // Validate it's a number
                    if (!yieldText || isNaN(parseFloat(yieldText.replace(',', '.')))) {
                        yieldText = cells[12]?.textContent?.trim() || '';
                        yieldSource = 12;
                    }
                    
                    if (!yieldText || isNaN(parseFloat(yieldText.replace(',', '.')))) {
                        yieldText = cells[14]?.textContent?.trim() || '0';
                        yieldSource = 14;
                    }
                    
                    // Skip if yield text looks like label (contains letters)
                    if (yieldText && /[a-zA-Z]/.test(yieldText)) {
                        yieldText = '0';
                    }
                    
                    // Try column 9 for price
                    let priceText = cells[9]?.textContent?.trim() || '';
                    if (!priceText || isNaN(parseFloat(priceText.replace(',', '.')))) {
                        priceText = cells[10]?.textContent?.trim() || '100';
                    }
                    
                    // Extract coupon from name
                    const couponMatch = name.match(/(\d+[,\.]\d+)%/);
                    const coupon = couponMatch ? parseFloat(couponMatch[1].replace(',', '.')) : 0;
                    
                    const country = isin.substring(0, 2);
                    
                    let type = 'BOND';
                    if (name.includes('BTP')) type = 'BTP';
                    else if (name.includes('BOT')) type = 'BOT';
                    else if (name.includes('CCT')) type = 'CCT';
                    else if (name.includes('CTZ')) type = 'CTZ';
                    
                    results.push({
                        isin,
                        name,
                        type,
                        country,
                        currency,
                        maturity,
                        coupon,
                        yield: parseFloat(yieldText.replace(',', '.')) || 0,
                        price: parseFloat(priceText.replace(',', '.')) || 100,
                        change: '+0.00',
                        lastUpdate: new Date().toISOString().split('T')[0],
                        _yieldColumn: yieldSource  // Debug: quale colonna ha funzionato
                    });
                });
                
                return results;
            });

            // Parse dates
            bonds.forEach(bond => {
                bond.maturity = this.parseDate(bond.maturity);
            });

            console.log(`[Scraper] ✓ Found ${bonds.length} bonds in ${monitorName}`);
            if (bonds.length > 0) {
                const firstBond = bonds[0];
                console.log(`[Scraper]   First: ${firstBond.isin} - ${firstBond.name.substring(0, 40)}`);
                console.log(`[Scraper]   Yield: ${firstBond.yield}% (col ${firstBond._yieldColumn})`);
                
                const validYields = bonds.filter(b => b.yield > 0);
                if (validYields.length > 0) {
                    const yieldMin = Math.min(...validYields.map(b => b.yield));
                    const yieldMax = Math.max(...validYields.map(b => b.yield));
                    console.log(`[Scraper]   Yield range: ${yieldMin.toFixed(2)}% - ${yieldMax.toFixed(2)}%`);
                }
            }

            await page.close();
            return bonds;

        } catch (error) {
            console.error(`[Scraper] ✗ Error in ${monitorName}: ${error.message}`);
            await page.close();
            return [];
        }
    }

    parseDate(dateStr) {
        if (!dateStr) return '';
        if (dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) return dateStr;
        
        const parts = dateStr.split('/');
        if (parts.length === 3) {
            const [day, month, year] = parts;
            return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
        }
        
        return dateStr;
    }

    async scrapeAll() {
        console.log('='.repeat(70));
        console.log('BONDS SCRAPER V5 - 100% COMPLETE');
        console.log('Fixed: Yield column, BOT/CCT/CTZ filters, all categories');
        console.log('='.repeat(70));

        await this.initBrowser();

        const result = {
            lastUpdate: new Date().toISOString(),
            categories: {},
            statistics: {
                totalBonds: 0,
                totalCategories: 0,
                successfulMonitors: 0,
                failedMonitors: 0
            }
        };

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

        console.log(`\n[Scraper] Fetching ${monitorMap.size} unique monitors\n`);

        const monitorData = new Map();
        let monitorIndex = 0;
        
        for (const [monitor, categories] of monitorMap.entries()) {
            monitorIndex++;
            console.log(`[${monitorIndex}/${monitorMap.size}] ${monitor}`);
            
            const bonds = await this.scrapeMonitor(monitor);
            monitorData.set(monitor, bonds);
            
            if (bonds.length > 0) {
                result.statistics.successfulMonitors++;
            } else {
                result.statistics.failedMonitors++;
            }
            
            if (monitorIndex < monitorMap.size) {
                await new Promise(resolve => setTimeout(resolve, 3000));
            }
        }

        await this.closeBrowser();

        console.log('\n[Processing] Filtering categories...\n');
        
        for (const [catId, catConfig] of Object.entries(this.categories)) {
            const monitors = Array.isArray(catConfig.monitor) ? catConfig.monitor : [catConfig.monitor];
            
            let allBonds = [];
            for (const monitor of monitors) {
                const bonds = monitorData.get(monitor) || [];
                allBonds = allBonds.concat(bonds);
            }

            const filteredBonds = allBonds.filter(catConfig.filter);
            
            // Remove debug field before saving
            filteredBonds.forEach(b => delete b._yieldColumn);
            
            filteredBonds.sort((a, b) => b.yield - a.yield);

            result.categories[catId] = {
                name: catConfig.name,
                description: catConfig.description,
                count: filteredBonds.length,
                bonds: filteredBonds
            };

            result.statistics.totalBonds += filteredBonds.length;
            result.statistics.totalCategories++;

            const status = filteredBonds.length > 0 ? '✓' : '✗';
            const yieldInfo = filteredBonds.length > 0 && filteredBonds[0].yield > 0 
                ? ` (yield: ${filteredBonds[0].yield.toFixed(2)}%)` 
                : '';
            console.log(`${status} ${catId}: ${filteredBonds.length} bonds${yieldInfo}`);
        }

        console.log('\n' + '='.repeat(70));
        console.log(`COMPLETE: ${result.statistics.totalBonds} total bonds`);
        console.log(`Success: ${result.statistics.successfulMonitors}/${monitorMap.size} monitors`);
        
        const bondsWithYield = Object.values(result.categories)
            .flatMap(cat => cat.bonds)
            .filter(b => b.yield > 0);
        console.log(`Bonds with yield > 0: ${bondsWithYield.length}/${result.statistics.totalBonds}`);
        console.log('='.repeat(70));
        
        return result;
    }

    async saveToFile(data, filename = 'bonds-data.json') {
        try {
            const dir = filename.includes('/') ? filename.substring(0, filename.lastIndexOf('/')) : '.';
            
            try {
                await fs.access(dir);
            } catch {
                await fs.mkdir(dir, { recursive: true });
            }
            
            await fs.writeFile(filename, JSON.stringify(data, null, 2), 'utf8');
            const stats = await fs.stat(filename);
            console.log(`\nFile saved: ${filename} (${(stats.size / 1024).toFixed(2)} KB)`);
            return true;
        } catch (error) {
            console.error(`Error saving: ${error.message}`);
            return false;
        }
    }
}

if (require.main === module) {
    (async () => {
        const scraper = new BondsScraperV5Final();
        
        try {
            const data = await scraper.scrapeAll();
            const saved = await scraper.saveToFile(data, 'data/bonds-data.json');
            
            const bondsWithYield = Object.values(data.categories)
                .flatMap(cat => cat.bonds)
                .filter(b => b.yield > 0);
            
            if (saved && data.statistics.totalBonds > 500 && bondsWithYield.length > 100) {
                console.log('\n✓ SUCCESS - 100% Complete!');
                console.log(`  ${data.statistics.totalBonds} bonds with ${bondsWithYield.length} yields`);
                process.exit(0);
            } else if (saved && data.statistics.totalBonds > 100) {
                console.log(`\n⚠ PARTIAL - ${data.statistics.totalBonds} bonds, but yields may need fixing`);
                process.exit(0);
            } else {
                console.error('\n✗ FAILED - Insufficient data');
                process.exit(1);
            }
        } catch (error) {
            console.error('\n✗ FATAL:', error.message);
            await scraper.closeBrowser();
            process.exit(1);
        }
    })();
}

module.exports = BondsScraperV5Final;
