/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * FINAL VERSION - Based on real HTML structure analysis
 * Column indices confirmed: 0=ISIN, 3=Name, 4=Currency, 5=Maturity, 12=Yield
 */

const puppeteer = require('puppeteer');
const fs = require('fs').promises;

class BondsScraperFinal {
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
                filter: bond => this.isBondType(bond, 'BTP')
            },
            'gov-it-bot': {
                name: 'BOT - Buoni Ordinari del Tesoro',
                description: 'Titoli di Stato italiani a breve termine',
                monitor: 'buoni_ordinari',
                filter: () => true
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

    isBondType(bond, type) {
        const name = (bond.name || '').toUpperCase();
        return name.includes(type);
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

            // Extract bonds with CORRECT column indices
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
                
                if (!bondTable) return [];
                
                const rows = bondTable.querySelectorAll('tr');
                
                rows.forEach(row => {
                    const cells = row.querySelectorAll('td');
                    if (cells.length < 13) return; // Need at least 13 columns
                    
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
                    
                    // Column 12: Yield (confirmed from real HTML)
                    const yieldText = cells[12]?.textContent?.trim() || '0';
                    
                    // Try column 9 for price (may vary)
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
                        lastUpdate: new Date().toISOString().split('T')[0]
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
                console.log(`[Scraper]   First: ${bonds[0].isin} - ${bonds[0].name.substring(0, 40)}`);
                console.log(`[Scraper]   Yield range: ${Math.min(...bonds.map(b => b.yield)).toFixed(2)}% - ${Math.max(...bonds.map(b => b.yield)).toFixed(2)}%`);
            }

            await page.close();
            return bonds;

        } catch (error) {
            console.error(`[Scraper] ✗ Error: ${error.message}`);
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
        
        // Already in YYYY-MM-DD format
        if (dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) return dateStr;
        
        return dateStr;
    }

    async scrapeAll() {
        console.log('='.repeat(70));
        console.log('BONDS SCRAPER - FINAL VERSION (V4)');
        console.log('Based on real HTML structure analysis');
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
            console.log(`${status} ${catId}: ${filteredBonds.length} bonds`);
        }

        console.log('\n' + '='.repeat(70));
        console.log(`COMPLETE: ${result.statistics.totalBonds} total bonds`);
        console.log(`Success: ${result.statistics.successfulMonitors}/${monitorMap.size} monitors`);
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
        const scraper = new BondsScraperFinal();
        
        try {
            const data = await scraper.scrapeAll();
            const saved = await scraper.saveToFile(data, 'data/bonds-data.json');
            
            if (saved && data.statistics.totalBonds > 100) {
                console.log('\n✓ SUCCESS - Ready for production');
                process.exit(0);
            } else if (saved && data.statistics.totalBonds > 0) {
                console.log(`\n⚠ WARNING - Only ${data.statistics.totalBonds} bonds found`);
                process.exit(1);
            } else {
                console.error('\n✗ FAILED - No bonds found');
                process.exit(1);
            }
        } catch (error) {
            console.error('\n✗ FATAL:', error.message);
            await scraper.closeBrowser();
            process.exit(1);
        }
    })();
}

module.exports = BondsScraperFinal;
