/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * SimpleTools Bond Scraper - PUPPETEER V3 (AJAX WAIT + DEBUG)
 * Added: Dynamic wait, HTML save, screenshot, detailed logging
 */

const puppeteer = require('puppeteer');
const fs = require('fs').promises;

class PuppeteerBondScraperV3 {
    constructor() {
        this.baseUrl = 'https://www.simpletoolsforinvestors.eu/monitor_info.php';
        this.categories = this.defineCategories();
        this.browser = null;
        this.debugMode = true; // Save HTML and screenshots for first monitor
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
        if (name.includes(type)) return true;
        
        if (type === 'BOT' && bond.isin.startsWith('IT') && bond.maturity) {
            const maturityDate = new Date(bond.maturity);
            const now = new Date();
            const monthsDiff = (maturityDate - now) / (1000 * 60 * 60 * 24 * 30);
            return monthsDiff <= 12;
        }
        
        return false;
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
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process'
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

    async scrapeMonitor(monitorName, isFirstMonitor = false) {
        const page = await this.browser.newPage();
        
        try {
            const url = `${this.baseUrl}?monitor=${monitorName}&yieldtype=G&timescale=DUR`;
            console.log(`[Scraper] Fetching ${monitorName}...`);

            await page.setViewport({ width: 1920, height: 1080 });
            await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

            // Navigate with longer timeout
            await page.goto(url, { 
                waitUntil: 'networkidle2',
                timeout: 90000 
            });

            // CRITICAL: Wait for AJAX/JavaScript to load table data
            console.log('[Scraper] Waiting 10s for dynamic content...');
            await new Promise(resolve => setTimeout(resolve, 10000));

            // Wait for any table
            await page.waitForSelector('table', { timeout: 10000 });

            const htmlContent = await page.content();
            console.log(`[Debug] HTML length: ${htmlContent.length}`);

            // DEBUG MODE: Save HTML and screenshot for first monitor
            if (isFirstMonitor && this.debugMode) {
                try {
                    await fs.mkdir('debug', { recursive: true });
                    await fs.writeFile(`debug/${monitorName}.html`, htmlContent);
                    await page.screenshot({ path: `debug/${monitorName}.png`, fullPage: true });
                    console.log(`[Debug] Saved HTML and screenshot to debug/ folder`);
                } catch (err) {
                    console.log(`[Debug] Could not save debug files: ${err.message}`);
                }
            }

            // Extract with ENHANCED logging
            const bonds = await page.evaluate(() => {
                const results = [];
                const tables = document.querySelectorAll('table');
                
                console.log(`=== TABLE ANALYSIS ===`);
                console.log(`Found ${tables.length} tables total`);
                
                // Analyze each table
                let bestTable = null;
                let maxDataRows = 0;
                
                tables.forEach((table, tableIdx) => {
                    const rows = Array.from(table.querySelectorAll('tr'));
                    
                    // Count rows that look like data (have many cells)
                    const dataRows = rows.filter(row => {
                        const cells = row.querySelectorAll('td');
                        return cells.length >= 8;
                    });
                    
                    console.log(`Table ${tableIdx}:`);
                    console.log(`  - Total rows: ${rows.length}`);
                    console.log(`  - Data rows (8+ cells): ${dataRows.length}`);
                    console.log(`  - Classes: ${table.className}`);
                    console.log(`  - ID: ${table.id}`);
                    
                    // Sample first row
                    if (rows.length > 0) {
                        const firstRow = rows[0];
                        const cells = firstRow.querySelectorAll('td, th');
                        console.log(`  - First row cells: ${cells.length}`);
                        if (cells.length > 0) {
                            console.log(`  - First cell text: "${cells[0].textContent.trim().substring(0, 50)}"`);
                        }
                    }
                    
                    if (dataRows.length > maxDataRows) {
                        maxDataRows = dataRows.length;
                        bestTable = table;
                    }
                });
                
                console.log(`\n=== SELECTED TABLE ===`);
                if (!bestTable) {
                    console.log('ERROR: No suitable table found!');
                    return [];
                }
                
                console.log(`Using table with ${maxDataRows} data rows`);
                
                const rows = Array.from(bestTable.querySelectorAll('tr'));
                console.log(`\n=== ROW ANALYSIS (first 10) ===`);
                
                rows.slice(0, 10).forEach((row, idx) => {
                    const cells = row.querySelectorAll('td');
                    console.log(`Row ${idx}: ${cells.length} cells`);
                    
                    if (cells.length >= 3) {
                        console.log(`  [0]: "${cells[0].textContent.trim().substring(0, 30)}"`);
                        console.log(`  [1]: "${cells[1].textContent.trim().substring(0, 30)}"`);
                        console.log(`  [2]: "${cells[2].textContent.trim().substring(0, 30)}"`);
                    }
                });
                
                // Parse bonds with original logic
                rows.forEach((row, rowIdx) => {
                    const cells = row.querySelectorAll('td');
                    
                    if (cells.length < 8) return;
                    
                    const firstCellText = cells[0]?.textContent?.trim() || '';
                    
                    // Skip UI elements
                    if (!firstCellText || 
                        firstCellText.length < 10 || 
                        firstCellText.includes('SCALA') ||
                        firstCellText.includes('DURATION') ||
                        firstCellText.includes('TIME') ||
                        firstCellText.includes('Tipo') ||
                        firstCellText.includes('Emittente')) {
                        return;
                    }
                    
                    const isin = firstCellText.toUpperCase();
                    if (!/^[A-Z]{2}[A-Z0-9]{9,}/.test(isin)) {
                        return;
                    }
                    
                    const name = cells[2]?.textContent?.trim() || '';
                    if (!name || 
                        name.includes('Tipo yield') ||
                        name.includes('Lordo') ||
                        name.includes('Netto')) {
                        return;
                    }
                    
                    const currency = cells[3]?.textContent?.trim() || 'EUR';
                    const maturity = cells[4]?.textContent?.trim() || '';
                    
                    let priceText = cells[8]?.textContent?.trim() || '';
                    if (!priceText || isNaN(parseFloat(priceText.replace(',', '.')))) {
                        priceText = cells[9]?.textContent?.trim() || '';
                    }
                    if (!priceText || isNaN(parseFloat(priceText.replace(',', '.')))) {
                        priceText = cells[7]?.textContent?.trim() || '100';
                    }
                    
                    let yieldText = cells[12]?.textContent?.trim() || '';
                    if (!yieldText || isNaN(parseFloat(yieldText.replace(',', '.')))) {
                        yieldText = cells[13]?.textContent?.trim() || '';
                    }
                    if (!yieldText || isNaN(parseFloat(yieldText.replace(',', '.')))) {
                        yieldText = cells[11]?.textContent?.trim() || '0';
                    }
                    
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
                    
                    if (results.length <= 3) {
                        console.log(`Bond ${results.length}: ${isin} - ${name.substring(0, 30)}`);
                    }
                });
                
                console.log(`\n=== EXTRACTION COMPLETE ===`);
                console.log(`Total valid bonds: ${results.length}`);
                
                return results;
            });

            bonds.forEach(bond => {
                bond.maturity = this.parseDate(bond.maturity);
            });

            console.log(`[Scraper] ✓ Found ${bonds.length} valid bonds in ${monitorName}`);
            if (bonds.length > 0) {
                console.log(`[Scraper]   Sample: ${bonds[0].isin} - ${bonds[0].name.substring(0, 40)}...`);
            } else {
                console.log(`[Scraper]   ⚠ WARNING: No bonds found`);
            }

            await page.close();
            return bonds;

        } catch (error) {
            console.error(`[Scraper] ✗ Error fetching ${monitorName}:`, error.message);
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
        console.log('SIMPLETOOL BONDS SCRAPER - PUPPETEER V3 (AJAX WAIT + DEBUG)');
        console.log('Added: 10s dynamic wait, HTML save, detailed logging');
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

        console.log(`\n[Scraper] Will fetch ${monitorMap.size} unique monitors`);
        console.log('[Scraper] Using 3s delay between monitors...\n');

        const monitorData = new Map();
        let monitorIndex = 0;
        
        for (const [monitor, categories] of monitorMap.entries()) {
            monitorIndex++;
            console.log(`[Scraper] Monitor ${monitorIndex}/${monitorMap.size}: ${monitor}`);
            
            const isFirstMonitor = (monitorIndex === 1);
            const bonds = await this.scrapeMonitor(monitor, isFirstMonitor);
            monitorData.set(monitor, bonds);
            
            if (bonds.length > 0) {
                result.statistics.successfulMonitors++;
            } else {
                result.statistics.failedMonitors++;
            }
            
            if (monitorIndex < monitorMap.size) {
                console.log('[Scraper] Waiting 3s...\n');
                await new Promise(resolve => setTimeout(resolve, 3000));
            }
        }

        await this.closeBrowser();

        console.log('\n[Scraper] Processing categories...');
        
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
            console.log(`[Scraper] ${status} ${catId}: ${filteredBonds.length} bonds`);
        }

        console.log('\n' + '='.repeat(70));
        console.log(`SCRAPING COMPLETE`);
        console.log(`Total bonds: ${result.statistics.totalBonds}`);
        console.log(`Successful: ${result.statistics.successfulMonitors}/${monitorMap.size} monitors`);
        console.log(`Failed: ${result.statistics.failedMonitors}/${monitorMap.size} monitors`);
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
            console.log(`\n[File] Saved to ${filename} (${(stats.size / 1024).toFixed(2)} KB)`);
            return true;
        } catch (error) {
            console.error(`[File] Error saving:`, error.message);
            return false;
        }
    }
}

if (require.main === module) {
    (async () => {
        const scraper = new PuppeteerBondScraperV3();
        
        try {
            const data = await scraper.scrapeAll();
            const saved = await scraper.saveToFile(data, 'data/bonds-data.json');
            
            if (saved && data.statistics.totalBonds > 0) {
                console.log('\n✓ SUCCESS - Ready for deployment');
                process.exit(0);
            } else if (saved) {
                console.log('\n⚠ WARNING - File created but no bonds found');
                console.log('Check debug/ folder for HTML and screenshot');
                process.exit(1);
            } else {
                console.error('\n✗ FAILED - Could not save file');
                process.exit(1);
            }
        } catch (error) {
            console.error('\n✗ FATAL ERROR:', error);
            await scraper.closeBrowser();
            process.exit(1);
        }
    })();
}

module.exports = PuppeteerBondScraperV3;
