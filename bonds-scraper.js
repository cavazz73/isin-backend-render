/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Bonds Data Scraper - Borsa Italiana
 * Scrapes real ISIN codes and end-of-day prices from Borsa Italiana
 * Runs daily via GitHub Actions at 18:30 UTC
 */

const puppeteer = require('puppeteer');
const fs = require('fs').promises;
const path = require('path');

// Configuration
const CONFIG = {
    outputPath: path.join(__dirname, 'data', 'bonds-data.json'),
    timeout: 30000,
    headless: true,
    urls: {
        btp: 'https://www.borsaitaliana.it/borsa/obbligazioni/mot/btp/lista.html',
        bot: 'https://www.borsaitaliana.it/borsa/obbligazioni/mot/bot/lista.html',
        cct: 'https://www.borsaitaliana.it/borsa/obbligazioni/mot/cct/lista.html',
        ctz: 'https://www.borsaitaliana.it/borsa/obbligazioni/mot/ctz/lista.html'
    }
};

/**
 * Initialize Puppeteer browser
 */
async function initBrowser() {
    console.log('🚀 Initializing Puppeteer...');
    const browser = await puppeteer.launch({
        headless: CONFIG.headless,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu'
        ]
    });
    return browser;
}

/**
 * Scrape Italian Government Bonds (BTP, BOT, CCT, CTZ)
 */
async function scrapeItalianBonds(browser, type, url) {
    console.log(`\n📊 Scraping ${type} from Borsa Italiana...`);
    
    const page = await browser.newPage();
    await page.setViewport({ width: 1920, height: 1080 });
    
    try {
        // Navigate to page
        await page.goto(url, { 
            waitUntil: 'networkidle2',
            timeout: CONFIG.timeout 
        });
        
        // Wait for table to load
        await page.waitForSelector('table.m-table', { timeout: CONFIG.timeout });
        
        // Extract bond data
        const bonds = await page.evaluate((bondType) => {
            const rows = Array.from(document.querySelectorAll('table.m-table tbody tr'));
            const results = [];
            
            rows.forEach(row => {
                try {
                    const cells = row.querySelectorAll('td');
                    if (cells.length < 6) return;
                    
                    // Extract data from cells
                    const nameCell = cells[0]?.innerText?.trim() || '';
                    const isinCell = cells[1]?.innerText?.trim() || '';
                    const priceCell = cells[2]?.innerText?.trim() || '';
                    const yieldCell = cells[3]?.innerText?.trim() || '';
                    const couponCell = cells[4]?.innerText?.trim() || '';
                    const maturityCell = cells[5]?.innerText?.trim() || '';
                    
                    // Parse values
                    const isin = isinCell;
                    const name = nameCell;
                    const price = parseFloat(priceCell.replace(',', '.')) || 0;
                    const yieldValue = parseFloat(yieldCell.replace(',', '.')) || 0;
                    const coupon = parseFloat(couponCell.replace(',', '.')) || 0;
                    
                    // Parse maturity date
                    let maturity = '';
                    if (maturityCell) {
                        const parts = maturityCell.split('/');
                        if (parts.length === 3) {
                            maturity = `${parts[2]}-${parts[1]}-${parts[0]}`;
                        }
                    }
                    
                    if (isin && isin.length === 12) {
                        results.push({
                            isin,
                            name,
                            type: bondType,
                            country: 'IT',
                            currency: 'EUR',
                            maturity,
                            coupon,
                            yield: yieldValue,
                            price,
                            change: '+0.00', // Not available in static table
                            lastUpdate: new Date().toISOString().split('T')[0]
                        });
                    }
                } catch (err) {
                    console.error('Error parsing row:', err);
                }
            });
            
            return results;
        }, type);
        
        console.log(`✅ Scraped ${bonds.length} ${type} bonds`);
        return bonds;
        
    } catch (error) {
        console.error(`❌ Error scraping ${type}:`, error.message);
        return [];
    } finally {
        await page.close();
    }
}

/**
 * Main scraper function
 */
async function scrapeBonds() {
    console.log('===============================================');
    console.log('🏦 BONDS DATA SCRAPER - Borsa Italiana');
    console.log('===============================================\n');
    
    const startTime = Date.now();
    let browser;
    
    try {
        browser = await initBrowser();
        
        // Scrape all Italian government bonds
        const btpBonds = await scrapeItalianBonds(browser, 'BTP', CONFIG.urls.btp);
        const botBonds = await scrapeItalianBonds(browser, 'BOT', CONFIG.urls.bot);
        const cctBonds = await scrapeItalianBonds(browser, 'CCT', CONFIG.urls.cct);
        const ctzBonds = await scrapeItalianBonds(browser, 'CTZ', CONFIG.urls.ctz);
        
        // Combine all Italian bonds
        const allItalianBonds = [...btpBonds, ...botBonds, ...cctBonds, ...ctzBonds];
        
        // Build output structure
        const output = {
            lastUpdate: new Date().toISOString(),
            categories: {
                'it-governativi': {
                    name: 'IT Governativi Italia',
                    description: 'BTP, BOT, CCT, CTZ - Titoli di Stato italiani',
                    count: allItalianBonds.length,
                    bonds: allItalianBonds
                },
                'eu-governativi': {
                    name: 'EU Governativi Europa',
                    description: 'Titoli di Stato europei (in sviluppo)',
                    count: 0,
                    bonds: []
                },
                'sovranazionali': {
                    name: 'Sovranazionali',
                    description: 'BEI, EFSF, ESM (in sviluppo)',
                    count: 0,
                    bonds: []
                },
                'corporate': {
                    name: 'Corporate',
                    description: 'Obbligazioni societarie (in sviluppo)',
                    count: 0,
                    bonds: []
                }
            },
            statistics: {
                totalBonds: allItalianBonds.length,
                byCategory: {
                    'it-governativi': allItalianBonds.length,
                    'eu-governativi': 0,
                    'sovranazionali': 0,
                    'corporate': 0
                },
                byType: {
                    BTP: btpBonds.length,
                    BOT: botBonds.length,
                    CCT: cctBonds.length,
                    CTZ: ctzBonds.length
                },
                errors: []
            }
        };
        
        // Save to file
        const outputDir = path.dirname(CONFIG.outputPath);
        await fs.mkdir(outputDir, { recursive: true });
        await fs.writeFile(
            CONFIG.outputPath, 
            JSON.stringify(output, null, 2),
            'utf8'
        );
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        
        console.log('\n===============================================');
        console.log('✅ SCRAPING COMPLETED SUCCESSFULLY');
        console.log('===============================================');
        console.log(`📊 Total bonds scraped: ${output.statistics.totalBonds}`);
        console.log(`   - BTP: ${btpBonds.length}`);
        console.log(`   - BOT: ${botBonds.length}`);
        console.log(`   - CCT: ${cctBonds.length}`);
        console.log(`   - CTZ: ${ctzBonds.length}`);
        console.log(`⏱️  Duration: ${duration}s`);
        console.log(`💾 Saved to: ${CONFIG.outputPath}`);
        console.log('===============================================\n');
        
        return output;
        
    } catch (error) {
        console.error('\n❌ SCRAPING FAILED:', error);
        throw error;
    } finally {
        if (browser) {
            await browser.close();
        }
    }
}

// Run if called directly
if (require.main === module) {
    scrapeBonds()
        .then(() => process.exit(0))
        .catch((error) => {
            console.error('Fatal error:', error);
            process.exit(1);
        });
}

module.exports = { scrapeBonds };
