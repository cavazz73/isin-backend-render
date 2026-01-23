/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Certificates Scraper v9.0 - CertificatiEDerivati.it
 * Scrapes certificates on indices, commodities, currencies, rates
 * 
 * Usage: node certificates-scraper.js
 */

const puppeteer = require('puppeteer');
const fs = require('fs');

// Configuration
const CONFIG = {
    searchUrl: 'https://www.certificatiederivati.it/db_bs_estrazione_ricerca.asp',
    detailUrl: 'https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin=',
    emissionsUrl: 'https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp',
    timeout: 30000,
    waitBetweenPages: 1500,
    maxPages: 30,
    maxDetails: 100
};

// Keywords to identify non-stock underlyings
const INDEX_KEYWORDS = ['basket di indici', 'indici', 'index', 'indices', 'ftse', 'stoxx', 'dax', 's&p', 'nasdaq'];
const RATE_KEYWORDS = ['btp', 'bund', 'euribor', 'tasso', 'rate', 'bond', 'oat'];
const COMMODITY_KEYWORDS = ['gold', 'oro', 'silver', 'oil', 'petrolio', 'commodity', 'gas', 'copper'];
const CURRENCY_KEYWORDS = ['eur/usd', 'usd/jpy', 'forex', 'currency', 'cambio'];

/**
 * Categorize underlying based on keywords
 */
function categorizeUnderlying(sottostante, name) {
    const text = `${sottostante} ${name}`.toLowerCase();
    
    for (const kw of INDEX_KEYWORDS) {
        if (text.includes(kw)) return 'index';
    }
    for (const kw of RATE_KEYWORDS) {
        if (text.includes(kw)) return 'rate';
    }
    for (const kw of COMMODITY_KEYWORDS) {
        if (text.includes(kw)) return 'commodity';
    }
    for (const kw of CURRENCY_KEYWORDS) {
        if (text.includes(kw)) return 'currency';
    }
    return 'stock';
}

/**
 * Detect certificate type from name
 */
function detectType(name) {
    const n = name.toLowerCase();
    const types = [
        ['phoenix memory', 'Phoenix Memory'],
        ['cash collect', 'Cash Collect'],
        ['bonus cap', 'Bonus Cap'],
        ['top bonus', 'Top Bonus'],
        ['express', 'Express'],
        ['equity protection', 'Equity Protection'],
        ['credit linked', 'Credit Linked'],
        ['digital', 'Digital'],
        ['airbag', 'Airbag'],
        ['autocall', 'Autocallable'],
        ['memory', 'Memory'],
        ['phoenix', 'Phoenix'],
        ['reverse', 'Reverse'],
        ['fixed', 'Fixed Coupon'],
        ['benchmark', 'Benchmark'],
        ['tracker', 'Tracker'],
        ['outperformance', 'Outperformance'],
        ['twin win', 'Twin Win'],
    ];
    
    for (const [kw, type] of types) {
        if (n.includes(kw)) return type;
    }
    return 'Certificate';
}

/**
 * Normalize issuer name
 */
function normalizeIssuer(issuer) {
    const mappings = [
        ['bnp paribas', 'BNP Paribas'],
        ['societe generale', 'Société Générale'],
        ['unicredit', 'UniCredit'],
        ['vontobel', 'Vontobel'],
        ['barclays', 'Barclays'],
        ['mediobanca', 'Mediobanca'],
        ['intesa sanpaolo', 'Intesa Sanpaolo'],
        ['leonteq', 'Leonteq Securities'],
        ['marex', 'Marex Financial'],
        ['goldman sachs', 'Goldman Sachs'],
        ['morgan stanley', 'Morgan Stanley'],
        ['deutsche bank', 'Deutsche Bank'],
    ];
    
    const lower = issuer.toLowerCase();
    for (const [k, v] of mappings) {
        if (lower.includes(k)) return v;
    }
    return issuer;
}

/**
 * Parse Italian date format (dd/mm/yyyy) to ISO
 */
function parseDate(dateStr) {
    if (!dateStr) return null;
    const match = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (match) {
        const [, d, m, y] = match;
        return `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
    }
    return dateStr;
}

/**
 * Main scraper
 */
async function scrapeCertificates() {
    console.log('='.repeat(70));
    console.log('CERTIFICATES SCRAPER v9.0 - JavaScript');
    console.log('Source: certificatiederivati.it');
    console.log('Target: INDICES | RATES | COMMODITIES | CURRENCIES');
    console.log('Copyright (c) 2024-2025 Mutna S.R.L.S.');
    console.log('='.repeat(70));
    console.log('Date:', new Date().toISOString());
    console.log('='.repeat(70));
    
    let browser;
    const certificates = [];
    const seenIsins = new Set();
    const stats = {
        pagesScanned: 0,
        totalRows: 0,
        matched: 0,
        skippedStocks: 0,
        skippedLeverage: 0
    };
    
    try {
        browser = await puppeteer.launch({
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage'
            ]
        });
        
        console.log('✅ Browser launched');
        
        const page = await browser.newPage();
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');
        
        // Scan database pages
        console.log('\n📋 Scanning database...');
        
        for (let pageNum = 1; pageNum <= CONFIG.maxPages; pageNum++) {
            const url = `${CONFIG.searchUrl}?p=${pageNum}&db=2&fase=quotazione&FiltroDal=2020-1-1&FiltroAl=2099-12-31`;
            
            try {
                await page.goto(url, { 
                    waitUntil: 'domcontentloaded', 
                    timeout: CONFIG.timeout 
                });
                await page.waitForTimeout(1500);
                
                // Extract table data
                const rows = await page.evaluate(() => {
                    const results = [];
                    const tables = document.querySelectorAll('table');
                    
                    tables.forEach(table => {
                        const trs = table.querySelectorAll('tr');
                        trs.forEach((tr, idx) => {
                            if (idx === 0) return; // Skip header
                            const tds = tr.querySelectorAll('td');
                            if (tds.length >= 7) {
                                results.push({
                                    isin: tds[0]?.textContent?.trim() || '',
                                    name: tds[1]?.textContent?.trim() || '',
                                    emittente: tds[2]?.textContent?.trim() || '',
                                    sottostante: tds[3]?.textContent?.trim() || '',
                                    scadenza: tds[7]?.textContent?.trim() || ''
                                });
                            }
                        });
                    });
                    
                    return results;
                });
                
                // Process rows
                for (const row of rows) {
                    stats.totalRows++;
                    
                    // Validate ISIN
                    if (!/^[A-Z]{2}[A-Z0-9]{10}$/.test(row.isin)) continue;
                    
                    // Skip duplicates
                    if (seenIsins.has(row.isin)) continue;
                    seenIsins.add(row.isin);
                    
                    // Skip leverage products
                    const nameUpper = row.name.toUpperCase();
                    if (['TURBO', 'LEVA FISSA', 'MINI FUTURE', 'STAYUP', 'STAYDOWN', 'CORRIDOR', 'DAILY LEVERAGE'].some(kw => nameUpper.includes(kw))) {
                        stats.skippedLeverage++;
                        continue;
                    }
                    
                    // Categorize
                    const category = categorizeUnderlying(row.sottostante, row.name);
                    
                    if (category === 'stock') {
                        stats.skippedStocks++;
                        continue;
                    }
                    
                    // Match!
                    stats.matched++;
                    
                    certificates.push({
                        isin: row.isin,
                        name: row.name,
                        type: detectType(row.name),
                        issuer: normalizeIssuer(row.emittente),
                        underlyingRaw: row.sottostante,
                        underlyingCategory: category,
                        maturityDate: parseDate(row.scadenza)
                    });
                }
                
                stats.pagesScanned++;
                
                if (pageNum % 10 === 0) {
                    console.log(`   Page ${pageNum}: ${certificates.length} matched so far`);
                }
                
                // Stop if we have enough
                if (certificates.length >= CONFIG.maxDetails) {
                    console.log(`   Reached ${CONFIG.maxDetails} certificates, stopping scan`);
                    break;
                }
                
                await page.waitForTimeout(CONFIG.waitBetweenPages);
                
            } catch (error) {
                console.log(`   ⚠️ Error on page ${pageNum}: ${error.message.substring(0, 40)}`);
            }
        }
        
        console.log(`   ✅ Scanned ${stats.pagesScanned} pages`);
        console.log(`   ✅ Found ${certificates.length} non-stock certificates`);
        
        // Fetch details for certificates (optional, slower)
        if (certificates.length > 0 && certificates.length <= 50) {
            console.log(`\n📋 Fetching details for ${certificates.length} certificates...`);
            
            for (let i = 0; i < certificates.length; i++) {
                const cert = certificates[i];
                try {
                    await page.goto(`${CONFIG.detailUrl}${cert.isin}`, {
                        waitUntil: 'domcontentloaded',
                        timeout: CONFIG.timeout
                    });
                    await page.waitForTimeout(1000);
                    
                    const details = await page.evaluate(() => {
                        const data = {};
                        const tables = document.querySelectorAll('table');
                        
                        tables.forEach(table => {
                            const rows = table.querySelectorAll('tr');
                            rows.forEach(row => {
                                const cells = row.querySelectorAll('td');
                                if (cells.length >= 2) {
                                    const label = cells[0]?.textContent?.trim().toUpperCase() || '';
                                    const value = cells[1]?.textContent?.trim() || '';
                                    
                                    if (label.includes('BARRIERA') && !label.includes('%')) {
                                        const match = value.match(/(\d+[,.]?\d*)/);
                                        if (match) data.barrier = parseFloat(match[1].replace(',', '.'));
                                    }
                                    if (label.includes('CEDOLA') || label.includes('COUPON')) {
                                        const match = value.match(/(\d+[,.]?\d*)/);
                                        if (match) data.coupon = parseFloat(match[1].replace(',', '.'));
                                    }
                                    if (label.includes('MERCATO')) {
                                        data.market = value;
                                    }
                                }
                            });
                        });
                        
                        return data;
                    });
                    
                    if (details.barrier) cert.barrierDown = details.barrier;
                    if (details.coupon) cert.coupon = details.coupon;
                    if (details.market) cert.market = details.market;
                    
                } catch (err) {
                    // Silent fail for individual details
                }
                
                if ((i + 1) % 20 === 0) {
                    console.log(`   ${i + 1}/${certificates.length}`);
                }
            }
        }
        
    } finally {
        if (browser) {
            await browser.close();
            console.log('\n🔒 Browser closed');
        }
    }
    
    // Build output with frontend-compatible format
    const output = certificates.map(cert => {
        const underlyingName = cert.underlyingRaw || 'Unknown';
        const underlyings = [];
        
        if (underlyingName.includes(',')) {
            underlyingName.split(',').slice(0, 5).forEach(part => {
                underlyings.push({ name: part.trim(), worst_of: true });
            });
        } else {
            underlyings.push({ name: underlyingName, worst_of: false });
        }
        
        // Ensure underlyings is never empty
        if (underlyings.length === 0) {
            underlyings.push({ name: 'N/A', worst_of: false });
        }
        
        // Calculate annual yield
        let annualYield = 8; // Default
        if (cert.coupon) {
            annualYield = cert.coupon < 5 ? cert.coupon * 12 : cert.coupon;
        }
        
        // Default maturity if missing
        const maturity = cert.maturityDate || new Date(Date.now() + 730 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
        
        return {
            isin: cert.isin,
            name: cert.name,
            type: cert.type,
            issuer: cert.issuer,
            market: cert.market || 'SeDeX',
            currency: 'EUR',
            underlying: underlyingName,
            underlying_category: cert.underlyingCategory,
            underlyings: underlyings,
            issue_date: new Date().toISOString().split('T')[0],
            maturity_date: maturity,
            barrier_down: cert.barrierDown || 60,
            coupon: cert.coupon || 0.5,
            annual_coupon_yield: annualYield,
            buffer_from_barrier: 30,
            effective_annual_yield: annualYield,
            scraped_at: new Date().toISOString()
        };
    });
    
    // Summary
    console.log('\n' + '='.repeat(70));
    console.log('📊 SUMMARY');
    console.log('='.repeat(70));
    console.log(`Pages scanned: ${stats.pagesScanned}`);
    console.log(`Total rows: ${stats.totalRows}`);
    console.log(`Skipped (leverage): ${stats.skippedLeverage}`);
    console.log(`Skipped (stocks): ${stats.skippedStocks}`);
    console.log(`Matched: ${output.length}`);
    
    // By category
    const byCat = {};
    output.forEach(c => {
        byCat[c.underlying_category] = (byCat[c.underlying_category] || 0) + 1;
    });
    console.log('\nBy category:');
    Object.entries(byCat).sort((a, b) => b[1] - a[1]).forEach(([cat, count]) => {
        const icon = { index: '📊', commodity: '🛢️', currency: '💱', rate: '💹' }[cat] || '📄';
        console.log(`   ${icon} ${cat}: ${count}`);
    });
    
    // By issuer
    const byIssuer = {};
    output.forEach(c => {
        byIssuer[c.issuer] = (byIssuer[c.issuer] || 0) + 1;
    });
    console.log('\nTop issuers:');
    Object.entries(byIssuer).sort((a, b) => b[1] - a[1]).slice(0, 8).forEach(([issuer, count]) => {
        console.log(`   ${issuer}: ${count}`);
    });
    
    console.log('='.repeat(70));
    
    // Save to file
    const data = {
        metadata: {
            version: '9.0-javascript',
            timestamp: new Date().toISOString(),
            source: 'certificatiederivati.it',
            total: output.length,
            pages_scanned: stats.pagesScanned,
            categories: {
                index: byCat.index || 0,
                commodity: byCat.commodity || 0,
                currency: byCat.currency || 0,
                rate: byCat.rate || 0
            }
        },
        certificates: output
    };
    
    fs.writeFileSync('certificates-data.json', JSON.stringify(data, null, 2));
    console.log(`\n💾 Saved ${output.length} certificates to certificates-data.json`);
    
    return data;
}

// Run
scrapeCertificates().catch(err => {
    console.error('❌ Scraper failed:', err);
    process.exit(1);
});
