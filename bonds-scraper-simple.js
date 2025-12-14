/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Bonds Data Scraper - Simple Tools for Investors
 * Scrapes real ISIN codes and bond data from simpletoolsforinvestors.eu
 * Much simpler and more reliable than scraping Borsa Italiana directly!
 */

const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs').promises;
const path = require('path');

// Configuration
const CONFIG = {
    outputPath: path.join(__dirname, 'data', 'bonds-data.json'),
    sourceUrl: 'https://www.simpletoolsforinvestors.eu/yieldtable.php?datatype=EOD&volumerating=4',
    timeout: 30000
};

/**
 * Parse maturity date from DD/MM/YYYY to YYYY-MM-DD
 */
function parseMaturityDate(dateStr) {
    if (!dateStr || dateStr === '-') return '';
    
    // Format: YYYY-MM-DD already
    if (dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) {
        return dateStr;
    }
    
    // Format: DD/MM/YYYY
    const parts = dateStr.split(/[-\/]/);
    if (parts.length === 3) {
        // Check if year is first (YYYY-MM-DD)
        if (parts[0].length === 4) {
            return dateStr;
        }
        // Otherwise DD/MM/YYYY
        return `${parts[2]}-${parts[1]}-${parts[0]}`;
    }
    
    return dateStr;
}

/**
 * Parse float value, handling European format (comma as decimal separator)
 */
function parseFloat(value) {
    if (!value || value === '-' || value === '') return 0;
    const cleaned = value.replace(',', '.');
    const num = Number.parseFloat(cleaned);
    return isNaN(num) ? 0 : num;
}

/**
 * Determine bond type from name/description
 */
function getBondType(name, issuer) {
    const nameUpper = name.toUpperCase();
    const issuerUpper = issuer.toUpperCase();
    
    if (nameUpper.includes('BTP') && !nameUpper.includes('BOT')) return 'BTP';
    if (nameUpper.includes('BOT')) return 'BOT';
    if (nameUpper.includes('CCT')) return 'CCT';
    if (nameUpper.includes('CTZ')) return 'CTZ';
    
    // Determine from issuer
    if (issuerUpper.includes('ITALIA')) {
        if (nameUpper.includes('ZERO') || nameUpper.includes('ZC')) return 'BOT';
        return 'BTP';
    }
    
    return 'OTHER';
}

/**
 * Get country code from issuer/ISIN
 */
function getCountryCode(isin, issuer) {
    // ISIN first 2 chars = country code
    if (isin && isin.length >= 2) {
        return isin.substring(0, 2);
    }
    
    const issuerUpper = issuer.toUpperCase();
    if (issuerUpper.includes('ITALIA') || issuerUpper.includes('ITALIAN')) return 'IT';
    if (issuerUpper.includes('FRANCIA') || issuerUpper.includes('FRANCE')) return 'FR';
    if (issuerUpper.includes('GERMANIA') || issuerUpper.includes('GERMANY')) return 'DE';
    if (issuerUpper.includes('SPAGNA') || issuerUpper.includes('SPAIN')) return 'ES';
    
    return 'IT'; // Default to IT
}

/**
 * Scrape bonds from Simple Tools
 */
async function scrapeBonds() {
    console.log('🚀 Scraping bonds from Simple Tools for Investors...');
    console.log(`📊 URL: ${CONFIG.sourceUrl}`);
    
    try {
        // Fetch HTML
        const response = await axios.get(CONFIG.sourceUrl, {
            timeout: CONFIG.timeout,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        });
        
        console.log('✅ HTML fetched successfully');
        
        // Parse HTML
        const $ = cheerio.load(response.data);
        const bonds = [];
        
        // Find table rows
        const rows = $('table tbody tr');
        console.log(`📋 Found ${rows.length} table rows`);
        
        rows.each((index, row) => {
            try {
                const cells = $(row).find('td');
                if (cells.length < 11) return; // Skip invalid rows
                
                // Extract data from cells
                const isin = $(cells[0]).text().trim();
                const description = $(cells[1]).text().trim();
                const currency = $(cells[2]).text().trim();
                const maturity = $(cells[3]).text().trim();
                const couponStr = $(cells[4]).text().trim();
                const minLot = $(cells[5]).text().trim();
                const status = $(cells[6]).text().trim();
                const market = $(cells[7]).text().trim();
                const priceStr = $(cells[8]).text().trim();
                const volume = $(cells[9]).text().trim();
                const yieldGrossStr = $(cells[11]).text().trim();
                const yieldNetStr = $(cells[12]).text().trim();
                
                // Validate ISIN (12 chars alphanumeric)
                if (!isin || isin.length !== 12 || !/^[A-Z]{2}[A-Z0-9]{10}$/.test(isin)) {
                    return; // Skip invalid ISIN
                }
                
                // Parse numeric values
                const coupon = parseFloat(couponStr);
                const price = parseFloat(priceStr);
                const yieldGross = parseFloat(yieldGrossStr);
                const yieldNet = parseFloat(yieldNetStr);
                
                // Determine bond type and country
                const country = getCountryCode(isin, description);
                const type = getBondType(description, description);
                
                // Skip non-Italian government bonds for now
                if (country !== 'IT' || !['BTP', 'BOT', 'CCT', 'CTZ'].includes(type)) {
                    return;
                }
                
                // Create bond object
                const bond = {
                    isin: isin,
                    name: description,
                    type: type,
                    country: country,
                    currency: currency || 'EUR',
                    maturity: parseMaturityDate(maturity),
                    coupon: coupon,
                    yield: yieldNet, // Use net yield (after taxes)
                    price: price,
                    change: '+0.00', // Not available from Simple Tools
                    lastUpdate: new Date().toISOString().split('T')[0]
                };
                
                bonds.push(bond);
                
            } catch (err) {
                console.error(`❌ Error parsing row ${index}:`, err.message);
            }
        });
        
        console.log(`✅ Scraped ${bonds.length} Italian government bonds`);
        
        // Group by type
        const bondsByType = {
            BTP: bonds.filter(b => b.type === 'BTP'),
            BOT: bonds.filter(b => b.type === 'BOT'),
            CCT: bonds.filter(b => b.type === 'CCT'),
            CTZ: bonds.filter(b => b.type === 'CTZ')
        };
        
        console.log(`   BTP: ${bondsByType.BTP.length} bonds`);
        console.log(`   BOT: ${bondsByType.BOT.length} bonds`);
        console.log(`   CCT: ${bondsByType.CCT.length} bonds`);
        console.log(`   CTZ: ${bondsByType.CTZ.length} bonds`);
        
        return bonds;
        
    } catch (error) {
        console.error('❌ Error scraping bonds:', error.message);
        return [];
    }
}

/**
 * Save bonds data to JSON file
 */
async function saveBondsData(bonds) {
    try {
        // Ensure data directory exists
        const dataDir = path.dirname(CONFIG.outputPath);
        await fs.mkdir(dataDir, { recursive: true });
        
        // Group bonds by type
        const bondsByType = {
            BTP: bonds.filter(b => b.type === 'BTP'),
            BOT: bonds.filter(b => b.type === 'BOT'),
            CCT: bonds.filter(b => b.type === 'CCT'),
            CTZ: bonds.filter(b => b.type === 'CTZ')
        };
        
        // Create output structure
        const output = {
            lastUpdate: new Date().toISOString(),
            categories: {
                'it-governativi': {
                    name: 'Titoli di Stato Italiani',
                    description: 'BTP, BOT, CCT, CTZ - Italian Government Bonds',
                    count: bonds.length,
                    bonds: bonds
                }
            },
            statistics: {
                totalBonds: bonds.length,
                byCategory: {
                    'it-governativi': bonds.length
                },
                byType: {
                    BTP: bondsByType.BTP.length,
                    BOT: bondsByType.BOT.length,
                    CCT: bondsByType.CCT.length,
                    CTZ: bondsByType.CTZ.length
                },
                source: 'Simple Tools for Investors',
                sourceUrl: CONFIG.sourceUrl
            }
        };
        
        // Write to file
        await fs.writeFile(
            CONFIG.outputPath,
            JSON.stringify(output, null, 2),
            'utf8'
        );
        
        console.log(`💾 Saved ${bonds.length} bonds to: ${CONFIG.outputPath}`);
        console.log(`📊 File size: ${JSON.stringify(output).length} bytes`);
        
        return true;
        
    } catch (error) {
        console.error('❌ Error saving bonds data:', error.message);
        return false;
    }
}

/**
 * Main execution
 */
async function main() {
    console.log('');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('  BONDS DATA SCRAPER - Simple Tools for Investors');
    console.log('  Copyright (c) 2024-2025 Mutna S.R.L.S.');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('');
    
    try {
        // Scrape bonds
        const bonds = await scrapeBonds();
        
        if (bonds.length === 0) {
            console.error('❌ No bonds scraped! Check the scraper logic.');
            process.exit(1);
        }
        
        // Save to file
        const saved = await saveBondsData(bonds);
        
        if (!saved) {
            console.error('❌ Failed to save bonds data!');
            process.exit(1);
        }
        
        console.log('');
        console.log('✅ SCRAPING COMPLETED SUCCESSFULLY!');
        console.log(`📊 Total bonds: ${bonds.length}`);
        console.log(`💾 Output: ${CONFIG.outputPath}`);
        console.log('');
        
        process.exit(0);
        
    } catch (error) {
        console.error('❌ FATAL ERROR:', error.message);
        console.error(error.stack);
        process.exit(1);
    }
}

// Run if called directly
if (require.main === module) {
    main();
}

module.exports = { scrapeBonds, saveBondsData };
