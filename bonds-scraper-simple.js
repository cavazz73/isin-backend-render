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
 * Categorize bond based on issuer/description
 */
function categorizeBond(name, isin) {
    const nameUpper = name.toUpperCase();
    
    // Italian Government Bonds
    if (nameUpper.includes('BTP') && !nameUpper.includes('BOT')) {
        return { category: 'gov-it-btp', type: 'BTP', country: 'IT' };
    }
    if (nameUpper.includes('BOT')) {
        return { category: 'gov-it-bot', type: 'BOT', country: 'IT' };
    }
    if (nameUpper.includes('CCT')) {
        return { category: 'gov-it-cct', type: 'CCT', country: 'IT' };
    }
    if (nameUpper.includes('CTZ')) {
        return { category: 'gov-it-ctz', type: 'CTZ', country: 'IT' };
    }
    
    // European Government Bonds
    if (nameUpper.includes('FRANCIA') || nameUpper.includes('FRANCE')) {
        return { category: 'gov-eu-france', type: 'GOV', country: 'FR' };
    }
    if (nameUpper.includes('GERMANIA') || nameUpper.includes('GERMANY') || nameUpper.includes('DEUTSCHE')) {
        return { category: 'gov-eu-germany', type: 'GOV', country: 'DE' };
    }
    if (nameUpper.includes('SPAGNA') || nameUpper.includes('SPAIN')) {
        return { category: 'gov-eu-spain', type: 'GOV', country: 'ES' };
    }
    if (nameUpper.includes('AUSTRIA')) {
        return { category: 'gov-eu-austria', type: 'GOV', country: 'AT' };
    }
    if (nameUpper.includes('BELGIO') || nameUpper.includes('BELGIUM')) {
        return { category: 'gov-eu-belgium', type: 'GOV', country: 'BE' };
    }
    if (nameUpper.includes('PAESI BASSI') || nameUpper.includes('NETHERLANDS')) {
        return { category: 'gov-eu-netherlands', type: 'GOV', country: 'NL' };
    }
    
    // Supranational Bonds
    if (nameUpper.includes('EUROPEAN INVESTMENT BANK') || nameUpper.includes('BEI') || nameUpper.includes('EIB')) {
        return { category: 'supranational', type: 'SUPRANATIONAL', country: 'EU' };
    }
    if (nameUpper.includes('EUROPEAN FINANCIAL STABILITY') || nameUpper.includes('EFSF')) {
        return { category: 'supranational', type: 'SUPRANATIONAL', country: 'EU' };
    }
    if (nameUpper.includes('EUROPEAN STABILITY MECHANISM') || nameUpper.includes('ESM')) {
        return { category: 'supranational', type: 'SUPRANATIONAL', country: 'EU' };
    }
    if (nameUpper.includes('WORLD BANK') || nameUpper.includes('IBRD')) {
        return { category: 'supranational', type: 'SUPRANATIONAL', country: 'WORLD' };
    }
    if (nameUpper.includes('EUROPEAN UNION')) {
        return { category: 'supranational', type: 'SUPRANATIONAL', country: 'EU' };
    }
    
    // Corporate Bonds
    if (nameUpper.includes('UBS') || nameUpper.includes('CREDIT SUISSE') || 
        nameUpper.includes('DEUTSCHE BANK') || nameUpper.includes('MORGAN') ||
        nameUpper.includes('BARCLAYS') || nameUpper.includes('GOLDMAN') ||
        nameUpper.includes('UNICREDIT') || nameUpper.includes('INTESA') ||
        nameUpper.includes('GENERALI') || nameUpper.includes('ENI') ||
        nameUpper.includes('ENEL') || nameUpper.includes('TELECOM')) {
        return { category: 'corporate-all', type: 'CORPORATE', country: getCountryFromIsin(isin) };
    }
    
    // Default: if it has ISIN starting with IT, it's probably Italian
    if (isin && isin.startsWith('IT')) {
        return { category: 'gov-it-other', type: 'GOV', country: 'IT' };
    }
    
    // Default: corporate or other
    return { category: 'corporate-all', type: 'OTHER', country: getCountryFromIsin(isin) };
}

/**
 * Get country code from ISIN
 */
function getCountryFromIsin(isin) {
    if (!isin || isin.length < 2) return 'XX';
    return isin.substring(0, 2);
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
                
                // Categorize bond
                const bondInfo = categorizeBond(description, isin);
                
                // Create bond object
                const bond = {
                    isin: isin,
                    name: description,
                    type: bondInfo.type,
                    category: bondInfo.category,
                    country: bondInfo.country,
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
        
        console.log(`✅ Scraped ${bonds.length} bonds from Simple Tools`);
        
        // Group by category
        const bondsByCategory = {};
        bonds.forEach(bond => {
            if (!bondsByCategory[bond.category]) {
                bondsByCategory[bond.category] = [];
            }
            bondsByCategory[bond.category].push(bond);
        });
        
        // Log statistics
        console.log('   Bonds by category:');
        Object.keys(bondsByCategory).sort().forEach(cat => {
            console.log(`   ${cat}: ${bondsByCategory[cat].length} bonds`);
        });
        
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
        
        // Group bonds by category
        const bondsByCategory = {};
        bonds.forEach(bond => {
            if (!bondsByCategory[bond.category]) {
                bondsByCategory[bond.category] = [];
            }
            bondsByCategory[bond.category].push(bond);
        });
        
        // Create categories object
        const categories = {};
        
        // Italian Government Bonds
        const itGovBonds = [
            ...(bondsByCategory['gov-it-btp'] || []),
            ...(bondsByCategory['gov-it-bot'] || []),
            ...(bondsByCategory['gov-it-cct'] || []),
            ...(bondsByCategory['gov-it-ctz'] || []),
            ...(bondsByCategory['gov-it-other'] || [])
        ];
        
        if (itGovBonds.length > 0) {
            categories['it-governativi'] = {
                name: 'Titoli di Stato Italiani',
                description: 'BTP, BOT, CCT, CTZ - Italian Government Bonds',
                count: itGovBonds.length,
                bonds: itGovBonds
            };
        }
        
        // European Government Bonds
        const euGovBonds = [
            ...(bondsByCategory['gov-eu-france'] || []),
            ...(bondsByCategory['gov-eu-germany'] || []),
            ...(bondsByCategory['gov-eu-spain'] || []),
            ...(bondsByCategory['gov-eu-austria'] || []),
            ...(bondsByCategory['gov-eu-belgium'] || []),
            ...(bondsByCategory['gov-eu-netherlands'] || [])
        ];
        
        if (euGovBonds.length > 0) {
            categories['eu-governativi'] = {
                name: 'Titoli di Stato Europei',
                description: 'European Government Bonds',
                count: euGovBonds.length,
                bonds: euGovBonds
            };
        }
        
        // Supranational Bonds
        const supranationalBonds = bondsByCategory['supranational'] || [];
        if (supranationalBonds.length > 0) {
            categories['sovranazionali'] = {
                name: 'Obbligazioni Sovranazionali',
                description: 'BEI, EFSF, ESM, World Bank',
                count: supranationalBonds.length,
                bonds: supranationalBonds
            };
        }
        
        // Corporate Bonds
        const corporateBonds = bondsByCategory['corporate-all'] || [];
        if (corporateBonds.length > 0) {
            categories['corporate'] = {
                name: 'Obbligazioni Corporate',
                description: 'Corporate Bonds',
                count: corporateBonds.length,
                bonds: corporateBonds
            };
        }
        
        // Count by type for statistics
        const bondsByType = {};
        bonds.forEach(bond => {
            bondsByType[bond.type] = (bondsByType[bond.type] || 0) + 1;
        });
        
        // Create output structure
        const output = {
            lastUpdate: new Date().toISOString(),
            categories: categories,
            statistics: {
                totalBonds: bonds.length,
                byCategory: Object.keys(bondsByCategory).reduce((acc, cat) => {
                    acc[cat] = bondsByCategory[cat].length;
                    return acc;
                }, {}),
                byType: bondsByType,
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
        console.log(`📊 Categories created: ${Object.keys(categories).join(', ')}`);
        console.log(`📊 File size: ${(JSON.stringify(output).length / 1024).toFixed(2)} KB`);
        
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
