/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Test Script for SimpleTools Bond Scraper
 */

const SimpleToolsBondScraper = require('./bonds-scraper-simpletoolsforinvestors');
const fs = require('fs').promises;

async function testScraper() {
    console.log('='.repeat(70));
    console.log('TESTING SIMPLETOOLSFORINVESTORS BOND SCRAPER');
    console.log('='.repeat(70));

    const scraper = new SimpleToolsBondScraper();

    // Test 1: Scrape a single monitor
    console.log('\n[TEST 1] Testing single monitor scrape (italia)...');
    const italiaBonds = await scraper.scrapeMonitor('italia');
    console.log(`✓ Italia bonds: ${italiaBonds.length}`);
    if (italiaBonds.length > 0) {
        console.log('Sample bond:', JSON.stringify(italiaBonds[0], null, 2));
    }

    // Test 2: Scrape all categories
    console.log('\n[TEST 2] Testing complete scrape...');
    const data = await scraper.scrapeAll();
    
    console.log('\n[RESULTS]');
    console.log(`Total categories: ${data.statistics.totalCategories}`);
    console.log(`Total bonds: ${data.statistics.totalBonds}`);
    
    console.log('\n[CATEGORY BREAKDOWN]');
    for (const [catId, catData] of Object.entries(data.categories)) {
        console.log(`  ${catId}: ${catData.count} bonds`);
    }

    // Test 3: Save to file
    console.log('\n[TEST 3] Saving to file...');
    const saved = await scraper.saveToFile(data, 'data/bonds-data.json');
    
    if (saved) {
        const fileStats = await fs.stat('data/bonds-data.json');
        console.log(`✓ File saved: ${(fileStats.size / 1024).toFixed(2)} KB`);
    }

    // Test 4: Validate JSON structure
    console.log('\n[TEST 4] Validating JSON structure...');
    const fileContent = await fs.readFile('data/bonds-data.json', 'utf8');
    const parsed = JSON.parse(fileContent);
    
    const validations = [
        { check: parsed.lastUpdate !== undefined, msg: 'lastUpdate field exists' },
        { check: parsed.categories !== undefined, msg: 'categories field exists' },
        { check: parsed.statistics !== undefined, msg: 'statistics field exists' },
        { check: Object.keys(parsed.categories).length > 0, msg: 'has categories' },
        { check: parsed.statistics.totalBonds > 0, msg: 'has bonds' }
    ];

    validations.forEach(v => {
        console.log(`${v.check ? '✓' : '✗'} ${v.msg}`);
    });

    console.log('\n' + '='.repeat(70));
    console.log('TEST COMPLETE');
    console.log('='.repeat(70));
}

// Run test
testScraper().catch(console.error);
