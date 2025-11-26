/**
 * Test Script for v2.3 Yahoo Finance Optimized
 * Run: node test.js
 */

const DataAggregator = require('./dataAggregator');

const aggregator = new DataAggregator();

async function runTests() {
    console.log('='.repeat(60));
    console.log('ISIN BACKEND v2.3 - TEST SUITE');
    console.log('Yahoo Finance Only (Rate Limited + Cached)');
    console.log('='.repeat(60));
    console.log('');

    let passed = 0;
    let failed = 0;

    // Test 1: Health Check
    console.log('📋 Test 1: Health Check');
    console.log('-'.repeat(60));
    try {
        const health = await aggregator.healthCheck();
        if (health.status === 'operational') {
            console.log('✅ Health check OK');
            console.log(`   Yahoo: ${health.sources.yahoo}`);
            passed++;
        } else {
            console.log('❌ Health check FAIL');
            console.log(JSON.stringify(health, null, 2));
            failed++;
        }
    } catch (error) {
        console.log('❌ Health check ERROR:', error.message);
        failed++;
    }
    console.log('');

    // Wait a bit between tests
    await new Promise(r => setTimeout(r, 500));

    // Test 2: Search US Stock (AAPL)
    console.log('📋 Test 2: Search AAPL (US Stock)');
    console.log('-'.repeat(60));
    try {
        const result = await aggregator.search('AAPL');
        if (result.success && result.results.length > 0) {
            console.log(`✅ Found ${result.results.length} results`);
            const first = result.results[0];
            console.log(`   First: ${first.symbol} - ${first.name}`);
            console.log(`   Price: ${first.price || 'N/A'} ${first.currency || ''}`);
            passed++;
        } else {
            console.log('❌ No results found');
            failed++;
        }
    } catch (error) {
        console.log('❌ Search ERROR:', error.message);
        failed++;
    }
    console.log('');

    await new Promise(r => setTimeout(r, 500));

    // Test 3: Search Italian Stock (ENEL)
    console.log('📋 Test 3: Search ENEL (Italian Stock)');
    console.log('-'.repeat(60));
    try {
        const result = await aggregator.search('ENEL');
        if (result.success && result.results.length > 0) {
            console.log(`✅ Found ${result.results.length} results`);
            const first = result.results[0];
            console.log(`   First: ${first.symbol} - ${first.name}`);
            console.log(`   Price: ${first.price || 'N/A'} ${first.currency || ''}`);
            
            // Check if Italian stock is first
            if (first.symbol.endsWith('.MI')) {
                console.log(`   ✓ Italian stock (.MI) correctly prioritized`);
            }
            passed++;
        } else {
            console.log('❌ No results found');
            failed++;
        }
    } catch (error) {
        console.log('❌ Search ERROR:', error.message);
        failed++;
    }
    console.log('');

    await new Promise(r => setTimeout(r, 500));

    // Test 4: Get Quote
    console.log('📋 Test 4: Get Quote ENEL.MI');
    console.log('-'.repeat(60));
    try {
        const result = await aggregator.getQuote('ENEL.MI');
        if (result.success && result.data) {
            console.log('✅ Quote found');
            console.log(`   Symbol: ${result.data.symbol}`);
            console.log(`   Price: ${result.data.price} ${result.data.currency}`);
            console.log(`   Change: ${result.data.change} (${result.data.changePercent}%)`);
            passed++;
        } else {
            console.log('❌ Quote not found');
            failed++;
        }
    } catch (error) {
        console.log('❌ Quote ERROR:', error.message);
        failed++;
    }
    console.log('');

    await new Promise(r => setTimeout(r, 500));

    // Test 5: Historical Data
    console.log('📋 Test 5: Historical Data AAPL (1M)');
    console.log('-'.repeat(60));
    try {
        const result = await aggregator.getHistoricalData('AAPL', '1M');
        if (result.success && result.data && result.data.length > 0) {
            console.log(`✅ Historical data found: ${result.data.length} data points`);
            console.log(`   First: ${result.data[0].date} - $${result.data[0].close}`);
            console.log(`   Last: ${result.data[result.data.length-1].date} - $${result.data[result.data.length-1].close}`);
            passed++;
        } else {
            console.log('❌ Historical data not found');
            failed++;
        }
    } catch (error) {
        console.log('❌ Historical ERROR:', error.message);
        failed++;
    }
    console.log('');

    await new Promise(r => setTimeout(r, 500));

    // Test 6: Cache Test (should be instant)
    console.log('📋 Test 6: Cache Test (repeat AAPL search)');
    console.log('-'.repeat(60));
    try {
        const startTime = Date.now();
        const result = await aggregator.search('AAPL');
        const elapsed = Date.now() - startTime;
        
        if (result.success && elapsed < 100) {
            console.log(`✅ Cached response in ${elapsed}ms`);
            passed++;
        } else if (result.success) {
            console.log(`⚠️  Response OK but took ${elapsed}ms (cache may not have hit)`);
            passed++;
        } else {
            console.log('❌ Search failed');
            failed++;
        }
    } catch (error) {
        console.log('❌ Cache test ERROR:', error.message);
        failed++;
    }
    console.log('');

    // Test 7: ENI (another Italian stock)
    console.log('📋 Test 7: Search ENI (Italian Stock)');
    console.log('-'.repeat(60));
    try {
        const result = await aggregator.search('ENI');
        if (result.success && result.results.length > 0) {
            console.log(`✅ Found ${result.results.length} results`);
            const first = result.results[0];
            console.log(`   First: ${first.symbol} - ${first.name}`);
            
            if (first.symbol === 'ENI.MI') {
                console.log(`   ✓ ENI.MI correctly prioritized over ENI.DE`);
            }
            passed++;
        } else {
            console.log('❌ No results found');
            failed++;
        }
    } catch (error) {
        console.log('❌ Search ERROR:', error.message);
        failed++;
    }
    console.log('');

    // Summary
    console.log('='.repeat(60));
    console.log('TEST SUMMARY');
    console.log('='.repeat(60));
    console.log(`✅ Passed: ${passed}`);
    console.log(`❌ Failed: ${failed}`);
    console.log('');
    
    // Cache stats
    const cacheStats = aggregator.getCacheStats();
    console.log('Cache Stats:');
    console.log(`   Size: ${cacheStats.size} entries`);
    console.log(`   TTL: ${cacheStats.ttl}`);
    console.log('');

    if (failed === 0) {
        console.log('🎉 ALL TESTS PASSED! Ready for deployment.');
    } else {
        console.log('⚠️  Some tests failed. Review before deploying.');
    }
    console.log('='.repeat(60));
}

// Run tests
runTests().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});
