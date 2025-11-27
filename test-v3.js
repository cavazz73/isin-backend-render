/**
 * Test Script for Multi-Source Backend V3.0
 * Run: node test-v3.js
 */

require('dotenv').config();
const DataAggregator = require('./dataAggregator'); // ← FLAT STRUCTURE

// Initialize with API keys from .env
const aggregator = new DataAggregator({
    twelveDataKey: process.env.TWELVE_DATA_API_KEY,
    finnhubKey: process.env.FINNHUB_API_KEY,
    alphavantageKey: process.env.ALPHA_VANTAGE_API_KEY
});

async function runTests() {
    console.log('='.repeat(60));
    console.log('ISIN RESEARCH BACKEND V3.0 - TEST SUITE');
    console.log('TwelveData PRIMARY for European Markets');
    console.log('='.repeat(60));
    console.log('');

    // Test 1: Health Check
    console.log('📋 Test 1: Health Check V3.0');
    console.log('-'.repeat(60));
    try {
        const health = await aggregator.healthCheck();
        console.log('✅ Health check OK');
        console.log('Sources:', JSON.stringify(health.sources, null, 2));
        console.log('TwelveData Usage:', JSON.stringify(health.twelveDataUsage, null, 2));
    } catch (error) {
        console.log('❌ Health check failed:', error.message);
    }
    console.log('');

    // Test 2: Search ENEL (Italian stock - PRIMARY TEST)
    console.log('📋 Test 2: Search ENEL (Italian Stock - EUR Test)');
    console.log('-'.repeat(60));
    try {
        const result = await aggregator.search('ENEL');
        if (result.success && result.results.length > 0) {
            const enel = result.results[0];
            console.log(`✅ Found ENEL`);
            console.log(`   Symbol: ${enel.symbol}`);
            console.log(`   Name: ${enel.name}`);
            console.log(`   Currency: ${enel.currency} ${enel.currency === 'EUR' ? '✅ CORRECT!' : '❌ WRONG! Should be EUR'}`);
            console.log(`   Exchange: ${enel.exchange}`);
            console.log(`   Price: ${enel.price || 'N/A'}`);
            console.log(`   Sources: ${result.metadata.sources.join(', ')}`);
        } else {
            console.log('❌ No results for ENEL');
        }
    } catch (error) {
        console.log('❌ Search failed:', error.message);
    }
    console.log('');

    // Test 3: Get Quote ENEL - Real-time
    console.log('📋 Test 3: Get Real-Time Quote ENEL');
    console.log('-'.repeat(60));
    try {
        const result = await aggregator.getQuote('ENEL');
        if (result.success && result.data) {
            console.log('✅ Quote found');
            console.log(`   Price: ${result.data.price} ${result.data.currency}`);
            console.log(`   Change: ${result.data.change > 0 ? '+' : ''}${result.data.change}`);
            console.log(`   Change %: ${result.data.changePercent > 0 ? '+' : ''}${result.data.changePercent.toFixed(2)}%`);
            console.log(`   Currency: ${result.data.currency} ${result.data.currency === 'EUR' ? '✅' : '❌'}`);
            console.log(`   Source: ${result.source}`);
        } else {
            console.log('❌ Quote not found');
        }
    } catch (error) {
        console.log('❌ Quote failed:', error.message);
    }
    console.log('');

    // Test 4: Search ENI (Another Italian stock)
    console.log('📋 Test 4: Search ENI (Italian Stock)');
    console.log('-'.repeat(60));
    try {
        const result = await aggregator.search('ENI');
        if (result.success && result.results.length > 0) {
            const eni = result.results[0];
            console.log(`✅ Found ENI`);
            console.log(`   Symbol: ${eni.symbol}`);
            console.log(`   Currency: ${eni.currency} ${eni.currency === 'EUR' ? '✅' : '❌'}`);
            console.log(`   Price: ${eni.price || 'N/A'}`);
        } else {
            console.log('❌ No results for ENI');
        }
    } catch (error) {
        console.log('❌ Search failed:', error.message);
    }
    console.log('');

    // Test 5: Search AAPL (US stock - fallback test)
    console.log('📋 Test 5: Search AAPL (US Stock - Yahoo Fallback)');
    console.log('-'.repeat(60));
    try {
        const result = await aggregator.search('AAPL');
        if (result.success && result.results.length > 0) {
            const aapl = result.results[0];
            console.log(`✅ Found AAPL`);
            console.log(`   Symbol: ${aapl.symbol}`);
            console.log(`   Currency: ${aapl.currency}`);
            console.log(`   Price: ${aapl.price || 'N/A'}`);
            console.log(`   Sources: ${result.metadata.sources.join(', ')}`);
        } else {
            console.log('❌ No results for AAPL');
        }
    } catch (error) {
        console.log('❌ Search failed:', error.message);
    }
    console.log('');

    // Test 6: Historical Data ENEL
    console.log('📋 Test 6: Historical Data ENEL (1M)');
    console.log('-'.repeat(60));
    try {
        const result = await aggregator.getHistoricalData('ENEL', '1M');
        if (result.success && result.data) {
            console.log(`✅ Historical data found: ${result.data.length} data points`);
            console.log('   First 3 data points:');
            result.data.slice(0, 3).forEach(point => {
                console.log(`   ${point.date}: ${point.close}`);
            });
            console.log(`   Source: ${result.source}`);
        } else {
            console.log('❌ Historical data not found');
        }
    } catch (error) {
        console.log('❌ Historical data failed:', error.message);
    }
    console.log('');

    // Test 7: Multiple Italian Stocks
    console.log('📋 Test 7: Multiple Italian Stocks (Batch Test)');
    console.log('-'.repeat(60));
    const italianStocks = ['ENEL', 'ENI', 'INTESA', 'UNICREDIT'];
    for (const stock of italianStocks) {
        try {
            const result = await aggregator.search(stock);
            if (result.success && result.results.length > 0) {
                const item = result.results[0];
                const currencyOK = item.currency === 'EUR' ? '✅' : '❌';
                console.log(`${currencyOK} ${stock}: ${item.currency} (${item.exchange || 'N/A'})`);
            } else {
                console.log(`❌ ${stock}: Not found`);
            }
        } catch (error) {
            console.log(`❌ ${stock}: Error - ${error.message}`);
        }
    }
    console.log('');

    // Test 8: TwelveData Usage Statistics
    console.log('📋 Test 8: TwelveData Usage Statistics');
    console.log('-'.repeat(60));
    try {
        const usage = aggregator.twelvedata.getUsageStats();
        console.log('✅ Usage stats:');
        console.log(`   Requests made: ${usage.requestsMade}`);
        console.log(`   Daily limit: ${usage.dailyLimit}`);
        console.log(`   Remaining: ${usage.remainingDaily}`);
        console.log(`   Minute limit: ${usage.minuteLimit} req/min`);
    } catch (error) {
        console.log('❌ Usage stats failed:', error.message);
    }
    console.log('');

    console.log('='.repeat(60));
    console.log('TEST SUITE COMPLETE');
    console.log('='.repeat(60));
    console.log('');
    console.log('✅ SUCCESS CRITERIA:');
    console.log('   1. ENEL shows EUR (not USD)');
    console.log('   2. ENI shows EUR (not USD)');
    console.log('   3. TwelveData used as primary source for EU stocks');
    console.log('   4. Yahoo used for US stocks (AAPL)');
    console.log('');
}

// Run tests
runTests().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});
