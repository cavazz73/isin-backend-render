/**
 * FINANCIAL API - WITH LIVE ISIN SEARCH
 * 3-Level Search Strategy:
 * 1. Local DB (fast)
 * 2. Live Scraping (on-demand for missing ISINs)
 * 3. External APIs (stocks fallback)
 */

const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const util = require('util');
const execPromise = util.promisify(exec);

// Import existing data aggregator for stocks
let DataAggregator;
try {
    DataAggregator = require('./dataAggregator');
} catch (e) {
    console.warn('⚠️  DataAggregator not found, stocks search disabled');
}

const aggregator = DataAggregator ? new DataAggregator() : null;

// ==========================================
// HELPER: SEARCH LOCAL FILES
// ==========================================
function searchLocalFile(filename, query, typeLabel) {
    try {
        const filePath = path.join(__dirname, 'data', filename);
        if (!fs.existsSync(filePath)) {
            console.log(`📁 File not found: ${filename}`);
            return [];
        }
        
        const raw = fs.readFileSync(filePath, 'utf8');
        const data = JSON.parse(raw);
        let items = [];

        // Extract items from different data structures
        if (data.certificates) {
            items = data.certificates;
        } else if (data.categories) {
            // Bonds structure
            Object.values(data.categories).forEach(cat => {
                if (cat.bonds) items.push(...cat.bonds);
            });
        } else if (Array.isArray(data)) {
            items = data;
        }

        const q = query.toUpperCase();
        const matches = items.filter(i => {
            const isinMatch = i.isin && i.isin.toUpperCase().includes(q);
            const nameMatch = i.name && i.name.toUpperCase().includes(q);
            return isinMatch || nameMatch;
        });

        return matches.slice(0, 10).map(item => ({
            symbol: item.isin || item.symbol,
            name: item.name,
            type: typeLabel,
            price: item.price || item.last_price || item.bid_price || 0,
            currency: item.currency || 'EUR',
            market: item.market || 'N/A',
            issuer: item.issuer,
            source: 'Local DB',
            score: item.isin === q ? 100 : 50  // Exact match gets highest score
        }));
    } catch (e) {
        console.error(`Error reading ${filename}:`, e.message);
        return [];
    }
}

// ==========================================
// HELPER: LIVE SCRAPING
// ==========================================
async function liveSearchISIN(isin) {
    const scriptPath = path.join(__dirname, 'live_search.py');
    
    // Check if script exists
    if (!fs.existsSync(scriptPath)) {
        console.error('❌ live_search.py not found');
        return null;
    }

    try {
        console.log(`🔍 Live search: ${isin}...`);
        
        // Call Python script with timeout
        const { stdout, stderr } = await execPromise(
            `python3 "${scriptPath}" --isin ${isin}`,
            { 
                timeout: 25000,  // 25s timeout
                maxBuffer: 1024 * 1024  // 1MB buffer
            }
        );

        // Log stderr if any (for debugging)
        if (stderr) {
            console.warn('Python stderr:', stderr);
        }

        // Parse JSON output
        if (stdout && stdout.trim()) {
            const result = JSON.parse(stdout.trim());
            
            if (result.error) {
                console.log(`⚠️  Not found: ${isin}`);
                return null;
            }

            console.log(`✅ Found: ${result.name}`);
            
            // Transform to API format
            return {
                symbol: result.isin,
                name: result.name,
                type: result.type,
                price: result.price || result.last_price || 0,
                currency: result.currency || 'EUR',
                market: result.market || 'N/A',
                issuer: result.issuer,
                barrier: result.barrier,
                coupon: result.coupon,
                source: result.source,
                score: 100,
                live: true  // Mark as live-scraped
            };
        }

        return null;

    } catch (error) {
        // Handle different error types
        if (error.killed) {
            console.error('⏱️  Scraper timeout');
        } else if (error.code) {
            console.error(`❌ Scraper error (code ${error.code}):`, error.message);
        } else {
            console.error('❌ Scraper failed:', error.message);
        }
        return null;
    }
}

// ==========================================
// ENDPOINT: INTELLIGENT SEARCH
// ==========================================
router.get('/search', async (req, res) => {
    const query = req.query.q;
    
    if (!query || query.length < 2) {
        return res.json({ 
            success: false, 
            error: 'Query too short (min 2 characters)' 
        });
    }

    const qUpper = query.toUpperCase().trim();
    console.log(`\n${'='.repeat(60)}`);
    console.log(`🔍 SEARCH: "${qUpper}"`);
    console.log('='.repeat(60));

    try {
        let results = [];
        const startTime = Date.now();

        // ==========================================
        // LEVEL 1: LOCAL DATABASE (FAST)
        // ==========================================
        console.log('📊 Level 1: Searching local database...');
        
        const localCerts = searchLocalFile('certificates-data.json', qUpper, 'CERTIFICATE');
        const localBonds = searchLocalFile('bonds-data.json', qUpper, 'BOND');
        
        results = [...localCerts, ...localBonds];
        console.log(`   Found ${results.length} local matches`);

        // ==========================================
        // LEVEL 2: LIVE SCRAPING (IF ISIN NOT FOUND)
        // ==========================================
        const isISIN = /^[A-Z]{2}[A-Z0-9]{9}\d$/.test(qUpper);
        const exactMatch = results.find(r => r.symbol === qUpper);
        
        if (isISIN && !exactMatch) {
            console.log('🌐 Level 2: ISIN not in DB, trying live scraping...');
            
            const liveResult = await liveSearchISIN(qUpper);
            
            if (liveResult) {
                // Add to top of results
                results.unshift(liveResult);
                console.log(`   ✅ Live scraping successful`);
            } else {
                console.log(`   ⚠️  Live scraping found nothing`);
            }
        }

        // ==========================================
        // LEVEL 3: EXTERNAL APIs (STOCKS)
        // ==========================================
        if ((!isISIN || results.length === 0) && aggregator) {
            console.log('📡 Level 3: Searching external APIs (stocks)...');
            
            try {
                const apiRes = await aggregator.search(query);
                
                if (apiRes && apiRes.results && apiRes.results.length > 0) {
                    // Avoid duplicates
                    const newItems = apiRes.results.filter(ext => 
                        !results.some(loc => loc.symbol === ext.symbol)
                    );
                    
                    results.push(...newItems);
                    console.log(`   Found ${newItems.length} API matches`);
                }
            } catch (e) {
                console.warn('   API search failed:', e.message);
            }
        }

        // ==========================================
        // RESPONSE
        // ==========================================
        const elapsed = Date.now() - startTime;
        console.log(`⏱️  Total time: ${elapsed}ms`);
        console.log(`📊 Total results: ${results.length}`);
        console.log('='.repeat(60) + '\n');

        // Sort by score (exact matches first)
        results.sort((a, b) => (b.score || 0) - (a.score || 0));

        res.json({
            success: true,
            count: results.length,
            query: query,
            results: results.slice(0, 20),  // Limit to 20 results
            search_time_ms: elapsed,
            levels_used: {
                local_db: localCerts.length + localBonds.length > 0,
                live_scraping: results.some(r => r.live),
                external_api: results.some(r => r.source && r.source.includes('Yahoo'))
            }
        });

    } catch (error) {
        console.error('❌ Search critical error:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// ==========================================
// ENDPOINT: GET QUOTE (EXISTING)
// ==========================================
router.get('/quote/:symbol', async (req, res) => {
    // Keep existing implementation
    if (aggregator) {
        try {
            const quote = await aggregator.getQuote(req.params.symbol);
            res.json(quote);
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    } else {
        res.status(503).json({ success: false, error: 'DataAggregator not available' });
    }
});

// ==========================================
// HEALTH CHECK
// ==========================================
router.get('/health', (req, res) => {
    const scriptsOk = fs.existsSync(path.join(__dirname, 'live_search.py'));
    
    res.json({
        status: 'ok',
        features: {
            local_db: true,
            live_scraping: scriptsOk,
            external_api: !!aggregator
        }
    });
});

module.exports = router;
