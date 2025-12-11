/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Main Script - Daily Bonds Data Update
 * Fetches all bond categories and updates bonds-data.json
 */

const fs = require('fs').promises;
const path = require('path');

// Import category fetchers
const fetchItalyBonds = require('./fetch-bonds-italy');
const fetchEUBonds = require('./fetch-bonds-eu');
const fetchSupranationalBonds = require('./fetch-bonds-supranational');
const fetchCorporateBonds = require('./fetch-bonds-corporate');

async function updateAllBonds() {
    console.log('='.repeat(70));
    console.log('🔄 BONDS DATA UPDATE - Starting...');
    console.log('='.repeat(70));
    console.log(`⏰ Start time: ${new Date().toISOString()}`);
    console.log('');

    const results = {
        'it-governativi': { bonds: [], errors: [] },
        'eu-governativi': { bonds: [], errors: [] },
        'sovranazionali': { bonds: [], errors: [] },
        'corporate': { bonds: [], errors: [] }
    };

    // 1. Fetch IT Governativi (BTP, BOT, CCT, CTZ)
    console.log('📊 [1/4] Fetching Italian Government Bonds...');
    try {
        results['it-governativi'].bonds = await fetchItalyBonds();
        console.log(`✅ Italian Bonds: ${results['it-governativi'].bonds.length} bonds found`);
    } catch (error) {
        console.error(`❌ Italian Bonds Error: ${error.message}`);
        results['it-governativi'].errors.push(error.message);
    }
    console.log('');

    // 2. Fetch EU Governativi (Bund, OAT, Bonos)
    console.log('📊 [2/4] Fetching EU Government Bonds...');
    try {
        results['eu-governativi'].bonds = await fetchEUBonds();
        console.log(`✅ EU Bonds: ${results['eu-governativi'].bonds.length} bonds found`);
    } catch (error) {
        console.error(`❌ EU Bonds Error: ${error.message}`);
        results['eu-governativi'].errors.push(error.message);
    }
    console.log('');

    // 3. Fetch Sovranazionali (BEI, EFSF, ESM)
    console.log('📊 [3/4] Fetching Supranational Bonds...');
    try {
        results['sovranazionali'].bonds = await fetchSupranationalBonds();
        console.log(`✅ Supranational Bonds: ${results['sovranazionali'].bonds.length} bonds found`);
    } catch (error) {
        console.error(`❌ Supranational Bonds Error: ${error.message}`);
        results['sovranazionali'].errors.push(error.message);
    }
    console.log('');

    // 4. Fetch Corporate
    console.log('📊 [4/4] Fetching Corporate Bonds...');
    try {
        results['corporate'].bonds = await fetchCorporateBonds();
        console.log(`✅ Corporate Bonds: ${results['corporate'].bonds.length} bonds found`);
    } catch (error) {
        console.error(`❌ Corporate Bonds Error: ${error.message}`);
        results['corporate'].errors.push(error.message);
    }
    console.log('');

    // Calculate statistics
    const totalBonds = Object.values(results).reduce((sum, cat) => sum + cat.bonds.length, 0);
    
    // Build final data structure
    const bondsData = {
        lastUpdate: new Date().toISOString(),
        categories: {
            'it-governativi': {
                name: "IT Governativi Italia",
                description: "BTP, BOT, CCT, CTZ",
                bonds: results['it-governativi'].bonds,
                count: results['it-governativi'].bonds.length
            },
            'eu-governativi': {
                name: "EU Governativi Europa",
                description: "Bund Germania, OAT Francia, Bonos Spagna",
                bonds: results['eu-governativi'].bonds,
                count: results['eu-governativi'].bonds.length
            },
            'sovranazionali': {
                name: "Sovranazionali",
                description: "BEI, EFSF, ESM",
                bonds: results['sovranazionali'].bonds,
                count: results['sovranazionali'].bonds.length
            },
            'corporate': {
                name: "Corporate",
                description: "Obbligazioni societarie",
                bonds: results['corporate'].bonds,
                count: results['corporate'].bonds.length
            }
        },
        statistics: {
            totalBonds: totalBonds,
            byCategory: {
                'it-governativi': results['it-governativi'].bonds.length,
                'eu-governativi': results['eu-governativi'].bonds.length,
                'sovranazionali': results['sovranazionali'].bonds.length,
                'corporate': results['corporate'].bonds.length
            },
            errors: Object.entries(results)
                .filter(([_, data]) => data.errors.length > 0)
                .map(([category, data]) => ({ category, errors: data.errors }))
        }
    };

    // Save to file
    const dataPath = path.join(__dirname, '..', 'data', 'bonds-data.json');
    await fs.writeFile(dataPath, JSON.stringify(bondsData, null, 2), 'utf8');

    console.log('='.repeat(70));
    console.log('✅ BONDS DATA UPDATE - Completed!');
    console.log('='.repeat(70));
    console.log(`📊 Total Bonds: ${totalBonds}`);
    console.log(`   • IT Governativi: ${results['it-governativi'].bonds.length}`);
    console.log(`   • EU Governativi: ${results['eu-governativi'].bonds.length}`);
    console.log(`   • Sovranazionali: ${results['sovranazionali'].bonds.length}`);
    console.log(`   • Corporate: ${results['corporate'].bonds.length}`);
    console.log(`⏰ Completed at: ${new Date().toISOString()}`);
    console.log(`💾 Saved to: ${dataPath}`);
    
    if (bondsData.statistics.errors.length > 0) {
        console.log(`\n⚠️  Errors encountered:`);
        bondsData.statistics.errors.forEach(({ category, errors }) => {
            console.log(`   • ${category}: ${errors.join(', ')}`);
        });
    }
    
    console.log('='.repeat(70));

    return bondsData;
}

// Run if called directly
if (require.main === module) {
    updateAllBonds()
        .then(() => {
            console.log('✅ Script completed successfully');
            process.exit(0);
        })
        .catch(error => {
            console.error('❌ Script failed:', error);
            process.exit(1);
        });
}

module.exports = updateAllBonds;
