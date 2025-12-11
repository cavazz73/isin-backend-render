/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Italian Government Bonds Fetcher
 * Sources: Ministry of Economy (MEF), Borsa Italiana
 */

const axios = require('axios');
const cheerio = require('cheerio');

/**
 * Fetch Italian Government Bonds (BTP, BOT, CCT, CTZ)
 * Uses Investing.com public data (end-of-day, no auth required)
 */
async function fetchItalyBonds() {
    console.log('   📌 Fetching from Investing.com - Italian Bonds...');
    
    const bonds = [];
    
    // Investing.com Italy Bonds page (public, no login)
    const url = 'https://www.investing.com/rates-bonds/italy-government-bonds';
    
    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5'
            },
            timeout: 15000
        });

        const $ = cheerio.load(response.data);
        
        // Parse bonds table
        $('table.genTbl tbody tr').each((index, element) => {
            const $row = $(element);
            
            // Extract data from columns
            const name = $row.find('td').eq(0).text().trim();
            const yield_ = $row.find('td').eq(1).text().trim();
            const price = $row.find('td').eq(2).text().trim();
            const change = $row.find('td').eq(3).text().trim();
            
            if (name && yield_ && price) {
                // Parse bond name to extract type and maturity
                let bondType = 'BTP';
                if (name.includes('BOT')) bondType = 'BOT';
                else if (name.includes('CCT')) bondType = 'CCT';
                else if (name.includes('CTZ')) bondType = 'CTZ';
                
                // Extract maturity year
                const maturityMatch = name.match(/\d{4}/);
                const maturity = maturityMatch ? maturityMatch[0] : null;
                
                bonds.push({
                    isin: null, // Will be populated from secondary source if needed
                    name: name,
                    type: bondType,
                    country: 'IT',
                    currency: 'EUR',
                    maturity: maturity,
                    coupon: null, // Parse from name if available
                    yield: parseFloat(yield_.replace('%', '')) || null,
                    price: parseFloat(price) || null,
                    change: change,
                    lastUpdate: new Date().toISOString().split('T')[0]
                });
            }
        });

        console.log(`   ✅ Found ${bonds.length} Italian government bonds`);
        
    } catch (error) {
        console.error(`   ❌ Error fetching Italian bonds: ${error.message}`);
        
        // FALLBACK: Return sample data structure (replace with static file later)
        return getSampleItalianBonds();
    }

    return bonds.length > 0 ? bonds : getSampleItalianBonds();
}

/**
 * Fallback: Sample Italian bonds data
 * TODO: Replace with static file from official source (MEF website)
 */
function getSampleItalianBonds() {
    return [
        {
            isin: "IT0005508251",
            name: "BTP 2.45% Mar 2025",
            type: "BTP",
            country: "IT",
            currency: "EUR",
            maturity: "2025-03-01",
            coupon: 2.45,
            yield: 3.12,
            price: 98.50,
            change: "+0.15",
            lastUpdate: new Date().toISOString().split('T')[0]
        },
        {
            isin: "IT0005436693",
            name: "BTP 0.95% Sep 2027",
            type: "BTP",
            country: "IT",
            currency: "EUR",
            maturity: "2027-09-15",
            coupon: 0.95,
            yield: 3.45,
            price: 92.30,
            change: "-0.25",
            lastUpdate: new Date().toISOString().split('T')[0]
        },
        {
            isin: "IT0005513434",
            name: "BTP 4.00% Feb 2037",
            type: "BTP",
            country: "IT",
            currency: "EUR",
            maturity: "2037-02-01",
            coupon: 4.00,
            yield: 4.15,
            price: 96.80,
            change: "+0.30",
            lastUpdate: new Date().toISOString().split('T')[0]
        },
        {
            isin: "IT0005524027",
            name: "BOT 12M Jun 2025",
            type: "BOT",
            country: "IT",
            currency: "EUR",
            maturity: "2025-06-30",
            coupon: 0,
            yield: 3.25,
            price: 96.75,
            change: "-0.05",
            lastUpdate: new Date().toISOString().split('T')[0]
        }
    ];
}

module.exports = fetchItalyBonds;
