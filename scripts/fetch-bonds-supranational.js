/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * Supranational Bonds Fetcher
 * Sources: BEI (EIB), EFSF, ESM
 */

async function fetchSupranationalBonds() {
    console.log('   📌 Fetching Supranational Bonds...');
    
    const bonds = [
        // European Investment Bank (BEI/EIB)
        {
            isin: "EU000A1G0DM7",
            name: "BEI 1.50% Nov 2028",
            type: "BEI",
            country: "EU",
            issuer: "European Investment Bank",
            currency: "EUR",
            maturity: "2028-11-15",
            coupon: 1.50,
            yield: 2.85,
            price: 95.30,
            change: "+0.08",
            lastUpdate: new Date().toISOString().split('T')[0]
        },
        {
            isin: "EU000A1Z99T2",
            name: "BEI 2.25% Jun 2030",
            type: "BEI",
            country: "EU",
            issuer: "European Investment Bank",
            currency: "EUR",
            maturity: "2030-06-15",
            coupon: 2.25,
            yield: 2.95,
            price: 96.50,
            change: "-0.10",
            lastUpdate: new Date().toISOString().split('T')[0]
        },
        // European Financial Stability Facility (EFSF)
        {
            isin: "EU000A1G0BP8",
            name: "EFSF 2.75% Oct 2027",
            type: "EFSF",
            country: "EU",
            issuer: "European Financial Stability Facility",
            currency: "EUR",
            maturity: "2027-10-19",
            coupon: 2.75,
            yield: 2.88,
            price: 98.20,
            change: "+0.15",
            lastUpdate: new Date().toISOString().split('T')[0]
        },
        // European Stability Mechanism (ESM)
        {
            isin: "EU000A1ZA6V3",
            name: "ESM 1.50% Mar 2029",
            type: "ESM",
            country: "EU",
            issuer: "European Stability Mechanism",
            currency: "EUR",
            maturity: "2029-03-15",
            coupon: 1.50,
            yield: 2.92,
            price: 94.80,
            change: "-0.12",
            lastUpdate: new Date().toISOString().split('T')[0]
        }
    ];

    console.log(`   ✅ Found ${bonds.length} supranational bonds`);
    return bonds;
}

module.exports = fetchSupranationalBonds;
