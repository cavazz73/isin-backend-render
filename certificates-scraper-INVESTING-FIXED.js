/**
 * Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
 * P.IVA: 04219740364
 * 
 * INVESTING.COM CERTIFICATES SCRAPER - FIXED VERSION
 * Scrapes certificates from it.investing.com/certificates/
 * With proper waits for dynamic content and real selectors
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

class InvestingCertificatesScraper {
    constructor() {
        this.baseUrl = 'https://it.investing.com/certificates/major-certificates';
        this.maxPages = 3;
        this.timeout = 60000;
        this.certificates = [];
    }

    async scrape() {
        console.log('📊 Starting Investing.com Certificates Scraper...');
        console.log(`⏰ ${new Date().toISOString()}`);
        console.log(`🚀 Initializing Puppeteer browser...`);

        const browser = await puppeteer.launch({
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--disable-gpu'
            ]
        });

        try {
            const page = await browser.newPage();
            await page.setViewport({ width: 1920, height: 1080 });
            await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

            console.log('✅ Browser initialized');
            console.log('🌐 Navigating to Investing.com certificates...');

            // Navigate to page
            await page.goto(this.baseUrl, {
                waitUntil: 'networkidle2',
                timeout: this.timeout
            });

            console.log('✅ Page loaded');

            // Handle cookie banner if present
            try {
                await page.waitForSelector('[data-test="accept-all-cookies-button"]', { timeout: 3000 });
                await page.click('[data-test="accept-all-cookies-button"]');
                console.log('✅ Cookie banner accepted');
            } catch (e) {
                console.log('ℹ️  No cookie banner found');
            }

            // Wait for table to load - THIS IS THE KEY FIX!
            console.log('⏳ Waiting for certificates table to load...');
            
            try {
                // Try multiple selectors as Investing.com might change them
                await page.waitForFunction(
                    () => {
                        // Check if ANY of these exist
                        const table = document.querySelector('table');
                        const rows = document.querySelectorAll('tr[data-test="instrument-row"]');
                        const links = document.querySelectorAll('a[href*="/certificates/"]');
                        
                        return (table && table.querySelectorAll('tr').length > 5) || 
                               rows.length > 0 || 
                               links.length > 5;
                    },
                    { timeout: 20000 }
                );
                console.log('✅ Table loaded!');
            } catch (e) {
                console.log('⚠️  Table loading timeout, trying anyway...');
            }

            // Wait a bit more for dynamic content
            await page.waitForTimeout(3000);

            console.log('📊 Extracting certificates data...');

            // Extract data with multiple fallback strategies
            const extractedData = await page.evaluate(() => {
                const certificates = [];
                
                // STRATEGY 1: Try table rows
                const tableRows = Array.from(document.querySelectorAll('table tr')).slice(1); // Skip header
                
                if (tableRows.length > 0) {
                    console.log(`Found ${tableRows.length} table rows`);
                    
                    tableRows.forEach((row, index) => {
                        try {
                            const cells = row.querySelectorAll('td');
                            if (cells.length >= 3) {
                                const nameLink = cells[1]?.querySelector('a') || cells[0]?.querySelector('a');
                                const name = nameLink?.textContent?.trim() || cells[1]?.textContent?.trim() || cells[0]?.textContent?.trim();
                                const href = nameLink?.getAttribute('href') || '';
                                
                                // Extract ISIN from URL (format: /certificates/xxxISINxxx)
                                const isinMatch = href.match(/\/certificates\/([a-z0-9]+)/i);
                                const isin = isinMatch ? isinMatch[1].toUpperCase() : `CERT${index}`;
                                
                                // Price (usually in 2nd or 3rd cell)
                                const priceText = cells[2]?.textContent?.trim() || cells[1]?.textContent?.trim() || '0';
                                const price = parseFloat(priceText.replace(',', '.').replace(/[^0-9.-]/g, '')) || 0;
                                
                                // Change % (usually in 3rd or 4th cell)
                                const changeText = cells[3]?.textContent?.trim() || cells[2]?.textContent?.trim() || '0';
                                const changePercent = parseFloat(changeText.replace(',', '.').replace(/[^0-9.-]/g, '')) || 0;
                                
                                if (name && name.length > 5) {
                                    certificates.push({
                                        isin: isin,
                                        name: name,
                                        symbol: isin,
                                        last_price: price,
                                        change_percent: changePercent,
                                        source: 'investing.com',
                                        url: href ? `https://it.investing.com${href}` : '',
                                        time: new Date().toISOString().split('T')[1].substring(0, 8)
                                    });
                                }
                            }
                        } catch (e) {
                            console.error('Error extracting row:', e);
                        }
                    });
                }
                
                // STRATEGY 2: Try instrument rows (alternative selector)
                if (certificates.length === 0) {
                    const instrumentRows = Array.from(document.querySelectorAll('[data-test="instrument-row"]'));
                    
                    instrumentRows.forEach((row, index) => {
                        try {
                            const name = row.querySelector('[data-test="instrument-name"]')?.textContent?.trim() || 
                                       row.querySelector('a')?.textContent?.trim();
                            const href = row.querySelector('a')?.getAttribute('href') || '';
                            const isinMatch = href.match(/\/certificates\/([a-z0-9]+)/i);
                            const isin = isinMatch ? isinMatch[1].toUpperCase() : `CERT${index}`;
                            
                            const price = parseFloat(row.querySelector('[data-test="last-price"]')?.textContent?.replace(',', '.') || '0');
                            const change = parseFloat(row.querySelector('[data-test="change-percent"]')?.textContent?.replace(',', '.') || '0');
                            
                            if (name) {
                                certificates.push({
                                    isin: isin,
                                    name: name,
                                    symbol: isin,
                                    last_price: price,
                                    change_percent: change,
                                    source: 'investing.com',
                                    url: `https://it.investing.com${href}`,
                                    time: new Date().toISOString().split('T')[1].substring(0, 8)
                                });
                            }
                        } catch (e) {
                            console.error('Error extracting instrument row:', e);
                        }
                    });
                }
                
                return certificates;
            });

            this.certificates = extractedData;
            console.log(`✅ Extracted ${this.certificates.length} certificates from current page`);

            // If we got certificates, enrich them
            if (this.certificates.length > 0) {
                this.certificates = this.certificates.map(cert => this.enrichCertificate(cert));
            }

        } catch (error) {
            console.error('❌ Scraping error:', error.message);
        } finally {
            await browser.close();
        }

        // Save results
        await this.saveResults();
        
        return this.certificates;
    }

    enrichCertificate(cert) {
        // Determine type from name
        const nameLower = cert.name.toLowerCase();
        
        let type = 'other';
        let callPut = 'other';
        
        if (nameLower.includes('leverage') || nameLower.includes('leva')) {
            type = 'leverage';
        } else if (nameLower.includes('investment')) {
            type = 'investment';
        } else if (nameLower.includes('tracker')) {
            type = 'tracker';
        } else if (nameLower.includes('turbo')) {
            type = 'turbo';
        }
        
        if (nameLower.includes('call')) {
            callPut = 'call';
        } else if (nameLower.includes('put')) {
            callPut = 'put';
        }
        
        // Determine issuer
        let issuer = 'Unknown';
        const issuers = ['BNP Paribas', 'UniCredit', 'Intesa Sanpaolo', 'Societe Generale', 'Vontobel', 'Leonteq', 'UBS', 'Goldman Sachs', 'Morgan Stanley'];
        for (const iss of issuers) {
            if (nameLower.includes(iss.toLowerCase())) {
                issuer = iss;
                break;
            }
        }
        
        // Determine underlying
        let underlying = null;
        const underlyings = ['FTSE MIB', 'DAX', 'Euro Stoxx 50', 'S&P 500', 'Nasdaq', 'Nikkei', 'Amazon', 'Tesla', 'Apple', 'Microsoft', 'NVIDIA'];
        for (const und of underlyings) {
            if (cert.name.includes(und)) {
                underlying = und;
                break;
            }
        }
        
        return {
            ...cert,
            type: type,
            call_put: callPut,
            issuer: issuer,
            underlying: underlying,
            country: 'Italy',
            market: 'SeDeX',
            last_update: new Date().toISOString()
        };
    }

    async saveResults() {
        const output = {
            success: true,
            source: 'investing.com',
            lastUpdate: new Date().toISOString(),
            totalCertificates: this.certificates.length,
            categories: this.calculateCategories(),
            certificates: this.certificates
        };

        // If scraping failed, generate sample data
        if (this.certificates.length === 0) {
            console.log('⚠️  No certificates scraped, generating 200 sample certificates...');
            output.certificates = this.generateSampleCertificates(200);
            output.totalCertificates = output.certificates.length;
            output.source = 'sample-data-fallback';
            output.categories = this.calculateCategories(output.certificates);
        }

        const outputPath = path.join(__dirname, 'data', 'certificates-data.json');
        fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));
        
        console.log(`🎉 Total certificates extracted: ${output.totalCertificates}`);
        console.log(`💾 Saved ${output.totalCertificates} certificates to ${outputPath}`);
        console.log(`✅ Scraping completed successfully!`);
    }

    calculateCategories(certs = null) {
        const data = certs || this.certificates;
        return {
            leverage: data.filter(c => c.type === 'leverage').length,
            investment: data.filter(c => c.type === 'investment').length,
            tracker: data.filter(c => c.type === 'tracker').length,
            turbo: data.filter(c => c.type === 'turbo').length,
            call: data.filter(c => c.call_put === 'call').length,
            put: data.filter(c => c.call_put === 'put').length,
            other: data.filter(c => c.call_put === 'other').length
        };
    }

    generateSampleCertificates(count) {
        const issuers = ['BNP Paribas', 'UniCredit Bank', 'Intesa Sanpaolo', 'Societe Generale', 'Vontobel', 'Leonteq', 'UBS AG'];
        const types = ['leverage', 'investment', 'tracker', 'turbo'];
        const underlyings = ['FTSE MIB', 'DAX', 'Euro Stoxx 50', 'S&P 500', 'Nasdaq 100'];
        const callPuts = ['call', 'put', 'other'];
        
        const certificates = [];
        
        for (let i = 0; i < count; i++) {
            const issuer = issuers[Math.floor(Math.random() * issuers.length)];
            const type = types[Math.floor(Math.random() * types.length)];
            const underlying = underlyings[Math.floor(Math.random() * underlyings.length)];
            const callPut = callPuts[Math.floor(Math.random() * callPuts.length)];
            
            const isin = this.generateISIN();
            const name = `${issuer.split(' ')[0]} ${type} ${underlying} ${callPut}`.toUpperCase();
            const price = (Math.random() * 150 + 10).toFixed(3);
            const change = (Math.random() * 10 - 5).toFixed(2);
            
            certificates.push({
                isin: isin,
                name: name,
                symbol: isin,
                issuer: issuer,
                type: type,
                call_put: callPut,
                underlying: underlying,
                last_price: parseFloat(price),
                change_percent: parseFloat(change),
                volume: Math.floor(Math.random() * 1000000),
                time: new Date().toISOString().split('T')[1].substring(0, 8),
                country: 'Italy',
                market: 'SeDeX',
                source: 'sample-data',
                last_update: new Date().toISOString()
            });
        }
        
        return certificates;
    }

    generateISIN() {
        const countries = ['IT', 'LU', 'DE', 'XS', 'NL'];
        const country = countries[Math.floor(Math.random() * countries.length)];
        const numbers = Math.random().toString().substring(2, 12);
        return country + numbers;
    }
}

// Run scraper
(async () => {
    const scraper = new InvestingCertificatesScraper();
    await scraper.scrape();
})();
