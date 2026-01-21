/**
 * Test Scraper - Single Certificate
 * Tests the scraper on a single ISIN to verify it works
 */

const { chromium } = require('playwright');

async function testSingleCertificate(isin) {
    console.log('🧪 Testing scraper on single certificate...');
    console.log(`📋 ISIN: ${isin}`);
    console.log('━'.repeat(60));

    const browser = await chromium.launch({
        headless: false,  // Show browser for debugging
        args: ['--no-sandbox']
    });

    const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    });

    const page = await context.newPage();

    try {
        // Test Borsa Italiana
        console.log('\n🇮🇹 Testing Borsa Italiana...');
        const url = `https://www.borsaitaliana.it/borsa/certificates/scheda/${isin}.html`;
        console.log(`📄 URL: ${url}`);

        await page.goto(url, {
            waitUntil: 'domcontentloaded',
            timeout: 30000
        });

        console.log('✅ Page loaded');

        // Wait for content
        await page.waitForTimeout(3000);

        // Take screenshot
        await page.screenshot({ 
            path: `test-screenshot-${isin}.png`,
            fullPage: true 
        });
        console.log(`📸 Screenshot saved: test-screenshot-${isin}.png`);

        // Extract data
        const data = await page.evaluate(() => {
            const getText = (selector) => {
                const el = document.querySelector(selector);
                return el ? el.textContent.trim() : null;
            };

            const getAllText = () => {
                return document.body.textContent;
            };

            // Try multiple selectors
            return {
                // Basic info
                title: document.querySelector('h1')?.textContent.trim(),
                
                // All text on page (for debugging)
                fullText: getAllText(),
                
                // Specific fields
                name: getText('h1, .title, .cert-name, [class*="name"]'),
                issuer: getText('.issuer, .emittente, [class*="issuer"]'),
                underlying: getText('.underlying, .sottostante, [class*="underlying"]'),
                barrier: getText('.barrier, .barriera, [class*="barrier"]'),
                coupon: getText('.coupon, .cedola, [class*="coupon"]'),
                strike: getText('.strike, [class*="strike"]'),
                price: getText('.price, .prezzo, [class*="price"]'),
                maturity: getText('.maturity, .scadenza, [class*="maturity"]'),
                
                // Count elements
                divCount: document.querySelectorAll('div').length,
                tableCount: document.querySelectorAll('table').length,
                
                // HTML structure
                html: document.documentElement.outerHTML.substring(0, 5000)
            };
        });

        console.log('\n📊 Extracted Data:');
        console.log('─'.repeat(60));
        console.log('Title:', data.title);
        console.log('Name:', data.name);
        console.log('Issuer:', data.issuer);
        console.log('Underlying:', data.underlying);
        console.log('Barrier:', data.barrier);
        console.log('Coupon:', data.coupon);
        console.log('Strike:', data.strike);
        console.log('Price:', data.price);
        console.log('Maturity:', data.maturity);
        console.log('─'.repeat(60));
        console.log(`\nPage has ${data.divCount} divs, ${data.tableCount} tables`);

        // Look for "Alibaba" or "DAX" in text
        const hasAlibaba = data.fullText.includes('Alibaba') || data.fullText.includes('ALIBABA');
        const hasDAX = data.fullText.includes('DAX');
        
        console.log('\n🔍 Text Search:');
        console.log(`  Alibaba found: ${hasAlibaba ? '✅ YES' : '❌ NO'}`);
        console.log(`  DAX found: ${hasDAX ? '✅ YES' : '❌ NO'}`);

        // Save full HTML for inspection
        const fs = require('fs');
        fs.writeFileSync(`test-html-${isin}.html`, data.html, 'utf8');
        console.log(`\n💾 HTML saved: test-html-${isin}.html`);

        console.log('\n✅ Test completed!');
        console.log('📝 Review:');
        console.log('  1. Check the screenshot');
        console.log('  2. Check the HTML file');
        console.log('  3. Update selectors in scraper if needed');

    } catch (error) {
        console.error('\n❌ Test failed:', error);
    } finally {
        await browser.close();
    }
}

// Run test
const testISIN = process.argv[2] || 'DE000VH6MX98';
testSingleCertificate(testISIN)
    .then(() => process.exit(0))
    .catch(err => {
        console.error(err);
        process.exit(1);
    });
