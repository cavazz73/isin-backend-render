"""
Certificate Scraper - Based on Gemini's approach
Using Playwright + BeautifulSoup
"""

import asyncio
import json
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime

async def scrape_certificate(isin):
    """Scrape single certificate data"""
    url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
    
    async with async_playwright() as p:
        # Launch browser with realistic User-Agent
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        print(f"[{isin}] Loading page...")
        
        try:
            await page.goto(url, wait_until="networkidle", timeout=20000)
            
            # KEY SOLUTION: Wait for specific content to load
            # Try multiple possible selectors
            try:
                await page.wait_for_selector("text=Scheda Sottostante", timeout=10000)
                print(f"[{isin}] Found 'Scheda Sottostante' - page loaded!")
            except:
                # Alternative selectors
                try:
                    await page.wait_for_selector("td.titolo_scheda", timeout=5000)
                    print(f"[{isin}] Found title - page loaded!")
                except:
                    print(f"[{isin}] Warning: Selectors not found, trying anyway...")
            
            # Wait a bit more for dynamic content
            await asyncio.sleep(2)
            
            # Get rendered HTML
            content = await page.content()
            
            # Save HTML for debugging
            with open(f'debug_{isin}.html', 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"[{isin}] Saved HTML to debug_{isin}.html")
            
        except Exception as e:
            print(f"[{isin}] Error loading page: {e}")
            await browser.close()
            return None
        
        await browser.close()
    
    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')
    
    # Extract data
    data = {
        'isin': isin,
        'url': url,
        'scraped': True,
        'timestamp': datetime.now().isoformat()
    }
    
    # Helper function to find value by label
    def get_value_by_label(label_text):
        """Find value in table by searching for label"""
        label = soup.find(string=re.compile(label_text, re.IGNORECASE))
        if label:
            parent_td = label.find_parent('td')
            if parent_td:
                next_td = parent_td.find_next_sibling('td')
                if next_td:
                    return next_td.get_text(strip=True)
        return None
    
    # Try to extract name/title
    # Try multiple selectors
    name = None
    name_selectors = [
        ('td', {'class': 'titolo_scheda'}),
        ('font', {'size': '+1'}),
        ('h1', {}),
        ('h2', {})
    ]
    for tag, attrs in name_selectors:
        elem = soup.find(tag, attrs)
        if elem:
            name = elem.get_text(strip=True)
            if name and len(name) > 5:
                break
    
    data['name'] = name if name else f"Certificate {isin}"
    
    # Extract issuer
    data['issuer'] = get_value_by_label("Emittente") or "N/A"
    
    # Extract category/type
    data['type'] = get_value_by_label("Categoria") or get_value_by_label("Tipo") or "N/A"
    
    # Extract price
    price_str = get_value_by_label("Prezzo") or get_value_by_label("Ultimo")
    if price_str:
        # Try to extract number
        price_match = re.search(r'(\d+[.,]\d+)', price_str)
        if price_match:
            data['price'] = float(price_match.group(1).replace(',', '.'))
    
    # Extract coupon
    coupon_str = get_value_by_label("Cedola") or get_value_by_label("Premio")
    if coupon_str:
        coupon_match = re.search(r'(\d+[.,]\d+)', coupon_str)
        if coupon_match:
            data['coupon'] = float(coupon_match.group(1).replace(',', '.'))
    
    # Extract barrier
    barrier_str = get_value_by_label("Barriera")
    if barrier_str:
        barrier_match = re.search(r'(\d+)[%]?', barrier_str)
        if barrier_match:
            data['barrier'] = int(barrier_match.group(1))
    
    print(f"[{isin}] Extracted data: {data}")
    return data

async def scrape_multiple(isins):
    """Scrape multiple certificates"""
    results = []
    
    for isin in isins:
        try:
            cert = await scrape_certificate(isin)
            if cert:
                results.append(cert)
        except Exception as e:
            print(f"Error scraping {isin}: {e}")
        
        # Rate limiting
        await asyncio.sleep(2)
    
    return results

async def main():
    """Main test function"""
    print("=" * 60)
    print("CERTIFICATE SCRAPER - PLAYWRIGHT + BEAUTIFULSOUP")
    print("=" * 60)
    print("")
    
    # Test ISINs
    test_isins = [
        'IT0006771510',
        'DE000HD8SXZ1',
        'XS2470031936',
        'CH1390857220'
    ]
    
    print(f"Testing {len(test_isins)} certificates...")
    print("")
    
    results = await scrape_multiple(test_isins)
    
    print("")
    print("=" * 60)
    print(f"RESULTS: {len(results)} / {len(test_isins)} successful")
    print("=" * 60)
    print("")
    
    # Save results
    output = {
        'success': True,
        'timestamp': datetime.now().isoformat(),
        'total': len(test_isins),
        'successful': len(results),
        'certificates': results
    }
    
    with open('playwright_test_results.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("Saved: playwright_test_results.json")
    print("")
    
    # Display sample
    if results:
        print("SAMPLE (first certificate):")
        print(json.dumps(results[0], indent=2, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())
