#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

CED Lab Pro Certificates Scraper v1.7
Simple version with pagination + post-scraping filter (no single stocks)

Environment variables required:
- CED_USERNAME: CED Lab Pro username
- CED_PASSWORD: CED Lab Pro password
"""

import json
import re
import os
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

CONFIG = {
    'search_url': 'https://cedlabpro.it/menu/ricerca-avanzata',
    'max_certificates': 500,
    'max_pages': 30,
    'timeout': 90000,
    'output_path': 'data/certificates-data.json'
}

# Keywords to identify single stocks (to EXCLUDE)
STOCK_KEYWORDS = [
    'apple', 'microsoft', 'amazon', 'google', 'meta', 'nvidia', 'tesla', 'netflix',
    'alibaba', 'tencent', 'samsung', 'intel', 'amd', 'qualcomm', 'cisco', 'oracle',
    'salesforce', 'adobe', 'paypal', 'uber', 'airbnb', 'spotify', 'zoom', 'shopify',
    'stellantis', 'ferrari', 'eni', 'enel', 'intesa', 'unicredit', 'generali', 'leonardo',
    'stmicroelectronics', 'amplifon', 'moncler', 'prada', 'luxottica', 'campari',
    'volkswagen', 'bmw', 'daimler', 'mercedes', 'porsche', 'siemens', 'basf', 'bayer',
    'deutsche bank', 'allianz', 'sap', 'adidas', 'puma', 'henkel', 'beiersdorf',
    'lvmh', 'total', 'sanofi', 'bnp', 'societe generale', 'axa', 'danone', 'loreal',
    'hermes', 'kering', 'schneider', 'air liquide', 'vinci', 'safran', 'thales',
    'repsol', 'iberdrola', 'telefonica', 'santander', 'bbva', 'inditex', 'amadeus',
    'shell', 'bp', 'hsbc', 'barclays', 'gsk', 'astrazeneca', 'unilever', 'rio tinto',
    'glencore', 'diageo', 'vodafone', 'rolls royce', 'bae systems',
    'pfizer', 'johnson', 'merck', 'abbvie', 'eli lilly', 'bristol', 'amgen', 'gilead',
    'moderna', 'biontech', 'novartis', 'roche', 'nestle', 'zurich', 'ubs', 'credit suisse',
    'nintendo', 'sony', 'toyota', 'honda', 'softbank', 'mitsubishi', 'panasonic',
    'electronic arts', 'ubisoft', 'activision', 'take-two', 'ea games',
    'coinbase', 'robinhood', 'palantir', 'snowflake', 'datadog', 'crowdstrike',
    'beyond meat', 'peloton', 'docusign', 'twilio', 'okta', 'zscaler',
    # Italian companies
    'telecom italia', 'tim', 'poste italiane', 'mediobanca', 'finecobank', 'bper',
    'banco bpm', 'nexi', 'recordati', 'diasorin', 'pirelli', 'tenaris', 'saipem',
    'buzzi', 'italgas', 'snam', 'terna', 'hera', 'a2a', 'inwit', 'atlantia'
]

# Keywords to identify wanted underlying types (to INCLUDE)
INDEX_KEYWORDS = ['index', 'indice', 'stoxx', 'ftse', 's&p', 'nasdaq', 'dax', 'mib', 'nikkei', 'cac', 'ibex', 'hang seng', 'kospi', 'russell', 'dow jones', 'euro stoxx', 'msci']
COMMODITY_KEYWORDS = ['gold', 'oro', 'silver', 'argento', 'oil', 'petrolio', 'wti', 'brent', 'gas', 'natural gas', 'copper', 'rame', 'platinum', 'palladium', 'wheat', 'corn', 'soybean', 'coffee', 'sugar', 'cotton', 'commodity', 'generic 1st', 'future generic']
CURRENCY_KEYWORDS = ['eur/usd', 'usd/jpy', 'gbp/usd', 'usd/chf', 'eur/gbp', 'aud/usd', 'usd/cad', 'forex', 'fx', 'currency', 'cambio']
RATE_KEYWORDS = ['btp', 'bund', 'treasury', 'euribor', 'libor', 'sofr', 'rate', 'tasso', 'bond', 'obbligazione', 'yield', 'swap']
CREDIT_KEYWORDS = ['credit', 'cln', 'credito', 'default', 'cds']


def log(msg, level='INFO'):
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] [{level}] {msg}")


def get_credentials():
    username = os.environ.get('CED_USERNAME')
    password = os.environ.get('CED_PASSWORD')
    if not username or not password:
        log("ERROR: CED_USERNAME and CED_PASSWORD required!", 'ERROR')
        sys.exit(1)
    return username, password


def parse_number(text):
    if not text or text.strip() in ['', '-', '--', 'N/A', 'N/a']:
        return None
    text = str(text).strip().replace('€', '').replace('%', '').replace(' ', '')
    if ',' in text and '.' in text:
        text = text.replace('.', '').replace(',', '.')
    elif ',' in text:
        text = text.replace(',', '.')
    try:
        return float(text)
    except:
        return None


def categorize_underlying(text):
    """Categorize based on underlying text"""
    text_lower = text.lower()
    
    if any(kw in text_lower for kw in INDEX_KEYWORDS):
        return 'index'
    if any(kw in text_lower for kw in COMMODITY_KEYWORDS):
        return 'commodity'
    if any(kw in text_lower for kw in CURRENCY_KEYWORDS):
        return 'currency'
    if any(kw in text_lower for kw in RATE_KEYWORDS):
        return 'rate'
    if any(kw in text_lower for kw in CREDIT_KEYWORDS):
        return 'credit_linked'
    
    return 'stock'


def is_single_stock(cert):
    """Check if certificate is based on single stocks (to exclude)"""
    text = (
        cert.get('underlying_name', '') + ' ' +
        cert.get('name', '') + ' ' +
        ' '.join([u.get('name', '') for u in cert.get('underlyings', [])])
    ).lower()
    
    # If categorized as index/commodity/currency/rate/credit, keep it
    if cert.get('underlying_category') in ['index', 'commodity', 'currency', 'rate', 'credit_linked']:
        return False
    
    # Check for known stock names
    if any(stock in text for stock in STOCK_KEYWORDS):
        return True
    
    # If category is 'stock', exclude
    if cert.get('underlying_category') == 'stock':
        return True
    
    return False


def perform_login(page, username, password):
    """Perform login"""
    log("Performing login...")
    page.wait_for_timeout(2000)
    
    try:
        page.locator('input[placeholder="Username"], input[type="text"]').first.fill(username)
        page.locator('input[placeholder="Password"], input[type="password"]').first.fill(password)
        page.wait_for_timeout(500)
        page.locator('button:has-text("Login")').first.click()
        page.wait_for_timeout(5000)
        log("✅ Login completed")
        return True
    except Exception as e:
        log(f"Login error: {e}", 'ERROR')
        return False


def navigate_to_search(page, username, password):
    """Navigate to search page"""
    log("Navigating to Ricerca Avanzata...")
    
    page.goto(CONFIG['search_url'], timeout=CONFIG['timeout'], wait_until='networkidle')
    page.wait_for_timeout(3000)
    
    if 'login' in page.url.lower() or 'identity' in page.url.lower():
        log("Login required")
        page.screenshot(path='login_page.png')
        if not perform_login(page, username, password):
            return False
        page.goto(CONFIG['search_url'], timeout=CONFIG['timeout'], wait_until='networkidle')
        page.wait_for_timeout(3000)
    
    log("✅ On search page")
    return True


def extract_from_table(page):
    """Extract certificates from results table"""
    certificates = []
    html = page.content()
    soup = BeautifulSoup(html, 'html.parser')
    
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if len(rows) < 2:
            continue
        
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(['th', 'td'])]
        
        if not any(h in headers for h in ['isin', 'nome', 'sottostante', 'emittente']):
            continue
        
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) < 4:
                continue
            
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            cert = {'scraped': True, 'timestamp': datetime.now().isoformat(), 'currency': 'EUR'}
            
            for i, header in enumerate(headers):
                if i >= len(cell_texts):
                    break
                value = cell_texts[i]
                
                if 'isin' in header:
                    link = cells[i].find('a')
                    cert['isin'] = link.get_text(strip=True) if link else value
                elif 'nome' in header:
                    cert['name'] = value
                elif header in ['sottostante', 'sottostanti']:
                    cert['underlying_name'] = value
                elif 'direzione' in header:
                    cert['direction'] = value
                elif 'emittente' in header:
                    cert['issuer'] = value
                elif 'worst' in header:
                    cert['worst_of'] = value.lower() in ['si', 'sì', 'yes', 's']
                elif 'basket' in header:
                    if value:
                        cert['underlyings'] = [{'name': u.strip()} for u in value.split('\n') if u.strip()]
                elif 'bid' in header or 'denaro' in header:
                    cert['bid_price'] = parse_number(value)
                elif 'ask' in header or 'lettera' in header:
                    cert['ask_price'] = parse_number(value)
            
            if cert.get('isin') and re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', cert['isin']):
                if cert.get('bid_price') and cert.get('ask_price'):
                    cert['price'] = (cert['bid_price'] + cert['ask_price']) / 2
                
                # Categorize
                text = cert.get('underlying_name', '') + ' ' + cert.get('name', '')
                cert['underlying_category'] = categorize_underlying(text)
                
                # Determine type
                name = cert.get('name', '').lower()
                cert['type'] = 'Certificate'
                for pattern, type_name in [
                    ('phoenix memory', 'Phoenix Memory'),
                    ('cash collect memory', 'Cash Collect Memory'),
                    ('cash collect', 'Cash Collect'),
                    ('fixed coupon', 'Fixed Coupon'),
                    ('bonus plus', 'Bonus Plus'),
                    ('express', 'Express'),
                    ('mini future', 'Mini Future'),
                    ('turbo', 'Turbo'),
                    ('memory', 'Memory'),
                    ('phoenix', 'Phoenix'),
                ]:
                    if pattern in name:
                        cert['type'] = type_name
                        break
                
                certificates.append(cert)
    
    return certificates


def scrape_all_pages(page):
    """Scrape with pagination"""
    all_certs = []
    page_num = 0
    
    page.screenshot(path='search_page_before.png')
    
    # Scroll and click Cerca
    log("Scrolling and clicking Cerca...")
    for _ in range(10):
        page.evaluate('window.scrollBy(0, 300)')
        page.wait_for_timeout(200)
    
    try:
        page.locator('button:has-text("Cerca")').first.click()
        log("✅ Clicked Cerca")
    except:
        log("Could not click Cerca", 'WARN')
    
    page.wait_for_timeout(8000)
    
    # Scroll to see results
    for _ in range(5):
        page.evaluate('window.scrollBy(0, 500)')
        page.wait_for_timeout(300)
    
    page.screenshot(path='search_page_after.png', full_page=True)
    
    # Extract pages
    while page_num < CONFIG['max_pages'] and len(all_certs) < CONFIG['max_certificates']:
        page_num += 1
        log(f"Page {page_num}...")
        
        page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
        page.wait_for_timeout(1000)
        
        certs = extract_from_table(page)
        
        if not certs:
            log(f"No certs on page {page_num}")
            break
        
        existing = {c['isin'] for c in all_certs}
        new_certs = [c for c in certs if c['isin'] not in existing]
        all_certs.extend(new_certs)
        
        log(f"  Found {len(certs)}, {len(new_certs)} new, total: {len(all_certs)}")
        
        if len(all_certs) >= CONFIG['max_certificates']:
            break
        
        # Next page
        try:
            next_clicked = False
            for sel in ['a:has-text("»")', 'button:has-text("»")', 'a:has-text("Next")']:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible():
                        btn.click()
                        next_clicked = True
                        page.wait_for_timeout(3000)
                        break
                except:
                    continue
            
            if not next_clicked:
                try:
                    page.locator(f'a:has-text("{page_num + 1}")').first.click()
                    next_clicked = True
                    page.wait_for_timeout(3000)
                except:
                    pass
            
            if not next_clicked:
                log("No more pages")
                break
        except:
            break
    
    return all_certs


def scrape_cedlab():
    log("=" * 70)
    log("CED LAB PRO SCRAPER v1.7")
    log("Scrapes all, then filters out single stocks")
    log("Copyright (c) 2024-2025 Mutna S.R.L.S.")
    log("=" * 70)
    
    username, password = get_credentials()
    log(f"Username: {username[:3]}***")
    
    all_certificates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        context = browser.new_context(viewport={'width': 1920, 'height': 1080})
        page = context.new_page()
        
        if not navigate_to_search(page, username, password):
            browser.close()
            sys.exit(1)
        
        all_certificates = scrape_all_pages(page)
        browser.close()
    
    log(f"\n📊 Total scraped: {len(all_certificates)}")
    
    # Filter out single stocks
    log("Filtering out single stocks...")
    filtered = [c for c in all_certificates if not is_single_stock(c)]
    removed = len(all_certificates) - len(filtered)
    log(f"  Removed {removed} single stock certificates")
    log(f"  Kept {len(filtered)} certificates")
    
    # Stats
    by_category = {}
    by_issuer = {}
    by_type = {}
    
    for c in filtered:
        cat = c.get('underlying_category', 'other')
        by_category[cat] = by_category.get(cat, 0) + 1
        iss = c.get('issuer', 'Unknown')
        by_issuer[iss] = by_issuer.get(iss, 0) + 1
        t = c.get('type', 'Certificate')
        by_type[t] = by_type.get(t, 0) + 1
    
    log(f"\nBy category: {by_category}")
    log(f"By type: {dict(list(by_type.items())[:8])}")
    log(f"Top issuers: {dict(list(sorted(by_issuer.items(), key=lambda x: -x[1])[:5]))}")
    
    # Save
    os.makedirs('data', exist_ok=True)
    
    output = {
        'metadata': {
            'version': '1.7-cedlab',
            'timestamp': datetime.now().isoformat(),
            'source': 'cedlabpro.it',
            'total': len(filtered),
            'total_before_filter': len(all_certificates),
            'filtered_out': removed,
            'categories': by_category,
            'types': by_type,
            'issuers': list(by_issuer.keys())
        },
        'certificates': filtered
    }
    
    with open(CONFIG['output_path'], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    log(f"\n💾 Saved {len(filtered)} certificates (filtered from {len(all_certificates)})")
    
    return output


if __name__ == '__main__':
    try:
        scrape_cedlab()
    except Exception as e:
        log(f"❌ Failed: {e}", 'ERROR')
        import traceback
        traceback.print_exc()
        sys.exit(1)
