#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

CED Lab Pro Certificates Scraper v1.8
More robust Cerca button handling

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

STOCK_KEYWORDS = [
    'apple', 'microsoft', 'amazon', 'google', 'meta', 'nvidia', 'tesla', 'netflix',
    'alibaba', 'alibaba group', 'tencent', 'samsung', 'intel', 'amd', 'qualcomm',
    'stellantis', 'ferrari', 'eni', 'enel', 'intesa', 'unicredit', 'generali',
    'volkswagen', 'bmw', 'daimler', 'siemens', 'basf', 'bayer', 'sap', 'adidas',
    'lvmh', 'total', 'sanofi', 'bnp', 'axa', 'loreal', 'hermes', 'kering',
    'shell', 'bp', 'hsbc', 'barclays', 'astrazeneca', 'unilever', 'vodafone',
    'pfizer', 'johnson', 'merck', 'moderna', 'biontech', 'novartis', 'roche', 'nestle',
    'nintendo', 'sony', 'toyota', 'softbank', 'ubisoft', 'electronic arts',
    'telecom italia', 'tim', 'poste italiane', 'mediobanca', 'nexi', 'pirelli', 'saipem'
]

INDEX_KEYWORDS = ['index', 'indice', 'stoxx', 'ftse', 's&p', 'nasdaq', 'dax', 'mib', 'nikkei', 'cac', 'ibex', 'hang seng', 'russell', 'dow jones', 'euro stoxx', 'msci']
COMMODITY_KEYWORDS = ['gold', 'oro', 'silver', 'oil', 'petrolio', 'wti', 'brent', 'gas', 'copper', 'platinum', 'palladium', 'commodity', 'generic 1st', 'future generic']
CURRENCY_KEYWORDS = ['eur/usd', 'usd/jpy', 'gbp/usd', 'usd/chf', 'forex', 'currency', 'cambio']
RATE_KEYWORDS = ['btp', 'bund', 'treasury', 'euribor', 'rate', 'tasso', 'bond', 'swap']


def log(msg, level='INFO'):
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] [{level}] {msg}")


def get_credentials():
    username = os.environ.get('CED_USERNAME')
    password = os.environ.get('CED_PASSWORD')
    if not username or not password:
        log("CED_USERNAME and CED_PASSWORD required!", 'ERROR')
        sys.exit(1)
    return username, password


def parse_number(text):
    if not text or text.strip() in ['', '-', '--', 'N/A']:
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
    text_lower = text.lower()
    if any(kw in text_lower for kw in INDEX_KEYWORDS):
        return 'index'
    if any(kw in text_lower for kw in COMMODITY_KEYWORDS):
        return 'commodity'
    if any(kw in text_lower for kw in CURRENCY_KEYWORDS):
        return 'currency'
    if any(kw in text_lower for kw in RATE_KEYWORDS):
        return 'rate'
    return 'stock'


def is_single_stock(cert):
    text = (cert.get('underlying_name', '') + ' ' + cert.get('name', '')).lower()
    if cert.get('underlying_category') in ['index', 'commodity', 'currency', 'rate']:
        return False
    if any(stock in text for stock in STOCK_KEYWORDS):
        return True
    return cert.get('underlying_category') == 'stock'


def perform_login(page, username, password):
    log("Performing login...")
    page.wait_for_timeout(2000)
    try:
        page.locator('input[placeholder="Username"], input[type="text"]').first.fill(username)
        page.locator('input[placeholder="Password"], input[type="password"]').first.fill(password)
        page.wait_for_timeout(500)
        page.locator('button:has-text("Login")').first.click()
        page.wait_for_timeout(5000)
        log("✅ Login done")
        return True
    except Exception as e:
        log(f"Login error: {e}", 'ERROR')
        return False


def navigate_to_search(page, username, password):
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
    
    log(f"✅ On page: {page.url}")
    return True


def click_cerca_and_wait(page):
    """Click the main Cerca button and wait for results"""
    log("Looking for Cerca button...")
    
    page.screenshot(path='before_cerca.png')
    
    # Scroll to bottom of form to see Cerca button
    log("Scrolling to bottom of form...")
    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
    page.wait_for_timeout(1000)
    
    page.screenshot(path='after_scroll.png')
    
    # Try multiple methods to click Cerca
    cerca_clicked = False
    
    # Method 1: Find the orange Cerca button (main search button)
    # It's in the bottom-right corner of the form
    try:
        # The main Cerca button is usually styled differently
        cerca_buttons = page.locator('button:has-text("Cerca")').all()
        log(f"Found {len(cerca_buttons)} Cerca buttons")
        
        for i, btn in enumerate(cerca_buttons):
            try:
                bbox = btn.bounding_box()
                if bbox:
                    log(f"  Button {i}: position ({bbox['x']:.0f}, {bbox['y']:.0f})")
                    # The main one is usually on the right side (x > 800)
                    if bbox['x'] > 800:
                        btn.click()
                        cerca_clicked = True
                        log(f"✅ Clicked Cerca button {i} (right side)")
                        break
            except:
                continue
        
        # If no right-side button found, click the last one (usually the main one)
        if not cerca_clicked and cerca_buttons:
            cerca_buttons[-1].click()
            cerca_clicked = True
            log("✅ Clicked last Cerca button")
    except Exception as e:
        log(f"Method 1 failed: {e}")
    
    # Method 2: Click by CSS class (btn-warning is orange in Bootstrap)
    if not cerca_clicked:
        try:
            page.locator('.btn-warning:has-text("Cerca"), .btn-primary:has-text("Cerca")').first.click()
            cerca_clicked = True
            log("✅ Clicked Cerca by class")
        except Exception as e:
            log(f"Method 2 failed: {e}")
    
    # Method 3: Use coordinates - the button is at bottom right
    if not cerca_clicked:
        try:
            # Click at approximate position of Cerca button
            page.mouse.click(970, 840)
            cerca_clicked = True
            log("✅ Clicked at Cerca coordinates")
        except Exception as e:
            log(f"Method 3 failed: {e}")
    
    if not cerca_clicked:
        log("⚠️ Could not click Cerca!", 'WARN')
        return False
    
    # Wait for results to load
    log("Waiting for results...")
    page.wait_for_timeout(10000)
    
    # Scroll down to see results table
    log("Scrolling to see results table...")
    for _ in range(8):
        page.evaluate('window.scrollBy(0, 400)')
        page.wait_for_timeout(500)
    
    page.screenshot(path='search_page_after.png', full_page=True)
    
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
        
        log(f"Found results table with {len(rows)-1} rows")
        
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
                    cert['worst_of'] = value.lower() in ['si', 'sì', 'yes']
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
                
                text = cert.get('underlying_name', '') + ' ' + cert.get('name', '')
                cert['underlying_category'] = categorize_underlying(text)
                
                name = cert.get('name', '').lower()
                cert['type'] = 'Certificate'
                for pattern, type_name in [
                    ('phoenix memory', 'Phoenix Memory'),
                    ('cash collect', 'Cash Collect'),
                    ('bonus plus', 'Bonus Plus'),
                    ('express', 'Express'),
                    ('mini future', 'Mini Future'),
                    ('turbo', 'Turbo'),
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
    
    # Click Cerca and wait for results
    if not click_cerca_and_wait(page):
        log("Failed to execute search", 'ERROR')
        return []
    
    # Extract from each page
    while page_num < CONFIG['max_pages'] and len(all_certs) < CONFIG['max_certificates']:
        page_num += 1
        log(f"Page {page_num}...")
        
        page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
        page.wait_for_timeout(1500)
        
        certs = extract_from_table(page)
        
        if not certs:
            log(f"No certs on page {page_num}")
            if page_num == 1:
                # Save debug info
                with open('debug_page.html', 'w') as f:
                    f.write(page.content())
                log("Saved debug_page.html")
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
            for sel in ['a:has-text("»")', 'button:has-text("»")', 'li.next a', '.pagination a:last-child']:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible():
                        btn.click()
                        next_clicked = True
                        page.wait_for_timeout(4000)
                        break
                except:
                    continue
            
            if not next_clicked:
                try:
                    page.locator(f'a:has-text("{page_num + 1}")').first.click()
                    next_clicked = True
                    page.wait_for_timeout(4000)
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
    log("CED LAB PRO SCRAPER v1.8")
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
    
    # Filter
    log("Filtering out single stocks...")
    filtered = [c for c in all_certificates if not is_single_stock(c)]
    removed = len(all_certificates) - len(filtered)
    log(f"  Removed {removed}, kept {len(filtered)}")
    
    # Stats
    by_category = {}
    for c in filtered:
        cat = c.get('underlying_category', 'other')
        by_category[cat] = by_category.get(cat, 0) + 1
    
    log(f"Categories: {by_category}")
    
    # Save
    os.makedirs('data', exist_ok=True)
    
    output = {
        'metadata': {
            'version': '1.8-cedlab',
            'timestamp': datetime.now().isoformat(),
            'source': 'cedlabpro.it',
            'total': len(filtered),
            'total_before_filter': len(all_certificates),
            'categories': by_category
        },
        'certificates': filtered
    }
    
    with open(CONFIG['output_path'], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    log(f"\n💾 Saved {len(filtered)} certificates")
    
    return output


if __name__ == '__main__':
    try:
        scrape_cedlab()
    except Exception as e:
        log(f"❌ Failed: {e}", 'ERROR')
        import traceback
        traceback.print_exc()
        sys.exit(1)
