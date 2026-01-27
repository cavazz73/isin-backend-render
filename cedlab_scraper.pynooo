#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

CED Lab Pro Certificates Scraper v1.6
With filters: Indici, Commodity, Tassi, Valute, Credit Linked (NO single stocks)

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
    'base_url': 'https://cedlabpro.it',
    'search_url': 'https://cedlabpro.it/menu/ricerca-avanzata',
    'certificate_url': 'https://cedlabpro.it/menu/scheda-certificato?isin=',
    'max_certificates': 500,
    'max_pages': 25,
    'timeout': 90000,
    'output_path': 'data/certificates-data.json'
}

# Underlying types to SELECT (no single stocks/azioni)
WANTED_UNDERLYING_TYPES = [
    'indice', 'indici', 'index',
    'commodity', 'commodities', 'materie prime',
    'tasso', 'tassi', 'rate', 'interest',
    'valuta', 'valute', 'currency', 'forex',
    'credit', 'credit linked', 'credito',
    'obbligazione', 'bond', 'btp',
    'etf', 'etc'
]


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


def parse_date(text):
    if not text or text.strip() in ['', '-', '--', 'N/A']:
        return None
    text = text.strip()
    match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{2})$', text)
    if match:
        d, m, y = match.groups()
        year = 2000 + int(y) if int(y) < 50 else 1900 + int(y)
        return f"{year}-{m.zfill(2)}-{d.zfill(2)}"
    match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', text)
    if match:
        d, m, y = match.groups()
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return text


def perform_login(page, username, password):
    """Perform login on the login page"""
    log("Performing login...")
    page.wait_for_timeout(2000)
    
    try:
        page.locator('input[placeholder="Username"], input[type="text"]').first.fill(username)
        log("✅ Filled username")
    except Exception as e:
        log(f"Failed to fill username: {e}", 'ERROR')
        return False
    
    try:
        page.locator('input[placeholder="Password"], input[type="password"]').first.fill(password)
        log("✅ Filled password")
    except Exception as e:
        log(f"Failed to fill password: {e}", 'ERROR')
        return False
    
    page.wait_for_timeout(500)
    
    try:
        page.locator('button:has-text("Login")').first.click()
        log("✅ Clicked Login button")
    except Exception as e:
        log(f"Failed to click login: {e}", 'ERROR')
        return False
    
    page.wait_for_timeout(5000)
    log(f"After login URL: {page.url}")
    return True


def navigate_to_search(page, username, password):
    """Navigate to search page, handling login redirect"""
    log("Navigating to Ricerca Avanzata...")
    
    page.goto(CONFIG['search_url'], timeout=CONFIG['timeout'], wait_until='networkidle')
    page.wait_for_timeout(3000)
    
    current_url = page.url
    log(f"Current URL: {current_url}")
    
    if 'login' in current_url.lower() or 'identity' in current_url.lower():
        log("Redirected to login page")
        page.screenshot(path='login_page.png')
        
        if not perform_login(page, username, password):
            return False
        
        log("Navigating back to Ricerca Avanzata...")
        page.goto(CONFIG['search_url'], timeout=CONFIG['timeout'], wait_until='networkidle')
        page.wait_for_timeout(3000)
        
        if 'login' in page.url.lower():
            log("Still on login page!", 'ERROR')
            return False
    
    log("✅ On search page")
    return True


def apply_filters(page):
    """Apply filters to exclude single stocks and select wanted underlying types"""
    log("Applying filters...")
    
    # Scroll to find the filter section
    for _ in range(3):
        page.evaluate('window.scrollBy(0, 300)')
        page.wait_for_timeout(300)
    
    # Take screenshot before filters
    page.screenshot(path='filters_before.png')
    
    # Find and click the "Tipo sottostante" dropdown
    log("Looking for 'Tipo sottostante' dropdown...")
    
    try:
        # Try to find the dropdown by label or placeholder
        dropdown_selectors = [
            'select:near(:text("Tipo sottostante"))',
            '[placeholder*="Tipo sottostante"]',
            'select[name*="tipo"]',
            'select[id*="tipo"]',
            '.tipo-sottostante select',
            'text=Tipo sottostante >> .. >> select',
        ]
        
        dropdown_found = False
        
        # Method 1: Look for select element near "Tipo sottostante" text
        try:
            # First scroll to make the dropdown visible
            tipo_label = page.locator('text=Tipo sottostante').first
            if tipo_label.is_visible():
                tipo_label.scroll_into_view_if_needed()
                page.wait_for_timeout(500)
                
                # The dropdown is likely a sibling or nearby element
                # Try clicking on the dropdown area
                dropdown = page.locator('select').filter(has_text='').nth(1)  # Second select might be Tipo sottostante
                
                # Alternative: find by position relative to label
                parent = tipo_label.locator('xpath=../..')
                select_in_parent = parent.locator('select')
                if select_in_parent.count() > 0:
                    select_in_parent.first.click()
                    dropdown_found = True
                    log("Found Tipo sottostante dropdown via parent")
        except Exception as e:
            log(f"Method 1 failed: {str(e)[:50]}")
        
        # Method 2: Try all selects and find the right one
        if not dropdown_found:
            try:
                selects = page.locator('select').all()
                log(f"Found {len(selects)} select elements")
                
                for i, sel in enumerate(selects):
                    try:
                        # Check if this select is near "Tipo sottostante"
                        bbox = sel.bounding_box()
                        if bbox:
                            # Click to open and see options
                            sel.click()
                            page.wait_for_timeout(500)
                            
                            # Check options
                            options = sel.locator('option').all()
                            option_texts = [opt.text_content() for opt in options[:5]]
                            log(f"Select {i} options: {option_texts}")
                            
                            # Check if this looks like the tipo sottostante dropdown
                            if any('indic' in str(t).lower() or 'commod' in str(t).lower() or 'azione' in str(t).lower() for t in option_texts):
                                log(f"✅ Found Tipo sottostante dropdown (select {i})")
                                dropdown_found = True
                                
                                # Select wanted options
                                for opt in options:
                                    opt_text = opt.text_content().lower()
                                    
                                    # Check if this is a wanted type
                                    is_wanted = any(wanted in opt_text for wanted in WANTED_UNDERLYING_TYPES)
                                    is_stock = 'azione' in opt_text or 'azioni' in opt_text or 'stock' in opt_text
                                    
                                    if is_wanted and not is_stock:
                                        opt_value = opt.get_attribute('value')
                                        if opt_value:
                                            sel.select_option(value=opt_value)
                                            log(f"   Selected: {opt_text}")
                                
                                break
                            else:
                                # Close this dropdown
                                page.keyboard.press('Escape')
                    except:
                        continue
            except Exception as e:
                log(f"Method 2 failed: {str(e)[:50]}")
        
        # Method 3: Try clicking on dropdown container/wrapper
        if not dropdown_found:
            try:
                # Many UI frameworks use custom dropdowns
                dropdown_wrappers = page.locator('[class*="dropdown"], [class*="select"], [class*="tipo"]').all()
                log(f"Found {len(dropdown_wrappers)} dropdown wrappers")
                
                for wrapper in dropdown_wrappers:
                    try:
                        text = wrapper.text_content()
                        if 'tipo' in text.lower() and 'sottostante' in text.lower():
                            wrapper.click()
                            page.wait_for_timeout(1000)
                            log("Clicked on Tipo sottostante wrapper")
                            
                            # Take screenshot to see options
                            page.screenshot(path='dropdown_open.png')
                            
                            # Try to find and click options
                            for wanted in ['Indice', 'Indici', 'Commodity', 'Tasso', 'Valuta', 'Credit']:
                                try:
                                    option = page.locator(f'text="{wanted}"').first
                                    if option.is_visible():
                                        option.click()
                                        log(f"   Selected: {wanted}")
                                        page.wait_for_timeout(300)
                                except:
                                    pass
                            
                            dropdown_found = True
                            break
                    except:
                        continue
            except Exception as e:
                log(f"Method 3 failed: {str(e)[:50]}")
        
        if not dropdown_found:
            log("⚠️ Could not find Tipo sottostante dropdown - will search all types", 'WARN')
        
    except Exception as e:
        log(f"Filter error: {str(e)[:100]}", 'WARN')
    
    # Take screenshot after filters
    page.screenshot(path='filters_after.png')
    page.wait_for_timeout(1000)
    
    return True


def extract_from_table(page):
    """Extract certificates directly from the search results table"""
    certificates = []
    
    html = page.content()
    soup = BeautifulSoup(html, 'html.parser')
    
    tables = soup.find_all('table')
    
    for table in tables:
        rows = table.find_all('tr')
        
        if len(rows) < 2:
            continue
        
        header_row = rows[0]
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all(['th', 'td'])]
        
        if not any(h in headers for h in ['isin', 'nome', 'sottostante', 'emittente']):
            continue
        
        log(f"Found certificate table with {len(rows)-1} rows")
        
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) < 4:
                continue
            
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            cert = {
                'scraped': True,
                'timestamp': datetime.now().isoformat(),
                'currency': 'EUR'
            }
            
            for i, header in enumerate(headers):
                if i >= len(cell_texts):
                    break
                
                value = cell_texts[i]
                
                if 'isin' in header:
                    link = cells[i].find('a')
                    cert['isin'] = link.get_text(strip=True) if link else value
                elif 'nome' in header:
                    cert['name'] = value
                elif header == 'sottostante' or header == 'sottostanti':
                    cert['underlying_name'] = value
                elif 'direzione' in header:
                    cert['direction'] = value
                elif 'emittente' in header:
                    cert['issuer'] = value
                elif 'worst' in header:
                    cert['worst_of'] = value.lower() in ['si', 'sì', 'yes', 's']
                elif 'basket' in header and 'sottostanti' in header:
                    if value:
                        cert['underlyings'] = [{'name': u.strip()} for u in value.split('\n') if u.strip()]
                elif 'bid' in header or 'denaro' in header:
                    cert['bid_price'] = parse_number(value)
                elif 'ask' in header or 'lettera' in header:
                    cert['ask_price'] = parse_number(value)
            
            if cert.get('isin') and re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', cert['isin']):
                if cert.get('bid_price') and cert.get('ask_price'):
                    cert['price'] = (cert['bid_price'] + cert['ask_price']) / 2
                
                cert['underlying_category'] = categorize_underlying(cert)
                
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


def categorize_underlying(cert):
    """Categorize certificate based on underlying"""
    text = (cert.get('underlying_name', '') + ' ' + cert.get('name', '')).lower()
    
    if any(kw in text for kw in ['indice', 'index', 'stoxx', 'ftse', 's&p', 'nasdaq', 'dax', 'mib', 'nikkei', 'cac', 'ibex']):
        return 'index'
    if any(kw in text for kw in ['oro', 'gold', 'silver', 'argento', 'oil', 'petrolio', 'wti', 'brent', 'gas', 'commodity', 'copper', 'palladium', 'natural']):
        return 'commodity'
    if any(kw in text for kw in ['eur/usd', 'usd/', '/usd', 'forex', 'currency', '/eur', 'gbp', 'jpy', 'chf']):
        return 'currency'
    if any(kw in text for kw in ['btp', 'bund', 'tasso', 'rate', 'euribor', 'treasury', 'bond', 'interest']):
        return 'rate'
    if any(kw in text for kw in ['credit', 'cln', 'credito']):
        return 'credit_linked'
    
    return 'stock'


def scrape_with_pagination(page):
    """Scrape all certificates using pagination"""
    all_certificates = []
    page_num = 0
    
    page.screenshot(path='search_page_before.png')
    
    # Apply filters first
    apply_filters(page)
    
    # Scroll to find Cerca button
    log("Scrolling to find Cerca button...")
    for _ in range(10):
        page.evaluate('window.scrollBy(0, 300)')
        page.wait_for_timeout(200)
    
    page.screenshot(path='search_page_scrolled.png')
    
    # Click Cerca
    log("Clicking Cerca...")
    try:
        cerca_btn = page.locator('button:has-text("Cerca")').first
        cerca_btn.scroll_into_view_if_needed()
        page.wait_for_timeout(500)
        cerca_btn.click()
        log("✅ Clicked Cerca")
    except Exception as e:
        log(f"Could not click Cerca: {e}", 'WARN')
    
    # Wait for results
    log("Waiting for results...")
    page.wait_for_timeout(10000)
    
    # Scroll to see results
    for _ in range(5):
        page.evaluate('window.scrollBy(0, 500)')
        page.wait_for_timeout(300)
    
    page.screenshot(path='search_page_after.png', full_page=True)
    
    # Extract from each page
    while page_num < CONFIG['max_pages'] and len(all_certificates) < CONFIG['max_certificates']:
        page_num += 1
        log(f"Processing page {page_num}...")
        
        page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
        page.wait_for_timeout(1000)
        
        certs = extract_from_table(page)
        
        if not certs:
            log(f"No certificates on page {page_num}")
            break
        
        existing_isins = {c['isin'] for c in all_certificates}
        new_certs = [c for c in certs if c['isin'] not in existing_isins]
        all_certificates.extend(new_certs)
        
        log(f"Page {page_num}: {len(certs)} found, {len(new_certs)} new, total: {len(all_certificates)}")
        
        if len(all_certificates) >= CONFIG['max_certificates']:
            break
        
        # Try next page
        try:
            next_clicked = False
            
            for selector in ['a:has-text("»")', 'button:has-text("»")', '.pagination-next', 'a:has-text("Next")']:
                try:
                    next_btn = page.locator(selector).first
                    if next_btn.is_visible():
                        next_btn.click()
                        next_clicked = True
                        log("Clicked next page")
                        page.wait_for_timeout(3000)
                        break
                except:
                    continue
            
            if not next_clicked:
                try:
                    next_page_num = page_num + 1
                    page.locator(f'a:has-text("{next_page_num}")').first.click()
                    next_clicked = True
                    log(f"Clicked page {next_page_num}")
                    page.wait_for_timeout(3000)
                except:
                    pass
            
            if not next_clicked:
                log("No more pages")
                break
                
        except Exception as e:
            log(f"Pagination error: {e}")
            break
    
    return all_certificates


def scrape_cedlab():
    log("=" * 70)
    log("CED LAB PRO SCRAPER v1.6")
    log("Filters: Indici, Commodity, Tassi, Valute, Credit (NO azioni)")
    log("Copyright (c) 2024-2025 Mutna S.R.L.S.")
    log("=" * 70)
    
    username, password = get_credentials()
    log(f"Username: {username[:3]}***")
    
    certificates = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()
        
        if not navigate_to_search(page, username, password):
            browser.close()
            sys.exit(1)
        
        certificates = scrape_with_pagination(page)
        
        browser.close()
    
    # Summary
    log("\n" + "=" * 70)
    log(f"📊 SUMMARY: Scraped {len(certificates)} certificates")
    
    by_category = {}
    by_issuer = {}
    by_type = {}
    
    for c in certificates:
        cat = c.get('underlying_category', 'other')
        by_category[cat] = by_category.get(cat, 0) + 1
        
        iss = c.get('issuer', 'Unknown')
        by_issuer[iss] = by_issuer.get(iss, 0) + 1
        
        t = c.get('type', 'Certificate')
        by_type[t] = by_type.get(t, 0) + 1
    
    log(f"\nBy category: {by_category}")
    log(f"By type: {dict(list(by_type.items())[:10])}")
    
    # Save
    os.makedirs('data', exist_ok=True)
    
    output = {
        'metadata': {
            'version': '1.6-cedlab',
            'timestamp': datetime.now().isoformat(),
            'source': 'cedlabpro.it',
            'total': len(certificates),
            'categories': by_category,
            'types': by_type,
            'issuers': list(by_issuer.keys())
        },
        'certificates': certificates
    }
    
    with open(CONFIG['output_path'], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    log(f"\n💾 Saved {len(certificates)} certificates")
    
    return output


if __name__ == '__main__':
    try:
        scrape_cedlab()
    except Exception as e:
        log(f"❌ Failed: {e}", 'ERROR')
        import traceback
        traceback.print_exc()
        sys.exit(1)
