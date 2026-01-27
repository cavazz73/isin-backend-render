#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

CED Lab Pro Certificates Scraper v1.4
Added scroll support to find Cerca button and results table

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
    'max_certificates': 200,
    'timeout': 90000,
    'output_path': 'data/certificates-data.json'
}


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
    """Perform actual login on the login page"""
    log("Performing login...")
    
    page.wait_for_timeout(2000)
    
    try:
        username_field = page.locator('input[placeholder="Username"], input[name="Username"], #Username, input[type="text"]').first
        username_field.fill(username)
        log("✅ Filled username")
    except Exception as e:
        log(f"Failed to fill username: {e}", 'ERROR')
        return False
    
    try:
        password_field = page.locator('input[placeholder="Password"], input[name="Password"], #Password, input[type="password"]').first
        password_field.fill(password)
        log("✅ Filled password")
    except Exception as e:
        log(f"Failed to fill password: {e}", 'ERROR')
        return False
    
    page.wait_for_timeout(500)
    
    try:
        login_btn = page.locator('button:has-text("Login"), button[type="submit"]').first
        login_btn.click()
        log("✅ Clicked Login button")
    except Exception as e:
        log(f"Failed to click login: {e}", 'ERROR')
        return False
    
    page.wait_for_timeout(5000)
    
    new_url = page.url
    log(f"After login URL: {new_url}")
    
    return True


def navigate_to_search(page, username, password):
    """Navigate to search page, handling login redirect"""
    log("Navigating to Ricerca Avanzata...")
    
    page.goto(CONFIG['search_url'], timeout=CONFIG['timeout'], wait_until='networkidle')
    page.wait_for_timeout(3000)
    
    current_url = page.url
    log(f"Current URL: {current_url}")
    
    if 'login' in current_url.lower() or 'identity' in current_url.lower() or 'account' in current_url.lower():
        log("Redirected to login page - need to authenticate")
        page.screenshot(path='login_page.png')
        
        if not perform_login(page, username, password):
            return False
        
        log("Navigating back to Ricerca Avanzata after login...")
        page.goto(CONFIG['search_url'], timeout=CONFIG['timeout'], wait_until='networkidle')
        page.wait_for_timeout(3000)
        
        current_url = page.url
        log(f"URL after re-navigation: {current_url}")
        
        if 'login' in current_url.lower() or 'identity' in current_url.lower():
            log("Still being redirected to login!", 'ERROR')
            page.screenshot(path='login_failed.png')
            return False
    
    log("✅ Successfully on search page")
    return True


def extract_isins_from_search(page):
    """Scroll, click Cerca, and extract ISINs from results"""
    
    # Take screenshot of initial page
    page.screenshot(path='search_page_before.png')
    log("Screenshot: search_page_before.png")
    
    # Scroll down to find the Cerca button
    log("Scrolling to find Cerca button...")
    
    for i in range(5):
        page.evaluate('window.scrollBy(0, 500)')
        page.wait_for_timeout(500)
    
    # Take screenshot after scroll
    page.screenshot(path='search_page_scrolled.png')
    log("Screenshot: search_page_scrolled.png")
    
    # Try to find and click Cerca button
    log("Looking for Cerca button...")
    
    cerca_clicked = False
    
    # Method 1: By exact text
    try:
        cerca_btn = page.locator('button:has-text("Cerca")').first
        if cerca_btn.is_visible():
            cerca_btn.scroll_into_view_if_needed()
            page.wait_for_timeout(500)
            cerca_btn.click()
            cerca_clicked = True
            log("✅ Clicked 'Cerca' button (method 1)")
    except Exception as e:
        log(f"Method 1 failed: {str(e)[:50]}")
    
    # Method 2: By class
    if not cerca_clicked:
        try:
            cerca_btn = page.locator('.btn-primary:has-text("Cerca"), .btn-warning:has-text("Cerca")').first
            if cerca_btn.is_visible():
                cerca_btn.click()
                cerca_clicked = True
                log("✅ Clicked 'Cerca' button (method 2)")
        except Exception as e:
            log(f"Method 2 failed: {str(e)[:50]}")
    
    # Method 3: Find all buttons and click the one with "Cerca"
    if not cerca_clicked:
        try:
            buttons = page.query_selector_all('button')
            for btn in buttons:
                try:
                    text = btn.text_content() or ''
                    if 'cerca' in text.lower():
                        btn.scroll_into_view_if_needed()
                        page.wait_for_timeout(300)
                        btn.click()
                        cerca_clicked = True
                        log("✅ Clicked 'Cerca' button (method 3)")
                        break
                except:
                    continue
        except Exception as e:
            log(f"Method 3 failed: {str(e)[:50]}")
    
    # Method 4: Look for any element with "Cerca" text that's clickable
    if not cerca_clicked:
        try:
            page.locator('text=Cerca').first.click()
            cerca_clicked = True
            log("✅ Clicked 'Cerca' (method 4 - text locator)")
        except Exception as e:
            log(f"Method 4 failed: {str(e)[:50]}")
    
    if not cerca_clicked:
        log("⚠️ Could not click Cerca button!", 'WARN')
    
    # Wait for results to load
    log("Waiting for results...")
    page.wait_for_timeout(10000)
    
    # Scroll down to see results table
    log("Scrolling to see results...")
    for i in range(10):
        page.evaluate('window.scrollBy(0, 300)')
        page.wait_for_timeout(300)
    
    # Take screenshot after search
    page.screenshot(path='search_page_after.png', full_page=True)
    log("Screenshot: search_page_after.png (full page)")
    
    # Extract ISINs
    log("Extracting ISINs from page...")
    
    html = page.content()
    soup = BeautifulSoup(html, 'html.parser')
    
    isins = []
    isin_pattern = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')
    
    # Method 1: Look in table rows
    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            for cell in cells:
                text = cell.get_text(strip=True)
                if isin_pattern.match(text) and text not in isins:
                    isins.append(text)
                    
                # Also check links inside cells
                for link in cell.find_all('a'):
                    link_text = link.get_text(strip=True)
                    if isin_pattern.match(link_text) and link_text not in isins:
                        isins.append(link_text)
    
    log(f"Found {len(isins)} ISINs from tables")
    
    # Method 2: Look in all links
    for link in soup.find_all('a', href=True):
        text = link.get_text(strip=True)
        if isin_pattern.match(text) and text not in isins:
            isins.append(text)
        
        href = link.get('href', '')
        if 'isin=' in href.lower():
            match = re.search(r'isin=([A-Z]{2}[A-Z0-9]{10})', href, re.IGNORECASE)
            if match:
                isin = match.group(1).upper()
                if isin not in isins:
                    isins.append(isin)
    
    log(f"Found {len(isins)} ISINs after checking links")
    
    # Method 3: Search all text
    if len(isins) < 5:
        all_text = soup.get_text()
        found = re.findall(r'\b([A-Z]{2}[A-Z0-9]{10})\b', all_text)
        for isin in found:
            if isin not in isins:
                isins.append(isin)
        log(f"Found {len(isins)} ISINs after text search")
    
    # Debug if no ISINs
    if len(isins) == 0:
        log("⚠️ No ISINs found! Debug info:", 'WARN')
        log(f"  Page title: {soup.title.string if soup.title else 'N/A'}")
        log(f"  Tables found: {len(soup.find_all('table'))}")
        log(f"  Links found: {len(soup.find_all('a'))}")
        
        # Check page content
        page_text = soup.get_text()[:1000]
        log(f"  Page preview: {page_text[:200]}...")
        
        # Save full HTML for debugging
        with open('debug_page.html', 'w') as f:
            f.write(html)
        log("  Saved full HTML to debug_page.html")
    
    return isins[:CONFIG['max_certificates']]


def extract_certificate_details(page, isin):
    """Extract full details for a certificate"""
    cert = {
        'isin': isin,
        'scraped': True,
        'timestamp': datetime.now().isoformat(),
        'currency': 'EUR'
    }
    
    try:
        url = f"{CONFIG['certificate_url']}{isin}"
        page.goto(url, timeout=CONFIG['timeout'], wait_until='networkidle')
        page.wait_for_timeout(2000)
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text()
        
        # Extract fields
        patterns = {
            'name': r'Nome[:\s]+([A-Z][A-Z\s]+(?:MEMORY|COLLECT|EXPRESS|BONUS|PHOENIX|CALLABLE|COUPON)[A-Z\s]*)',
            'issuer': r'Emittente[:\s]+([A-Za-z\s]+?)(?:\n|Fase|Data)',
            'bid_price': r'Prezzo Denaro[:\s]+([\d.,]+)\s*€',
            'ask_price': r'Prezzo Lettera[:\s]+([\d.,]+)\s*€',
            'reference_price': r'Prezzo di Riferimento[:\s]+([\d.,]+)\s*€',
            'issue_date': r'Data Emissione[:\s]+(\d{2}/\d{2}/\d{2,4})',
            'maturity_date': r'Data Scadenza[:\s]+(\d{2}/\d{2}/\d{2,4})',
            'market': r'Mercato[:\s]+([A-Z\-X]+)',
            'barrier_down': r'Barriera Down[:\s]+([\d.,]+)\s*%',
            'coupon': r'Premio[:\s]+([\d.,]+)\s*%',
            'barrier_type': r'Tipo Barriera[:\s]+([A-Z]+)',
            'annual_coupon_yield': r'Rendimento Cedolare\s*Annuo[:\s]*([\d.,]+)\s*%',
            'effective_annual_yield': r'Rendimento Effettivo\s*Annuo[:\s]*([\d.,]+)\s*%',
            'buffer_from_barrier': r'Buffer.*Barriera[:\s]*([\d.,]+)\s*%',
        }
        
        for field, pattern in patterns.items():
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                if field in ['bid_price', 'ask_price', 'reference_price', 'barrier_down', 'coupon', 
                            'annual_coupon_yield', 'effective_annual_yield', 'buffer_from_barrier']:
                    cert[field] = parse_number(value)
                elif field in ['issue_date', 'maturity_date']:
                    cert[field] = parse_date(value)
                else:
                    cert[field] = value
        
        # Extract underlyings
        underlyings = []
        for table in soup.find_all('table'):
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            if any(h in headers for h in ['sottostante', 'strike', 'spot']):
                for row in table.find_all('tr')[1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        underlying = {
                            'name': cells[0].get_text(strip=True),
                            'strike': parse_number(cells[1].get_text(strip=True)),
                            'spot': parse_number(cells[2].get_text(strip=True)),
                            'barrier': parse_number(cells[3].get_text(strip=True)),
                            'worst_of': 'W' in row.get_text()
                        }
                        if underlying['name']:
                            underlyings.append(underlying)
        
        cert['underlyings'] = underlyings
        if underlyings:
            cert['underlying_name'] = ', '.join([u['name'] for u in underlyings[:3]])
        
        # Type detection
        name = cert.get('name', '').lower()
        cert['type'] = 'Certificate'
        for pattern, type_name in [
            ('phoenix memory', 'Phoenix Memory'),
            ('cash collect memory', 'Cash Collect Memory'),
            ('cash collect', 'Cash Collect'),
            ('express', 'Express'),
            ('bonus', 'Bonus'),
        ]:
            if pattern in name:
                cert['type'] = type_name
                break
        
        if cert.get('bid_price') and cert.get('ask_price'):
            cert['price'] = (cert['bid_price'] + cert['ask_price']) / 2
        
    except Exception as e:
        cert['error'] = str(e)[:100]
    
    return cert


def categorize_underlying(cert):
    text = (cert.get('underlying_name', '') + ' ' + cert.get('name', '')).lower()
    if any(kw in text for kw in ['indice', 'index', 'stoxx', 'ftse', 's&p', 'nasdaq', 'dax', 'mib']):
        return 'index'
    if any(kw in text for kw in ['oro', 'gold', 'oil', 'petrolio', 'wti', 'brent']):
        return 'commodity'
    if any(kw in text for kw in ['eur/usd', 'usd/', 'forex']):
        return 'currency'
    if any(kw in text for kw in ['btp', 'bund', 'tasso', 'rate']):
        return 'rate'
    return 'other'


def scrape_cedlab():
    log("=" * 70)
    log("CED LAB PRO SCRAPER v1.4")
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
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        if not navigate_to_search(page, username, password):
            log("Failed to access search page!", 'ERROR')
            browser.close()
            sys.exit(1)
        
        isins = extract_isins_from_search(page)
        
        log(f"\n📋 Found {len(isins)} certificates to scrape")
        
        if isins:
            for i, isin in enumerate(isins):
                try:
                    cert = extract_certificate_details(page, isin)
                    cert['underlying_category'] = categorize_underlying(cert)
                    certificates.append(cert)
                    
                    if (i + 1) % 20 == 0:
                        log(f"   Progress: {i + 1}/{len(isins)}")
                except Exception as e:
                    log(f"   Error on {isin}: {str(e)[:40]}", 'WARN')
        
        browser.close()
    
    log("\n" + "=" * 70)
    log(f"📊 SUMMARY: Scraped {len(certificates)} certificates")
    
    os.makedirs('data', exist_ok=True)
    
    output = {
        'metadata': {
            'version': '1.4-cedlab',
            'timestamp': datetime.now().isoformat(),
            'source': 'cedlabpro.it',
            'total': len(certificates)
        },
        'certificates': certificates
    }
    
    with open(CONFIG['output_path'], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    log(f"💾 Saved {len(certificates)} certificates")
    
    return output


if __name__ == '__main__':
    try:
        scrape_cedlab()
    except Exception as e:
        log(f"❌ Failed: {e}", 'ERROR')
        import traceback
        traceback.print_exc()
        sys.exit(1)
