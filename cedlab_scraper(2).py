#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

CED Lab Pro Certificates Scraper v1.2
Improved search and ISIN extraction

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


def login_if_needed(page, username, password):
    """Login only if we're on login page"""
    log("Navigating to CED Lab Pro...")
    
    page.goto(CONFIG['base_url'], timeout=CONFIG['timeout'], wait_until='networkidle')
    page.wait_for_timeout(3000)
    
    current_url = page.url
    log(f"Current URL: {current_url}")
    
    if 'identity' in current_url or 'login' in current_url.lower():
        log("Login required, filling credentials...")
        
        page.wait_for_timeout(2000)
        
        # Fill username
        try:
            page.fill('#Username', username)
            log("Filled username")
        except:
            page.fill('input[type="text"]', username)
        
        # Fill password
        try:
            page.fill('#Password', password)
            log("Filled password")
        except:
            page.fill('input[type="password"]', password)
        
        # Click login
        page.click('button[type="submit"]')
        page.wait_for_timeout(5000)
        
        log(f"After login URL: {page.url}")
    
    return True


def extract_isins_from_search(page):
    """Navigate to search, click Cerca, and extract ISINs"""
    log("Going to Ricerca Avanzata...")
    
    page.goto(CONFIG['search_url'], timeout=CONFIG['timeout'], wait_until='networkidle')
    page.wait_for_timeout(3000)
    
    log(f"Search page URL: {page.url}")
    
    # Take screenshot for debugging
    try:
        page.screenshot(path='search_page_before.png')
        log("Screenshot saved: search_page_before.png")
    except:
        pass
    
    # Click "Cerca" button - try multiple approaches
    log("Looking for Cerca button...")
    
    cerca_clicked = False
    
    # Method 1: By text content
    try:
        cerca_btn = page.locator('button:has-text("Cerca")').first
        if cerca_btn.is_visible():
            cerca_btn.click()
            cerca_clicked = True
            log("Clicked 'Cerca' button (by text)")
    except Exception as e:
        log(f"Method 1 failed: {str(e)[:50]}")
    
    # Method 2: By class (often btn-primary or similar)
    if not cerca_clicked:
        try:
            cerca_btn = page.locator('.btn-primary:has-text("Cerca"), .btn-search').first
            if cerca_btn.is_visible():
                cerca_btn.click()
                cerca_clicked = True
                log("Clicked 'Cerca' button (by class)")
        except Exception as e:
            log(f"Method 2 failed: {str(e)[:50]}")
    
    # Method 3: Look for any button with Cerca
    if not cerca_clicked:
        try:
            buttons = page.query_selector_all('button')
            for btn in buttons:
                if 'cerca' in btn.text_content().lower():
                    btn.click()
                    cerca_clicked = True
                    log("Clicked 'Cerca' button (by iteration)")
                    break
        except Exception as e:
            log(f"Method 3 failed: {str(e)[:50]}")
    
    # Method 4: Click by coordinates (last resort based on screenshot)
    if not cerca_clicked:
        try:
            # The Cerca button appears to be at bottom right of filter section
            page.locator('text=Cerca').click()
            cerca_clicked = True
            log("Clicked 'Cerca' (by text locator)")
        except Exception as e:
            log(f"Method 4 failed: {str(e)[:50]}")
    
    if cerca_clicked:
        log("Waiting for results to load...")
        page.wait_for_timeout(8000)  # Wait longer for results
    else:
        log("Could not click Cerca button!", 'WARN')
    
    # Take screenshot after search
    try:
        page.screenshot(path='search_page_after.png')
        log("Screenshot saved: search_page_after.png")
    except:
        pass
    
    # Now extract ISINs
    log("Extracting ISINs from results...")
    
    html = page.content()
    soup = BeautifulSoup(html, 'html.parser')
    
    isins = []
    isin_pattern = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')
    
    # Method 1: Look for ISINs in table cells (first column typically)
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if cells:
                first_cell_text = cells[0].get_text(strip=True)
                if isin_pattern.match(first_cell_text):
                    if first_cell_text not in isins:
                        isins.append(first_cell_text)
    
    log(f"Found {len(isins)} ISINs from tables")
    
    # Method 2: Look in links
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        text = link.get_text(strip=True)
        
        # Check if link text is ISIN
        if isin_pattern.match(text):
            if text not in isins:
                isins.append(text)
        
        # Check if href contains ISIN
        if 'isin=' in href.lower():
            match = re.search(r'isin=([A-Z]{2}[A-Z0-9]{10})', href, re.IGNORECASE)
            if match:
                isin = match.group(1).upper()
                if isin not in isins:
                    isins.append(isin)
        
        # Check href for ISIN pattern
        match = re.search(r'/([A-Z]{2}[A-Z0-9]{10})(?:\?|$|/)', href)
        if match:
            isin = match.group(1)
            if isin not in isins:
                isins.append(isin)
    
    log(f"Found {len(isins)} ISINs total after checking links")
    
    # Method 3: Search entire page text for ISIN patterns
    if len(isins) < 10:
        all_text = soup.get_text()
        found_in_text = re.findall(r'\b([A-Z]{2}[A-Z0-9]{10})\b', all_text)
        for isin in found_in_text:
            if isin not in isins:
                isins.append(isin)
        log(f"Found {len(isins)} ISINs after text search")
    
    # Method 4: Look for elements with specific classes (from screenshot, ISINs are orange links)
    for elem in soup.find_all(['a', 'span', 'div'], class_=lambda x: x and ('isin' in str(x).lower() or 'link' in str(x).lower() or 'orange' in str(x).lower())):
        text = elem.get_text(strip=True)
        if isin_pattern.match(text) and text not in isins:
            isins.append(text)
    
    # Print some page info for debugging
    if len(isins) == 0:
        log("No ISINs found! Page info for debugging:")
        log(f"  Page title: {soup.title.string if soup.title else 'N/A'}")
        log(f"  Number of tables: {len(soup.find_all('table'))}")
        log(f"  Number of links: {len(soup.find_all('a'))}")
        
        # Print first 500 chars of body text
        body_text = soup.get_text()[:500].replace('\n', ' ')
        log(f"  Page text preview: {body_text}")
    
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
        
        # Extract fields with regex
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
        
        # Price
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
    log("CED LAB PRO SCRAPER v1.2")
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
        
        # Login
        login_if_needed(page, username, password)
        
        # Extract ISINs
        isins = extract_isins_from_search(page)
        
        log(f"\n📋 Found {len(isins)} certificates to scrape")
        
        if isins:
            # Scrape each certificate
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
    
    # Summary
    log("\n" + "=" * 70)
    log(f"📊 SUMMARY: Scraped {len(certificates)} certificates")
    
    # Save
    os.makedirs('data', exist_ok=True)
    
    output = {
        'metadata': {
            'version': '1.2-cedlab',
            'timestamp': datetime.now().isoformat(),
            'source': 'cedlabpro.it',
            'total': len(certificates)
        },
        'certificates': certificates
    }
    
    with open(CONFIG['output_path'], 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    log(f"💾 Saved {len(certificates)} certificates to {CONFIG['output_path']}")
    
    # Upload screenshots as artifacts for debugging
    log("\n📸 Screenshots saved for debugging (check artifacts)")
    
    return output


if __name__ == '__main__':
    try:
        scrape_cedlab()
    except Exception as e:
        log(f"❌ Failed: {e}", 'ERROR')
        import traceback
        traceback.print_exc()
        sys.exit(1)
