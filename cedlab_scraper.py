#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

CED Lab Pro Certificates Scraper v1.0
Scrapes certificates data from cedlabpro.it with authentication

Environment variables required:
- CED_USERNAME: CED Lab Pro username
- CED_PASSWORD: CED Lab Pro password

Usage: python cedlab_scraper.py
"""

import json
import re
import os
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# ===================================
# CONFIGURATION
# ===================================

CONFIG = {
    'login_url': 'https://identity.cedlabpro.it/Account/Login',
    'base_url': 'https://cedlabpro.it',
    'search_url': 'https://cedlabpro.it/menu/ricerca-avanzata',
    'certificate_url': 'https://cedlabpro.it/menu/scheda-certificato?isin=',
    'max_certificates': 200,
    'timeout': 60000,
    'wait_after_login': 3000,
    'wait_between_certificates': 1500,
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


def login(page, username, password):
    log("Logging in to CED Lab Pro...")
    
    try:
        page.goto(CONFIG['login_url'], timeout=CONFIG['timeout'])
        page.wait_for_timeout(2000)
        
        page.fill('input[name="Username"], input[type="text"]', username)
        page.fill('input[name="Password"], input[type="password"]', password)
        page.click('button[type="submit"], button:has-text("Login")')
        
        page.wait_for_timeout(CONFIG['wait_after_login'])
        
        if 'login' in page.url.lower() and 'error' in page.content().lower():
            log("Login failed!", 'ERROR')
            return False
        
        log("✅ Login successful!")
        return True
        
    except Exception as e:
        log(f"Login error: {str(e)[:100]}", 'ERROR')
        return False


def extract_isin_list(page):
    log("Extracting ISIN list...")
    isins = []
    
    try:
        page.goto(CONFIG['search_url'], timeout=CONFIG['timeout'])
        page.wait_for_timeout(2000)
        
        # Click search button
        try:
            page.click('button:has-text("Cerca")', timeout=5000)
            page.wait_for_timeout(3000)
        except:
            pass
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        isin_pattern = re.compile(r'\b([A-Z]{2}[A-Z0-9]{10})\b')
        
        for element in soup.find_all(['td', 'a', 'span']):
            text = element.get_text(strip=True)
            matches = isin_pattern.findall(text)
            for isin in matches:
                if isin not in isins:
                    isins.append(isin)
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if 'isin=' in href.lower():
                match = re.search(r'isin=([A-Z]{2}[A-Z0-9]{10})', href, re.IGNORECASE)
                if match:
                    isin = match.group(1).upper()
                    if isin not in isins:
                        isins.append(isin)
        
        log(f"Found {len(isins)} ISINs")
        
        # Pagination
        page_num = 1
        while len(isins) < CONFIG['max_certificates'] and page_num < 20:
            try:
                next_btn = page.query_selector('button:has-text("Next"), button:has-text("Avanti"), a:has-text(">")')
                if not next_btn:
                    break
                
                next_btn.click()
                page.wait_for_timeout(2000)
                page_num += 1
                
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                for element in soup.find_all(['td', 'a', 'span']):
                    text = element.get_text(strip=True)
                    matches = isin_pattern.findall(text)
                    for isin in matches:
                        if isin not in isins:
                            isins.append(isin)
                
                log(f"   Page {page_num}: {len(isins)} ISINs")
            except:
                break
        
        return isins[:CONFIG['max_certificates']]
        
    except Exception as e:
        log(f"Error: {str(e)[:100]}", 'ERROR')
        return isins


def extract_certificate_details(page, isin):
    cert = {
        'isin': isin,
        'scraped': True,
        'timestamp': datetime.now().isoformat(),
        'currency': 'EUR'
    }
    
    try:
        url = f"{CONFIG['certificate_url']}{isin}"
        page.goto(url, timeout=CONFIG['timeout'])
        page.wait_for_timeout(CONFIG['wait_between_certificates'])
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text()
        
        # Extract key fields using regex patterns
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
            'emission_price': r'Prezzo Emissione[:\s]+([\d.,]+)',
            'nominal': r'Prezzo Nominale[:\s]+([\d.,]+)',
            'annual_coupon_yield': r'Rendimento Cedolare\s*Annuo[:\s]*([\d.,]+)\s*%',
            'effective_annual_yield': r'Rendimento Effettivo\s*Annuo[:\s]*([\d.,]+)\s*%',
            'buffer_from_barrier': r'Buffer.*Barriera[:\s]*([\d.,]+)\s*%',
        }
        
        for field, pattern in patterns.items():
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                if field in ['bid_price', 'ask_price', 'reference_price', 'barrier_down', 'coupon', 
                            'emission_price', 'nominal', 'annual_coupon_yield', 'effective_annual_yield', 'buffer_from_barrier']:
                    cert[field] = parse_number(value)
                elif field in ['issue_date', 'maturity_date']:
                    cert[field] = parse_date(value)
                else:
                    cert[field] = value
        
        # Extract underlyings table
        underlyings = []
        tables = soup.find_all('table')
        
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            
            if any(h in headers for h in ['sottostante', 'strike', 'spot']):
                rows = table.find_all('tr')[1:]
                
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        underlying = {
                            'name': cells[0].get_text(strip=True),
                            'strike': parse_number(cells[1].get_text(strip=True)),
                            'spot': parse_number(cells[2].get_text(strip=True)),
                            'barrier': parse_number(cells[3].get_text(strip=True)),
                            'worst_of': 'W' in row.get_text() or 'Worst' in row.get_text()
                        }
                        
                        if len(cells) > 4:
                            var_text = cells[4].get_text(strip=True)
                            var_match = re.search(r'(-?\d+[,.]?\d*)', var_text)
                            if var_match:
                                underlying['variation_pct'] = parse_number(var_match.group(1))
                        
                        if underlying['name']:
                            underlyings.append(underlying)
        
        cert['underlyings'] = underlyings
        if underlyings:
            cert['underlying_name'] = ', '.join([u['name'] for u in underlyings[:3]])
        
        # Extract scenario analysis
        for table in tables:
            table_text = table.get_text().lower()
            if 'var %' in table_text and 'rimborso' in table_text:
                scenarios = []
                rows = table.find_all('tr')
                
                variations = []
                redemptions = []
                
                for row in rows:
                    cells = row.find_all(['th', 'td'])
                    row_text = row.get_text().lower()
                    
                    if 'var' in row_text and 'sottostante' in row_text:
                        for cell in cells[1:]:
                            text = cell.get_text(strip=True)
                            var = parse_number(text.replace('(B - TC)', '').replace('(B-TC)', ''))
                            if var is not None:
                                variations.append(var)
                    
                    if 'rimborso' in row_text:
                        for cell in cells[1:]:
                            val = parse_number(cell.get_text(strip=True))
                            if val is not None:
                                redemptions.append(val)
                
                for i, var in enumerate(variations):
                    if i < len(redemptions):
                        scenarios.append({
                            'variation_pct': var,
                            'redemption': redemptions[i],
                            'pl_pct': round((redemptions[i] - 1000) / 10, 2) if redemptions[i] > 100 else round(redemptions[i] - 100, 2)
                        })
                
                if scenarios:
                    cert['scenario_analysis'] = {'scenarios': scenarios}
                break
        
        # Determine type
        name = cert.get('name', '').lower()
        for pattern, type_name in [
            ('phoenix memory', 'Phoenix Memory'),
            ('cash collect memory', 'Cash Collect Memory'),
            ('fixed cash collect', 'Fixed Cash Collect'),
            ('cash collect', 'Cash Collect'),
            ('bonus plus', 'Bonus Plus'),
            ('express', 'Express'),
            ('softcallable', 'Softcallable'),
            ('memory', 'Memory'),
            ('phoenix', 'Phoenix'),
        ]:
            if pattern in name:
                cert['type'] = type_name
                break
        else:
            cert['type'] = 'Certificate'
        
        # Set price
        if not cert.get('price'):
            if cert.get('bid_price') and cert.get('ask_price'):
                cert['price'] = (cert['bid_price'] + cert['ask_price']) / 2
            elif cert.get('reference_price'):
                cert['price'] = cert['reference_price']
        
    except Exception as e:
        cert['error'] = str(e)[:100]
    
    return cert


def categorize_underlying(cert):
    text = (cert.get('underlying_name', '') + ' ' + cert.get('name', '')).lower()
    
    if any(kw in text for kw in ['indice', 'index', 'stoxx', 'ftse', 's&p', 'nasdaq', 'dax', 'mib']):
        return 'index'
    if any(kw in text for kw in ['oro', 'gold', 'oil', 'petrolio', 'wti', 'brent', 'commodity']):
        return 'commodity'
    if any(kw in text for kw in ['eur/usd', 'usd/', '/usd', 'forex', 'currency']):
        return 'currency'
    if any(kw in text for kw in ['btp', 'bund', 'tasso', 'rate', 'euribor']):
        return 'rate'
    
    return 'other'


def scrape_cedlab():
    log("=" * 70)
    log("CED LAB PRO SCRAPER v1.0")
    log("Copyright (c) 2024-2025 Mutna S.R.L.S.")
    log("=" * 70)
    
    username, password = get_credentials()
    log(f"Username: {username[:3]}***")
    
    certificates = []
    stats = {'total_found': 0, 'scraped': 0, 'errors': 0}
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()
        
        if not login(page, username, password):
            browser.close()
            sys.exit(1)
        
        isins = extract_isin_list(page)
        stats['total_found'] = len(isins)
        
        log(f"\n📋 Found {len(isins)} certificates to scrape")
        
        for i, isin in enumerate(isins):
            try:
                cert = extract_certificate_details(page, isin)
                cert['underlying_category'] = categorize_underlying(cert)
                certificates.append(cert)
                stats['scraped'] += 1
                
                if (i + 1) % 20 == 0:
                    log(f"   Progress: {i + 1}/{len(isins)}")
            except Exception as e:
                stats['errors'] += 1
        
        browser.close()
    
    # Summary
    log("\n" + "=" * 70)
    log("📊 SUMMARY")
    log(f"Total: {stats['total_found']} | Scraped: {stats['scraped']} | Errors: {stats['errors']}")
    
    by_category = {}
    for c in certificates:
        cat = c.get('underlying_category', 'other')
        by_category[cat] = by_category.get(cat, 0) + 1
    
    by_issuer = {}
    for c in certificates:
        iss = c.get('issuer', 'Unknown')
        by_issuer[iss] = by_issuer.get(iss, 0) + 1
    
    # Save
    os.makedirs('data', exist_ok=True)
    
    output = {
        'metadata': {
            'version': '1.0-cedlab',
            'timestamp': datetime.now().isoformat(),
            'source': 'cedlabpro.it',
            'total': len(certificates),
            'categories': by_category,
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
