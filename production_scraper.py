#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

Production Certificates Scraper v11.0
Based on actual certificatiederivati.it page structure

Sections on detail page:
- Header: Tipo certificato, ISIN, Mercato, Date
- Scheda Sottostante: Descrizione, Strike, Peso
- Barriera Down: Barriera %, Tipo, Valuation
- Date rilevamento: Cedola %, Trigger Cedola %
- Additional: Prezzo emissione, Nominale, Trigger, etc.

Usage: python production_scraper.py
"""

import json
import re
import os
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time

# ===================================
# CONFIGURATION
# ===================================

CONFIG = {
    'base_url': 'https://www.certificatiederivati.it',
    'search_url': 'https://www.certificatiederivati.it/db_bs_estrazione_ricerca.asp',
    'detail_url': 'https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin=',
    'max_pages': 50,
    'max_certificates': 150,
    'timeout': 30000,
    'wait_between_pages': 1500,
    'wait_between_details': 1200,
    'output_path': 'data/certificates-data.json'
}

# Keywords for categorization
CATEGORY_KEYWORDS = {
    'index': [
        'basket di indici', 'indici', 'index', 'indices', 
        'ftse', 'stoxx', 'dax', 's&p', 'nasdaq', 'nikkei', 
        'hang seng', 'cac', 'ibex', 'smi', 'aex', 'omx',
        'euro stoxx', 'dow jones', 'russell', 'msci', 'mib'
    ],
    'rate': [
        'btp', 'bund', 'euribor', 'tasso', 'rate', 'treasury',
        'oat', 'gilt', 'swap', 'libor', 'sofr', 'ester',
        'governativo', 'bond index', 'yield', 'interest'
    ],
    'commodity': [
        'gold', 'oro', 'silver', 'argento', 'oil', 'petrolio', 
        'commodity', 'gas', 'copper', 'rame', 'platinum', 'platino',
        'palladium', 'palladio', 'wheat', 'corn', 'soybean',
        'coffee', 'sugar', 'cotton', 'brent', 'wti', 'natural gas'
    ],
    'currency': [
        'eur/usd', 'usd/jpy', 'gbp/usd', 'forex', 'currency', 
        'cambio', 'fx', 'dollar', 'euro', 'yen', 'sterling',
        'usd/chf', 'aud/usd', 'nzd/usd', 'usd/cad'
    ],
    'credit_linked': [
        'credit linked', 'creditlinked', 'cln', 'credit default',
        'credit risk', 'reference entity', 'credit event'
    ]
}

# Leverage products to skip
LEVERAGE_KEYWORDS = [
    'turbo', 'leva fissa', 'mini future', 'stayup', 'staydown',
    'corridor', 'daily leverage', 'leverage', 'short', 'ultra'
]

# Issuer normalization
ISSUER_MAP = {
    'bnp paribas': 'BNP Paribas',
    'bnp': 'BNP Paribas',
    'societe generale': 'Société Générale',
    'socgen': 'Société Générale',
    'sg ': 'Société Générale',
    'unicredit': 'UniCredit',
    'vontobel': 'Vontobel',
    'barclays': 'Barclays',
    'mediobanca': 'Mediobanca',
    'intesa sanpaolo': 'Intesa Sanpaolo',
    'intesa': 'Intesa Sanpaolo',
    'leonteq': 'Leonteq Securities',
    'marex': 'Marex Financial',
    'goldman sachs': 'Goldman Sachs',
    'goldman': 'Goldman Sachs',
    'morgan stanley': 'Morgan Stanley',
    'deutsche bank': 'Deutsche Bank',
    'jp morgan': 'JP Morgan',
    'jpmorgan': 'JP Morgan',
    'citigroup': 'Citigroup',
    'citi': 'Citigroup',
    'credit suisse': 'Credit Suisse',
    'ubs': 'UBS',
    'hsbc': 'HSBC',
    'natixis': 'Natixis',
    'commerzbank': 'Commerzbank',
    'eka': 'EKA Finance',
    'smart': 'SmartETN',
    'banca akros': 'Banca Akros',
    'banca imi': 'Banca IMI',
    'exane': 'Exane',
    'crédit agricole': 'Crédit Agricole',
    'credit agricole': 'Crédit Agricole',
    'bbva': 'BBVA',
    'santander': 'Santander'
}

# Certificate type detection
TYPE_PATTERNS = [
    ('phoenix memory', 'Phoenix Memory'),
    ('cash collect memory', 'Cash Collect Memory'),
    ('cash collect', 'Cash Collect'),
    ('bonus cap', 'Bonus Cap'),
    ('top bonus', 'Top Bonus'),
    ('express', 'Express'),
    ('equity protection', 'Equity Protection'),
    ('credit linked', 'Credit Linked'),
    ('digital', 'Digital'),
    ('airbag', 'Airbag'),
    ('autocall', 'Autocallable'),
    ('memory', 'Memory'),
    ('phoenix', 'Phoenix'),
    ('reverse', 'Reverse'),
    ('fixed cash collect', 'Fixed Cash Collect'),
    ('fixed', 'Fixed Coupon'),
    ('benchmark', 'Benchmark'),
    ('tracker', 'Tracker'),
    ('outperformance', 'Outperformance'),
    ('twin win', 'Twin Win'),
    ('callable', 'Callable'),
    ('maxi', 'Maxi Cash Collect'),
    ('softcallable', 'Softcallable'),
    ('athena', 'Athena'),
    ('coupon', 'Coupon'),
]


def log(msg, level='INFO'):
    """Print log message with timestamp"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] [{level}] {msg}")


def categorize_underlying(text):
    """Categorize underlying based on keywords"""
    text_lower = text.lower()
    
    for category, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                return category
    
    return 'stock'


def is_leverage_product(name):
    """Check if certificate is a leverage product"""
    name_lower = name.lower()
    return any(kw in name_lower for kw in LEVERAGE_KEYWORDS)


def normalize_issuer(issuer):
    """Normalize issuer name"""
    if not issuer:
        return 'Unknown'
    
    issuer_lower = issuer.lower().strip()
    
    for key, value in ISSUER_MAP.items():
        if key in issuer_lower:
            return value
    
    return issuer.strip().title()


def detect_type(name):
    """Detect certificate type from name"""
    name_lower = name.lower()
    
    for pattern, cert_type in TYPE_PATTERNS:
        if pattern in name_lower:
            return cert_type
    
    return 'Certificate'


def parse_date(date_str):
    """Parse Italian date format to ISO"""
    if not date_str:
        return None
    
    date_str = date_str.strip()
    
    # Skip invalid dates
    if '1900' in date_str or 'N/A' in date_str.upper() or not date_str:
        return None
    
    # Try dd/mm/yyyy format
    match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', date_str)
    if match:
        d, m, y = match.groups()
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    
    # Try yyyy-mm-dd format
    match = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', date_str)
    if match:
        return date_str
    
    return None


def parse_number(text):
    """Parse number from text (handles Italian format with comma as decimal)"""
    if not text:
        return None
    
    text = str(text).strip().replace('%', '').replace('€', '').replace(' ', '')
    
    # Handle Italian number format: 17.690,49 -> 17690.49
    if ',' in text and '.' in text:
        text = text.replace('.', '').replace(',', '.')
    elif ',' in text:
        text = text.replace(',', '.')
    
    try:
        return float(text)
    except:
        return None


def parse_percentage(text):
    """Parse percentage value"""
    if not text:
        return None
    
    text = str(text).strip().replace('%', '').replace(' ', '')
    text = text.replace(',', '.')
    
    try:
        val = float(text)
        return val
    except:
        return None


def generate_scenario_analysis(barrier, purchase_price=100):
    """Generate scenario analysis for a certificate"""
    if not barrier:
        barrier = 60
    
    scenarios = []
    variations = [-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50]
    
    for var in variations:
        underlying_level = 100 + var
        
        if underlying_level < barrier:
            redemption = underlying_level
            pl = redemption - purchase_price
        else:
            redemption = 100
            pl = redemption - purchase_price
        
        scenarios.append({
            'variation_pct': var,
            'underlying_level': underlying_level,
            'redemption': round(redemption, 2),
            'pl': round(pl, 2),
            'pl_pct': round((pl / purchase_price) * 100, 2) if purchase_price else 0
        })
    
    return {
        'scenarios': scenarios,
        'purchase_price': purchase_price
    }


def extract_detail_data(page, isin):
    """
    Extract detailed data from certificate detail page
    Based on actual page structure from certificatiederivati.it
    """
    data = {
        'type': None,
        'market': None,
        'issue_date': None,
        'maturity_date': None,
        'barrier': None,
        'barrier_type': None,
        'coupon': None,
        'coupon_frequency': 'trimestrale',
        'trigger_coupon': None,
        'trigger_autocall': None,
        'price': None,
        'emission_price': None,
        'nominal': None,
        'underlyings': [],
        'issuer': None,
        'autocallable': False,
        'memory_effect': False
    }
    
    try:
        url = f"{CONFIG['detail_url']}{isin}"
        page.goto(url, timeout=CONFIG['timeout'])
        page.wait_for_timeout(1500)
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        # ===== HEADER SECTION =====
        header = soup.find('td', class_='titoloprodotto') or soup.find('th', class_='titoloprodotto')
        if header:
            data['type'] = header.get_text(strip=True)
        
        # ===== FIND ALL TABLES =====
        all_tables = soup.find_all('table')
        
        for table in all_tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).upper()
                    value = cells[1].get_text(strip=True)
                    
                    # Market
                    if 'MERCATO' in label:
                        data['market'] = value
                    
                    # Dates
                    if 'DATA EMISSIONE' in label:
                        data['issue_date'] = parse_date(value)
                    
                    if 'DATA SCADENZA' in label:
                        data['maturity_date'] = parse_date(value)
                    
                    if 'VALUTAZIONE FINALE' in label:
                        if not data['maturity_date']:
                            data['maturity_date'] = parse_date(value)
                    
                    # Price info
                    if 'PREZZO EMISSIONE' in label:
                        data['emission_price'] = parse_number(value)
                    
                    if 'NOMINALE' in label:
                        data['nominal'] = parse_number(value)
                    
                    # Trigger (0.75 = 75%)
                    if label == 'TRIGGER':
                        num = parse_number(value)
                        if num and num < 2:
                            data['trigger_autocall'] = num * 100
                        elif num:
                            data['trigger_autocall'] = num
        
        # ===== SCHEDA SOTTOSTANTE =====
        for table in all_tables:
            table_text = table.get_text(strip=True).upper()
            
            if 'SOTTOSTANTE' in table_text and ('DESCRIZIONE' in table_text or 'STRIKE' in table_text):
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        name = cells[0].get_text(strip=True)
                        strike_text = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                        strike = parse_number(strike_text)
                        
                        # Skip headers and empty rows
                        if name and name.upper() not in ['DESCRIZIONE', 'STRIKE', 'PESO', '', 'SOTTOSTANTE']:
                            underlying = {
                                'name': name,
                                'strike': strike,
                                'worst_of': True
                            }
                            data['underlyings'].append(underlying)
        
        # ===== BARRIERA DOWN =====
        for table in all_tables:
            table_text = table.get_text(strip=True).upper()
            
            if 'BARRIERA' in table_text:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    row_text = row.get_text(strip=True)
                    
                    # Look for percentage in cells
                    for cell in cells:
                        cell_text = cell.get_text(strip=True)
                        if '%' in cell_text:
                            barrier_val = parse_percentage(cell_text)
                            if barrier_val and 10 <= barrier_val <= 100:
                                data['barrier'] = barrier_val
                                break
                        # Also try without % symbol
                        elif re.match(r'^\d{1,2}[\.,]?\d*$', cell_text):
                            barrier_val = parse_percentage(cell_text)
                            if barrier_val and 10 <= barrier_val <= 100:
                                data['barrier'] = barrier_val
                                break
                    
                    # Barrier type
                    if 'DISCRETA' in row_text.upper():
                        data['barrier_type'] = 'Discreta'
                    elif 'CONTINUA' in row_text.upper():
                        data['barrier_type'] = 'Continua'
                    elif 'EUROPEA' in row_text.upper():
                        data['barrier_type'] = 'Europea'
        
        # ===== DATE RILEVAMENTO (Coupon) =====
        for table in all_tables:
            # Find table with CEDOLA header
            headers = [th.get_text(strip=True).upper() for th in table.find_all('th')]
            
            if 'CEDOLA' in headers or 'TRIGGER CEDOLA' in headers:
                rows = table.find_all('tr')[1:]  # Skip header row
                
                if rows:
                    # Get first data row
                    first_row = rows[0]
                    cells = first_row.find_all('td')
                    
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True)
                        pct = parse_percentage(cell_text)
                        
                        if pct is not None:
                            # Small values (0.1-5) are likely coupon rates
                            if 0.1 <= pct <= 5 and not data['coupon']:
                                data['coupon'] = pct
                            # Larger values (50-100) are likely trigger percentages
                            elif 50 <= pct <= 100 and not data['trigger_coupon']:
                                data['trigger_coupon'] = pct
        
        # ===== SCHEDA EMITTENTE =====
        for table in all_tables:
            table_text = table.get_text(strip=True).upper()
            
            if 'EMITTENTE' in table_text:
                cells = table.find_all('td')
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    for key, name in ISSUER_MAP.items():
                        if key in cell_text.lower():
                            data['issuer'] = name
                            break
                    if data['issuer']:
                        break
        
        # ===== FEATURES =====
        page_text = soup.get_text().lower()
        if 'memory' in page_text or 'memoria' in page_text:
            data['memory_effect'] = True
        if 'autocall' in page_text or 'rimborso anticipato' in page_text:
            data['autocallable'] = True
        
    except Exception as e:
        log(f"Error extracting {isin}: {str(e)[:60]}", 'WARN')
    
    return data


def scrape_certificates():
    """Main scraper function"""
    log("=" * 70)
    log("PRODUCTION CERTIFICATES SCRAPER v11.0")
    log("Source: certificatiederivati.it")
    log("Filter: INDICES | RATES | COMMODITIES | CURRENCIES | CREDIT LINKED")
    log("Copyright (c) 2024-2025 Mutna S.R.L.S.")
    log("=" * 70)
    
    certificates = []
    seen_isins = set()
    stats = {
        'pages_scanned': 0,
        'total_rows': 0,
        'matched': 0,
        'skipped_leverage': 0,
        'skipped_stocks': 0,
        'details_fetched': 0,
        'details_with_barrier': 0,
        'details_with_coupon': 0
    }
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        log("✅ Browser launched")
        
        # Phase 1: Scan database pages
        log("\n📋 PHASE 1: Scanning database...")
        
        for page_num in range(1, CONFIG['max_pages'] + 1):
            try:
                url = f"{CONFIG['search_url']}?p={page_num}&db=2&fase=quotazione&FiltroDal=2020-1-1&FiltroAl=2099-12-31"
                page.goto(url, timeout=CONFIG['timeout'])
                page.wait_for_timeout(CONFIG['wait_between_pages'])
                
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                tables = soup.find_all('table')
                rows_found = 0
                
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows[1:]:
                        cells = row.find_all('td')
                        if len(cells) >= 7:
                            isin = cells[0].get_text(strip=True)
                            name = cells[1].get_text(strip=True)
                            issuer = cells[2].get_text(strip=True)
                            sottostante = cells[3].get_text(strip=True)
                            scadenza = cells[7].get_text(strip=True) if len(cells) > 7 else ''
                            
                            stats['total_rows'] += 1
                            rows_found += 1
                            
                            if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                                continue
                            
                            if isin in seen_isins:
                                continue
                            seen_isins.add(isin)
                            
                            if is_leverage_product(name):
                                stats['skipped_leverage'] += 1
                                continue
                            
                            category = categorize_underlying(f"{sottostante} {name}")
                            
                            if category == 'stock':
                                stats['skipped_stocks'] += 1
                                continue
                            
                            stats['matched'] += 1
                            
                            certificates.append({
                                'isin': isin,
                                'name': name,
                                'issuer_raw': issuer,
                                'underlying_raw': sottostante,
                                'category': category,
                                'maturity_raw': scadenza
                            })
                
                stats['pages_scanned'] += 1
                
                if page_num % 10 == 0:
                    log(f"   Page {page_num}: {len(certificates)} matched so far")
                
                if len(certificates) >= CONFIG['max_certificates']:
                    log(f"   Reached {CONFIG['max_certificates']} certificates")
                    break
                
                if rows_found == 0:
                    log(f"   No more data at page {page_num}")
                    break
                
            except Exception as e:
                log(f"Error on page {page_num}: {str(e)[:50]}", 'WARN')
        
        log(f"   ✅ Scanned {stats['pages_scanned']} pages")
        log(f"   ✅ Found {len(certificates)} matching certificates")
        
        # Phase 2: Fetch details
        log(f"\n📋 PHASE 2: Fetching details for {len(certificates)} certificates...")
        
        for i, cert in enumerate(certificates):
            try:
                details = extract_detail_data(page, cert['isin'])
                cert['details'] = details
                stats['details_fetched'] += 1
                
                if details.get('barrier'):
                    stats['details_with_barrier'] += 1
                if details.get('coupon'):
                    stats['details_with_coupon'] += 1
                
                if (i + 1) % 20 == 0:
                    log(f"   Progress: {i + 1}/{len(certificates)} (barrier: {stats['details_with_barrier']}, coupon: {stats['details_with_coupon']})")
                
                page.wait_for_timeout(CONFIG['wait_between_details'])
                
            except Exception as e:
                cert['details'] = {}
                log(f"   Failed {cert['isin']}: {str(e)[:30]}", 'WARN')
        
        log(f"   ✅ Details fetched: {stats['details_fetched']}")
        log(f"   ✅ With barrier: {stats['details_with_barrier']}")
        log(f"   ✅ With coupon: {stats['details_with_coupon']}")
        
        browser.close()
        log("\n🔒 Browser closed")
    
    # Phase 3: Build output
    log("\n📋 PHASE 3: Building output...")
    
    output = []
    now = datetime.now().isoformat()
    
    for cert in certificates:
        details = cert.get('details', {})
        
        # Underlyings
        underlyings = details.get('underlyings', [])
        if not underlyings and cert.get('underlying_raw'):
            raw = cert['underlying_raw']
            if ',' in raw:
                for part in raw.split(',')[:5]:
                    underlyings.append({'name': part.strip(), 'worst_of': True})
            else:
                underlyings.append({'name': raw, 'worst_of': False})
        
        if not underlyings:
            underlyings.append({'name': 'N/A', 'worst_of': False})
        
        if len(underlyings) == 1:
            underlyings[0]['worst_of'] = False
        
        # Values
        barrier = details.get('barrier')
        coupon = details.get('coupon')
        trigger_coupon = details.get('trigger_coupon')
        
        # Annual yield
        if coupon:
            if coupon < 5:
                annual_yield = coupon * 4  # Quarterly
            else:
                annual_yield = coupon
        else:
            annual_yield = None
        
        # Issuer
        issuer = details.get('issuer') or normalize_issuer(cert.get('issuer_raw', ''))
        
        # Dates
        issue_date = details.get('issue_date') or now[:10]
        maturity_date = details.get('maturity_date') or parse_date(cert.get('maturity_raw'))
        
        if not maturity_date:
            future = datetime.now() + timedelta(days=730)
            maturity_date = future.strftime('%Y-%m-%d')
        
        # Type
        cert_type = details.get('type') or detect_type(cert['name'])
        
        # Scenario
        scenario = None
        if barrier:
            scenario = generate_scenario_analysis(barrier, 100)
            scenario['worst_underlying'] = underlyings[0]['name'] if underlyings else 'N/A'
        
        output_cert = {
            'isin': cert['isin'],
            'name': cert['name'],
            'type': cert_type,
            'issuer': issuer,
            'market': details.get('market') or 'SeDeX',
            'currency': 'EUR',
            'underlying_name': cert.get('underlying_raw', 'N/A'),
            'underlying_category': cert['category'],
            'underlyings': underlyings,
            'issue_date': issue_date,
            'maturity_date': maturity_date,
            'barrier': barrier,
            'barrier_down': barrier,
            'barrier_type': details.get('barrier_type'),
            'coupon': coupon,
            'coupon_monthly': coupon,
            'trigger_coupon': trigger_coupon,
            'trigger_autocall': details.get('trigger_autocall'),
            'annual_coupon_yield': annual_yield,
            'effective_annual_yield': annual_yield,
            'price': details.get('emission_price') or 100,
            'last_price': 100,
            'emission_price': details.get('emission_price') or 100,
            'nominal': details.get('nominal') or 1000,
            'buffer_from_barrier': round(100 - barrier, 2) if barrier else None,
            'autocallable': details.get('autocallable', False),
            'memory_effect': details.get('memory_effect', False),
            'scenario_analysis': scenario,
            'scraped': True,
            'timestamp': now
        }
        
        output.append(output_cert)
    
    # Summary
    log("\n" + "=" * 70)
    log("📊 SUMMARY")
    log("=" * 70)
    log(f"Pages scanned: {stats['pages_scanned']}")
    log(f"Total rows: {stats['total_rows']}")
    log(f"Skipped (leverage): {stats['skipped_leverage']}")
    log(f"Skipped (stocks): {stats['skipped_stocks']}")
    log(f"Matched: {len(output)}")
    log(f"With real barrier: {stats['details_with_barrier']}")
    log(f"With real coupon: {stats['details_with_coupon']}")
    
    # By category
    by_category = {}
    for c in output:
        cat = c['underlying_category']
        by_category[cat] = by_category.get(cat, 0) + 1
    
    log("\nBy category:")
    icons = {'index': '📊', 'commodity': '🛢️', 'currency': '💱', 'rate': '💹', 'credit_linked': '🏦'}
    for cat, count in sorted(by_category.items(), key=lambda x: -x[1]):
        icon = icons.get(cat, '📄')
        log(f"   {icon} {cat}: {count}")
    
    # By issuer
    by_issuer = {}
    for c in output:
        iss = c['issuer']
        by_issuer[iss] = by_issuer.get(iss, 0) + 1
    
    log("\nTop issuers:")
    for iss, count in sorted(by_issuer.items(), key=lambda x: -x[1])[:10]:
        log(f"   {iss}: {count}")
    
    # Data quality
    with_barrier = sum(1 for c in output if c['barrier'])
    with_coupon = sum(1 for c in output if c['annual_coupon_yield'])
    with_scenario = sum(1 for c in output if c['scenario_analysis'])
    
    log(f"\nData quality:")
    log(f"   With barrier: {with_barrier}/{len(output)} ({100*with_barrier//len(output) if output else 0}%)")
    log(f"   With yield: {with_coupon}/{len(output)} ({100*with_coupon//len(output) if output else 0}%)")
    log(f"   With scenario: {with_scenario}/{len(output)} ({100*with_scenario//len(output) if output else 0}%)")
    
    log("=" * 70)
    
    # Save
    os.makedirs('data', exist_ok=True)
    
    data = {
        'metadata': {
            'version': '11.0',
            'timestamp': now,
            'source': 'certificatiederivati.it',
            'method': 'playwright-production-v11',
            'total': len(output),
            'pages_scanned': stats['pages_scanned'],
            'details_fetched': stats['details_fetched'],
            'with_barrier': with_barrier,
            'with_coupon': with_coupon,
            'filter': 'indices, commodities, rates, currencies, credit_linked',
            'categories': by_category,
            'issuers': list(by_issuer.keys()),
            'issuers_count': len(by_issuer)
        },
        'certificates': output
    }
    
    with open(CONFIG['output_path'], 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    log(f"\n💾 Saved {len(output)} certificates to {CONFIG['output_path']}")
    
    return data


if __name__ == '__main__':
    try:
        scrape_certificates()
    except Exception as e:
        log(f"❌ Scraper failed: {e}", 'ERROR')
        import traceback
        traceback.print_exc()
        raise
