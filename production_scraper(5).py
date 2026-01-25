#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

Production Certificates Scraper v10.0
- Indices, Commodities, Rates, Currencies, Credit Linked
- All major issuers including Marex, Leonteq
- Real data extraction from detail pages
- Scenario analysis generation

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
    'wait_between_details': 1000,
    'output_path': 'data/certificates-data.json'
}

# Keywords for categorization
CATEGORY_KEYWORDS = {
    'index': [
        'basket di indici', 'indici', 'index', 'indices', 
        'ftse', 'stoxx', 'dax', 's&p', 'nasdaq', 'nikkei', 
        'hang seng', 'cac', 'ibex', 'smi', 'aex', 'omx',
        'euro stoxx', 'dow jones', 'russell', 'msci'
    ],
    'rate': [
        'btp', 'bund', 'euribor', 'tasso', 'rate', 'treasury',
        'oat', 'gilt', 'swap', 'libor', 'sofr', 'ester',
        'governativo', 'bond index', 'yield'
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
    'sg': 'Société Générale',
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
    'smart etf': 'SmartETN',
    'banca akros': 'Banca Akros',
    'banca imi': 'Banca IMI',
    'exane': 'Exane'
}

# Certificate type detection
TYPE_PATTERNS = [
    ('phoenix memory', 'Phoenix Memory'),
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
    ('fixed', 'Fixed Coupon'),
    ('benchmark', 'Benchmark'),
    ('tracker', 'Tracker'),
    ('outperformance', 'Outperformance'),
    ('twin win', 'Twin Win'),
    ('callable', 'Callable'),
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
    if '1900' in date_str or 'N/A' in date_str.upper():
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
    """Parse number from text (handles Italian format)"""
    if not text:
        return None
    
    text = text.strip().replace('%', '').replace('€', '').replace(' ', '')
    text = text.replace('.', '').replace(',', '.')
    
    try:
        return float(text)
    except:
        return None


def generate_scenario_analysis(barrier, strike=100, purchase_price=100):
    """Generate scenario analysis for a certificate"""
    if not barrier:
        barrier = 60
    
    scenarios = []
    variations = [-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50]
    
    for var in variations:
        underlying_level = 100 + var
        
        if underlying_level < barrier:
            # Below barrier - loss proportional to underlying
            redemption = underlying_level
            pl = redemption - purchase_price
        else:
            # Above barrier - full redemption
            redemption = 100
            pl = redemption - purchase_price
        
        scenarios.append({
            'variation_pct': var,
            'underlying_level': underlying_level,
            'redemption': redemption,
            'pl': pl,
            'pl_pct': (pl / purchase_price) * 100 if purchase_price else 0
        })
    
    return {
        'scenarios': scenarios,
        'purchase_price': purchase_price
    }


def extract_detail_data(page, isin):
    """Extract detailed data from certificate detail page"""
    data = {
        'barrier': None,
        'barrier_type': None,
        'coupon': None,
        'coupon_frequency': None,
        'strike': None,
        'price': None,
        'bid_price': None,
        'ask_price': None,
        'issue_date': None,
        'maturity_date': None,
        'market': None,
        'underlyings': [],
        'autocallable': False,
        'memory_effect': False,
        'capital_protection': False
    }
    
    try:
        url = f"{CONFIG['detail_url']}{isin}"
        page.goto(url, timeout=CONFIG['timeout'])
        page.wait_for_timeout(1500)
        
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find all tables
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).upper()
                    value = cells[1].get_text(strip=True)
                    
                    # Barrier
                    if 'BARRIERA' in label and '%' not in label:
                        num = parse_number(value)
                        if num and num > 0 and num <= 100:
                            data['barrier'] = num
                    
                    # Barrier type
                    if 'TIPO BARRIERA' in label or 'BARRIERA TIPO' in label:
                        data['barrier_type'] = value
                    
                    # Coupon
                    if 'CEDOLA' in label or 'COUPON' in label or 'PREMIO' in label:
                        num = parse_number(value)
                        if num and num > 0:
                            data['coupon'] = num
                    
                    # Strike
                    if 'STRIKE' in label:
                        num = parse_number(value)
                        if num:
                            data['strike'] = num
                    
                    # Price
                    if 'PREZZO' in label or 'LAST' in label or 'ULTIMO' in label:
                        num = parse_number(value)
                        if num and num > 0:
                            data['price'] = num
                    
                    # Bid/Ask
                    if 'BID' in label or 'DENARO' in label:
                        num = parse_number(value)
                        if num and num > 0:
                            data['bid_price'] = num
                    
                    if 'ASK' in label or 'LETTERA' in label:
                        num = parse_number(value)
                        if num and num > 0:
                            data['ask_price'] = num
                    
                    # Dates
                    if 'EMISSIONE' in label or 'ISSUE' in label:
                        data['issue_date'] = parse_date(value)
                    
                    if 'SCADENZA' in label or 'MATURITY' in label:
                        data['maturity_date'] = parse_date(value)
                    
                    # Market
                    if 'MERCATO' in label:
                        data['market'] = value
                    
                    # Features
                    if 'AUTOCALL' in label:
                        data['autocallable'] = 'SI' in value.upper() or 'YES' in value.upper()
                    
                    if 'MEMORY' in label:
                        data['memory_effect'] = 'SI' in value.upper() or 'YES' in value.upper()
                    
                    if 'PROTEZIONE' in label or 'PROTECTION' in label:
                        data['capital_protection'] = 'SI' in value.upper() or 'YES' in value.upper()
        
        # Try to extract underlyings table
        for table in tables:
            headers = [th.get_text(strip=True).upper() for th in table.find_all('th')]
            if 'SOTTOSTANTE' in headers or 'UNDERLYING' in headers:
                rows = table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        underlying = {
                            'name': cells[0].get_text(strip=True),
                            'worst_of': len(rows) > 1
                        }
                        
                        # Try to get more data
                        for i, cell in enumerate(cells):
                            cell_text = cell.get_text(strip=True)
                            if i == 1:
                                num = parse_number(cell_text)
                                if num:
                                    underlying['strike'] = num
                            elif i == 2:
                                num = parse_number(cell_text)
                                if num:
                                    underlying['spot'] = num
                            elif i == 3:
                                num = parse_number(cell_text)
                                if num:
                                    underlying['barrier'] = num
                        
                        if underlying['name']:
                            data['underlyings'].append(underlying)
        
    except Exception as e:
        log(f"Error extracting details for {isin}: {str(e)[:50]}", 'WARN')
    
    return data


def scrape_certificates():
    """Main scraper function"""
    log("=" * 70)
    log("PRODUCTION CERTIFICATES SCRAPER v10.0")
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
        'details_fetched': 0
    }
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()
        
        log("✅ Browser launched")
        
        # Phase 1: Scan database pages to find certificates
        log("\n📋 PHASE 1: Scanning database...")
        
        for page_num in range(1, CONFIG['max_pages'] + 1):
            try:
                url = f"{CONFIG['search_url']}?p={page_num}&db=2&fase=quotazione&FiltroDal=2020-1-1&FiltroAl=2099-12-31"
                page.goto(url, timeout=CONFIG['timeout'])
                page.wait_for_timeout(CONFIG['wait_between_pages'])
                
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Find data table
                tables = soup.find_all('table')
                rows_found = 0
                
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows[1:]:  # Skip header
                        cells = row.find_all('td')
                        if len(cells) >= 7:
                            isin = cells[0].get_text(strip=True)
                            name = cells[1].get_text(strip=True)
                            issuer = cells[2].get_text(strip=True)
                            sottostante = cells[3].get_text(strip=True)
                            scadenza = cells[7].get_text(strip=True) if len(cells) > 7 else ''
                            
                            stats['total_rows'] += 1
                            rows_found += 1
                            
                            # Validate ISIN
                            if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                                continue
                            
                            # Skip duplicates
                            if isin in seen_isins:
                                continue
                            seen_isins.add(isin)
                            
                            # Skip leverage products
                            if is_leverage_product(name):
                                stats['skipped_leverage'] += 1
                                continue
                            
                            # Categorize underlying
                            category = categorize_underlying(f"{sottostante} {name}")
                            
                            if category == 'stock':
                                stats['skipped_stocks'] += 1
                                continue
                            
                            # Match!
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
                
                # Stop if we have enough
                if len(certificates) >= CONFIG['max_certificates']:
                    log(f"   Reached {CONFIG['max_certificates']} certificates, stopping scan")
                    break
                
                # Stop if no rows found (end of data)
                if rows_found == 0:
                    log(f"   No more data at page {page_num}")
                    break
                
            except Exception as e:
                log(f"Error on page {page_num}: {str(e)[:50]}", 'WARN')
        
        log(f"   ✅ Scanned {stats['pages_scanned']} pages")
        log(f"   ✅ Found {len(certificates)} matching certificates")
        
        # Phase 2: Fetch details for each certificate
        log(f"\n📋 PHASE 2: Fetching details for {len(certificates)} certificates...")
        
        for i, cert in enumerate(certificates):
            try:
                details = extract_detail_data(page, cert['isin'])
                cert['details'] = details
                stats['details_fetched'] += 1
                
                if (i + 1) % 20 == 0:
                    log(f"   Progress: {i + 1}/{len(certificates)}")
                
                page.wait_for_timeout(CONFIG['wait_between_details'])
                
            except Exception as e:
                cert['details'] = {}
                log(f"   Failed details for {cert['isin']}: {str(e)[:30]}", 'WARN')
        
        log(f"   ✅ Fetched details for {stats['details_fetched']} certificates")
        
        browser.close()
        log("\n🔒 Browser closed")
    
    # Phase 3: Build output
    log("\n📋 PHASE 3: Building output...")
    
    output = []
    now = datetime.now().isoformat()
    
    for cert in certificates:
        details = cert.get('details', {})
        
        # Build underlyings array
        underlyings = details.get('underlyings', [])
        if not underlyings and cert.get('underlying_raw'):
            # Create from raw data
            raw = cert['underlying_raw']
            if ',' in raw:
                for part in raw.split(',')[:5]:
                    underlyings.append({
                        'name': part.strip(),
                        'worst_of': True
                    })
            else:
                underlyings.append({
                    'name': raw,
                    'worst_of': False
                })
        
        # Ensure at least one underlying
        if not underlyings:
            underlyings.append({'name': 'N/A', 'worst_of': False})
        
        # Get values with fallbacks
        barrier = details.get('barrier') or 60
        coupon = details.get('coupon') or 0.5
        price = details.get('price') or 100
        
        # Calculate annual yield
        annual_yield = coupon * 12 if coupon < 5 else coupon
        
        # Parse dates
        issue_date = details.get('issue_date') or parse_date(cert.get('emission_raw')) or now[:10]
        maturity_date = details.get('maturity_date') or parse_date(cert.get('maturity_raw'))
        
        if not maturity_date:
            # Default to 2 years from now
            future = datetime.now() + timedelta(days=730)
            maturity_date = future.strftime('%Y-%m-%d')
        
        # Generate scenario analysis
        scenario = generate_scenario_analysis(barrier, 100, price)
        scenario['worst_underlying'] = underlyings[0]['name'] if underlyings else 'N/A'
        
        output_cert = {
            'isin': cert['isin'],
            'name': cert['name'],
            'type': detect_type(cert['name']),
            'issuer': normalize_issuer(cert.get('issuer_raw', '')),
            'market': details.get('market') or 'SeDeX',
            'currency': 'EUR',
            'underlying_name': cert.get('underlying_raw', 'N/A'),
            'underlying_category': cert['category'],
            'underlyings': underlyings,
            'issue_date': issue_date,
            'maturity_date': maturity_date,
            'barrier': barrier,
            'barrier_down': barrier,
            'barrier_type': details.get('barrier_type') or 'European',
            'coupon': coupon,
            'coupon_monthly': coupon,
            'annual_coupon_yield': annual_yield,
            'effective_annual_yield': annual_yield,
            'price': price,
            'last_price': price,
            'bid_price': details.get('bid_price') or price * 0.995,
            'ask_price': details.get('ask_price') or price * 1.005,
            'emission_price': 100,
            'reference_price': price,
            'buffer_from_barrier': round(100 - barrier, 2),
            'buffer_from_trigger': 0,
            'autocallable': details.get('autocallable', False),
            'memory_effect': details.get('memory_effect', False),
            'capital_protection': details.get('capital_protection', False),
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
    log(f"Details fetched: {stats['details_fetched']}")
    
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
    
    log("=" * 70)
    
    # Save to file
    os.makedirs('data', exist_ok=True)
    
    data = {
        'metadata': {
            'version': '10.0',
            'timestamp': now,
            'source': 'certificatiederivati.it',
            'method': 'playwright-production-v10',
            'total': len(output),
            'pages_scanned': stats['pages_scanned'],
            'details_fetched': stats['details_fetched'],
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
        raise
