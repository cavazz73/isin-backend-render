#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

Production Certificates Scraper v9.0 - DATABASE PAGINATION
Scrapes multiple pages from the full database to find certificates on:
- Indices (Basket di indici)
- Rates/Bonds
- Commodities
- Currencies

Strategy:
1. Paginate through database results (fase=quotazione)
2. Filter for non-stock underlyings
3. Get details for matched certificates
"""

import json
import re
import time
import os
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

CONFIG = {
    'search_url': 'https://www.certificatiederivati.it/db_bs_estrazione_ricerca.asp',
    'detail_url': 'https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin=',
    'page_timeout': 30000,
    'wait_for_content': 2000,
    'wait_between_pages': 1000,
    'wait_between_details': 500,
    'output_dir': 'data',
    'max_pages': 50,  # Scan first 50 pages (1000 certificates)
    'max_details': 150,  # Limit detail page visits
}

# Keywords to identify non-stock underlyings
INDEX_KEYWORDS = ['basket di indici', 'indici', 'index', 'indices']
RATE_KEYWORDS = ['btp', 'bund', 'euribor', 'tasso', 'rate', 'bond', 'oat']
COMMODITY_KEYWORDS = ['gold', 'oro', 'silver', 'oil', 'petrolio', 'commodity', 'gas', 'copper']
CURRENCY_KEYWORDS = ['eur/usd', 'usd/jpy', 'forex', 'currency', 'cambio']


class DatabaseScraper:
    def __init__(self):
        self.certificates = []
        self.seen_isins = set()
        self.browser = None
        self.context = None
        self.playwright = None
        self.stats = {
            'pages_scanned': 0,
            'total_rows': 0,
            'matched': 0,
            'skipped_stocks': 0,
            'skipped_leverage': 0,
        }
        
    def start_browser(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        self.context = self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
    def close_browser(self):
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
    
    def run(self):
        print('=' * 70)
        print('CERTIFICATES SCRAPER v9.0 - DATABASE PAGINATION')
        print('Target: INDICES | RATES | COMMODITIES | CURRENCIES')
        print('Copyright (c) 2024-2025 Mutna S.R.L.S.')
        print('=' * 70)
        print(f'Date: {datetime.now().isoformat()}')
        print(f'Max pages: {CONFIG["max_pages"]} (~{CONFIG["max_pages"] * 20} certificates)')
        print('=' * 70)
        
        self.start_browser()
        
        try:
            # Step 1: Scan database pages
            self.scan_database()
            
            # Step 2: Get details for matched certificates
            if self.certificates:
                self.fetch_details()
            
            # Step 3: Summary and save
            self.print_summary()
            self.save_results()
            
        finally:
            self.close_browser()
            print('\n🔒 Browser closed')
    
    def scan_database(self):
        """Scan database pages for non-stock certificates"""
        print(f'\n📋 Scanning database...')
        
        page = self.context.new_page()
        
        try:
            for page_num in range(1, CONFIG['max_pages'] + 1):
                # Build URL for this page
                url = f'{CONFIG["search_url"]}?p={page_num}&db=2&fase=quotazione&FiltroDal=2020-1-1&FiltroAl=2099-12-31'
                
                try:
                    page.goto(url, timeout=CONFIG['page_timeout'], wait_until='domcontentloaded')
                    page.wait_for_timeout(CONFIG['wait_for_content'])
                    
                    soup = BeautifulSoup(page.content(), 'html.parser')
                    
                    # Find table
                    found_on_page = 0
                    tables = soup.find_all('table')
                    
                    for table in tables:
                        rows = table.find_all('tr')
                        
                        for row in rows[1:]:  # Skip header
                            cells = row.find_all('td')
                            if len(cells) >= 7:
                                self.stats['total_rows'] += 1
                                
                                isin = cells[0].get_text(strip=True)
                                
                                # Validate ISIN
                                if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                                    continue
                                
                                # Skip duplicates
                                if isin in self.seen_isins:
                                    continue
                                self.seen_isins.add(isin)
                                
                                name = cells[1].get_text(strip=True)
                                emittente = cells[2].get_text(strip=True)
                                sottostante = cells[3].get_text(strip=True)
                                scadenza = cells[7].get_text(strip=True) if len(cells) > 7 else ''
                                
                                # Skip leverage products
                                name_upper = name.upper()
                                if any(x in name_upper for x in ['TURBO', 'LEVA FISSA', 'MINI FUTURE', 'STAYUP', 'STAYDOWN', 'CORRIDOR', 'DAILY LEVERAGE']):
                                    self.stats['skipped_leverage'] += 1
                                    continue
                                
                                # Categorize underlying
                                category = self.categorize_underlying(sottostante, name)
                                
                                if category == 'stock':
                                    self.stats['skipped_stocks'] += 1
                                    continue
                                
                                # Match! Add certificate
                                self.stats['matched'] += 1
                                found_on_page += 1
                                
                                cert = {
                                    'isin': isin,
                                    'name': name,
                                    'type': self.detect_type(name),
                                    'issuer': self.normalize_issuer(emittente),
                                    'underlying_raw': sottostante,
                                    'underlying_category': category,
                                    'maturity_date': self.parse_date(scadenza),
                                    'currency': 'EUR',
                                }
                                
                                self.certificates.append(cert)
                    
                    self.stats['pages_scanned'] += 1
                    
                    # Progress
                    if page_num % 10 == 0:
                        print(f'   Page {page_num}: {len(self.certificates)} matched so far')
                    
                    # Stop if we have enough
                    if len(self.certificates) >= CONFIG['max_details']:
                        print(f'   Reached {CONFIG["max_details"]} certificates, stopping scan')
                        break
                    
                    time.sleep(CONFIG['wait_between_pages'] / 1000)
                    
                except Exception as e:
                    print(f'   ⚠️ Error on page {page_num}: {str(e)[:40]}')
                    continue
                    
        finally:
            page.close()
        
        print(f'   ✅ Scanned {self.stats["pages_scanned"]} pages')
        print(f'   ✅ Found {len(self.certificates)} non-stock certificates')
    
    def categorize_underlying(self, sottostante, name):
        """Categorize underlying based on keywords"""
        text = f'{sottostante} {name}'.lower()
        
        # Check for indices
        for kw in INDEX_KEYWORDS:
            if kw in text:
                return 'index'
        
        # Check for rates
        for kw in RATE_KEYWORDS:
            if kw in text:
                return 'rate'
        
        # Check for commodities
        for kw in COMMODITY_KEYWORDS:
            if kw in text:
                return 'commodity'
        
        # Check for currencies
        for kw in CURRENCY_KEYWORDS:
            if kw in text:
                return 'currency'
        
        # Default: stock
        return 'stock'
    
    def fetch_details(self):
        """Fetch additional details from certificate pages"""
        print(f'\n📋 Fetching details for {len(self.certificates)} certificates...')
        
        for i, cert in enumerate(self.certificates):
            detail = self.get_certificate_detail(cert['isin'])
            if detail:
                cert.update(detail)
            
            if (i + 1) % 20 == 0:
                print(f'   {i + 1}/{len(self.certificates)}')
            
            time.sleep(CONFIG['wait_between_details'] / 1000)
    
    def get_certificate_detail(self, isin):
        """Get additional details from certificate page"""
        page = self.context.new_page()
        data = {}
        
        try:
            url = f'{CONFIG["detail_url"]}{isin}'
            page.goto(url, timeout=CONFIG['page_timeout'], wait_until='domcontentloaded')
            page.wait_for_timeout(1500)
            
            soup = BeautifulSoup(page.content(), 'html.parser')
            
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).upper()
                        value = cells[1].get_text(strip=True)
                        
                        if 'BARRIERA' in label and '%' not in label and 'DOWN' in label:
                            m = re.search(r'(\d+[,.]?\d*)', value)
                            if m:
                                data['barrier_down'] = float(m.group(1).replace(',', '.'))
                        
                        elif any(x in label for x in ['CEDOLA', 'COUPON', 'PREMIO']):
                            m = re.search(r'(\d+[,.]?\d*)', value)
                            if m:
                                data['coupon'] = float(m.group(1).replace(',', '.'))
                        
                        elif 'MERCATO' in label:
                            data['market'] = value
                        
                        elif 'EMISSIONE' in label and 'DATA' in label:
                            data['issue_date'] = self.parse_date(value)
                        
                        # Extract specific underlying from detail page
                        elif 'SOTTOSTANTE' in label and 'SCHEDA' not in label:
                            if value and len(value) > 2:
                                data['underlying'] = value
        except:
            pass
        finally:
            page.close()
        
        return data
    
    def detect_type(self, name):
        n = name.lower()
        for kw, t in [
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
        ]:
            if kw in n:
                return t
        return 'Certificate'
    
    def normalize_issuer(self, issuer):
        for k, v in [
            ('bnp paribas', 'BNP Paribas'),
            ('societe generale', 'Société Générale'),
            ('unicredit', 'UniCredit'),
            ('credit agricole', 'Crédit Agricole'),
            ('goldman sachs', 'Goldman Sachs'),
            ('vontobel', 'Vontobel'),
            ('barclays', 'Barclays'),
            ('citigroup', 'Citigroup'),
            ('mediobanca', 'Mediobanca'),
            ('intesa sanpaolo', 'Intesa Sanpaolo'),
            ('natixis', 'Natixis'),
            ('marex', 'Marex Financial'),
            ('leonteq', 'Leonteq Securities'),
            ('morgan stanley', 'Morgan Stanley'),
            ('jp morgan', 'JP Morgan'),
            ('deutsche bank', 'Deutsche Bank'),
            ('ubs', 'UBS'),
            ('credit suisse', 'Credit Suisse'),
            ('hsbc', 'HSBC'),
            ('nomura', 'Nomura'),
            ('smart', 'SmartETN'),
            ('akros', 'Banco BPM'),
        ]:
            if k in issuer.lower():
                return v
        return issuer
    
    def parse_date(self, s):
        if not s:
            return None
        m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', s)
        if m:
            d, mo, y = m.groups()
            return f'{y}-{mo.zfill(2)}-{d.zfill(2)}'
        return s
    
    def print_summary(self):
        print('\n' + '=' * 70)
        print('📊 SUMMARY')
        print('=' * 70)
        print(f'Pages scanned: {self.stats["pages_scanned"]}')
        print(f'Total rows processed: {self.stats["total_rows"]}')
        print(f'Skipped (leverage): {self.stats["skipped_leverage"]}')
        print(f'Skipped (stocks): {self.stats["skipped_stocks"]}')
        print(f'Matched certificates: {len(self.certificates)}')
        
        # By category
        by_cat = {}
        for c in self.certificates:
            cat = c.get('underlying_category', 'unknown')
            by_cat[cat] = by_cat.get(cat, 0) + 1
        
        print('\nBy category:')
        icons = {'index': '📊', 'commodity': '🛢️', 'currency': '💱', 'rate': '💹'}
        for cat, n in sorted(by_cat.items(), key=lambda x: -x[1]):
            print(f'   {icons.get(cat, "📄")} {cat}: {n}')
        
        # By issuer
        by_i = {}
        for c in self.certificates:
            i = c.get('issuer', '?')
            by_i[i] = by_i.get(i, 0) + 1
        
        print('\nTop issuers:')
        for i, n in sorted(by_i.items(), key=lambda x: -x[1])[:8]:
            print(f'   {i}: {n}')
        
        # By type
        by_t = {}
        for c in self.certificates:
            t = c.get('type', '?')
            by_t[t] = by_t.get(t, 0) + 1
        
        print('\nTop types:')
        for t, n in sorted(by_t.items(), key=lambda x: -x[1])[:6]:
            print(f'   {t}: {n}')
        
        # Sample underlyings
        print('\nSample underlyings:')
        seen = set()
        for c in self.certificates[:30]:
            u = c.get('underlying') or c.get('underlying_raw', '?')
            if u not in seen:
                seen.add(u)
                print(f'   • {u[:50]}')
            if len(seen) >= 8:
                break
        
        print('=' * 70)
    
    def save_results(self):
        os.makedirs(CONFIG['output_dir'], exist_ok=True)
        
        out = []
        for c in self.certificates:
            out.append({
                'isin': c['isin'],
                'name': c['name'],
                'type': c['type'],
                'issuer': c['issuer'],
                'market': c.get('market', 'SeDeX'),
                'currency': c.get('currency', 'EUR'),
                'underlying': c.get('underlying') or c.get('underlying_raw'),
                'underlying_category': c.get('underlying_category'),
                'issue_date': c.get('issue_date'),
                'maturity_date': c.get('maturity_date'),
                'barrier_down': c.get('barrier_down'),
                'coupon': c.get('coupon'),
                'scraped_at': datetime.now().isoformat()
            })
        
        data = {
            'metadata': {
                'version': '9.0-database-pagination',
                'timestamp': datetime.now().isoformat(),
                'source': 'certificatiederivati.it',
                'total': len(out),
                'pages_scanned': self.stats['pages_scanned'],
                'categories': {
                    'index': len([c for c in out if c['underlying_category'] == 'index']),
                    'commodity': len([c for c in out if c['underlying_category'] == 'commodity']),
                    'currency': len([c for c in out if c['underlying_category'] == 'currency']),
                    'rate': len([c for c in out if c['underlying_category'] == 'rate']),
                },
                'method': 'database_pagination'
            },
            'certificates': out
        }
        
        p1 = os.path.join(CONFIG['output_dir'], 'certificates-data.json')
        with open(p1, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        with open('certificates-data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f'\n💾 Saved {len(out)} certificates')
        print(f'   → {p1}')
        print(f'   → certificates-data.json')


if __name__ == '__main__':
    scraper = DatabaseScraper()
    scraper.run()
