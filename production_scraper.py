#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

Production Certificates Scraper v8.0 - ALL NEW EMISSIONS
Gets ALL certificates from new emissions page (no filtering)
Frontend can filter by underlying category as needed.

Output includes underlying_category field for filtering.
"""

import json
import re
import time
import os
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

CONFIG = {
    'emissions_url': 'https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp',
    'detail_url': 'https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin=',
    'page_timeout': 30000,
    'wait_for_content': 3000,
    'wait_between_details': 600,
    'output_dir': 'data',
    'max_details': 100,  # Limit detail page visits
}

# Keywords to categorize underlyings
UNDERLYING_CATEGORIES = {
    'index': [
        'FTSE MIB', 'FTSEMIB', 'EURO STOXX', 'EUROSTOXX', 'S&P 500', 'S&P500',
        'NASDAQ', 'DAX', 'CAC 40', 'CAC40', 'NIKKEI', 'HANG SENG', 'IBEX',
        'STOXX 600', 'DOW JONES', 'RUSSELL', 'SMI', 'MSCI', 'TOPIX',
        'INDICE', 'INDEX', 'BASKET DI INDICI', 'BASKET OF INDICES',
    ],
    'commodity': [
        'GOLD', 'ORO', 'SILVER', 'ARGENTO', 'OIL', 'PETROLIO', 'WTI', 'BRENT',
        'NATURAL GAS', 'GAS NATURALE', 'COPPER', 'RAME', 'PLATINUM', 'PLATINO',
        'PALLADIUM', 'PALLADIO', 'WHEAT', 'GRANO', 'CORN', 'MAIS', 'COFFEE',
        'SUGAR', 'COTTON', 'SOYBEAN', 'COMMODITY', 'COMMODITIES',
    ],
    'currency': [
        'EUR/USD', 'EUR/JPY', 'EUR/GBP', 'EUR/CHF', 'USD/JPY', 'GBP/USD',
        'USD/CHF', 'EUR/CAD', 'FOREX', 'CURRENCY', 'VALUTA', 'CAMBIO',
    ],
    'rate': [
        'EURIBOR', 'LIBOR', 'SOFR', 'BTP', 'BUND', 'T-BOND', 'CMS', 'SWAP',
        'TASSO', 'RATE', 'OAT', 'T-NOTE', 'INTEREST',
    ],
}


class EmissionsScraper:
    def __init__(self):
        self.certificates = []
        self.browser = None
        self.context = None
        self.playwright = None
        
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
        print('CERTIFICATES SCRAPER v8.0 - ALL NEW EMISSIONS')
        print('Gets all certificates, categories for filtering')
        print('Copyright (c) 2024-2025 Mutna S.R.L.S.')
        print('=' * 70)
        print(f'Date: {datetime.now().isoformat()}')
        print('=' * 70)
        
        self.start_browser()
        
        try:
            # Step 1: Get all new emissions
            self.scrape_emissions()
            
            # Step 2: Get details for certificates (limited)
            self.fetch_details()
            
            # Step 3: Summary and save
            self.print_summary()
            self.save_results()
            
        finally:
            self.close_browser()
            print('\n🔒 Browser closed')
    
    def scrape_emissions(self):
        """Scrape all certificates from new emissions page"""
        page = self.context.new_page()
        
        try:
            print(f'\n📋 Loading new emissions...')
            page.goto(CONFIG['emissions_url'], timeout=CONFIG['page_timeout'], wait_until='domcontentloaded')
            page.wait_for_timeout(CONFIG['wait_for_content'])
            
            soup = BeautifulSoup(page.content(), 'html.parser')
            
            # Find all tables
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows[1:]:  # Skip header
                    cells = row.find_all('td')
                    if len(cells) >= 6:
                        isin = cells[0].get_text(strip=True)
                        
                        # Validate ISIN
                        if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                            continue
                        
                        name = cells[1].get_text(strip=True)
                        emittente = cells[2].get_text(strip=True)
                        sottostante_raw = cells[3].get_text(strip=True)
                        mercato = cells[4].get_text(strip=True)
                        data_str = cells[5].get_text(strip=True)
                        
                        # Skip leverage products
                        name_upper = name.upper()
                        if any(x in name_upper for x in ['TURBO', 'LEVA FISSA', 'MINI FUTURE', 'STAYUP', 'STAYDOWN', 'CORRIDOR']):
                            continue
                        
                        # Extract underlying from name (e.g., "su Tesla, NVIDIA")
                        underlying, category = self.extract_underlying(name, sottostante_raw)
                        
                        cert = {
                            'isin': isin,
                            'name': name,
                            'type': self.detect_type(name),
                            'issuer': self.normalize_issuer(emittente),
                            'market': mercato,
                            'currency': 'EUR',
                            'underlying': underlying,
                            'underlying_raw': sottostante_raw,
                            'underlying_category': category,
                            'issue_date': self.parse_date(data_str),
                        }
                        
                        self.certificates.append(cert)
            
            print(f'   Found: {len(self.certificates)} investment certificates')
            
            # Show category breakdown
            by_cat = {}
            for c in self.certificates:
                cat = c['underlying_category']
                by_cat[cat] = by_cat.get(cat, 0) + 1
            
            icons = {'index': '📊', 'commodity': '🛢️', 'currency': '💱', 'rate': '💹', 'stock': '📈', 'unknown': '❓'}
            for cat, n in sorted(by_cat.items(), key=lambda x: -x[1]):
                print(f'   {icons.get(cat, "📄")} {cat}: {n}')
                
        except Exception as e:
            print(f'   ⚠️ Error: {e}')
        finally:
            page.close()
    
    def extract_underlying(self, name, sottostante_raw):
        """Extract underlying and categorize"""
        # Combine name and raw sottostante for matching
        text = f'{name} {sottostante_raw}'.upper()
        
        # Check each category
        for category, keywords in UNDERLYING_CATEGORIES.items():
            for kw in keywords:
                if kw in text:
                    # Extract specific underlying name
                    underlying = self.extract_specific_underlying(name, sottostante_raw, category)
                    return underlying, category
        
        # If no category match, it's probably a stock
        underlying = self.extract_specific_underlying(name, sottostante_raw, 'stock')
        return underlying, 'stock'
    
    def extract_specific_underlying(self, name, sottostante_raw, category):
        """Extract specific underlying name from certificate name"""
        # Try to extract from "su X, Y, Z" pattern
        match = re.search(r'\bsu\s+([^,]+(?:,\s*[^,]+)*)', name, re.IGNORECASE)
        if match:
            underlying = match.group(1).strip()
            # Clean up
            underlying = re.sub(r'\s+', ' ', underlying)
            return underlying
        
        # Use raw sottostante if available and not generic
        if sottostante_raw and not any(x in sottostante_raw.lower() for x in ['basket', 'worst of', 'singolo']):
            return sottostante_raw
        
        # Category-based defaults
        if category == 'index' and 'BASKET DI INDICI' in sottostante_raw.upper():
            return 'Basket of Indices'
        
        return sottostante_raw or 'Unknown'
    
    def fetch_details(self):
        """Fetch additional details from certificate pages"""
        # Prioritize non-stock certificates for detail fetching
        priority = [c for c in self.certificates if c['underlying_category'] != 'stock']
        others = [c for c in self.certificates if c['underlying_category'] == 'stock']
        
        to_fetch = priority + others[:max(0, CONFIG['max_details'] - len(priority))]
        
        if not to_fetch:
            return
        
        print(f'\n📋 Fetching details for {len(to_fetch)} certificates...')
        
        for i, cert in enumerate(to_fetch):
            detail = self.get_certificate_detail(cert['isin'])
            if detail:
                cert.update(detail)
            
            if (i + 1) % 20 == 0:
                print(f'   {i + 1}/{len(to_fetch)}')
            
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
                        
                        if 'BARRIERA' in label and '%' not in label:
                            m = re.search(r'(\d+[,.]?\d*)', value)
                            if m:
                                data['barrier_down'] = float(m.group(1).replace(',', '.'))
                        
                        elif any(x in label for x in ['CEDOLA', 'COUPON', 'PREMIO']):
                            m = re.search(r'(\d+[,.]?\d*)', value)
                            if m:
                                data['coupon'] = float(m.group(1).replace(',', '.'))
                        
                        elif 'SCADENZA' in label and 'DATA' in label:
                            data['maturity_date'] = self.parse_date(value)
                        
                        elif 'FASE' in label:
                            data['phase'] = value.lower()
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
        print(f'Total certificates: {len(self.certificates)}')
        
        # By category
        by_cat = {}
        for c in self.certificates:
            cat = c.get('underlying_category', 'unknown')
            by_cat[cat] = by_cat.get(cat, 0) + 1
        
        print('\nBy category:')
        icons = {'index': '📊', 'commodity': '🛢️', 'currency': '💱', 'rate': '💹', 'stock': '📈', 'unknown': '❓'}
        for cat, n in sorted(by_cat.items(), key=lambda x: -x[1]):
            print(f'   {icons.get(cat, "📄")} {cat}: {n}')
        
        # Non-stock count
        non_stock = len([c for c in self.certificates if c['underlying_category'] != 'stock'])
        print(f'\n✅ Non-stock (indices/commodities/rates/currencies): {non_stock}')
        
        # By issuer
        by_i = {}
        for c in self.certificates:
            i = c.get('issuer', '?')
            by_i[i] = by_i.get(i, 0) + 1
        
        print('\nBy issuer:')
        for i, n in sorted(by_i.items(), key=lambda x: -x[1])[:8]:
            print(f'   {i}: {n}')
        
        # By type
        by_t = {}
        for c in self.certificates:
            t = c.get('type', '?')
            by_t[t] = by_t.get(t, 0) + 1
        
        print('\nBy type:')
        for t, n in sorted(by_t.items(), key=lambda x: -x[1])[:6]:
            print(f'   {t}: {n}')
        
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
                'underlying': c.get('underlying'),
                'underlying_category': c.get('underlying_category'),
                'issue_date': c.get('issue_date'),
                'maturity_date': c.get('maturity_date'),
                'barrier_down': c.get('barrier_down'),
                'coupon': c.get('coupon'),
                'scraped_at': datetime.now().isoformat()
            })
        
        data = {
            'metadata': {
                'version': '8.0-all-emissions',
                'timestamp': datetime.now().isoformat(),
                'source': 'certificatiederivati.it',
                'total': len(out),
                'non_stock': len([c for c in out if c['underlying_category'] != 'stock']),
                'categories': {
                    'index': len([c for c in out if c['underlying_category'] == 'index']),
                    'commodity': len([c for c in out if c['underlying_category'] == 'commodity']),
                    'currency': len([c for c in out if c['underlying_category'] == 'currency']),
                    'rate': len([c for c in out if c['underlying_category'] == 'rate']),
                    'stock': len([c for c in out if c['underlying_category'] == 'stock']),
                },
                'method': 'new_emissions_all'
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
    scraper = EmissionsScraper()
    scraper.run()
