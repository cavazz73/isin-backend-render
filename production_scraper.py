#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

Production Certificates Scraper v7.0 - ADVANCED SEARCH
Uses the advanced search to find certificates on:
- INDICES (FTSE MIB, Euro Stoxx 50, S&P 500, DAX, Nasdaq 100, Nikkei 225...)
- COMMODITIES (Gold, Silver, Oil, Gas, Copper...)
- CURRENCIES (EUR/USD, EUR/JPY, GBP/USD...)
- RATES (Euribor, BTP, Bund...)

Strategy:
1. Query advanced search for each target underlying
2. Collect all matching certificates
3. Get details from certificate pages
"""

import json
import re
import time
import os
from datetime import datetime
from urllib.parse import quote
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

CONFIG = {
    'search_url': 'https://www.certificatiederivati.it/db_bs_estrazione_ricerca.asp',
    'detail_url': 'https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin=',
    'page_timeout': 20000,
    'wait_for_content': 2000,
    'wait_between_searches': 1500,
    'wait_between_details': 800,
    'output_dir': 'data'
}

# Target underlyings to search for
TARGET_UNDERLYINGS = {
    # INDICES
    'index': [
        'FTSE MIB',
        'Euro Stoxx 50',
        'Euro STOXX 50',
        'EUROSTOXX 50',
        'DAX',
        'S&P 500',
        'Nasdaq 100',
        'Nasdaq-100',
        'Nikkei 225',
        'CAC 40',
        'IBEX 35',
        'Hang Seng',
        'Eurostoxx Banks',
        'Euro STOXX Banks',
        'STOXX 600',
        'Dow Jones',
        'Russell 2000',
        'SMI',
    ],
    
    # COMMODITIES
    'commodity': [
        'Gold',
        'Oro',
        'Silver',
        'Argento',
        'WTI',
        'Crude Oil',
        'Brent',
        'Natural Gas',
        'Gas Naturale',
        'Copper',
        'Rame',
        'Platinum',
        'Platino',
        'Palladium',
        'Palladio',
        'Wheat',
        'Grano',
        'Corn',
        'Mais',
        'Soybean',
        'Coffee',
        'Sugar',
        'Cotton',
    ],
    
    # CURRENCIES
    'currency': [
        'EUR/USD',
        'EUR/JPY',
        'EUR/GBP',
        'EUR/CHF',
        'USD/JPY',
        'GBP/USD',
        'USD/CHF',
        'EUR/CAD',
    ],
    
    # RATES
    'rate': [
        'Euribor',
        'BTP',
        'Bund',
        'Euro Bund',
        'T-Bond',
        'T-Note',
        'CMS',
    ],
}


class AdvancedSearchScraper:
    def __init__(self):
        self.certificates = {}  # Use dict to dedupe by ISIN
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
        print('CERTIFICATES SCRAPER v7.0 - ADVANCED SEARCH')
        print('Target: INDICES | COMMODITIES | CURRENCIES | RATES')
        print('Copyright (c) 2024-2025 Mutna S.R.L.S.')
        print('=' * 70)
        print(f'Date: {datetime.now().isoformat()}')
        print('=' * 70)
        
        self.start_browser()
        
        try:
            # Search for each category
            for category, underlyings in TARGET_UNDERLYINGS.items():
                icon = {'index': '📊', 'commodity': '🛢️', 'currency': '💱', 'rate': '💹'}.get(category, '📄')
                print(f'\n{icon} Searching {category.upper()} underlyings...')
                
                for underlying in underlyings:
                    count = self.search_underlying(underlying, category)
                    if count > 0:
                        print(f'   ✓ {underlying}: {count} certificates')
                    time.sleep(CONFIG['wait_between_searches'] / 1000)
            
            print(f'\n✅ Total unique certificates found: {len(self.certificates)}')
            
            # Get details for all certificates
            if self.certificates:
                print(f'\n📋 Fetching details...')
                certs_list = list(self.certificates.values())
                for i, cert in enumerate(certs_list):
                    detail = self.get_certificate_detail(cert['isin'])
                    if detail:
                        cert.update(detail)
                    if (i + 1) % 20 == 0:
                        print(f'   {i + 1}/{len(certs_list)}')
                    time.sleep(CONFIG['wait_between_details'] / 1000)
            
            # Summary and save
            self.print_summary()
            self.save_results()
            
        finally:
            self.close_browser()
            print('\n🔒 Browser closed')
    
    def search_underlying(self, underlying, category):
        """Search for certificates with specific underlying"""
        page = self.context.new_page()
        count = 0
        
        try:
            # Build search URL
            # db=2 = investment certificates
            # fase=quotazione = currently trading
            params = f'db=2&sottostanteC={quote(underlying)}&fase=quotazione&FiltroDal=2020-1-1&FiltroAl=2099-12-31'
            url = f'{CONFIG["search_url"]}?{params}'
            
            page.goto(url, timeout=CONFIG['page_timeout'], wait_until='domcontentloaded')
            page.wait_for_timeout(CONFIG['wait_for_content'])
            
            soup = BeautifulSoup(page.content(), 'html.parser')
            
            # Find results table
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header
                    cells = row.find_all('td')
                    if len(cells) >= 5:
                        isin = cells[0].get_text(strip=True)
                        
                        if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                            continue
                        
                        # Skip if already found
                        if isin in self.certificates:
                            continue
                        
                        name = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                        emittente = cells[2].get_text(strip=True) if len(cells) > 2 else ''
                        
                        # Skip leverage products
                        name_upper = name.upper()
                        if 'TURBO' in name_upper or 'LEVA FISSA' in name_upper or 'MINI FUTURE' in name_upper:
                            continue
                        
                        self.certificates[isin] = {
                            'isin': isin,
                            'name': name,
                            'type': self.detect_type(name),
                            'issuer': self.normalize_issuer(emittente),
                            'underlying': self.normalize_underlying(underlying),
                            'underlying_category': category,
                            'currency': 'EUR',
                        }
                        count += 1
            
        except Exception as e:
            pass  # Silent fail for individual searches
        finally:
            page.close()
        
        return count
    
    def get_certificate_detail(self, isin):
        """Get additional details from certificate page"""
        page = self.context.new_page()
        data = {}
        
        try:
            url = f'{CONFIG["detail_url"]}{isin}'
            page.goto(url, timeout=CONFIG['page_timeout'], wait_until='domcontentloaded')
            page.wait_for_timeout(CONFIG['wait_for_content'])
            
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
                        
                        elif 'EMISSIONE' in label and 'DATA' in label:
                            data['issue_date'] = self.parse_date(value)
                        
                        elif 'MERCATO' in label:
                            data['market'] = value
                        
                        elif 'FASE' in label:
                            data['phase'] = value.lower()
        except:
            pass
        finally:
            page.close()
        
        return data
    
    def normalize_underlying(self, underlying):
        """Normalize underlying name"""
        mappings = {
            'FTSE MIB': 'FTSE MIB',
            'Euro Stoxx 50': 'Euro Stoxx 50',
            'Euro STOXX 50': 'Euro Stoxx 50',
            'EUROSTOXX 50': 'Euro Stoxx 50',
            'Eurostoxx Banks': 'Euro Stoxx Banks',
            'Euro STOXX Banks': 'Euro Stoxx Banks',
            'S&P 500': 'S&P 500',
            'Nasdaq 100': 'Nasdaq 100',
            'Nasdaq-100': 'Nasdaq 100',
            'Nikkei 225': 'Nikkei 225',
            'CAC 40': 'CAC 40',
            'Hang Seng': 'Hang Seng',
            'Gold': 'Gold',
            'Oro': 'Gold',
            'Silver': 'Silver',
            'Argento': 'Silver',
            'WTI': 'WTI Crude Oil',
            'Crude Oil': 'WTI Crude Oil',
            'Brent': 'Brent Crude',
            'Natural Gas': 'Natural Gas',
            'Gas Naturale': 'Natural Gas',
            'Copper': 'Copper',
            'Rame': 'Copper',
            'Platinum': 'Platinum',
            'Platino': 'Platinum',
            'Palladium': 'Palladium',
            'Palladio': 'Palladium',
            'Euro Bund': 'Euro Bund',
            'BTP': 'BTP',
            'Euribor': 'Euribor',
        }
        return mappings.get(underlying, underlying)
    
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
        certs = list(self.certificates.values())
        
        print('\n' + '=' * 70)
        print('📊 SUMMARY')
        print('=' * 70)
        print(f'Total certificates: {len(certs)}')
        
        # By category
        by_cat = {}
        for c in certs:
            cat = c.get('underlying_category', 'unknown')
            by_cat[cat] = by_cat.get(cat, 0) + 1
        
        print('\nBy category:')
        icons = {'index': '📊', 'commodity': '🛢️', 'currency': '💱', 'rate': '💹'}
        for cat, n in sorted(by_cat.items(), key=lambda x: -x[1]):
            print(f'   {icons.get(cat, "📄")} {cat}: {n}')
        
        # By underlying
        by_u = {}
        for c in certs:
            u = c.get('underlying', '?')
            by_u[u] = by_u.get(u, 0) + 1
        
        print('\nTop underlyings:')
        for u, n in sorted(by_u.items(), key=lambda x: -x[1])[:15]:
            print(f'   {u}: {n}')
        
        # By issuer
        by_i = {}
        for c in certs:
            i = c.get('issuer', '?')
            by_i[i] = by_i.get(i, 0) + 1
        
        print('\nBy issuer:')
        for i, n in sorted(by_i.items(), key=lambda x: -x[1])[:10]:
            print(f'   {i}: {n}')
        
        # By type
        by_t = {}
        for c in certs:
            t = c.get('type', '?')
            by_t[t] = by_t.get(t, 0) + 1
        
        print('\nBy type:')
        for t, n in sorted(by_t.items(), key=lambda x: -x[1])[:8]:
            print(f'   {t}: {n}')
        
        # With data
        with_barrier = len([c for c in certs if c.get('barrier_down')])
        with_coupon = len([c for c in certs if c.get('coupon')])
        print(f'\nWith barrier: {with_barrier}')
        print(f'With coupon: {with_coupon}')
        
        print('=' * 70)
    
    def save_results(self):
        os.makedirs(CONFIG['output_dir'], exist_ok=True)
        
        certs = list(self.certificates.values())
        
        # Filter only active certificates
        active_certs = [c for c in certs if c.get('phase', 'quotazione') == 'quotazione']
        
        out = []
        for c in active_certs:
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
                'version': '7.0-advanced-search',
                'timestamp': datetime.now().isoformat(),
                'source': 'certificatiederivati.it',
                'total': len(out),
                'categories': ['index', 'commodity', 'currency', 'rate'],
                'method': 'advanced_search'
            },
            'certificates': out
        }
        
        p1 = os.path.join(CONFIG['output_dir'], 'certificates-data.json')
        with open(p1, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        with open('certificates-data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f'\n💾 Saved {len(out)} active certificates')
        print(f'   → {p1}')
        print(f'   → certificates-data.json')


if __name__ == '__main__':
    scraper = AdvancedSearchScraper()
    scraper.run()
