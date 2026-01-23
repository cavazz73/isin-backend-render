#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

Production Certificates Scraper v6.0
Extracts certificates on: INDICES, RATES, CURRENCIES, COMMODITIES
EXCLUDES: Single stocks, basket of stocks

Target underlyings:
- Indices: FTSE MIB, Euro Stoxx 50, S&P 500, DAX, Nasdaq 100, etc.
- Rates: Euribor, interest rates
- Currencies: EUR/USD, USD/JPY, etc.
- Commodities: Gold, Silver, Oil, Gas, etc.
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
    'max_certificates': 100,
    'page_timeout': 15000,
    'wait_for_content': 3000,
    'wait_between_pages': 1000,
    'output_dir': 'data'
}

# ============================================
# ALLOWED UNDERLYINGS
# ============================================

INDICES = {
    'FTSE MIB', 'FTSEMIB', 'EURO STOXX 50', 'EUROSTOXX 50', 'EUROSTOXX50', 'SX5E',
    'STOXX 600', 'DAX', 'DAX 40', 'CAC 40', 'CAC40', 'IBEX 35', 'IBEX',
    'S&P 500', 'S&P500', 'SPX', 'SP500', 'NASDAQ', 'NASDAQ 100', 'NASDAQ100', 'NDX',
    'DOW JONES', 'DJIA', 'RUSSELL 2000', 'NIKKEI', 'NIKKEI 225', 'HANG SENG', 'HSI',
    'SMI', 'AEX', 'MSCI WORLD', 'MSCI EMERGING', 'STOXX BANKS', 'EURO STOXX BANKS',
}

RATES = {
    'EURIBOR', 'EURIBOR 3M', 'EURIBOR 6M', 'EURIBOR 12M',
    'LIBOR', 'SOFR', 'ESTR', 'BUND', 'BTP', 'TREASURY', 'SWAP', 'IRS', 'CMS',
}

CURRENCIES = {
    'EUR/USD', 'EURUSD', 'EUR/GBP', 'EURGBP', 'EUR/JPY', 'EURJPY',
    'EUR/CHF', 'EURCHF', 'USD/JPY', 'USDJPY', 'GBP/USD', 'GBPUSD',
    'USD/CHF', 'USDCHF', 'AUD/USD', 'AUDUSD', 'USD/CAD', 'USDCAD',
}

COMMODITIES = {
    'WTI', 'WTI CRUDE', 'CRUDE OIL', 'OIL', 'PETROLIO', 'BRENT', 'BRENT CRUDE',
    'NATURAL GAS', 'GAS NATURALE', 'NAT GAS', 'GOLD', 'ORO', 'XAU',
    'SILVER', 'ARGENTO', 'XAG', 'PLATINUM', 'PLATINO', 'PALLADIUM', 'PALLADIO',
    'COPPER', 'RAME', 'WHEAT', 'GRANO', 'CORN', 'MAIS', 'SOYBEAN', 'SOIA',
}


def normalize_underlying(name):
    """Normalize underlying name and return category"""
    if not name:
        return None, None
    
    name_upper = name.upper().strip()
    
    # Indices
    if any(x in name_upper for x in ['EURO STOXX 50', 'EUROSTOXX', 'SX5E']):
        return 'Euro Stoxx 50', 'index'
    if any(x in name_upper for x in ['FTSE MIB', 'FTSEMIB', 'FTSE/MIB']):
        return 'FTSE MIB', 'index'
    if any(x in name_upper for x in ['S&P 500', 'S&P500', 'SPX', 'SP500']):
        return 'S&P 500', 'index'
    if 'NASDAQ' in name_upper:
        return 'Nasdaq 100', 'index'
    if 'DAX' in name_upper:
        return 'DAX', 'index'
    if 'NIKKEI' in name_upper:
        return 'Nikkei 225', 'index'
    if 'HANG SENG' in name_upper or 'HSI' in name_upper:
        return 'Hang Seng', 'index'
    if 'CAC' in name_upper:
        return 'CAC 40', 'index'
    if 'IBEX' in name_upper:
        return 'IBEX 35', 'index'
    if 'STOXX 600' in name_upper:
        return 'Stoxx 600', 'index'
    if 'STOXX BANKS' in name_upper or 'SX7E' in name_upper:
        return 'Euro Stoxx Banks', 'index'
    if 'DOW JONES' in name_upper or 'DJIA' in name_upper:
        return 'Dow Jones', 'index'
    if 'RUSSELL' in name_upper:
        return 'Russell 2000', 'index'
    if 'SMI' in name_upper:
        return 'SMI', 'index'
    if 'AEX' in name_upper:
        return 'AEX', 'index'
    if 'MSCI' in name_upper:
        return 'MSCI World', 'index'
    
    # Rates
    if 'EURIBOR' in name_upper:
        if '3M' in name_upper:
            return 'Euribor 3M', 'rate'
        if '6M' in name_upper:
            return 'Euribor 6M', 'rate'
        return 'Euribor', 'rate'
    if 'CMS' in name_upper:
        return 'CMS', 'rate'
    if 'BTP' in name_upper:
        return 'BTP', 'rate'
    if 'BUND' in name_upper:
        return 'Bund', 'rate'
    
    # Currencies
    if 'EUR' in name_upper and 'USD' in name_upper:
        return 'EUR/USD', 'currency'
    if 'EUR' in name_upper and 'GBP' in name_upper:
        return 'EUR/GBP', 'currency'
    if 'USD' in name_upper and 'JPY' in name_upper:
        return 'USD/JPY', 'currency'
    if 'GBP' in name_upper and 'USD' in name_upper:
        return 'GBP/USD', 'currency'
    
    # Commodities
    if any(x in name_upper for x in ['GOLD', 'ORO', 'XAU']):
        return 'Gold', 'commodity'
    if any(x in name_upper for x in ['SILVER', 'ARGENTO', 'XAG']):
        return 'Silver', 'commodity'
    if any(x in name_upper for x in ['WTI', 'CRUDE', 'PETROLIO', 'OIL']):
        return 'WTI Crude Oil', 'commodity'
    if 'BRENT' in name_upper:
        return 'Brent Crude', 'commodity'
    if any(x in name_upper for x in ['NATURAL GAS', 'GAS NATURALE', 'NAT GAS']):
        return 'Natural Gas', 'commodity'
    if any(x in name_upper for x in ['COPPER', 'RAME']):
        return 'Copper', 'commodity'
    if any(x in name_upper for x in ['PLATINUM', 'PLATINO']):
        return 'Platinum', 'commodity'
    
    return None, None


def check_certificate(sottostante_raw, cert_name):
    """Check if certificate should be included and extract underlying"""
    
    text = (sottostante_raw + ' ' + cert_name).upper()
    
    # EXCLUDE: Basket of stocks
    if 'BASKET DI AZIONI' in text or 'AZIONI WORST OF' in text:
        return False, None, None
    
    # INCLUDE: Basket of indices
    if 'BASKET DI INDICI' in text or 'INDICI WORST OF' in text:
        return True, 'Basket of Indices', 'index'
    
    # Try extract from "su UNDERLYING" pattern
    match = re.search(r'\bsu\s+([A-Za-z0-9\s&/\-\.]+?)(?:\s+con|\s+Barriera|\s+\d|,|$)', cert_name, re.IGNORECASE)
    if match:
        underlying_text = match.group(1).strip()
        normalized, category = normalize_underlying(underlying_text)
        if normalized and category:
            return True, normalized, category
    
    # Check whole name
    normalized, category = normalize_underlying(cert_name)
    if normalized and category:
        return True, normalized, category
    
    # Check raw sottostante
    if sottostante_raw.lower() not in ['singolo sottostante', 'basket di azioni worst of']:
        normalized, category = normalize_underlying(sottostante_raw)
        if normalized and category:
            return True, normalized, category
    
    return False, None, None


class IndicesRatesCommoditiesScraper:
    def __init__(self):
        self.certificates = []
        self.excluded = 0
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
        print('CERTIFICATES SCRAPER v6.0')
        print('Target: INDICES | RATES | CURRENCIES | COMMODITIES')
        print('Excludes: Stocks, basket of stocks')
        print('Copyright (c) 2024-2025 Mutna S.R.L.S.')
        print('=' * 70)
        print(f'Date: {datetime.now().isoformat()}')
        print('=' * 70)
        
        self.start_browser()
        
        try:
            # Load emissions
            print('\n📋 Loading latest emissions from all issuers...')
            all_certs = self.get_emissions()
            print(f'   Total: {len(all_certs)} investment certificates')
            
            # Filter
            print('\n🔍 Filtering...\n')
            
            for cert in all_certs:
                ok, underlying, category = check_certificate(
                    cert.get('sottostante_raw', ''),
                    cert.get('name', '')
                )
                
                if ok and underlying:
                    cert['underlying'] = underlying
                    cert['underlying_category'] = category
                    self.certificates.append(cert)
                    
                    icon = {'index': '📊', 'rate': '💹', 'currency': '💱', 'commodity': '🛢️'}.get(category, '📄')
                    print(f'{icon} {cert["isin"]}: {underlying} | {cert["issuer"]}')
                else:
                    self.excluded += 1
            
            print(f'\n✅ Matched: {len(self.certificates)}')
            print(f'❌ Excluded (stocks): {self.excluded}')
            
            # Get details
            if self.certificates:
                print(f'\n📋 Fetching details for {len(self.certificates)} certificates...')
                for i, cert in enumerate(self.certificates):
                    detail = self.get_detail(cert['isin'])
                    if detail:
                        cert.update(detail)
                    if (i + 1) % 10 == 0:
                        print(f'   {i + 1}/{len(self.certificates)}')
                    time.sleep(CONFIG['wait_between_pages'] / 1000)
            
            self.print_summary()
            self.save()
            
        finally:
            self.close_browser()
            print('\n🔒 Browser closed')
    
    def get_emissions(self):
        page = self.context.new_page()
        certs = []
        
        try:
            page.goto(CONFIG['emissions_url'], timeout=CONFIG['page_timeout'], wait_until='domcontentloaded')
            page.wait_for_timeout(2000)
            
            soup = BeautifulSoup(page.content(), 'html.parser')
            
            for table in soup.find_all('table'):
                for row in table.find_all('tr')[1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 6:
                        isin = cells[0].get_text(strip=True)
                        if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                            continue
                        
                        name = cells[1].get_text(strip=True)
                        if 'TURBO' in name.upper() or 'LEVA FISSA' in name.upper():
                            continue
                        
                        certs.append({
                            'isin': isin,
                            'name': name,
                            'type': self.detect_type(name),
                            'issuer': self.normalize_issuer(cells[2].get_text(strip=True)),
                            'market': cells[4].get_text(strip=True),
                            'sottostante_raw': cells[3].get_text(strip=True),
                            'issue_date': self.parse_date(cells[5].get_text(strip=True)),
                            'currency': 'EUR',
                        })
        except Exception as e:
            print(f'   ⚠️ Error: {e}')
        finally:
            page.close()
        
        return certs
    
    def get_detail(self, isin):
        page = self.context.new_page()
        data = {}
        
        try:
            page.goto(f'{CONFIG["detail_url"]}{isin}', timeout=CONFIG['page_timeout'], wait_until='domcontentloaded')
            page.wait_for_timeout(CONFIG['wait_for_content'])
            
            soup = BeautifulSoup(page.content(), 'html.parser')
            
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).upper()
                        value = cells[1].get_text(strip=True)
                        
                        if 'BARRIERA' in label:
                            m = re.search(r'(\d+[,.]?\d*)', value)
                            if m:
                                data['barrier_down'] = float(m.group(1).replace(',', '.'))
                        
                        elif any(x in label for x in ['CEDOLA', 'COUPON', 'PREMIO']):
                            m = re.search(r'(\d+[,.]?\d*)', value)
                            if m:
                                data['coupon'] = float(m.group(1).replace(',', '.'))
                        
                        elif 'SCADENZA' in label and 'DATA' in label:
                            data['maturity_date'] = self.parse_date(value)
        except:
            pass
        finally:
            page.close()
        
        return data
    
    def detect_type(self, name):
        n = name.lower()
        for kw, t in [('phoenix memory', 'Phoenix Memory'), ('cash collect', 'Cash Collect'),
                      ('bonus cap', 'Bonus Cap'), ('top bonus', 'Top Bonus'), ('express', 'Express'),
                      ('equity protection', 'Equity Protection'), ('credit linked', 'Credit Linked'),
                      ('digital', 'Digital'), ('airbag', 'Airbag'), ('memory', 'Memory'),
                      ('phoenix', 'Phoenix'), ('reverse', 'Reverse'), ('fixed', 'Fixed Coupon')]:
            if kw in n:
                return t
        return 'Certificate'
    
    def normalize_issuer(self, issuer):
        for k, v in [('bnp paribas', 'BNP Paribas'), ('societe generale', 'Société Générale'),
                     ('unicredit', 'UniCredit'), ('credit agricole', 'Crédit Agricole'),
                     ('goldman sachs', 'Goldman Sachs'), ('vontobel', 'Vontobel'),
                     ('barclays', 'Barclays'), ('citigroup', 'Citigroup'),
                     ('mediobanca', 'Mediobanca'), ('intesa sanpaolo', 'Intesa Sanpaolo'),
                     ('natixis', 'Natixis'), ('marex', 'Marex Financial'),
                     ('leonteq', 'Leonteq Securities'), ('morgan stanley', 'Morgan Stanley')]:
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
        
        total = len(self.certificates)
        print(f'Certificates: {total}')
        
        by_cat = {}
        for c in self.certificates:
            cat = c.get('underlying_category', 'unknown')
            by_cat[cat] = by_cat.get(cat, 0) + 1
        
        print('\nBy category:')
        for cat, n in sorted(by_cat.items(), key=lambda x: -x[1]):
            icon = {'index': '📊', 'rate': '💹', 'currency': '💱', 'commodity': '🛢️'}.get(cat, '📄')
            print(f'   {icon} {cat}: {n}')
        
        by_u = {}
        for c in self.certificates:
            u = c.get('underlying', '?')
            by_u[u] = by_u.get(u, 0) + 1
        
        print('\nTop underlyings:')
        for u, n in sorted(by_u.items(), key=lambda x: -x[1])[:10]:
            print(f'   {u}: {n}')
        
        by_i = {}
        for c in self.certificates:
            i = c.get('issuer', '?')
            by_i[i] = by_i.get(i, 0) + 1
        
        print('\nBy issuer:')
        for i, n in sorted(by_i.items(), key=lambda x: -x[1])[:8]:
            print(f'   {i}: {n}')
        
        print('=' * 70)
    
    def save(self):
        os.makedirs(CONFIG['output_dir'], exist_ok=True)
        
        out = []
        for c in self.certificates:
            out.append({
                'isin': c['isin'],
                'name': c['name'],
                'type': c['type'],
                'issuer': c['issuer'],
                'market': c['market'],
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
                'version': '6.0',
                'timestamp': datetime.now().isoformat(),
                'source': 'certificatiederivati.it',
                'total': len(out),
                'filter': 'indices, rates, currencies, commodities'
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
    scraper = IndicesRatesCommoditiesScraper()
    scraper.run()
