#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

Production Certificates Scraper v4.0 - FAST VERSION
Extracts certificates directly from the emissions table (no detail pages needed)

SPEED OPTIMIZATION:
- Extracts all data from emissions page table in ONE request
- No need to visit individual certificate pages
- Completes in ~30 seconds instead of 10+ minutes
"""

import json
import re
import os
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

CONFIG = {
    'emissions_url': 'https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp',
    'max_certificates': 100,
    'timeout': 30000,
    'output_dir': 'data'
}


class FastCertificatesScraper:
    def __init__(self):
        self.certificates = []
        
    def run(self):
        print('=' * 70)
        print('CERTIFICATES SCRAPER v4.0 - FAST MODE')
        print('Copyright (c) 2024-2025 Mutna S.R.L.S.')
        print('=' * 70)
        print(f'Date: {datetime.now().isoformat()}')
        print(f'Source: {CONFIG["emissions_url"]}')
        print('=' * 70)
        
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()
        
        try:
            print('\n📋 Loading emissions page...')
            page.goto(CONFIG['emissions_url'], timeout=CONFIG['timeout'], wait_until='domcontentloaded')
            page.wait_for_timeout(3000)
            
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Find the investment certificates table (first table with ISIN header)
            tables = soup.find_all('table')
            print(f'   Found {len(tables)} tables')
            
            investment_certs = []
            leverage_certs = []
            
            for table in tables:
                rows = table.find_all('tr')
                
                # Check if this is a data table (has ISIN in first column)
                for row in rows[1:]:  # Skip header
                    cells = row.find_all('td')
                    if len(cells) >= 6:
                        isin = cells[0].get_text(strip=True)
                        
                        # Validate ISIN format
                        if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                            continue
                        
                        name = cells[1].get_text(strip=True)
                        emittente = cells[2].get_text(strip=True)
                        sottostante = cells[3].get_text(strip=True)
                        mercato = cells[4].get_text(strip=True)
                        data = cells[5].get_text(strip=True)
                        
                        # Categorize
                        name_upper = name.upper()
                        if 'TURBO' in name_upper or 'LEVA FISSA' in name_upper:
                            leverage_certs.append(isin)
                            continue
                        
                        # Build certificate object
                        cert = {
                            'isin': isin,
                            'name': name,
                            'type': self.detect_type(name),
                            'issuer': self.normalize_issuer(emittente),
                            'market': 'SeDeX' if mercato == 'SED' else 'Cert-X' if mercato == 'CX' else mercato,
                            'currency': 'EUR',
                            'underlying_name': self.parse_underlying(sottostante, name),
                            'issue_date': self.parse_date(data),
                            'reference_price': 100,
                            'scraped_at': datetime.now().isoformat()
                        }
                        
                        investment_certs.append(cert)
            
            print(f'\n✅ Extracted {len(investment_certs)} investment certificates')
            print(f'   (Skipped {len(leverage_certs)} leverage products)')
            
            # Limit to max
            self.certificates = investment_certs[:CONFIG['max_certificates']]
            
            # Show sample
            if self.certificates:
                print('\n📊 Sample certificates:')
                for cert in self.certificates[:5]:
                    print(f'   {cert["isin"]}: {cert["type"]} | {cert["issuer"]} | {cert["underlying_name"]}')
            
            # Save
            self.save_results()
            
        except Exception as e:
            print(f'\n❌ Error: {e}')
            raise
        finally:
            context.close()
            browser.close()
            playwright.stop()
            print('\n🔒 Browser closed')
    
    def detect_type(self, name):
        """Detect certificate type from name"""
        name_lower = name.lower()
        
        types = {
            'phoenix memory': 'Phoenix Memory',
            'phoenix': 'Phoenix',
            'cash collect': 'Cash Collect',
            'bonus': 'Bonus Cap',
            'express': 'Express',
            'twin win': 'Twin Win',
            'airbag': 'Airbag',
            'autocallable': 'Autocallable',
            'reverse': 'Reverse Convertible',
            'memory': 'Memory',
            'digital': 'Digital',
            'equity protection': 'Equity Protection',
            'credit linked': 'Credit Linked',
            'fixed': 'Fixed Coupon',
            'top bonus': 'Top Bonus',
            'maxi coupon': 'Maxi Coupon',
            'step down': 'Step Down',
            'callable': 'Callable'
        }
        
        for keyword, cert_type in types.items():
            if keyword in name_lower:
                return cert_type
        
        return 'Certificate'
    
    def normalize_issuer(self, issuer):
        """Normalize issuer name"""
        issuer_map = {
            'Bnp Paribas': 'BNP Paribas',
            'Societe Generale': 'Société Générale',
            'Unicredit': 'UniCredit',
            'Credit Agricole': 'Crédit Agricole',
            'Goldman Sachs': 'Goldman Sachs',
            'Vontobel': 'Vontobel',
            'Barclays': 'Barclays',
            'Citigroup': 'Citigroup',
            'Mediobanca': 'Mediobanca',
            'Intesa Sanpaolo': 'Intesa Sanpaolo',
            'Natixis': 'Natixis',
            'Marex Financial': 'Marex Financial'
        }
        
        for key, value in issuer_map.items():
            if key.lower() in issuer.lower():
                return value
        
        return issuer
    
    def parse_underlying(self, sottostante, name):
        """Parse underlying from sottostante field or certificate name"""
        # If it's a generic description, try to extract from name
        if sottostante.lower() in ['basket di azioni worst of', 'singolo sottostante', 
                                    'basket di indici worst of', 'basket obbligazioni',
                                    'basket sottostanti misti']:
            # Try to extract from name (pattern: "su UNDERLYING")
            match = re.search(r'\bsu\s+([A-Za-z][A-Za-z0-9\s,&\.\-\']+)', name, re.IGNORECASE)
            if match:
                underlying = match.group(1).strip()
                # Clean up
                underlying = re.sub(r'\s+con\s+.*$', '', underlying, flags=re.IGNORECASE)
                underlying = re.sub(r'\s+\d+.*$', '', underlying)
                if len(underlying) >= 2:
                    return underlying
            
            # Return the generic type
            return sottostante
        
        return sottostante
    
    def parse_date(self, date_str):
        """Parse date to ISO format"""
        if not date_str:
            return None
        
        # Format: DD/MM/YYYY
        match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', date_str)
        if match:
            day, month, year = match.groups()
            return f'{year}-{month.zfill(2)}-{day.zfill(2)}'
        
        return date_str
    
    def save_results(self):
        """Save to JSON"""
        os.makedirs(CONFIG['output_dir'], exist_ok=True)
        
        output = {
            'metadata': {
                'scraper_version': '4.0-fast',
                'timestamp': datetime.now().isoformat(),
                'source': 'certificatiederivati.it',
                'total_certificates': len(self.certificates),
                'scrape_date': datetime.now().strftime('%Y-%m-%d')
            },
            'certificates': self.certificates
        }
        
        # Save to data/
        path1 = os.path.join(CONFIG['output_dir'], 'certificates-data.json')
        with open(path1, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        # Save to root
        with open('certificates-data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f'\n💾 Saved {len(self.certificates)} certificates')
        print(f'   → {path1}')
        print(f'   → certificates-data.json')
        
        # Stats
        by_type = {}
        by_issuer = {}
        for c in self.certificates:
            by_type[c['type']] = by_type.get(c['type'], 0) + 1
            by_issuer[c['issuer']] = by_issuer.get(c['issuer'], 0) + 1
        
        print('\n📊 By type:')
        for t, count in sorted(by_type.items(), key=lambda x: -x[1])[:5]:
            print(f'   {t}: {count}')
        
        print('\n📊 By issuer:')
        for i, count in sorted(by_issuer.items(), key=lambda x: -x[1])[:5]:
            print(f'   {i}: {count}')


if __name__ == '__main__':
    scraper = FastCertificatesScraper()
    scraper.run()
