#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

Production Certificates Scraper v3.0 - CORRECTED URLs
Source: certificatiederivati.it

FIXES:
1. Correct URL: db_bs_scheda_certificato.asp?isin=ISIN (NOT /certificates/ISIN)
2. Correct emissions URL: db_bs_nuove_emissioni.asp
3. Wait for JavaScript content to load ("Scheda Sottostante")
4. Extract from actual page structure (tables with specific headers)
"""

import json
import re
import time
import os
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Configuration - CORRECTED URLs
CONFIG = {
    'base_url': 'https://www.certificatiederivati.it',
    'detail_url': 'https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin=',
    'emissions_url': 'https://www.certificatiederivati.it/db_bs_nuove_emissioni.asp',
    'search_url': 'https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp',
    'max_certificates': 100,
    'timeout': 60000,
    'wait_for_content': 5000,  # Wait for dynamic content
    'wait_between_pages': 2000,
    'output_dir': 'data'
}


class CertificatesScraperV3:
    def __init__(self):
        self.browser = None
        self.context = None
        self.playwright = None
        self.certificates = []
        self.processed_isins = set()
        
    def start_browser(self):
        """Initialize Playwright browser"""
        print('🚀 Starting browser...')
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--disable-gpu'
            ]
        )
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        print('✅ Browser started')
        
    def close_browser(self):
        """Close browser"""
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        print('🔒 Browser closed')
    
    def get_isin_list_from_emissions(self):
        """Get ISIN list from new emissions page (quotazione tab)"""
        print('\n📋 Fetching ISIN list from new emissions...')
        print(f'   URL: {CONFIG["emissions_url"]}')
        
        page = self.context.new_page()
        isins = []
        
        try:
            page.goto(CONFIG['emissions_url'], timeout=CONFIG['timeout'])
            page.wait_for_timeout(3000)
            
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Find all tables
            tables = soup.find_all('table')
            print(f'   Found {len(tables)} tables')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if cells:
                        # First cell should contain ISIN
                        first_cell = cells[0].get_text(strip=True)
                        # Validate ISIN format
                        if re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', first_cell):
                            # Skip TURBO and LEVA FISSA (leverage products)
                            if len(cells) >= 2:
                                name = cells[1].get_text(strip=True).upper() if len(cells) > 1 else ''
                                if 'TURBO' not in name and 'LEVA FISSA' not in name:
                                    isins.append(first_cell)
            
            print(f'   ✓ Found {len(isins)} investment certificates (excluding Turbo/Leva)')
            
        except Exception as e:
            print(f'   ⚠️ Error: {e}')
        finally:
            page.close()
        
        return isins[:CONFIG['max_certificates']]
    
    def scrape_certificate(self, isin):
        """Scrape single certificate from detail page"""
        if isin in self.processed_isins:
            return None
            
        url = f'{CONFIG["detail_url"]}{isin}'
        page = self.context.new_page()
        certificate = None
        
        try:
            page.goto(url, timeout=CONFIG['timeout'])
            
            # Wait for dynamic content to load
            # The page has "Caricamento in corso..." that gets replaced
            page.wait_for_timeout(CONFIG['wait_for_content'])
            
            # Try to wait for specific elements
            try:
                page.wait_for_selector('table', timeout=10000)
            except:
                pass
            
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # ---------------------------------
            # EXTRACT CERTIFICATE NAME
            # ---------------------------------
            cert_name = None
            # Look for h3 with certificate type
            h3_elems = soup.find_all('h3')
            for h3 in h3_elems:
                text = h3.get_text(strip=True)
                if text and len(text) > 3 and text.upper() not in ['BARRIERA DOWN', 'SCHEDA SOTTOSTANTE', 'SCHEDA EMITTENTE', 'DATE RILEVAMENTO']:
                    cert_name = text
                    break
            
            if not cert_name:
                cert_name = f'Certificate {isin}'
            
            # ---------------------------------
            # EXTRACT FROM MAIN INFO TABLE
            # ---------------------------------
            main_data = {}
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).upper()
                        value = cells[1].get_text(strip=True)
                        
                        if 'ISIN' in label:
                            main_data['isin'] = value
                        elif 'FASE' in label:
                            main_data['fase'] = value
                        elif 'RIMBORSO' in label and 'DATA' not in label:
                            try:
                                main_data['rimborso'] = float(value.replace(',', '.'))
                            except:
                                pass
                        elif 'DATA RIMBORSO' in label:
                            main_data['data_rimborso'] = self.parse_date(value)
                        elif 'MERCATO' in label:
                            main_data['mercato'] = value
                        elif 'DATA EMISSIONE' in label:
                            main_data['issue_date'] = self.parse_date(value)
                        elif 'DATA SCADENZA' in label:
                            main_data['maturity_date'] = self.parse_date(value)
                        elif 'VALUTA' in label and 'DATA' not in label:
                            main_data['currency'] = value
                        elif 'NOMINALE' in label:
                            try:
                                main_data['nominal'] = float(value.replace(',', '.'))
                            except:
                                pass
                        elif 'DATA STRIKE' in label:
                            main_data['strike_date'] = self.parse_date(value)
                        elif 'TRIGGER' in label and value != '':
                            try:
                                main_data['trigger'] = float(value.replace(',', '.'))
                            except:
                                pass
                        elif 'MULTIPLO' in label:
                            try:
                                main_data['multiplo'] = float(value.replace(',', '.'))
                            except:
                                pass
            
            # ---------------------------------
            # EXTRACT ISSUER (Scheda Emittente)
            # ---------------------------------
            issuer = None
            emittente_section = soup.find('h3', string=re.compile('Scheda Emittente', re.IGNORECASE))
            if emittente_section:
                parent = emittente_section.find_parent()
                if parent:
                    # Look for issuer name in next elements
                    for elem in parent.find_all(['td', 'p', 'div', 'span']):
                        text = elem.get_text(strip=True)
                        if text and len(text) > 2 and 'Rating' not in text and '@' not in text and 'http' not in text and not text.isdigit():
                            issuer = text
                            break
            
            # ---------------------------------
            # EXTRACT UNDERLYING (Scheda Sottostante)
            # ---------------------------------
            underlying_name = self.extract_underlying(soup, cert_name)
            
            # ---------------------------------
            # EXTRACT BARRIER (Barriera Down)
            # ---------------------------------
            barrier = None
            barrier_section = soup.find('h3', string=re.compile('Barriera Down', re.IGNORECASE))
            if barrier_section:
                # The barrier value is often loaded dynamically
                parent = barrier_section.find_parent()
                if parent:
                    text = parent.get_text()
                    # Look for percentage
                    match = re.search(r'(\d+[,.]?\d*)\s*%', text)
                    if match:
                        try:
                            barrier = float(match.group(1).replace(',', '.'))
                        except:
                            pass
            
            # Also try to find barrier in any table
            if not barrier:
                for table in tables:
                    text = table.get_text()
                    if 'barriera' in text.lower():
                        match = re.search(r'barriera[^\d]*(\d+[,.]?\d*)\s*%?', text, re.IGNORECASE)
                        if match:
                            try:
                                barrier = float(match.group(1).replace(',', '.'))
                            except:
                                pass
                            break
            
            # ---------------------------------
            # EXTRACT COUPON/CEDOLA
            # ---------------------------------
            coupon = None
            text_full = soup.get_text().lower()
            
            # Look for coupon patterns
            coupon_patterns = [
                r'cedola[:\s]*(\d+[,.]?\d*)\s*%',
                r'coupon[:\s]*(\d+[,.]?\d*)\s*%',
                r'premio[:\s]*(\d+[,.]?\d*)\s*%',
                r'(\d+[,.]?\d*)\s*%\s*(?:mensile|trimestrale|semestrale|annuale)'
            ]
            
            for pattern in coupon_patterns:
                match = re.search(pattern, text_full)
                if match:
                    try:
                        coupon = float(match.group(1).replace(',', '.'))
                        break
                    except:
                        pass
            
            # ---------------------------------
            # DETECT CERTIFICATE TYPE
            # ---------------------------------
            cert_type = self.detect_type(cert_name)
            
            # ---------------------------------
            # BUILD CERTIFICATE OBJECT
            # ---------------------------------
            certificate = {
                'isin': isin,
                'name': cert_name,
                'type': cert_type,
                'issuer': issuer or 'Unknown',
                'market': main_data.get('mercato', 'SeDeX'),
                'currency': main_data.get('currency', 'EUR'),
                
                'reference_price': main_data.get('nominal', 100),
                
                'issue_date': main_data.get('issue_date'),
                'maturity_date': main_data.get('maturity_date'),
                'strike_date': main_data.get('strike_date'),
                
                'barrier_down': barrier,
                'barrier_type': 'European',
                'coupon': coupon,
                'annual_coupon_yield': coupon * 12 if coupon and coupon < 5 else coupon,
                
                'underlying_name': underlying_name,
                
                'fase': main_data.get('fase'),
                'source_url': url,
                'scraped_at': datetime.now().isoformat()
            }
            
            self.processed_isins.add(isin)
            
            # Log result
            status = '✓' if underlying_name else '○'
            print(f'  {status} {isin}: {underlying_name or "N/A"} | Barrier: {barrier}% | Coupon: {coupon}%')
            
        except Exception as e:
            print(f'  ✗ Error scraping {isin}: {e}')
        finally:
            page.close()
        
        return certificate
    
    def extract_underlying(self, soup, cert_name):
        """
        Extract underlying name using multiple strategies
        """
        # Strategy 1: Extract from certificate name
        # Pattern: "CASH COLLECT MEMORY STEP DOWN su ALIBABA"
        if cert_name:
            match = re.search(r'\bsu\s+([A-Za-z][A-Za-z0-9\s,&\.\-\']+)', cert_name, re.IGNORECASE)
            if match:
                underlying = match.group(1).strip()
                # Clean up
                underlying = re.sub(r'\s+con\s+.*$', '', underlying, flags=re.IGNORECASE)
                underlying = re.sub(r'\s+Barriera\s+.*$', '', underlying, flags=re.IGNORECASE)
                if len(underlying) >= 2:
                    return underlying
        
        # Strategy 2: Look in Scheda Sottostante section
        section = soup.find('h3', string=re.compile('Scheda Sottostante', re.IGNORECASE))
        if section:
            parent = section.find_parent('div') or section.find_parent()
            if parent:
                # Get all text content and look for underlying patterns
                tables = parent.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        for i, cell in enumerate(cells):
                            text = cell.get_text(strip=True)
                            # Skip labels and numbers
                            if text and len(text) >= 2:
                                text_lower = text.lower()
                                # Skip if it's a label or number
                                if text_lower in ['nome', 'sottostante', 'ticker', 'strike', 'barriera', 'trigger']:
                                    continue
                                if re.match(r'^[\d\.,\-\+%€$£]+$', text):
                                    continue
                                if re.match(r'^\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}$', text):
                                    continue
                                # This might be the underlying name
                                if re.match(r'^[A-Za-z][A-Za-z0-9\s&\.\-\']+$', text) and len(text) <= 50:
                                    return text
        
        # Strategy 3: Look for common underlying names in page text
        text_full = soup.get_text()
        known_underlyings = [
            'Intesa Sanpaolo', 'UniCredit', 'Generali', 'ENEL', 'ENI', 'Stellantis',
            'Tesla', 'NVIDIA', 'AMD', 'Apple', 'Amazon', 'Microsoft', 'Meta', 'Alphabet',
            'Alibaba', 'Netflix', 'Coinbase', 'PayPal', 'Uber',
            'FTSE MIB', 'Euro Stoxx 50', 'S&P 500', 'DAX', 'Nasdaq 100',
            'WTI', 'Brent', 'Gold', 'Silver'
        ]
        
        for underlying in known_underlyings:
            if underlying.lower() in text_full.lower():
                # Verify it's not just mentioned incidentally
                pattern = rf'\b{re.escape(underlying)}\b'
                if re.search(pattern, text_full, re.IGNORECASE):
                    return underlying
        
        return None
    
    def detect_type(self, name):
        """Detect certificate type from name"""
        if not name:
            return 'Certificate'
        
        name_lower = name.lower()
        
        type_map = {
            'cash collect': 'Cash Collect',
            'phoenix': 'Phoenix Memory',
            'bonus': 'Bonus Cap',
            'express': 'Express',
            'twin win': 'Twin Win',
            'airbag': 'Airbag',
            'autocallable': 'Autocallable',
            'reverse': 'Reverse Convertible',
            'memory': 'Memory',
            'athena': 'Athena',
            'digital': 'Digital',
            'equity protection': 'Equity Protection',
            'credit linked': 'Credit Linked',
            'fixed': 'Fixed Coupon'
        }
        
        for keyword, cert_type in type_map.items():
            if keyword in name_lower:
                return cert_type
        
        return 'Certificate'
    
    def parse_date(self, date_str):
        """Parse date string to ISO format"""
        if not date_str:
            return None
        
        # Already ISO format
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
        
        # DD/MM/YYYY format
        match = re.match(r'^(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})$', date_str)
        if match:
            day, month, year = match.groups()
            if len(year) == 2:
                year = '20' + year
            return f'{year}-{month.zfill(2)}-{day.zfill(2)}'
        
        return date_str
    
    def run(self):
        """Main scraping routine"""
        print('=' * 70)
        print('CERTIFICATES SCRAPER v3.0 - CORRECTED URLS')
        print('Copyright (c) 2024-2025 Mutna S.R.L.S.')
        print('=' * 70)
        print(f'Date: {datetime.now().isoformat()}')
        print(f'Target: {CONFIG["max_certificates"]} certificates')
        print()
        print('URLs:')
        print(f'  Emissions: {CONFIG["emissions_url"]}')
        print(f'  Detail: {CONFIG["detail_url"]}[ISIN]')
        print('=' * 70)
        
        self.start_browser()
        
        try:
            # Get ISIN list from emissions page
            isins = self.get_isin_list_from_emissions()
            
            if not isins:
                print('\n⚠️ No ISINs found from emissions page!')
                print('   Using fallback ISINs from search results...')
                # Fallback ISINs from web search
                isins = [
                    'XS3127867375', 'XS3198884036', 'DE000VJ1EFM7',
                    'IT0005668931', 'IT0005684482', 'XS3220582764',
                    'DE000VJ1EFN5', 'XS3209109514', 'XS3127865676',
                    'XS3241330383', 'DE000VJ1EFP0', 'XS3211300465',
                    'IT0006772450', 'XS3241330540', 'IT0006772492',
                    'IT0006772500', 'XS3063313962', 'XS3127871054',
                    'XS3212570116', 'DE000VJ1AW03'
                ]
            
            print(f'\n🎯 Processing {len(isins)} certificates...\n')
            
            # Scrape each certificate
            success_count = 0
            for i, isin in enumerate(isins):
                print(f'[{i+1}/{len(isins)}] {isin}')
                
                cert = self.scrape_certificate(isin)
                if cert:
                    self.certificates.append(cert)
                    success_count += 1
                
                # Rate limiting
                if i < len(isins) - 1:
                    time.sleep(CONFIG['wait_between_pages'] / 1000)
            
            # Statistics
            with_underlying = len([c for c in self.certificates if c.get('underlying_name')])
            with_barrier = len([c for c in self.certificates if c.get('barrier_down')])
            with_coupon = len([c for c in self.certificates if c.get('coupon')])
            
            print('\n' + '=' * 70)
            print('📊 SCRAPING SUMMARY')
            print('=' * 70)
            print(f'Total processed: {len(isins)}')
            print(f'Successfully scraped: {success_count}')
            print(f'Success rate: {(success_count/max(1,len(isins))*100):.1f}%')
            print(f'With underlying: {with_underlying} ({with_underlying/max(1,success_count)*100:.1f}%)')
            print(f'With barrier: {with_barrier}')
            print(f'With coupon: {with_coupon}')
            print('=' * 70)
            
            # Save results
            self.save_results()
            
        finally:
            self.close_browser()
    
    def save_results(self):
        """Save scraped data to JSON"""
        os.makedirs(CONFIG['output_dir'], exist_ok=True)
        
        output = {
            'metadata': {
                'scraper_version': '3.0-corrected-urls',
                'timestamp': datetime.now().isoformat(),
                'source': 'certificatiederivati.it',
                'total_certificates': len(self.certificates),
                'scrape_date': datetime.now().strftime('%Y-%m-%d')
            },
            'certificates': self.certificates
        }
        
        # Save to data directory
        output_path = os.path.join(CONFIG['output_dir'], 'certificates-data.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f'\n💾 Saved to: {output_path}')
        
        # Also save to root
        with open('certificates-data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f'💾 Also saved to: certificates-data.json')
        
        print(f'📦 Total certificates: {len(self.certificates)}')


if __name__ == '__main__':
    scraper = CertificatesScraperV3()
    scraper.run()
