#!/usr/bin/env python3
"""
Copyright (c) 2024-2025 Mutna S.R.L.S. - All Rights Reserved
P.IVA: 04219740364

Production Certificates Scraper - FIXED VERSION
Extracts REAL certificates from certificatiederivati.it

FIX: Improved get_underlying_name() function with 3-strategy approach
to correctly extract underlying assets (e.g., "Alibaba" instead of "DAX")
"""

import json
import re
import time
import os
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Configuration
CONFIG = {
    'base_url': 'https://www.certificatiederivati.it',
    'list_url': 'https://www.certificatiederivati.it/certificates/top-certificates',
    'emissions_url': 'https://www.certificatiederivati.it/certificates/nuove-emissioni',
    'max_certificates': 100,
    'timeout': 60000,
    'wait_between_pages': 1500,
    'output_dir': 'data'
}

# Known underlying mappings (fallback)
UNDERLYING_MAPPINGS = {
    'EURO STOXX 50': 'Euro Stoxx 50',
    'EUROSTOXX50': 'Euro Stoxx 50',
    'S&P500': 'S&P 500',
    'S&P 500': 'S&P 500',
    'FTSEMIB': 'FTSE MIB',
    'FTSE MIB': 'FTSE MIB',
    'INTESA': 'Intesa Sanpaolo',
    'ISP': 'Intesa Sanpaolo',
    'UCG': 'UniCredit',
    'UNICREDIT': 'UniCredit',
    'ENI': 'ENI',
    'ENEL': 'ENEL',
    'TSLA': 'Tesla',
    'TESLA': 'Tesla',
    'NVDA': 'NVIDIA',
    'NVIDIA': 'NVIDIA',
    'AAPL': 'Apple',
    'APPLE': 'Apple',
    'AMZN': 'Amazon',
    'AMAZON': 'Amazon',
    'GOOGL': 'Alphabet',
    'GOOGLE': 'Alphabet',
    'META': 'Meta',
    'FACEBOOK': 'Meta',
    'MSFT': 'Microsoft',
    'MICROSOFT': 'Microsoft',
    'BABA': 'Alibaba',
    'ALIBABA': 'Alibaba',
    'AMD': 'AMD',
    'STELLANTIS': 'Stellantis',
    'STLA': 'Stellantis',
    'GENERALI': 'Generali',
    'DAX': 'DAX',
}


def normalize_underlying_name(name):
    """Normalize and clean underlying name"""
    if not name:
        return None
    
    # Clean up
    name = name.strip().upper()
    
    # Remove common suffixes
    name = re.sub(r'\s*(INDEX|PERFORMANCE|TOTAL RETURN|TR|PR|NET|GROSS).*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*\([^)]*\)$', '', name)  # Remove trailing parentheses
    
    # Check mappings
    if name in UNDERLYING_MAPPINGS:
        return UNDERLYING_MAPPINGS[name]
    
    # Title case for readability
    return name.title()


class CertificatesScraper:
    def __init__(self):
        self.browser = None
        self.context = None
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
    
    def get_isin_list(self):
        """Get list of ISINs from new emissions page"""
        print('\n📋 Fetching ISIN list from new emissions...')
        page = self.context.new_page()
        isins = set()
        
        try:
            # Try new emissions page
            page.goto(CONFIG['emissions_url'], timeout=CONFIG['timeout'])
            page.wait_for_timeout(3000)
            
            # Extract ISINs from page
            content = page.content()
            
            # Find all ISIN patterns
            isin_pattern = r'\b([A-Z]{2}[A-Z0-9]{10})\b'
            found_isins = re.findall(isin_pattern, content)
            isins.update(found_isins)
            
            print(f'  Found {len(isins)} ISINs from new emissions')
            
            # Also try top certificates page
            page.goto(CONFIG['list_url'], timeout=CONFIG['timeout'])
            page.wait_for_timeout(3000)
            content = page.content()
            found_isins = re.findall(isin_pattern, content)
            isins.update(found_isins)
            
            print(f'  Total unique ISINs: {len(isins)}')
            
        except Exception as e:
            print(f'  ⚠️ Error fetching ISIN list: {e}')
        finally:
            page.close()
        
        return list(isins)
    
    def scrape_certificate(self, isin):
        """Scrape single certificate details"""
        if isin in self.processed_isins:
            return None
            
        page = self.context.new_page()
        certificate = None
        
        try:
            url = f'{CONFIG["base_url"]}/certificates/{isin}'
            page.goto(url, timeout=CONFIG['timeout'])
            page.wait_for_timeout(2000)
            
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # ===================================
            # FIXED: UNDERLYING NAME EXTRACTION
            # Uses 3-strategy approach
            # ===================================
            
            def get_underlying_name():
                """
                FIXED: Extract underlying name with 3 strategies
                Strategy 1: Extract from page title (most reliable)
                Strategy 2: Search for labeled cells in table
                Strategy 3: Intelligent fallback with filtering
                """
                
                # ---------------------------------
                # STRATEGY 1: Extract from title
                # ---------------------------------
                # Look for patterns like "CashCollect su ALIBABA", "Phoenix su DAX"
                name_elem = soup.find('font', size='+1')
                if not name_elem:
                    name_elem = soup.find('h1')
                if not name_elem:
                    name_elem = soup.find('title')
                    
                if name_elem:
                    title = name_elem.get_text(strip=True)
                    
                    # Pattern: "su UNDERLYING_NAME"
                    match = re.search(r'\bsu\s+([A-Za-z][A-Za-z0-9\s&\.\-\']+?)(?:\s+\d|$|\s*[-–]|\s+con|\s+Barriera)', title, re.IGNORECASE)
                    if match:
                        underlying = match.group(1).strip()
                        # Clean up common suffixes
                        underlying = re.sub(r'\s+(GROUP|INC|CORP|LTD|AG|SPA|PLC).*$', '', underlying, flags=re.IGNORECASE)
                        underlying = re.sub(r'\s+$', '', underlying)
                        if len(underlying) >= 2 and not underlying.isdigit():
                            normalized = normalize_underlying_name(underlying)
                            if normalized:
                                print(f'    → Strategy 1 (title): {normalized}')
                                return normalized
                
                # ---------------------------------
                # STRATEGY 2: Search labeled cells
                # ---------------------------------
                section = soup.find('h3', string=re.compile('Scheda Sottostante|Sottostante', re.IGNORECASE))
                if section:
                    parent = section.find_parent('div')
                    if parent:
                        table = parent.find('table')
                        if table:
                            # Search for cells with labels
                            for row in table.find_all('tr'):
                                cells = row.find_all('td')
                                for i, cell in enumerate(cells):
                                    cell_text = cell.get_text(strip=True).lower()
                                    
                                    # If this cell is a label for underlying name
                                    if any(keyword in cell_text for keyword in ['nome sottostante', 'sottostante', 'nome', 'ticker', 'asset']):
                                        # Value should be in next cell
                                        if i + 1 < len(cells):
                                            value = cells[i + 1].get_text(strip=True)
                                            if value and len(value) >= 2 and not re.match(r'^[\d\.,\-\+%]+$', value):
                                                normalized = normalize_underlying_name(value)
                                                if normalized:
                                                    print(f'    → Strategy 2 (labeled): {normalized}')
                                                    return normalized
                                        # Or check if value is in same cell after colon
                                        if ':' in cell_text:
                                            parts = cell.get_text(strip=True).split(':')
                                            if len(parts) > 1:
                                                value = parts[1].strip()
                                                if value and len(value) >= 2:
                                                    normalized = normalize_underlying_name(value)
                                                    if normalized:
                                                        print(f'    → Strategy 2 (colon): {normalized}')
                                                        return normalized
                
                # ---------------------------------
                # STRATEGY 3: Intelligent fallback
                # ---------------------------------
                # Scan all tables for potential underlying names
                tables = soup.find_all('table')
                for table in tables:
                    for cell in table.find_all('td'):
                        text = cell.get_text(strip=True)
                        
                        # Skip if it looks like a number, percentage, date, or currency
                        if re.match(r'^[\d\.,\-\+%€$£]+$', text):
                            continue
                        if re.match(r'^\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}$', text):
                            continue
                        if len(text) < 3 or len(text) > 50:
                            continue
                        if text.lower() in ['nome', 'sottostante', 'ticker', 'asset', 'isin', 'data', 'prezzo', 'barriera', 'strike', 'scadenza']:
                            continue
                        
                        # Check if it looks like a company/index name
                        if re.match(r'^[A-Za-z][A-Za-z0-9\s&\.\-\']+$', text):
                            # Check against known underlyings
                            text_upper = text.upper()
                            for key in UNDERLYING_MAPPINGS.keys():
                                if key in text_upper or text_upper in key:
                                    normalized = normalize_underlying_name(text)
                                    if normalized:
                                        print(f'    → Strategy 3 (fallback): {normalized}')
                                        return normalized
                
                return None
            
            # ---------------------------------
            # Extract all certificate data
            # ---------------------------------
            
            def get_text(selector):
                """Helper to extract text from selector"""
                elem = soup.select_one(selector)
                return elem.get_text(strip=True) if elem else None
            
            def extract_value(pattern, text=None):
                """Extract numeric value using regex"""
                if text is None:
                    text = str(soup)
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    value = match.group(1).replace(',', '.')
                    try:
                        return float(value)
                    except:
                        return None
                return None
            
            def extract_percentage(label):
                """Extract percentage value near a label"""
                pattern = rf'{label}[:\s]*([0-9]+[,.]?[0-9]*)%?'
                return extract_value(pattern)
            
            def extract_date(label):
                """Extract date near a label"""
                pattern = rf'{label}[:\s]*(\d{{1,2}}[/\-\.]\d{{1,2}}[/\-\.]\d{{2,4}})'
                text = str(soup)
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    date_str = match.group(1)
                    # Convert to ISO format
                    parts = re.split(r'[/\-\.]', date_str)
                    if len(parts) == 3:
                        day, month, year = parts
                        if len(year) == 2:
                            year = '20' + year
                        return f'{year}-{month.zfill(2)}-{day.zfill(2)}'
                return None
            
            # Get certificate name from page
            name_elem = soup.find('font', size='+1') or soup.find('h1')
            cert_name = name_elem.get_text(strip=True) if name_elem else f'Certificate {isin}'
            
            # Get issuer
            issuer = None
            issuer_pattern = r'Emittente[:\s]*([A-Za-z][A-Za-z0-9\s&\.\-]+?)(?:\s*[<\|]|$)'
            match = re.search(issuer_pattern, str(soup), re.IGNORECASE)
            if match:
                issuer = match.group(1).strip()
            
            # Get certificate type
            cert_type = 'Certificate'
            type_keywords = {
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
                'corridor': 'Corridor'
            }
            name_lower = cert_name.lower()
            for keyword, type_name in type_keywords.items():
                if keyword in name_lower:
                    cert_type = type_name
                    break
            
            # Extract prices
            bid_price = extract_value(r'bid[:\s]*([0-9]+[,.]?[0-9]*)', str(soup))
            ask_price = extract_value(r'ask[:\s]*([0-9]+[,.]?[0-9]*)', str(soup))
            last_price = extract_value(r'(?:ultimo|last|prezzo)[:\s]*([0-9]+[,.]?[0-9]*)', str(soup))
            
            reference_price = last_price or ask_price or bid_price or 100.0
            
            # Extract barrier
            barrier = extract_percentage('barriera')
            if not barrier:
                barrier = extract_value(r'barriera[:\s]*([0-9]+[,.]?[0-9]*)%?')
            
            # Extract coupon/cedola
            coupon = extract_percentage('cedola')
            if not coupon:
                coupon = extract_percentage('coupon')
            if not coupon:
                coupon = extract_value(r'premio[:\s]*([0-9]+[,.]?[0-9]*)%?')
            
            # Extract dates
            issue_date = extract_date('emissione')
            maturity_date = extract_date('scadenza')
            strike_date = extract_date('strike')
            
            # Calculate annual yield
            annual_yield = 0
            if coupon:
                # Assuming monthly coupon, annualize
                annual_yield = coupon * 12 if coupon < 5 else coupon
            
            # Get underlying name (FIXED)
            underlying_name = get_underlying_name()
            
            # Build certificate object
            certificate = {
                'isin': isin,
                'name': cert_name,
                'type': cert_type,
                'issuer': issuer or 'Unknown',
                'market': 'SeDeX',
                'currency': 'EUR',
                
                'bid_price': bid_price,
                'ask_price': ask_price,
                'reference_price': reference_price,
                
                'issue_date': issue_date,
                'maturity_date': maturity_date,
                'strike_date': strike_date,
                
                'barrier_down': barrier,
                'barrier_type': 'European',
                'coupon': coupon,
                'annual_coupon_yield': annual_yield,
                
                # FIXED: Now correctly populated
                'underlying_name': underlying_name,
                
                'source_url': url,
                'scraped_at': datetime.now().isoformat()
            }
            
            self.processed_isins.add(isin)
            print(f'  ✓ {isin}: {underlying_name or "N/A"} | Barrier: {barrier}% | Coupon: {coupon}%')
            
        except Exception as e:
            print(f'  ✗ Error scraping {isin}: {e}')
            certificate = None
        finally:
            page.close()
        
        return certificate
    
    def run(self):
        """Main scraping routine"""
        print('=' * 60)
        print('CERTIFICATES SCRAPER - PRODUCTION v2.0 (FIXED)')
        print('Copyright (c) 2024-2025 Mutna S.R.L.S.')
        print('=' * 60)
        print(f'Date: {datetime.now().isoformat()}')
        print(f'Target: {CONFIG["max_certificates"]} certificates')
        print('=' * 60)
        
        self.start_browser()
        
        try:
            # Get ISIN list
            isins = self.get_isin_list()
            
            if not isins:
                print('⚠️ No ISINs found, using fallback list')
                # Fallback to some known ISINs
                isins = [
                    'DE000VH6MX98', 'XS2906636795', 'CH1327224759',
                    'DE000BNP4XY7', 'IT0005538541', 'XS2745896321'
                ]
            
            # Limit to max certificates
            isins = isins[:CONFIG['max_certificates']]
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
            
            # Calculate statistics
            with_underlying = len([c for c in self.certificates if c.get('underlying_name')])
            with_barrier = len([c for c in self.certificates if c.get('barrier_down')])
            with_coupon = len([c for c in self.certificates if c.get('coupon')])
            
            print('\n' + '=' * 60)
            print('📊 SCRAPING SUMMARY')
            print('=' * 60)
            print(f'Total processed: {len(isins)}')
            print(f'Successfully scraped: {success_count}')
            print(f'Success rate: {(success_count/len(isins)*100):.1f}%')
            print(f'With underlying: {with_underlying} ({with_underlying/max(1,success_count)*100:.1f}%)')
            print(f'With barrier: {with_barrier}')
            print(f'With coupon: {with_coupon}')
            print('=' * 60)
            
            # Save results
            self.save_results()
            
        finally:
            self.close_browser()
    
    def save_results(self):
        """Save scraped data to JSON file"""
        # Ensure output directory exists
        os.makedirs(CONFIG['output_dir'], exist_ok=True)
        
        output = {
            'metadata': {
                'scraper_version': '2.0-fixed',
                'timestamp': datetime.now().isoformat(),
                'source': 'certificatiederivati.it',
                'total_certificates': len(self.certificates),
                'scrape_date': datetime.now().strftime('%Y-%m-%d'),
                'fix_applied': 'get_underlying_name 3-strategy approach'
            },
            'certificates': self.certificates
        }
        
        output_path = os.path.join(CONFIG['output_dir'], 'certificates-data.json')
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f'\n💾 Saved to: {output_path}')
        print(f'📦 Total certificates: {len(self.certificates)}')
        
        # Also save to root for backward compatibility
        root_path = 'certificates-data.json'
        with open(root_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f'📦 Also saved to: {root_path}')


if __name__ == '__main__':
    scraper = CertificatesScraper()
    scraper.run()
