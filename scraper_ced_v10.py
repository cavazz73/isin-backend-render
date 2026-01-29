#!/usr/bin/env python3
"""
CED Scraper v10 - Fix estrazione emittente e barriera
- Emittente: da tabella "Scheda Emittente"  
- Barriera: da script JavaScript inline (non AJAX)
"""

import asyncio
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

TARGET_UNDERLYINGS = [
    ("FTSEMIB Index", "FTSE MIB"),
    ("SX5E Index", "Euro Stoxx 50"),
    ("DAX Index", "DAX"),
    ("SPX Index", "S&P 500"),
    ("NDX Index", "Nasdaq 100"),
    ("NKY Index", "Nikkei 225"),
    ("INDU Index", "Dow Jones"),
    ("CAC Index", "CAC40"),
    ("HSI Index", "Hang Seng"),
    ("GOLDS Comdty", "Gold"),
    ("XAG Curncy", "Silver"),
    ("CO1 comdty", "Brent"),
    ("NG1 Comdty", "Natural Gas"),
    ("EUR001M Index", "Euribor 1M"),
]


class CEDScraperV10:
    def __init__(self):
        self.certificates = {}
        self.stats = {'found': 0, 'detailed': 0, 'errors': 0, 'by_underlying': {}}

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def parse_date(self, s):
        if not s or s.strip() in ['-', 'N/A', '', 'n.d.']:
            return None
        try:
            match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', s)
            if match:
                d, m, y = match.groups()
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        except:
            pass
        return None

    def parse_num(self, s):
        if not s or s.strip() in ['-', 'N/A', '', 'n.d.']:
            return None
        try:
            c = re.sub(r'[^\d,.\-]', '', str(s))
            if not c:
                return None
            if ',' in c:
                c = c.replace('.', '').replace(',', '.')
            v = float(c)
            return v if v != 0 else None
        except:
            return None

    def parse_percent(self, s):
        """Parse percentage from string like '70&nbsp;%' or '70 %' or '70%'"""
        if not s:
            return None
        # Rimuovi &nbsp; e spazi
        clean = s.replace('&nbsp;', '').replace('%', '').strip()
        return self.parse_num(clean)

    async def search_underlying(self, page, value, label):
        self.log(f"Ricerca: {label}")
        try:
            await page.goto('https://www.certificatiederivati.it/db_bs_ricerca_avanzata.asp', 
                           wait_until='networkidle', timeout=60000)
            await asyncio.sleep(2)
            await page.select_option('select#sottostante', value=value)
            await asyncio.sleep(1)
            await page.click('input[value="Avvia Ricerca"]')
            await page.wait_for_load_state('networkidle', timeout=60000)
            await asyncio.sleep(2)
            
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            found = 0
            
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if 'scheda' in href.lower() and 'isin=' in href.lower():
                    match = re.search(r'isin=([A-Z0-9]{12})', href, re.I)
                    if match:
                        isin = match.group(1).upper()
                        if isin not in self.certificates:
                            name = link.get_text(strip=True)
                            if 'TURBO' in name.upper() or 'LEVA' in name.upper():
                                continue
                            self.certificates[isin] = {
                                'isin': isin,
                                'name': name,
                                'underlying_category': label
                            }
                            found += 1
            
            self.stats['by_underlying'][label] = found
            self.log(f"  Trovati: {found}")
            return found
        except Exception as e:
            self.log(f"  Errore: {str(e)[:60]}")
            return 0

    async def get_detail(self, page, isin, base_data):
        """Estrae dettagli - V10 con fix per emittente e barriera"""
        url = f"https://www.certificatiederivati.it/db_bs_scheda_certificato.asp?isin={isin}"
        
        try:
            await page.goto(url, wait_until='networkidle', timeout=45000)
            await asyncio.sleep(2)
            
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            detail = {
                'isin': isin,
                'name': base_data.get('name', ''),
                'type': '',
                'issuer': '',
                'market': '',
                'currency': 'EUR',
                'underlying_name': '',
                'underlying_category': base_data.get('underlying_category', ''),
                'underlyings': [],
                'issue_date': None,
                'maturity_date': None,
                'barrier_down': None,
                'barrier_type': None,
                'barrier_level': None,
                'coupon': None,
                'annual_coupon_yield': None,
                'scraped': True,
                'timestamp': datetime.now().isoformat()
            }
            
            # === 1. ESTRAI TIPO (da h2.page-header o panel-title) ===
            h2 = soup.find('h2', class_='page-header')
            if h2:
                detail['type'] = h2.get_text(strip=True)
            
            # === 2. ESTRAI BARRIERA DA JAVASCRIPT ===
            # Cerca nello script: barriera: "70&nbsp;%", livello: "28127,834", tipo: "CONTINUA"
            scripts = soup.find_all('script')
            for script in scripts:
                script_text = script.string or ''
                
                # Barriera percentuale
                barrier_match = re.search(r'barriera:\s*["\']([^"\']+)["\']', script_text)
                if barrier_match:
                    barrier_str = barrier_match.group(1)
                    detail['barrier_down'] = self.parse_percent(barrier_str)
                
                # Livello barriera (valore assoluto)
                level_match = re.search(r'livello:\s*["\']([^"\']+)["\']', script_text)
                if level_match:
                    detail['barrier_level'] = self.parse_num(level_match.group(1))
                
                # Tipo barriera
                tipo_match = re.search(r'tipo:\s*["\']([^"\']+)["\']', script_text)
                if tipo_match:
                    detail['barrier_type'] = tipo_match.group(1)
            
            # === 3. ESTRAI EMITTENTE DA "Scheda Emittente" ===
            for panel in soup.find_all('div', class_='panel'):
                heading = panel.find('div', class_='panel-heading')
                if heading and 'Emittente' in heading.get_text():
                    # Prima cella td nella tabella contiene l'emittente
                    table = panel.find('table')
                    if table:
                        first_td = table.find('td')
                        if first_td:
                            issuer_text = first_td.get_text(strip=True)
                            # Pulisci (rimuovi rating ecc)
                            if issuer_text and 'Rating' not in issuer_text:
                                detail['issuer'] = issuer_text
                    break
            
            # === 4. ESTRAI DATI DA TABELLE STANDARD ===
            for table in soup.find_all('table', class_='table'):
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    if th and td:
                        label = th.get_text(strip=True).upper()
                        value = td.get_text(strip=True)
                        
                        if 'MERCATO' in label:
                            detail['market'] = value
                        elif 'DATA EMISSIONE' in label:
                            detail['issue_date'] = self.parse_date(value)
                        elif 'DATA SCADENZA' in label or 'VALUTAZIONE FINALE' in label:
                            if not detail['maturity_date']:
                                detail['maturity_date'] = self.parse_date(value)
            
            # === 5. ESTRAI SOTTOSTANTI ===
            for panel in soup.find_all('div', class_='panel'):
                heading = panel.find('div', class_='panel-heading')
                if heading and 'Sottostante' in heading.get_text():
                    table = panel.find('table')
                    if table:
                        tbody = table.find('tbody')
                        if tbody:
                            for row in tbody.find_all('tr'):
                                cells = row.find_all('td')
                                if cells:
                                    und = {
                                        'name': cells[0].get_text(strip=True),
                                        'strike': self.parse_num(cells[1].get_text(strip=True)) if len(cells) > 1 else None,
                                        'worst_of': False
                                    }
                                    if und['name']:
                                        detail['underlyings'].append(und)
                    break
            
            # Fallback underlying
            if not detail['underlyings']:
                detail['underlyings'] = [{'name': base_data.get('underlying_category', 'N/A'), 'worst_of': False}]
            
            detail['underlying_name'] = detail['underlyings'][0]['name'] if detail['underlyings'] else ''
            
            self.stats['detailed'] += 1
            return detail
            
        except Exception as e:
            self.log(f"Errore {isin}: {str(e)[:50]}")
            self.stats['errors'] += 1
            return {**base_data, 'scraped': False, 'timestamp': datetime.now().isoformat()}

    async def run(self):
        self.log("=" * 60)
        self.log("CED SCRAPER V10 - PRODUCTION")
        self.log("=" * 60)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
            ctx = await browser.new_context(viewport={'width': 1920, 'height': 1080})
            page = await ctx.new_page()
            
            try:
                self.log("\n📋 FASE 1: Ricerca")
                for value, label in TARGET_UNDERLYINGS:
                    await self.search_underlying(page, value, label)
                    await asyncio.sleep(2)
                
                self.stats['found'] = len(self.certificates)
                self.log(f"\n✅ Trovati: {len(self.certificates)}")
                
                if self.certificates:
                    self.log(f"\n📊 FASE 2: Dettagli")
                    results = []
                    items = list(self.certificates.items())
                    for i, (isin, data) in enumerate(items):
                        self.log(f"[{i+1}/{len(items)}] {isin}")
                        detail = await self.get_detail(page, isin, data)
                        results.append(detail)
                        await asyncio.sleep(1.5)
                    
                    self.certificates = {r['isin']: r for r in results}
            finally:
                await browser.close()
        
        certs = list(self.certificates.values())
        with_barrier = sum(1 for c in certs if c.get('barrier_down'))
        with_issuer = sum(1 for c in certs if c.get('issuer'))
        
        return {
            'metadata': {
                'version': '10.0',
                'source': 'certificatiederivati.it',
                'timestamp': datetime.now().isoformat(),
                'total_certificates': len(certs),
                'with_barrier': with_barrier,
                'with_issuer': with_issuer,
                'stats': self.stats
            },
            'certificates': certs
        }


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='data/certificates-data.json')
    args = parser.parse_args()
    
    scraper = CEDScraperV10()
    results = await scraper.run()
    
    # Crea directory se non esiste
    import os
    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETATO - v10")
    print(f"{'='*60}")
    print(f"Certificati: {len(results['certificates'])}")
    print(f"Con barriera: {results['metadata']['with_barrier']}")
    print(f"Con emittente: {results['metadata']['with_issuer']}")

if __name__ == '__main__':
    asyncio.run(main())
