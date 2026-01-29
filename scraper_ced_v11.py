#!/usr/bin/env python3
"""
CED Scraper v11 - Estrazione COMPLETA
- Barriera da JavaScript
- Emittente da tabella
- Bonus/Cap da tabella caratteristiche
- Calcolo rendimento annualizzato
"""

import asyncio
import json
import re
import os
from datetime import datetime, timedelta
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


class CEDScraperV11:
    def __init__(self):
        self.certificates = {}
        self.stats = {'found': 0, 'detailed': 0, 'errors': 0, 'by_underlying': {}}

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def parse_date(self, s):
        if not s or s.strip() in ['-', 'N/A', '', 'n.d.', '01/01/1900']:
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
            # Rimuovi &nbsp; e caratteri non numerici eccetto virgola e punto
            c = s.replace('&nbsp;', '').replace('\xa0', '')
            c = re.sub(r'[^\d,.\-]', '', c)
            if not c:
                return None
            # Formato italiano: 1.234,56 -> 1234.56
            if ',' in c and '.' in c:
                c = c.replace('.', '').replace(',', '.')
            elif ',' in c:
                c = c.replace(',', '.')
            v = float(c)
            return v if v != 0 else None
        except:
            return None

    def parse_percent(self, s):
        if not s:
            return None
        clean = s.replace('&nbsp;', '').replace('\xa0', '').replace('%', '').strip()
        return self.parse_num(clean)

    def calculate_annual_yield(self, bonus, maturity_date, price=100):
        """Calcola rendimento annualizzato dal bonus"""
        if not bonus or not maturity_date:
            return None
        try:
            # Parse maturity date
            mat = datetime.strptime(maturity_date, '%Y-%m-%d')
            today = datetime.now()
            days_to_maturity = (mat - today).days
            
            if days_to_maturity <= 0:
                return None
            
            # Rendimento = (Bonus - Prezzo) / Prezzo
            # Annualizzato = Rendimento * 365 / giorni
            potential_return = (bonus - price) / price * 100
            annual_yield = potential_return * 365 / days_to_maturity
            
            return round(annual_yield, 2)
        except:
            return None

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
        """Estrae dettagli - V11 COMPLETO"""
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
                'bonus': None,
                'cap': None,
                'coupon': None,
                'annual_coupon_yield': None,
                'price': None,
                'emission_price': 100,
                'scraped': True,
                'timestamp': datetime.now().isoformat()
            }
            
            # === 1. TIPO (da h2.page-header) ===
            h2 = soup.find('h2', class_='page-header')
            if h2:
                detail['type'] = h2.get_text(strip=True)
            
            # === 2. TIPO PRODOTTO (da panel-title, es. "BONUS CAP") ===
            for panel_title in soup.find_all('h3', class_='panel-title'):
                txt = panel_title.get_text(strip=True).upper()
                if any(kw in txt for kw in ['BONUS', 'CASH COLLECT', 'PHOENIX', 'ATHENA', 'EXPRESS', 'AUTOCALL']):
                    detail['name'] = txt
                    break
            
            # === 3. BARRIERA DA JAVASCRIPT ===
            scripts = soup.find_all('script')
            for script in scripts:
                script_text = script.string or ''
                
                barrier_match = re.search(r'barriera:\s*["\']([^"\']+)["\']', script_text)
                if barrier_match:
                    detail['barrier_down'] = self.parse_percent(barrier_match.group(1))
                
                level_match = re.search(r'livello:\s*["\']([^"\']+)["\']', script_text)
                if level_match:
                    detail['barrier_level'] = self.parse_num(level_match.group(1))
                
                tipo_match = re.search(r'tipo:\s*["\']([^"\']+)["\']', script_text)
                if tipo_match:
                    detail['barrier_type'] = tipo_match.group(1)
            
            # === 4. EMITTENTE DA "Scheda Emittente" ===
            for panel in soup.find_all('div', class_='panel'):
                heading = panel.find('div', class_='panel-heading')
                if heading and 'Emittente' in heading.get_text():
                    table = panel.find('table')
                    if table:
                        first_td = table.find('td')
                        if first_td:
                            issuer_text = first_td.get_text(strip=True)
                            if issuer_text and 'Rating' not in issuer_text:
                                detail['issuer'] = issuer_text
                    break
            
            # === 5. DATI DA TABELLE (incluso BONUS/CAP) ===
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
                        elif 'DATA SCADENZA' in label:
                            detail['maturity_date'] = self.parse_date(value)
                        elif 'VALUTAZIONE FINALE' in label:
                            if not detail['maturity_date']:
                                detail['maturity_date'] = self.parse_date(value)
                        elif label == 'BONUS':
                            detail['bonus'] = self.parse_percent(value)
                        elif label == 'CAP':
                            detail['cap'] = self.parse_percent(value)
                        elif 'CEDOLA' in label or 'PREMIO' in label:
                            val = self.parse_percent(value)
                            if val:
                                detail['coupon'] = val
                        elif 'PREZZO EMISSIONE' in label:
                            detail['emission_price'] = self.parse_num(value) or 100
            
            # === 6. SOTTOSTANTI ===
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
            
            if not detail['underlyings']:
                detail['underlyings'] = [{'name': base_data.get('underlying_category', 'N/A'), 'worst_of': False}]
            
            detail['underlying_name'] = detail['underlyings'][0]['name'] if detail['underlyings'] else ''
            
            # === 7. CALCOLA RENDIMENTO ANNUALIZZATO ===
            # Se c'è cedola, usa quella
            if detail['coupon']:
                # Assumendo cedola mensile, annualizza
                detail['annual_coupon_yield'] = detail['coupon'] * 12
            # Altrimenti se c'è bonus, calcola rendimento potenziale
            elif detail['bonus'] and detail['maturity_date']:
                detail['annual_coupon_yield'] = self.calculate_annual_yield(
                    detail['bonus'], 
                    detail['maturity_date'],
                    detail['emission_price'] or 100
                )
            
            self.stats['detailed'] += 1
            return detail
            
        except Exception as e:
            self.log(f"Errore {isin}: {str(e)[:50]}")
            self.stats['errors'] += 1
            return {**base_data, 'scraped': False, 'timestamp': datetime.now().isoformat()}

    async def run(self):
        self.log("=" * 60)
        self.log("CED SCRAPER V11 - ESTRAZIONE COMPLETA")
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
        with_yield = sum(1 for c in certs if c.get('annual_coupon_yield'))
        with_bonus = sum(1 for c in certs if c.get('bonus'))
        
        self.log(f"\n📈 STATISTICHE:")
        self.log(f"  Con barriera: {with_barrier}/{len(certs)}")
        self.log(f"  Con emittente: {with_issuer}/{len(certs)}")
        self.log(f"  Con rendimento: {with_yield}/{len(certs)}")
        self.log(f"  Con bonus: {with_bonus}/{len(certs)}")
        
        return {
            'metadata': {
                'version': '11.0',
                'source': 'certificatiederivati.it',
                'timestamp': datetime.now().isoformat(),
                'total_certificates': len(certs),
                'with_barrier': with_barrier,
                'with_issuer': with_issuer,
                'with_yield': with_yield,
                'with_bonus': with_bonus,
                'stats': self.stats
            },
            'certificates': certs
        }


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='data/certificates-data.json')
    args = parser.parse_args()
    
    scraper = CEDScraperV11()
    results = await scraper.run()
    
    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETATO - v11")
    print(f"{'='*60}")
    m = results['metadata']
    print(f"Certificati: {m['total_certificates']}")
    print(f"Con barriera: {m['with_barrier']}")
    print(f"Con emittente: {m['with_issuer']}")
    print(f"Con rendimento: {m['with_yield']}")
    print(f"Con bonus: {m['with_bonus']}")

if __name__ == '__main__':
    asyncio.run(main())
